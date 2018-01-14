/*-------------------------------------------------------------------------
 *
 * verify_nbtree.c
 *		Verifies the integrity of GiST indexes based on invariants.
 *
 * Verification checks that all paths in GiST graph are contatining
 * consisnent keys: tuples on parent pages consistently include tuples
 * from children pages. Also, verification checks graph invariants:
 * internal page must have at least one downlinks, internal page can
 * reference either only leaf pages or only internal pages.
 *
 *
 * Portions Copyright (c) 2018, Andrey Borodin
 * Portions Copyright (c) 2016-2018, Peter Geoghegan
 * Portions Copyright (c) 1996-2018, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * IDENTIFICATION
 *	  amcheck_next/verify_gist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gist_private.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

typedef struct GistScanItem
{
	GistNSN		parentlsn;
	BlockNumber blkno;
	struct GistScanItem *next;
} GistScanItem;

/* 
 * Extract tuple attributes and check RTContainedByStrategyNumber 
 * relation with parent. If parent tuple contains null, child tuple must
 * also contain a null.
 */
static inline void
gist_check_tuple_keys(Relation rel, IndexTuple tuple, GISTENTRY *parent_entries, bool *parent_isnull, GISTSTATE *state, Page page)
{
	int i;
	GISTENTRY entries[INDEX_MAX_KEYS];
	bool isnull[INDEX_MAX_KEYS];

	gistDeCompressAtt(state, rel, tuple, page,
					  (OffsetNumber) 0, entries, isnull);

	for (i = 0; i < rel->rd_att->natts; i++)
	{
		Datum test;
		bool recheck;
		if (parent_isnull[i] != isnull[i])
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" has inconsistent null records",
							RelationGetRelationName(rel))));
		if (parent_isnull[i])
			continue;
		test = FunctionCall5Coll(&state->consistentFn[i],
									 state->supportCollation[i],
									 PointerGetDatum(&entries[i]),
									 parent_entries[i].key,
									 Int16GetDatum(RTContainedByStrategyNumber),
									 ObjectIdGetDatum(InvalidOid),
									 PointerGetDatum(&recheck));

		if (!DatumGetBool(test))
			ereport(ERROR,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" has inconsistent records",
							RelationGetRelationName(rel))));
	}
}

/*
 * For every tuple on page check if it is contained by tuple on parent page
 */
static inline void
gist_check_page_keys(Relation rel, Page parentpage, Page page, IndexTuple parent, GISTSTATE *state)
{
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(page);
	GISTENTRY parent_entries[INDEX_MAX_KEYS];
	bool isnull[INDEX_MAX_KEYS];

	gistDeCompressAtt(state, rel, parent, parentpage,
					  (OffsetNumber) 0, parent_entries, isnull);

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		ItemId iid = PageGetItemId(page, i);
		IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);

		gist_check_tuple_keys(rel, idxtuple, parent_entries, isnull, state, page);

		if (GistTupleIsInvalid(idxtuple))
			ereport(LOG,
					(errmsg("index \"%s\" contains an inner tuple marked as invalid",
							RelationGetRelationName(rel)),
					 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
					 errhint("Please REINDEX it.")));
	}
}

/* Check of an internal page. Hold locks on two pages at a time (parent+child). */
static inline bool
gist_check_internal_page(Relation rel, Page page, BufferAccessStrategy strategy, GISTSTATE *state)
{
	bool has_leafs = false;
	bool has_internals = false;
	OffsetNumber i,
				maxoff = PageGetMaxOffsetNumber(page);

	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
	{
		ItemId iid = PageGetItemId(page, i);
		IndexTuple idxtuple = (IndexTuple) PageGetItem(page, iid);

		BlockNumber child_blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));	
		Buffer		buffer;
		Page child_page;

		if (GistTupleIsInvalid(idxtuple))
			ereport(LOG,
					(errmsg("index \"%s\" contains an inner tuple marked as invalid",
							RelationGetRelationName(rel)),
					 errdetail("This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1."),
					 errhint("Please REINDEX it.")));
		
		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, child_blkno,
									RBM_NORMAL, strategy);

		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		child_page = (Page) BufferGetPage(buffer);

		has_leafs = has_leafs || GistPageIsLeaf(child_page);
		has_internals = has_internals || !GistPageIsLeaf(child_page);
		gist_check_page_keys(rel, page, child_page, idxtuple, state);

		UnlockReleaseBuffer(buffer);
	}

	if (!(has_leafs || has_internals))
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" internal page has no downlink references",
						RelationGetRelationName(rel))));


	if (has_leafs == has_internals)
		ereport(ERROR,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("index \"%s\" page references both internal and leaf pages",
						RelationGetRelationName(rel))));
	
	return has_internals;
}

/* add pages with unfinished split to scan */
static void
pushStackIfSplited(Page page, GistScanItem *stack)
{
	GISTPageOpaque opaque = GistPageGetOpaque(page);

	if (stack->blkno != GIST_ROOT_BLKNO && !XLogRecPtrIsInvalid(stack->parentlsn) &&
		(GistFollowRight(page) || stack->parentlsn < GistPageGetNSN(page)) &&
		opaque->rightlink != InvalidBlockNumber /* sanity check */ )
	{
		/* split page detected, install right link to the stack */

		GistScanItem *ptr = (GistScanItem *) palloc(sizeof(GistScanItem));

		ptr->blkno = opaque->rightlink;
		ptr->parentlsn = stack->parentlsn;
		ptr->next = stack->next;
		stack->next = ptr;
	}
}

/* 
 * Main entry point for GiST check. Allocates memory context and scans 
 * through GiST graph.
 */
static inline void
gist_check_keys_consistency(Relation rel)
{
	GistScanItem *stack,
			   *ptr;
	
	BufferAccessStrategy strategy = GetAccessStrategy(BAS_BULKREAD);

    MemoryContext mctx = AllocSetContextCreate(CurrentMemoryContext,
												 "amcheck context",
#if PG_VERSION_NUM >= 110000
												 ALLOCSET_DEFAULT_SIZES);
#else
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);
#endif

	MemoryContext oldcontext = MemoryContextSwitchTo(mctx);
	GISTSTATE *state = initGISTstate(rel);

	stack = (GistScanItem *) palloc0(sizeof(GistScanItem));
	stack->blkno = GIST_ROOT_BLKNO;

	while (stack)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber i,
					maxoff;
		IndexTuple	idxtuple;
		ItemId		iid;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, stack->blkno,
									RBM_NORMAL, strategy);
		LockBuffer(buffer, GIST_SHARE);
		gistcheckpage(rel, buffer);
		page = (Page) BufferGetPage(buffer);

		if (GistPageIsLeaf(page))
		{
			/* should never happen unless it is root */
			Assert(stack->blkno == GIST_ROOT_BLKNO);
		}
		else
		{
			/* check for split proceeded after look at parent */
			pushStackIfSplited(page, stack);

			maxoff = PageGetMaxOffsetNumber(page);

			if (gist_check_internal_page(rel, page, strategy, state))
			{
				for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
				{
					iid = PageGetItemId(page, i);
					idxtuple = (IndexTuple) PageGetItem(page, iid);

					ptr = (GistScanItem *) palloc(sizeof(GistScanItem));
					ptr->blkno = ItemPointerGetBlockNumber(&(idxtuple->t_tid));
					ptr->parentlsn = BufferGetLSNAtomic(buffer);
					ptr->next = stack->next;
					stack->next = ptr;
				}
			}
		}

		UnlockReleaseBuffer(buffer);

		ptr = stack->next;
		pfree(stack);
		stack = ptr;
	}

    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(mctx);
}

/* Check that relation is eligible for GiST verification */
static inline void
gist_index_checkable(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_INDEX ||
		rel->rd_rel->relam != GIST_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only GiST indexes are supported as targets for this verification"),
				 errdetail("Relation \"%s\" is not a GiST index.",
						   RelationGetRelationName(rel))));

	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions"),
				 errdetail("Index \"%s\" is associated with temporary relation.",
						   RelationGetRelationName(rel))));

	if (!IndexIsValid(rel->rd_index))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot check index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("Index is not valid")));
}

PG_FUNCTION_INFO_V1(gist_index_check_next);

Datum
gist_index_check_next(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);
	Relation	indrel;
	indrel = index_open(indrelid, ShareLock);

	gist_index_checkable(indrel);
	gist_check_keys_consistency(indrel);		

	index_close(indrel, ShareLock);

	PG_RETURN_VOID();
}
