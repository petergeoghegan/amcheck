/*-------------------------------------------------------------------------
 *
 * amcheck.c
 *		Verifies the integrity of access methods based on invariants.
 *
 * Provides SQL-callable functions for verifying that various logical
 * invariants in the structure of index access methods are respected.  This
 * includes, for example, the invariant that each page in the target B-Tree
 * index has "real" items in logical order as reported by an insertion scankey
 * (the insertion scankey sort-wise NULL semantics are useful for
 * verification).
 *
 *
 * Copyright (c) 2016, Peter Geoghegan
 *
 * IDENTIFICATION
 *	  contrib/amcheck/amcheck.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/transam.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "commands/tablecmds.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"


PG_MODULE_MAGIC;

/*
 * The current nbtree access method locking protocols were established in
 * version 9.4, where an overhaul of B-Tree page deletion and page splits
 * occurred.  There were some changes in PostgreSQL 9.5 and 9.6 that can make
 * VACUUM behave more aggressively, but these were far less invasive, and do
 * not enter into amcheck's considerations of locking protocols.
 */
#if PG_VERSION_NUM < 90400
#error "PostgreSQL versions prior to 9.4 are unsupported"
#endif

#define CHECK_SUPERUSER() { \
		if (!superuser()) \
			ereport(ERROR, \
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), \
					 (errmsg("must be superuser to use verification functions")))); }

/*
 * As noted in comments above _bt_compare(), there is special handling of the
 * first data item (that is, the first item with a valid downlink -- not the
 * high key item) on a non-leaf (internal) page.  There is clearly no point in
 * having amcheck functions make any comparison of or against these "minus
 * infinity" items, because they contain no actual information other than the
 * downlink.
 */
#define OFFSET_IS_MINUS_INFINITY(opaque, offset) \
	(!P_ISLEAF(opaque) && offset == P_FIRSTDATAKEY(opaque))

/*
 * Callers to verification functions should never receive a false positive
 * indication of corruption.  Therefore, when using amcheck functions for
 * stress testing, it may be useful to temporally change the CORRUPTION elevel
 * to PANIC, to immediately halt the server in the event of detecting an
 * invariant condition violation.  This may preserve more information about the
 * nature of the underlying problem.  Note that modifying the CORRUPTION
 * constant to be an elevel < ERROR is not well tested.
 */
#ifdef NOT_USED
#define CORRUPTION		PANIC
#define CONCERN			WARNING
#else
#define CORRUPTION		ERROR
#define CONCERN			DEBUG1
#endif

/*
 * A B-Tree cannot possibly have this many levels, since there must be one
 * block per level, which is bound by the range of BlockNumber:
 */
#define InvalidBtreeLevel	((uint32) InvalidBlockNumber)

/*
 * State associated with verifying a B-Tree index
 */
typedef struct BtreeCheckState
{
	/*
	 * Unchanging state, established at start of verification:
	 *
	 * rel:                 B-Tree Index Relation
	 * exclusivelock:       ExclusiveLock held on rel; else AccessShareLock
	 * checkstrategy:       Buffer access strategy
	 * targetcontext:       Target page memory context
	 */
	Relation				rel;
	bool					exclusivelock;
	BufferAccessStrategy 	checkstrategy;
	MemoryContext			targetcontext;

	/*
	 * Mutable state, for verification of particular page:
	 *
	 * target:              Main target page
	 * targetblock:         Main target page's block number
	 * targetlsn:           Main target page's LSN
	 *
	 * target is the point of reference for a verification operation.
	 *
	 * Note: targetlsn is always from "target".  It is stashed here out of
	 * convenience.
	 *
	 * Other B-Tree pages may be allocated, but those are always auxiliary
	 * (e.g. they are target's child pages).  Conceptually, only the target
	 * page is checked.  Each page found by verification's left/right,
	 * top/bottom scan becomes the target once.
	 *
	 * Memory is managed by resetting targetcontext after verification of some
	 * target page finishes (possibly including target verification that
	 * depends on non-target page state).
	 */
	Page					target;
	BlockNumber				targetblock;
	XLogRecPtr				targetlsn;

} BtreeCheckState;

/*
 * Starting point for verifying an entire B-Tree index level
 */
typedef struct BtreeLevel
{
	/* Level number (0 is leaf page level). */
	uint32			level;

	/* Left most block on level.  Scan of level begins here. */
	BlockNumber		leftmost;

	/* Is this level reported as "true" root level by meta page? */
	bool			istruerootlevel;
} BtreeLevel;

PG_FUNCTION_INFO_V1(bt_index_check);
PG_FUNCTION_INFO_V1(bt_index_parent_check);

static void btree_index_checkable(Relation rel);
static void bt_check_every_level(Relation rel, bool exclusivelock);
static BtreeLevel bt_check_level_from_leftmost(BtreeCheckState *state,
											   BtreeLevel level);
static void bt_target_page_check(BtreeCheckState *state);
static ScanKey bt_right_page_check_scankey(BtreeCheckState *state);
static void bt_downlink_check(BtreeCheckState *state, BlockNumber childblock,
							  ScanKey targetkey);
static bool invariant_key_less_than_equal_offset(BtreeCheckState *state,
												 ScanKey key,
												 OffsetNumber upperbound);
static bool invariant_key_greater_than_equal_offset(BtreeCheckState *state,
													ScanKey key,
													OffsetNumber lowerbound);
static bool invariant_key_less_than_equal_nontarget_offset(BtreeCheckState *state,
														   Page other,
														   ScanKey key,
														   OffsetNumber upperbound);
static Page palloc_btree_page(BtreeCheckState *state, BlockNumber blocknum);

/*
 * bt_index_check(index regclass)
 *
 * Verify integrity of B-Tree index.
 *
 * Only acquires AccessShareLock on index relation.  Does not consider
 * invariants that exist between parent/child pages.
 */
Datum
bt_index_check(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	indrel;

	CHECK_SUPERUSER();

	indrel = relation_open(relid, AccessShareLock);

	/* Relation suitable for checking as B-Tree? */
	btree_index_checkable(indrel);

	/* Check index */
	bt_check_every_level(indrel, false);

	relation_close(indrel, AccessShareLock);

	PG_RETURN_VOID();
}

/*
 * bt_index_parent_check(index regclass)
 *
 * Verify integrity of B-Tree index.
 *
 * Acquires ExclusiveLock on index relation, and ShareLock on the associated
 * heap relation, a bit like REINDEX.  Verifies that downlinks in parent pages
 * are valid lower bounds on child pages.
 */
Datum
bt_index_parent_check(PG_FUNCTION_ARGS)
{
	Oid			indrelid = PG_GETARG_OID(0);
	Oid			heapid;
	Relation	indrel;
	Relation	heaprel;

	CHECK_SUPERUSER();

	/*
	 * We must lock table before index to avoid deadlocks.  However, if the
	 * passed indrelid isn't an index then IndexGetRelation() will fail.
	 * Rather than emitting a not-very-helpful error message, postpone
	 * complaining, expecting that the is-it-an-index test below will fail.
	 */
	heapid = IndexGetRelation(indrelid, true);
	if (OidIsValid(heapid))
		heaprel = heap_open(heapid, ShareLock);
	else
		heaprel = NULL;

	/*
	 * Open the target index relations separately (like relation_openrv(), but
	 * with heap relation locked first to prevent deadlocking).  In hot standby
	 * mode this will raise an error.
	 */
	indrel = index_open(indrelid, ExclusiveLock);

	/* Check for active uses of the index in the current transaction */
	CheckTableNotInUse(indrel, "bt_index_parent_check");

	/* Relation suitable for checking as B-Tree? */
	btree_index_checkable(indrel);

	/* Check index */
	bt_check_every_level(indrel, true);

	index_close(indrel, ExclusiveLock);
	if (heaprel)
		heap_close(heaprel, ShareLock);

	PG_RETURN_VOID();
}

/*
 * btree_index_checkable()
 *
 * Basic checks about the suitability of a relation for checking as a B-Tree
 * index.
 */
static void
btree_index_checkable(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_INDEX ||
		rel->rd_rel->relam != BTREE_AM_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only nbtree access method indexes are supported"),
				 errdetail("index \"%s\" does not using the nbtree access method.",
						   RelationGetRelationName(rel))));

	if (RELATION_IS_OTHER_TEMP(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot access temporary tables of other sessions"),
				 errdetail("index \"%s\" is associated with temporary relation.",
						   RelationGetRelationName(rel))));

	if (!rel->rd_index->indisready)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot check index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("index is not yet ready for insertions")));

	if (!rel->rd_index->indisvalid)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot check index \"%s\"",
						RelationGetRelationName(rel)),
				 errdetail("index is not valid")));
}

/*
 * bt_check_every_level()
 *
 * Main entry point for B-Tree SQL-callable functions.  Walks the B-Tree in
 * logical order, verifying invariants as it goes.
 *
 * It is the caller's responsibility to acquire appropriate heavyweight lock on
 * the index relation, and advise us if extra checks are safe when an
 * ExclusiveLock is held.  An ExclusiveLock is generally assumed to prevent any
 * kind of physical modification to the index structure, including
 * modifications that VACUUM may make.
 */
static void
bt_check_every_level(Relation rel, bool exclusivelock)
{
	BtreeCheckState	   *state;
	Page				metapage;
	BTMetaPageData	   *metad;
	uint32				previouslevel;
	BtreeLevel			current;

	/*
	 * RecentGlobalXmin assertion matches index_getnext_tid().  See note on
	 * RecentGlobalXmin/B-Tree page deletion.
	 */
	Assert(TransactionIdIsValid(RecentGlobalXmin));

	/*
	 * Initialize state for entire verification operation
	 */
	state = palloc(sizeof(BtreeCheckState));
	state->rel = rel;
	state->exclusivelock = exclusivelock;
	state->checkstrategy = GetAccessStrategy(BAS_BULKREAD);
	/* Create context for page */
	state->targetcontext = AllocSetContextCreate(CurrentMemoryContext,
												 "amcheck page data",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

	/* Get true root block from meta-page */
	metapage = palloc_btree_page(state, BTREE_METAPAGE);
	metad = BTPageGetMeta(metapage);

	/*
	 * Certain deletion patterns can result in "skinny" B-Tree indexes, where
	 * the fast root and true root differ.
	 *
	 * Start from the true root, not the fast root, unlike conventional index
	 * scans.  This approach is more thorough, and removes the risk of
	 * following a stale fast root from the meta page.
	 */
	if (metad->btm_fastroot != metad->btm_root)
		ereport(CONCERN,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("fast root mismatch in index %s",
						RelationGetRelationName(rel)),
				 errdetail_internal("Fast block %u (level %u) "
									"differs from true root "
									"block %u (level %u).",
									metad->btm_fastroot, metad->btm_fastlevel,
									metad->btm_root, metad->btm_level)));

	/*
	 * Starting at the root, verify every level.  Move left to right, top to
	 * bottom.  Note that there may be no pages other than the meta page (meta
	 * page can indicate that root is P_NONE when the index is totally empty).
	 */
	previouslevel = InvalidBtreeLevel;
	current.level = metad->btm_level;
	current.leftmost = metad->btm_root;
	current.istruerootlevel =  true;
	while (current.leftmost != P_NONE)
	{
		/*
		 * Verify this level, and get left most page for next level down, if
		 * not at leaf level
		 */
		current = bt_check_level_from_leftmost(state, current);

		if (current.leftmost == InvalidBlockNumber)
			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" has no valid pages on level below %u or first level",
							RelationGetRelationName(rel), previouslevel)));

		previouslevel = current.level;
	}

	/* Be tidy: */
	MemoryContextDelete(state->targetcontext);
}

/*
 * bt_check_level_from_leftmost()
 *
 * Given a left-most block at some level, move right, verifying each page
 * individually (with more verification across pages for "exclusivelock"
 * callers).  Caller should pass the true root page as the leftmost initially,
 * working their way down by passing what is returned for the last call here
 * until level 0 (leaf page level) was reached.
 *
 * Returns state for next call, if any.  This includes left-most block number
 * one level lower that should be passed on next level/call, or P_NONE leaf
 * level is checked.  Level numbers follow the nbtree convention: higher levels
 * have higher numbers, because new levels are added only due to a root page
 * split.  Note that prior to the first root page split, the root is also a
 * leaf page.  This means that there is always a level 0 (leaf level), and it's
 * always the last level processed.
 *
 * Note on memory management:  State's per-page context is reset here, between
 * each call to bt_target_page_check().
 */
static BtreeLevel
bt_check_level_from_leftmost(BtreeCheckState *state, BtreeLevel level)
{
	/* State to establish early, concerning entire level */
	BTPageOpaque	opaque;
	MemoryContext 	oldcontext;
	BtreeLevel		nextleveldown;

	/* Variables for iterating across level using right links */
	BlockNumber		leftcurrent = P_NONE;
	BlockNumber		current = level.leftmost;

	/* Initialize return state */
	nextleveldown.leftmost = InvalidBlockNumber;
	nextleveldown.level = InvalidBtreeLevel;
	nextleveldown.istruerootlevel = false;

	/* Use page-level context for duration of this call */
	oldcontext = MemoryContextSwitchTo(state->targetcontext);

	elog(DEBUG2, "verifying level %u%s", level.level,
		 level.istruerootlevel?
		 " (true root level)" : level.level == 0 ? " (leaf level)" : "");

	do
	{
		/* Don't rely on CHECK_FOR_INTERRUPTS() calls at lower level */
		CHECK_FOR_INTERRUPTS();

		/* Initialize state for this iteration */
		state->targetblock = current;
		state->target = palloc_btree_page(state, state->targetblock);
		state->targetlsn = PageGetLSN(state->target);

		opaque = (BTPageOpaque) PageGetSpecialPointer(state->target);

		if (P_IGNORE(opaque))
		{
			if (P_RIGHTMOST(opaque))
				ereport(CORRUPTION,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("block %u fell off the end of index \"%s\"",
								current, RelationGetRelationName(state->rel))));
			else
				ereport(CONCERN,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("block %u of index \"%s\" ignored",
								current, RelationGetRelationName(state->rel))));
			goto nextpage;
		}
		else if (nextleveldown.leftmost == InvalidBlockNumber)
		{
			/*
			 * A concurrent page split could make the caller supplied leftmost
			 * block no longer contain the leftmost page, or no longer be the
			 * true root, but where that isn't possible due to heavyweight
			 * locking, check that the first valid page meets caller's
			 * expectations.
			 */
			if (state->exclusivelock)
			{
				if (!P_LEFTMOST(opaque))
					ereport(CORRUPTION,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("block %u is not leftmost in index \"%s\"",
									current, RelationGetRelationName(state->rel))));

				if (level.istruerootlevel && !P_ISROOT(opaque))
					ereport(CORRUPTION,
							(errcode(ERRCODE_INDEX_CORRUPTED),
							 errmsg("block %u is not true root in index \"%s\"",
									current, RelationGetRelationName(state->rel))));
			}

			/*
			 * Before beginning any non-trivial examination of level, establish
			 * next level down's leftmost block number, which next call here
			 * will pass as its leftmost (iff this isn't leaf level).
			 *
			 * There should be at least one non-ignorable page per level.
			 */
			if (!P_ISLEAF(opaque))
			{
				IndexTuple		itup;
				ItemId			itemid;

				/* Internal page -- downlink gets leftmost on next level */
				itemid = PageGetItemId(state->target, P_FIRSTDATAKEY(opaque));
				itup = (IndexTuple) PageGetItem(state->target, itemid);
				nextleveldown.leftmost = ItemPointerGetBlockNumber(&(itup->t_tid));
				nextleveldown.level = opaque->btpo.level - 1;
			}
			else
			{
				/*
				 * Leaf page -- final level caller must process.
				 *
				 * Note that this could also be the root page, if there has
				 * been no root page split yet.
				 */
				nextleveldown.leftmost = P_NONE;
				nextleveldown.level = InvalidBtreeLevel;
			}

			/*
			 * Finished setting up state for this call/level.  Control will
			 * never end up back here in any future loop iteration for this
			 * level.
			 */
		}

		if (state->exclusivelock && opaque->btpo_prev != leftcurrent)
			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("right link/left link pair in index \"%s\" not in mutual agreement",
							RelationGetRelationName(state->rel)),
					 errdetail_internal("Deleted block=%u left block=%u link reported block=%u.",
										current, leftcurrent, opaque->btpo_prev)));

		/* Verify invariants for page -- all important checks occur here */
		bt_target_page_check(state);

nextpage:

		/* Try to detect circular links */
		if (current == leftcurrent || current == opaque->btpo_prev)
			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("circular link chain found in block %u of index \"%s\"",
							current, RelationGetRelationName(state->rel))));

		leftcurrent = current;
		current = opaque->btpo_next;

		/* Free page and associated memory for this iteration */
		MemoryContextReset(state->targetcontext);
	}
	while (current != P_NONE);

	/* Don't change context for caller */
	MemoryContextSwitchTo(oldcontext);

	return nextleveldown;
}

/*
 * bt_target_page_check()
 *
 * Function performs the following checks on target page, or pages ancillary to
 * target page:
 *
 * - That every "real" data item is less than or equal to the high key, which
 *   is an upper bound on the items on the pages (where there is a high key at
 *   all -- pages that are rightmost lack one).
 *
 * - That within the page, every "real" item is less than or equal to the item
 *   immediately to its right, if any (i.e., that the items are in order within
 *   the page, so that the binary searches performed by index scans are sane).
 *
 * - That the last item stored on the page is less than or equal to the first
 *   "real" data item on the page to the right (if such a first item is
 *   available).
 *
 * Furthermore, when state passed shows ExclusiveLock held, function also
 * checks:
 *
 * - That all child pages respect downlinks lower bound (internal pages only).
 *
 * Note:  This routine is not especially proactive in freeing memory.  High
 * watermark memory consumption is bound to some small fixed multiple of
 * BLCKSZ, though.  Caller should reset the current context between calls here.
 */
static void
bt_target_page_check(BtreeCheckState *state)
{
	OffsetNumber	offset;
	OffsetNumber	max;
	BTPageOpaque	topaque;

	topaque = (BTPageOpaque) PageGetSpecialPointer(state->target);
	max = PageGetMaxOffsetNumber(state->target);

	elog(DEBUG2, "verifying %u items on %s block %u", max,
		 P_ISLEAF(topaque) ? "leaf":"internal", state->targetblock);

	/*
	 * Loop over page items, but don't start from P_HIKEY (don't have iteration
	 * directly considering high key item, if any).  That's something that is
	 * used as part of verifying all other items, but doesn't get its own
	 * iteration.
	 */
	for (offset = P_FIRSTDATAKEY(topaque);
		 offset <= max;
		 offset = OffsetNumberNext(offset))
	{
		ItemId		itemid;
		IndexTuple	itup;
		ScanKey		skey;

		CHECK_FOR_INTERRUPTS();

		/* Don't try to generate scankey using "minus infinity" garbage data */
		if (OFFSET_IS_MINUS_INFINITY(topaque, offset))
			continue;

		/* Build insertion scankey for current page offset */
		itemid = PageGetItemId(state->target, offset);
		itup = (IndexTuple) PageGetItem(state->target, itemid);
		skey = _bt_mkscankey(state->rel, itup);

		/*
		 *   ********************
		 *   * High key check   *
		 *   ********************
		 *
		 * If there is a high key, which there must be for a non-rightmost
		 * page, check that it actually is upper bound on all page items.
		 *
		 * We prefer to check all items, rather than checking just the first
		 * and trusting that the operator class obeys the transitive law (which
		 * implies that all subsequent items also respected the high key
		 * invariant if they pass the page order check).
		 *
		 * Ideally, we'd compare every item in the index against every other
		 * item in the index, and not trust opclass obedience of the transitive
		 * law to bridge the gap between children and their grandparents (as
		 * well as great-grandparents, and so on).  We don't go to those
		 * lengths because that would be prohibitively expensive, and probably
		 * not markedly more effective in practice.
		 */
		if (!P_RIGHTMOST(topaque) &&
			!invariant_key_less_than_equal_offset(state, skey, P_HIKEY))
		{
			char	   *itid,  *htid;

			itid = psprintf("(%u,%u)", state->targetblock, offset);
			htid = psprintf("(%u,%u)",
							ItemPointerGetBlockNumber(&(itup->t_tid)),
							ItemPointerGetOffsetNumber(&(itup->t_tid)));

			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("high key invariant violated for index \"%s\"",
							RelationGetRelationName(state->rel)),
					 errdetail_internal("Index tid=%s points to %s tid=%s "
										"page lsn=%X/%X.",
										itid,
										P_ISLEAF(topaque) ? "heap":"index",
										htid,
										(uint32) (state->targetlsn >> 32),
										(uint32) state->targetlsn)));
		}

		/*
		 *   ********************
		 *   * Page order check *
		 *   ********************
		 *
		 * Check that items are stored on page in logical order, by checking
		 * current item is less than or equal to next item (if any).
		 */
		if (OffsetNumberNext(offset) <= max &&
			!invariant_key_less_than_equal_offset(state, skey,
												  OffsetNumberNext(offset)))
		{
			char	   *itid,  *htid,
					   *nitid, *nhtid;

			itid = psprintf("(%u,%u)", state->targetblock, offset);
			htid = psprintf("(%u,%u)",
							ItemPointerGetBlockNumber(&(itup->t_tid)),
							ItemPointerGetOffsetNumber(&(itup->t_tid)));
			nitid = psprintf("(%u,%u)", state->targetblock,
							 OffsetNumberNext(offset));

			/* Reuse itup to get pointed-to heap location of second item */
			itemid = PageGetItemId(state->target, OffsetNumberNext(offset));
			itup = (IndexTuple) PageGetItem(state->target, itemid);
			nhtid = psprintf("(%u,%u)",
							ItemPointerGetBlockNumber(&(itup->t_tid)),
							ItemPointerGetOffsetNumber(&(itup->t_tid)));

			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("page order invariant violated for index \"%s\"",
							RelationGetRelationName(state->rel)),
					 errdetail_internal("Lower index tid=%s (points to %s tid=%s) "
										"higher index tid=%s (points to %s tid=%s) "
										"page lsn=%X/%X.",
										itid,
										P_ISLEAF(topaque) ? "heap":"index",
										htid,
										nitid,
										P_ISLEAF(topaque) ? "heap":"index",
										nhtid,
										(uint32) (state->targetlsn >> 32),
										(uint32) state->targetlsn)));
		}
		/*
		 *   ********************
		 *   * Last item check  *
		 *   ********************
		 *
		 * Check last item against next/right page's first data item's when
		 * last item on page is reached.
		 *
		 * The general idea here is that checking the ordering of items on the
		 * page should still perform some check on the last item on the page,
		 * if at all possible.  In other words, this is roughly the same
		 * process as the page order check that has already been performed for
		 * every other "real" item on target page by now; we just need to reach
		 * into the next page to get a scankey to compare against lower bound
		 * of max.
		 */
		else if (offset == max)
		{
			ScanKey		rightkey;

			/* Get item in next/right page */
			rightkey = bt_right_page_check_scankey(state);

			if (rightkey &&
				!invariant_key_greater_than_equal_offset(state, rightkey, max))
			{
				/*
				 * As discussed in bt_right_page_check_scankey(),
				 * non-ExclusiveLock case might have had target page deleted,
				 * in which case no error is raised
				 */
				if (!state->exclusivelock)
				{
					/* Get fresh copy of target page */
					state->target = palloc_btree_page(state, state->targetblock);
					topaque = (BTPageOpaque) PageGetSpecialPointer(state->target);

					/*
					 * Because of RecentGlobalXmin interlock against VACUUM's
					 * recycling of blocks, we can safely assume that this is
					 * substantively the same target page as before.
					 *
					 * Just return, because all !exclusivelock checks already
					 * performed against target.
					 */
					if (P_IGNORE(topaque))
						return;
				}
				ereport(CORRUPTION,
						(errcode(ERRCODE_INDEX_CORRUPTED),
						 errmsg("cross page order invariant violated for index \"%s\"",
								RelationGetRelationName(state->rel)),
						 errdetail_internal("Last item on page tid=(%u,%u) "
											"right page block=%u "
											"page lsn=%X/%X.",
											state->targetblock, offset,
											topaque->btpo_next,
											(uint32) (state->targetlsn >> 32),
											(uint32) state->targetlsn)));
			}
		}

		/*
		 *   ********************
		 *   * Downlink check   *
		 *   ********************
		 *
		 * Additional check of child items against target page (their parent),
		 * iff this is internal page and caller holds ExclusiveLock on index
		 * relation.  This involves a pass over each child page at the end of
		 * each iteration (excluding the minus infinity iteration which
		 * internal pages always have, which is immediately skipped).
		 */
		if (!P_ISLEAF(topaque) && state->exclusivelock)
		{
			BlockNumber childblock = ItemPointerGetBlockNumber(&(itup->t_tid));

			bt_downlink_check(state, childblock, skey);
		}
	}
}

/*
 * bt_right_page_check_scankey()
 *
 * Return a scankey for an item on page to right of current target (or the
 * first non-ignorable page), sufficient to check ordering invariant on last
 * item in current target page.  Returned scankey relies on local memory
 * allocated for the child page, which caller cannot pfree().  Caller's memory
 * context should be reset between calls here.
 *
 * This is the first data item, and so all adjacent items are checked against
 * their immediate sibling item (which may be on a sibling page, or even a
 * "cousin" page at parent boundaries where target's rightlink points to page
 * with different parent page).  If no such valid item is available, return
 * NULL instead.
 *
 * Note that !exclusivelock callers must reverify that target page has not been
 * concurrently deleted.
 */
static ScanKey
bt_right_page_check_scankey(BtreeCheckState *state)
{
	BTPageOpaque	opaque;
	ItemId			rightitem;
	BlockNumber		targetnext;
	Page			rightpage;
	OffsetNumber	nline;

	/* Determine target's next block number */
	opaque = (BTPageOpaque) PageGetSpecialPointer(state->target);

	/* If target is already rightmost, no right sibling; nothing to do here */
	if (P_RIGHTMOST(opaque))
		return NULL;

	/*
	 * General notes on concurrent page splits and page deletion:
	 *
	 * Concurrent page splits are not a problem for ordinary index scans, since
	 * the key space always moves in a way that lets index scans not miss
	 * things:  they might have to move right, but they never have to move left
	 * (leaving aside index scans backwards, a special case).  A concurrent
	 * page split could occur here, but just as with index scans we're
	 * following the stale right link, which will reliably get us further along
	 * in the key space, which is all we really need to get an item further
	 * along in key space to check invariant in target page.
	 *
	 * (Note that routines like _bt_search() don't require *any* page split
	 * interlock when descending the tree, including something very light like
	 * a buffer pin.  That's why it's okay that we don't either.)
	 *
	 * A deleted page won't actually be recycled by VACUUM early enough for us
	 * to fail to be able follow its right link (or left link, or downlink),
	 * because it doesn't do so until it knows that no possible index scan
	 * could land on the page with the expectation of at least being able to
	 * move right and eventually find a non-ignorable page.  (see page
	 * recycling/RecentGlobalXmin notes in nbtree README.)
	 *
	 * It's okay if we follow a rightlink and find a half-dead or dead
	 * (ignorable) page.  Either way, there must be a sane further right link
	 * to follow for these ignorable pages, because page deletion refuses to
	 * merge the key space between adjacent pages that do not share a common
	 * parent (that is, merging of the key space has to be among true sibling
	 * pages, never cousin pages).  We should succeed in finding a page to the
	 * right that isn't ignorable before too long.
	 */
	targetnext = opaque->btpo_next;
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		rightpage = palloc_btree_page(state, targetnext);
		opaque = (BTPageOpaque) PageGetSpecialPointer(rightpage);

		if (!P_IGNORE(opaque) || P_RIGHTMOST(opaque))
			break;

		/* We landed on a deleted page, so step right to find a live page */
		targetnext = opaque->btpo_next;
		ereport(CONCERN,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("level %u leftmost page of index \"%s\" was found deleted or half dead",
						opaque->btpo.level, RelationGetRelationName(state->rel)),
				 errdetail_internal("Deleted page found when building scankey from right sibling.")));

		/* Be slightly more pro-active in freeing this memory, just in case */
		pfree(rightpage);
	}

	/*
	 * No ExclusiveLock held case -- why it's safe to proceed.
	 *
	 * Problem:
	 *
	 * We must avoid false positive reports of corruption when caller treats
	 * item returned here as an upper bound on target's last item.  In general,
	 * false positives are disallowed.  Ensuring they don't happen in
	 * !exclusivelock case is subtle.
	 *
	 * A concurrent page deletion by VACUUM of the target page can result in
	 * the insertion of items on to this right sibling page that would
	 * previously have been inserted on our target page.  There might have been
	 * insertions that followed target's downlink after it was made to point to
	 * right sibling instead of target by page deletion's first phase.  The
	 * inserters insert items that would belong on target page.  This race is
	 * very tight, but it's possible.  This is our only problem.
	 *
	 * Non-problems:
	 *
	 * We are not hindered by a concurrent page split of the target; we'll
	 * never land on the second half of the page anyway.  A concurrent split of
	 * the right page will also not matter, because the first data item remains
	 * the same within the left half, which we'll reliably land on.  If we had
	 * to skip over ignorable/deleted pages, it cannot matter because their key
	 * space has already been atomically merged with the first non-ignorable
	 * page we eventually find (doesn't matter whether the page we eventually
	 * find is a true sibling or a cousin of target, which we go into below).
	 *
	 * Solution:
	 *
	 * Caller knows that it should reverify that target is not ignorable
	 * (half-dead or deleted) when cross-page sibling item comparison appears
	 * to indicate corruption (invariant fails).  This detects the single race
	 * condition that exists for caller.  This is correct because the continued
	 * existence of target block as non-ignorable (not half-dead or deleted)
	 * implies that target page was not merged into from the right by deletion;
	 * the key space at or after target never moved left.  Target's parent
	 * either has the same downlink to target as before, or a <= downlink due
	 * to deletion at the left of target.  Target either has the same highkey
	 * as before, or a highkey <= before when there is a page split. (The
	 * rightmost concurrently-split-from-target-page page will still have the
	 * same highkey as target was originally found to have, which for our
	 * purposes is equivalent to target's highkey itself never changing, since
	 * we reliably skip over concurrently-split-from-target-page pages.)
	 *
	 * In simpler terms, we allow that the key space of the target may expand
	 * left (the key space can move left on the left side of target only), but
	 * the target key space cannot expand right and get ahead of us without our
	 * detecting it.  The key space of the target cannot shrink, unless it
	 * shrinks to zero due to the deletion of the original page, our canary
	 * condition.  (To be very precise, we're a bit stricter than that because
	 * it might just have been that the target page split and only the original
	 * target page was deleted.  We can be more strict, just not more lax.)
	 *
	 * Top level tree walk caller moves on to next page (makes it the new
	 * target) following recovery from this race.  (cf.  The rationale for
	 * child/downlink verification needing an ExclusiveLock within
	 * bt_downlink_check(), where page deletion is also the main source of
	 * trouble.)
	 *
	 * Note that it doesn't matter if right sibling page here is actually a
	 * cousin page, because in order for the key space to be readjusted in a
	 * way that causes us issues in next level up (guiding problematic
	 * concurrent insertions to the cousin from the grandparent rather than to
	 * the sibling from the parent), there'd have to be page deletion of
	 * target's parent page (affecting target's parent's downlink in target's
	 * grandparent page).  Internal page deletion only occurs when there are no
	 * child pages (they were all fully deleted), and caller is checking that
	 * the target's parent has at least one non-deleted (so non-ignorable)
	 * child: the target page.  (Note that the first phase of deletion
	 * atomically marks the page to be deleted half-dead/ignorable at the same
	 * time downlink in its parent is removed, so we'll definitely be able to
	 * detect that this might have happened just from the target page.)
	 *
	 * This trick is inspired by the method backward scans use for dealing with
	 * concurrent page splits; concurrent page deletion is a problem that
	 * similarly receives special consideration sometimes (it's possible that
	 * the backwards scan will re-read its "original" block after failing to
	 * find a right-link to it, having already moved in the opposite direction
	 * (right/"forwards") a few times to try to locate one).  Just like us,
	 * that happens only to determine if there was a concurrent page deletion
	 * of a reference page, and just like us if there was a page deletion of
	 * that reference page it means we can move on from caring about the
	 * reference page.  See the nbtree README for a full description of how
	 * that works.
	 */
	nline = PageGetMaxOffsetNumber(rightpage);

	/*
	 * Get first data item.
	 *
	 * Importantly, this allows the verification of page order across target
	 * and rightmost page when rightmost page is the target's right sibling.
	 * Moreover, the only way logical inconsistencies can really be missed
	 * across a given level is if the transitive law is broken by an opclass,
	 * because contiguous pairs are always compared, even across page
	 * boundaries. (Or, we don't have a stable snapshot of structure, as in
	 * !exclusivelock case -- it can theoretically "just miss" corruption.)
	 */
	if (P_ISLEAF(opaque) && nline >= P_FIRSTDATAKEY(opaque))
	{
		/*
		 * For leaf page, return first data item (if any), which cannot be a
		 * "minus infinity" item
		 */
		rightitem = PageGetItemId(rightpage, P_FIRSTDATAKEY(opaque));
	}
	else if (!P_ISLEAF(opaque) &&
			 nline >= OffsetNumberNext(P_FIRSTDATAKEY(opaque)))
	{
		/*
		 * Return first item after the internal page's undefined "minus
		 * infinity" item, if any.  Also skip the high key if that's necessary.
		 */
		rightitem = PageGetItemId(rightpage,
								  OffsetNumberNext(P_FIRSTDATAKEY(opaque)));
	}
	else
	{
		/*
		 * No first item.  Page is probably empty leaf page, but it's also
		 * possible that it's an internal page with only a minus infinity item.
		 */
		ereport(CONCERN,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("%s block %u of index \"%s\" has no first data item",
						P_ISLEAF(opaque) ? "leaf":"internal", targetnext,
						RelationGetRelationName(state->rel))));
		return NULL;
	}

	/*
	 * Return first real item scankey.  Note that this relies on right page
	 * memory remaining allocated.
	 */
	return _bt_mkscankey(state->rel,
						 (IndexTuple) PageGetItem(rightpage, rightitem));
}

/*
 * bt_downlink_check()
 *
 * Checks one of target's downlink against its child page.
 *
 * Conceptually, the target page continues to be what is checked here.  The
 * target block is still blamed in the event of finding an invariant violation.
 * The downlink insertion into the target is probably where any problem raised
 * here arises, and there is no such thing as a parent link, so doing the
 * verification this way around is much more practical.
 */
static void
bt_downlink_check(BtreeCheckState *state, BlockNumber childblock,
				  ScanKey targetkey)
{
	OffsetNumber	offset;
	OffsetNumber	maxoffset;
	Page			child;
	BTPageOpaque	copaque;

	/*
	 * Caller must have ExclusiveLock on target relation, because of
	 * considerations around page deletion by VACUUM.
	 *
	 * N.B.: In general, page deletion deletes the right sibling's downlink,
	 * not the downlink of the page being deleted; the deleted page's downlink
	 * is reused for its sibling.  The key space is thereby consolidated
	 * between the deleted page and its right sibling.  (We cannot delete a
	 * parent page's rightmost page unless it is the last child page, and we
	 * intend to delete the parent itself.)
	 *
	 * If this verification happened without an ExclusiveLock, the following
	 * race condition could cause false positives (which are generally
	 * disallowed):
	 *
	 * Not having an ExclusiveLock would allow concurrent page deletion,
	 * including deletion of the left sibling of the child page that is
	 * examined here.  If such a page deletion occurred, and was then closely
	 * followed by an insertion into the newly expanded key space of the child,
	 * a false positive may result: our stale parent/target downlink would
	 * legitimately not be a lower bound on all items in the page anymore,
	 * because the key space was concurrently expanded "left" (insertion
	 * followed the "new" downlink for the child, not our now-stale downlink,
	 * which was concurrently physically removed in target/parent as part of
	 * deletion's first phase).
	 *
	 * Note that while the cross-page-same-level check uses a trick that allows
	 * it to perform verification for !exclusivelock callers, an analogous
	 * trick seems very difficult here.  The trick that that other check uses
	 * is, in essence, to lock down race conditions to those that occur due to
	 * concurrent page deletion of the target; that's a race that can be
	 * reliably detected before actually reporting corruption.  On the other
	 * hand, we'd need to lock down race conditions involving deletion of
	 * child's left page, at least for long enough to read the child page into
	 * memory.  Any more granular locking schemes all seem to involve multiple
	 * concurrently held buffer locks.  That's unacceptable for amcheck on
	 * general principle, though; amcheck functions never hold more than one
	 * buffer lock at a time.
	 */
	Assert(state->exclusivelock);

	/*
	 * Verify child page has the down-link key from target page (its parent) as
	 * a lower bound.
	 *
	 * We prefer to check all items, rather than checking just the first and
	 * trusting that the operator class obeys the transitive law (which implies
	 * that all subsequent items also respected the downlink-as-lower-bound
	 * invariant if they pass the page order check when the child later becomes
	 * our target for verification).
	 */
	child = palloc_btree_page(state, childblock);
	copaque = (BTPageOpaque) PageGetSpecialPointer(child);
	maxoffset = PageGetMaxOffsetNumber(child);

	for (offset = P_FIRSTDATAKEY(copaque);
		 offset <= maxoffset;
		 offset = OffsetNumberNext(offset))
	{
		/*
		 * Skip comparison of target page key against "minus infinity" item, if
		 * any.  Checking it would indicate that its not an upper bound, but
		 * that's only because of the hard-coding within _bt_compare().
		 */
		if (OFFSET_IS_MINUS_INFINITY(copaque, offset))
			continue;

		if (!invariant_key_less_than_equal_nontarget_offset(state, child,
															targetkey, offset))
			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("down-link lower bound invariant violated for index \"%s\"",
							RelationGetRelationName(state->rel)),
					 errdetail_internal("Parent block=%u child index tid=(%u,%u) "
										"parent page lsn=%X/%X.",
										state->targetblock, childblock, offset,
										(uint32) (state->targetlsn >> 32),
										(uint32) state->targetlsn)));
	}

	pfree(child);
}

/*
 * invariant_key_less_than_equal_offset()
 *
 * Does the invariant hold that the key is less than or equal to a given upper
 * bound offset item?
 *
 * If this function returns false, convention is that caller throws error due
 * to corruption.
 */
static bool
invariant_key_less_than_equal_offset(BtreeCheckState *state, ScanKey key,
									 OffsetNumber upperbound)
{
	int16		natts = state->rel->rd_rel->relnatts;
	int32		cmp;

	cmp = _bt_compare(state->rel, natts, key, state->target, upperbound);

	return cmp <= 0;
}

/*
 * invariant_key_greater_than_equal_offset()
 *
 * Does the invariant hold that the key is greater than or equal to a given
 * lower bound offset item?
 *
 * If this function returns false, convention is that caller throws error due
 * to corruption.
 */
static bool
invariant_key_greater_than_equal_offset(BtreeCheckState *state, ScanKey key,
										OffsetNumber lowerbound)
{
	int16		natts = state->rel->rd_rel->relnatts;
	int32		cmp;

	cmp = _bt_compare(state->rel, natts, key, state->target, lowerbound);

	return cmp >= 0;
}

/*
 * invariant_key_less_than_equal_nontarget_offset()
 *
 * Does the invariant hold that the key is less than or equal to a given upper
 * bound offset item, with the offset relating to a caller-supplied page that
 * is not the current target page? Caller's non-target page is typically a
 * child page of the target, checked as part of checking a property of the
 * target page (i.e. the key comes from the target).
 *
 * If this function returns false, convention is that caller throws error due
 * to corruption.
 */
static bool
invariant_key_less_than_equal_nontarget_offset(BtreeCheckState *state,
											   Page nontarget, ScanKey key,
											   OffsetNumber upperbound)
{
	int16		natts = state->rel->rd_rel->relnatts;
	int32		cmp;

	cmp = _bt_compare(state->rel, natts, key, nontarget, upperbound);

	return cmp <= 0;
}

/*
 * palloc_btree_page()
 *
 * Given a block number of a B-Tree page, return page in palloc()'d memory.
 * While at it, perform some basic checks of the page.
 *
 * There is never an attempt to get a consistent view of multiple pages using
 * multiple concurrent buffer locks; in general, we prefer to have only one pin
 * and buffer lock at a time, which is often all that the nbtree code requires.
 *
 * Operating on a copy of the page is useful because it prevents control
 * getting stuck in an uninterruptible state when an underlying operator class
 * misbehaves.
 */
static Page
palloc_btree_page(BtreeCheckState *state, BlockNumber blocknum)
{
	Buffer			buffer;
	Page			page;
	BTPageOpaque	opaque;

	page = palloc(BLCKSZ);

	/*
	 * We copy the page into local storage to avoid holding pin on the buffer
	 * longer than we must.
	 */
	buffer = ReadBufferExtended(state->rel, MAIN_FORKNUM, blocknum, RBM_NORMAL,
								state->checkstrategy);
	LockBuffer(buffer, BT_READ);

	/*
	 * Perform the same basic sanity checking that nbtree itself performs for
	 * every page:
	 */
	_bt_checkpage(state->rel, buffer);

	/* Only use copy of page in palloc()'d memory */
	memcpy(page, BufferGetPage(buffer), BLCKSZ);
	UnlockReleaseBuffer(buffer);

	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	if (opaque->btpo_flags & BTP_META && blocknum != BTREE_METAPAGE)
		ereport(CORRUPTION,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid meta page found at block %u in index \"%s\"",
						blocknum, RelationGetRelationName(state->rel))));

	/* Check page from block that ought to be meta page */
	if (blocknum == BTREE_METAPAGE)
	{
		BTMetaPageData *metad = BTPageGetMeta(page);

		if (!(opaque->btpo_flags & BTP_META) ||
			metad->btm_magic != BTREE_MAGIC)
			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("index \"%s\" meta page is corrupt",
							RelationGetRelationName(state->rel))));

		if (metad->btm_version != BTREE_VERSION)
			ereport(CORRUPTION,
					(errcode(ERRCODE_INDEX_CORRUPTED),
					 errmsg("version mismatch in index \"%s\": file version %d, code version %d",
							RelationGetRelationName(state->rel),
							metad->btm_version, BTREE_VERSION)));
	}

	/*
	 * Deleted pages have no sane "level" field, so can only check non-deleted
	 * page level
	 */
	if (P_ISLEAF(opaque) && !P_ISDELETED(opaque) && opaque->btpo.level != 0)
		ereport(CORRUPTION,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid leaf page level %u for block %u in index \"%s\"",
						opaque->btpo.level, blocknum, RelationGetRelationName(state->rel))));

	if (blocknum != BTREE_METAPAGE && !P_ISLEAF(opaque) &&
		!P_ISDELETED(opaque) && opaque->btpo.level == 0)
		ereport(CORRUPTION,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("invalid internal page level 0 for block %u in index \"%s\"",
						opaque->btpo.level, RelationGetRelationName(state->rel))));

	if (!P_ISLEAF(opaque) && P_HAS_GARBAGE(opaque))
		ereport(CORRUPTION,
				(errcode(ERRCODE_INDEX_CORRUPTED),
				 errmsg("internal page block %u in index \"%s\" has garbage items",
						blocknum, RelationGetRelationName(state->rel))));

	return page;
}
