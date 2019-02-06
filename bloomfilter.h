/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Space-efficient set membership testing
 *
 * Portions Copyright (c) 2016-2019, Peter Geoghegan
 * Portions Copyright (c) 1996-2019, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * IDENTIFICATION
 *	  amcheck_next/bloom_filter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

typedef struct bloom_filter bloom_filter;

extern bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem,
			 uint64 seed);
extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
				  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
					size_t len);
extern double bloom_prop_bits_set(bloom_filter *filter);

#endif							/* BLOOMFILTER_H */
