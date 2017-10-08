/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Minimal Bloom filter
 *
 * Portions Copyright (c) 2016-2017, Peter Geoghegan
 * Portions Copyright (c) 1996-2017, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * IDENTIFICATION
 *	  amcheck_next/bloom_filter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _BLOOMFILTER_H_
#define _BLOOMFILTER_H_

typedef struct bloom_filter bloom_filter;

extern bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem,
								  uint32 seed);
extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
							  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
								size_t len);
extern double bloom_prop_bits_set(bloom_filter *filter);

#endif
