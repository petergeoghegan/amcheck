/*-------------------------------------------------------------------------
 *
 * bloomfilter.c
 *		Minimal Bloom filter
 *
 * A Bloom filter is a probabilistic data structure that is used to test an
 * element's membership of a set.  False positives are possible, but false
 * negatives are not; a test of membership of the set returns either "possibly
 * in set" or "definitely not in set".  This can be very space efficient when
 * individual elements are larger than a few bytes, because elements are hashed
 * in order to set bits in the Bloom filter bitset.
 *
 * Elements can be added to the set, but not removed.  The more elements that
 * are added, the larger the probability of false positives.  Caller must hint
 * an estimated total size of the set when its Bloom filter is initialized.
 * This is used to balance the use of memory against the final false positive
 * rate.
 *
 * Portions Copyright (c) 2016-2017, Peter Geoghegan
 * Portions Copyright (c) 1996-2017, The PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, The Regents of the University of California
 *
 * IDENTIFICATION
 *	  amcheck_next/bloom_filter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/hash.h"
#include "bloomfilter.h"

#define MAX_HASH_FUNCS		10

typedef struct bloom_filter
{
	/* K hash functions are used, which are randomly seeded */
	int				k_hash_funcs;
	uint32			seed;
	/* Bitset is sized directly in bits.  It must be a power-of-two <= 2^32. */
	int64			bitset_bits;
	unsigned char	bitset[FLEXIBLE_ARRAY_MEMBER];
} bloom_filter;

static int my_bloom_power(int64 target_bitset_bits);
static int optimal_k(int64 bitset_bits, int64 total_elems);
static void k_hashes(bloom_filter *filter, uint32 *hashes, unsigned char *elem,
					 size_t len);
static uint32 sdbmhash(unsigned char *elem, size_t len);

/*
 * Create Bloom filter in caller's memory context.  This should get a false
 * positive rate of between 1% and 2% when bitset is not constrained by memory.
 *
 * total_elems is an estimate of the final size of the set.  It ought to be
 * approximately correct, but we can cope well with it being off by perhaps a
 * factor of five or more.  See "Bloom Filters in Probabilistic Verification"
 * (Dillinger & Manolios, 2004) for details of why this is the case.
 *
 * bloom_work_mem is sized in KB, in line with the general work_mem convention.
 *
 * The Bloom filter behaves non-deterministically when caller passes a random
 * seed value.  This ensures that the same false positives will not occur from
 * one run to the next, which is useful to some callers.
 *
 * Notes on appropriate use:
 *
 * To keep the implementation simple and predictable, the underlying bitset is
 * always sized as a power-of-two number of bits, and the largest possible
 * bitset is 512MB.  The implementation is therefore well suited to data
 * synchronization problems between unordered sets, where predictable
 * performance is more important than worst case guarantees around false
 * positives.  Another problem that the implementation is well suited for is
 * cache filtering where good performance already relies upon having a
 * relatively small and/or low cardinality set of things that are interesting
 * (with perhaps many more uninteresting things that never populate the
 * filter).
 */
bloom_filter *
bloom_create(int64 total_elems, int bloom_work_mem, uint32 seed)
{
	bloom_filter   *filter;
	int				bloom_power;
	int64			bitset_bytes;
	int64			bitset_bits;

	/*
	 * Aim for two bytes per element; this is sufficient to get a false
	 * positive rate below 1%, independent of the size of the bitset or total
	 * number of elements.  Also, if rounding down the size of the bitset to
	 * the next lowest power of two turns out to be a significant drop, the
	 * false positive rate still won't exceed 2% in almost all cases.
	 */
	bitset_bytes = Min(bloom_work_mem * 1024L, total_elems * 2);
	/* Minimum allowable size is 1MB */
	bitset_bytes = Max(1024L * 1024L, bitset_bytes);

	/* Size in bits should be the highest power of two within budget */
	bloom_power = my_bloom_power(bitset_bytes * BITS_PER_BYTE);
	/* bitset_bits is int64 because 2^32 is greater than UINT32_MAX */
	bitset_bits = INT64CONST(1) << bloom_power;
	bitset_bytes = bitset_bits / BITS_PER_BYTE;

	/* Allocate bloom filter as all-zeroes */
	filter = palloc0(offsetof(bloom_filter, bitset) +
					 sizeof(unsigned char) * bitset_bytes);
	filter->k_hash_funcs = optimal_k(bitset_bits, total_elems);
	filter->seed = seed;
	filter->bitset_bits = bitset_bits;

	return filter;
}

/*
 * Free Bloom filter
 */
void
bloom_free(bloom_filter *filter)
{
	pfree(filter);
}

/*
 * Add element to Bloom filter
 */
void
bloom_add_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	uint32	hashes[MAX_HASH_FUNCS];
	int		i;

	k_hashes(filter, hashes, elem, len);

	/* Map a bit-wise address to a byte-wise address + bit offset */
	for (i = 0; i < filter->k_hash_funcs; i++)
	{
		filter->bitset[hashes[i] >> 3] |= 1 << (hashes[i] & 7);
	}
}

/*
 * Test if Bloom filter definitely lacks element.
 *
 * Returns true if the element is definitely not in the set of elements
 * observed by bloom_add_element().  Otherwise, returns false, indicating that
 * element is probably present in set.
 */
bool
bloom_lacks_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	uint32	hashes[MAX_HASH_FUNCS];
	int		i;

	k_hashes(filter, hashes, elem, len);

	/* Map a bit-wise address to a byte-wise address + bit offset */
	for (i = 0; i < filter->k_hash_funcs; i++)
	{
		if (!(filter->bitset[hashes[i] >> 3] & (1 << (hashes[i] & 7))))
			return true;
	}

	return false;
}

/*
 * What proportion of bits are currently set?
 *
 * Returns proportion, expressed as a multiplier of filter size.
 *
 * This is a useful, generic indicator of whether or not a Bloom filter has
 * summarized the set optimally within the available memory budget.  If return
 * value exceeds 0.5 significantly, then that's either because there was a
 * dramatic underestimation of set size by the caller, or because available
 * work_mem is very low relative to the size of the set (less than 2 bits per
 * element).
 *
 * The value returned here should generally be close to 0.5, even when we have
 * more than enough memory to ensure a false positive rate within target 1% to
 * 2% band, since more hash functions are used as more memory is available per
 * element.
 */
double
bloom_prop_bits_set(bloom_filter *filter)
{
	int		bitset_bytes = filter->bitset_bits / BITS_PER_BYTE;
	int64	bits_set = 0;
	int		i;

	for (i = 0; i < bitset_bytes; i++)
	{
		unsigned char byte = filter->bitset[i];

		while (byte)
		{
			bits_set++;
			byte &= (byte - 1);
		}
	}

	return bits_set / (double) filter->bitset_bits;
}

/*
 * Which element in the sequence of powers-of-two is less than or equal to
 * target_bitset_bits?
 *
 * Value returned here must be generally safe as the basis for actual bitset
 * size.
 *
 * Bitset is never allowed to exceed 2 ^ 32 bits (512MB).  This is sufficient
 * for the needs of all current callers, and allows us to use 32-bit hash
 * functions.  It also makes it easy to stay under the MaxAllocSize restriction
 * (caller needs to leave room for non-bitset fields that appear before
 * flexible array member, so a 1GB bitset would use an allocation that just
 * exceeds MaxAllocSize).
 */
static int
my_bloom_power(int64 target_bitset_bits)
{
	int bloom_power = -1;

	while (target_bitset_bits > 0 && bloom_power < 32)
	{
		bloom_power++;
		target_bitset_bits >>= 1;
	}

	return bloom_power;
}

/*
 * Determine optimal number of hash functions based on size of filter in bits,
 * and projected total number of elements.  The optimal number is the number
 * that minimizes the false positive rate.
 */
static int
optimal_k(int64 bitset_bits, int64 total_elems)
{
	int		k = round(log(2.0) * bitset_bits / total_elems);

	return Max(1, Min(k, MAX_HASH_FUNCS));
}

/*
 * Generate k hash values for element.
 *
 * Caller passes array, which is filled-in with k values determined by hashing
 * caller's element.
 *
 * Only 2 real independent hash functions are actually used to support an
 * interface of up to MAX_HASH_FUNCS hash functions; "enhanced double hashing"
 * is used to make this work.  See Dillinger & Manolios for details of why
 * that's okay.  "Building a Better Bloom Filter" by Kirsch & Mitzenmacher also
 * has detailed analysis of the algorithm.
 */
static void
k_hashes(bloom_filter *filter, uint32 *hashes, unsigned char *elem, size_t len)
{
	uint32	hasha,
			hashb;
	int		i;

	hasha = DatumGetUInt32(hash_any(elem, len));
	hashb = (filter->k_hash_funcs > 1 ? sdbmhash(elem, len) : 0);

	/* Mix seed value */
	hasha += filter->seed;
	/* Apply "MOD m" to avoid losing bits/out-of-bounds array access */
	hasha = hasha % filter->bitset_bits;
	hashb = hashb % filter->bitset_bits;

	/* First hash */
	hashes[0] = hasha;

	/* Subsequent hashes */
	for (i = 1; i < filter->k_hash_funcs; i++)
	{
		hasha = (hasha + hashb) % filter->bitset_bits;
		hashb = (hashb + i) % filter->bitset_bits;

		/* Accumulate hash value for caller */
		hashes[i] = hasha;
	}
}

/*
 * Hash function is taken from sdbm, a public-domain reimplementation of the
 * ndbm database library.
 */
static uint32
sdbmhash(unsigned char *elem, size_t len)
{
	uint32	hash = 0;
	int		i;

	for (i = 0; i < len; elem++, i++)
	{
		hash = (*elem) + (hash << 6) + (hash << 16) - hash;
	}

	return hash;
}
