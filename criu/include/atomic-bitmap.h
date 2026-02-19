#ifndef __CR_ATOMIC_BITMAP_H__
#define __CR_ATOMIC_BITMAP_H__

#include <stdbool.h>
#include <stdint.h>

/*
 * Inline helpers for per-page bitmaps (1 bit per page).
 *
 * Two flavours:
 *   atomic_bitmap_{set,test,clear}  – use gcc atomics, safe across threads
 *   bitmap_test_nonatomic           – plain read, for single-thread contexts
 *
 * All functions take a byte-array bitmap and a zero-based page index.
 */

#define BITMAP_ALLOC_SIZE(nr_pages) (((nr_pages) + 7) / 8)

static inline void atomic_bitmap_set(uint8_t *bitmap, unsigned long page_idx)
{
	__atomic_fetch_or(&bitmap[page_idx / 8],
			  (uint8_t)(1 << (page_idx % 8)),
			  __ATOMIC_RELEASE);
}

static inline bool atomic_bitmap_test(const uint8_t *bitmap,
				      unsigned long page_idx)
{
	uint8_t val = __atomic_load_n(&((uint8_t *)bitmap)[page_idx / 8],
				      __ATOMIC_ACQUIRE);
	return (val & (1 << (page_idx % 8))) != 0;
}

static inline void atomic_bitmap_clear(uint8_t *bitmap, unsigned long page_idx)
{
	__atomic_fetch_and(&bitmap[page_idx / 8],
			   (uint8_t)~(1 << (page_idx % 8)),
			   __ATOMIC_RELEASE);
}

/*
 * Non-atomic test for single-threaded contexts (e.g. the sent_bitmap
 * is only tested by the same Thread 3 that sets it).
 */
static inline bool bitmap_test_nonatomic(const unsigned char *bitmap,
					 unsigned long page_idx)
{
	return (bitmap[page_idx / 8] & (1 << (page_idx % 8))) != 0;
}

#endif /* __CR_ATOMIC_BITMAP_H__ */
