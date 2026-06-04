#include <stdbool.h>
#include "types.h"
#include "page.h"
#include "mem.h"
#include "atomic-bitmap.h"
#include "criu-log.h"

void clone_set_bitmap(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	struct lazy_vma_entry *lve;
	unsigned long page_idx;

	lve = find_lazy_vma_by_addr(page_addr);
	if (!lve || !lve->clone_bitmap) {
		pr_warn("clone_set_bitmap: addr 0x%lx not in any tracked VMA\n",
			page_addr);
		return;
	}

	page_idx = (page_addr - lve->start) / PAGE_SIZE;

	atomic_bitmap_set(lve->clone_bitmap, page_idx);
}

void clone_clear_bitmap(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	struct lazy_vma_entry *lve;
	unsigned long page_idx;

	lve = find_lazy_vma_by_addr(page_addr);
	if (!lve || !lve->clone_bitmap)
		return;

	page_idx = (page_addr - lve->start) / PAGE_SIZE;

	atomic_bitmap_clear(lve->clone_bitmap, page_idx);
}

bool clone_test_bitmap(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	struct lazy_vma_entry *lve;
	unsigned long page_idx;

	lve = find_lazy_vma_by_addr(page_addr);
	if (!lve || !lve->clone_bitmap)
		return false;

	page_idx = (page_addr - lve->start) / PAGE_SIZE;

	return atomic_bitmap_test(lve->clone_bitmap, page_idx);
}

bool clone_test_and_set_bitmap(unsigned long vaddr)
{
	unsigned long page_addr = vaddr & ~(PAGE_SIZE - 1);
	struct lazy_vma_entry *lve;
	unsigned long page_idx;

	lve = find_lazy_vma_by_addr(page_addr);
	if (!lve || !lve->clone_bitmap)
		return false;

	page_idx = (page_addr - lve->start) / PAGE_SIZE;

	return atomic_bitmap_test_and_set(lve->clone_bitmap, page_idx);
}
