CRIU_ROOT := ../../..
CRIU_SRC := $(CRIU_ROOT)/criu
CRIU_INC := $(CRIU_SRC)/include
COMMON_INC := $(CRIU_ROOT)/include

CC ?= gcc
CFLAGS := -Wall -Wextra -g -O2 -pthread -std=gnu11 \
	  -iquote shims -iquote $(CRIU_INC) -iquote $(COMMON_INC) -iquote .

LDFLAGS := -pthread

# Header-only tests (portable, work on macOS + Linux)
TESTS_PORTABLE := test_batch_bitmap test_spsc_queue test_mpsc_queue

# Tests linking real COW .c files (Linux-only: need pthread_spinlock_t, mmap alignment)
TESTS_LINUX := test_unmapped_tracker test_page_pool test_page_state_tracker

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
TESTS := $(TESTS_PORTABLE) $(TESTS_LINUX)
else
TESTS := $(TESTS_PORTABLE)
endif

all: $(TESTS)

# Header-only tests
test_batch_bitmap: test_batch_bitmap.c test_harness.h shims/page.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

test_spsc_queue: test_spsc_queue.c test_harness.h shims/page.h shims/criu-log.h shims/xmalloc.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

test_mpsc_queue: test_mpsc_queue.c test_harness.h shims/page.h shims/criu-log.h shims/xmalloc.h
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

# Tests linking against COW source files (Linux-only)
test_unmapped_tracker: test_unmapped_tracker.c $(CRIU_SRC)/cow/unmapped-tracker.c \
		       test_harness.h shims/page.h shims/int.h shims/criu-log.h shims/xmalloc.h
	$(CC) $(CFLAGS) -o $@ test_unmapped_tracker.c $(CRIU_SRC)/cow/unmapped-tracker.c $(LDFLAGS)

test_page_pool: test_page_pool.c $(CRIU_SRC)/cow/page-pool.c \
		test_harness.h shims/page.h shims/criu-log.h
	$(CC) $(CFLAGS) -o $@ test_page_pool.c $(CRIU_SRC)/cow/page-pool.c $(LDFLAGS)

test_page_state_tracker: test_page_state_tracker.c $(CRIU_SRC)/cow/page-state-tracker.c \
			 test_harness.h shims/page.h shims/int.h shims/criu-log.h shims/xmalloc.h shims/util.h
	$(CC) $(CFLAGS) -DCONFIG_PAGE_STATE_TRACKER -o $@ \
		test_page_state_tracker.c $(CRIU_SRC)/cow/page-state-tracker.c $(LDFLAGS)

check: all
	@echo "=== COW Unit Tests ==="; \
	failures=0; \
	for t in $(TESTS); do \
		echo ""; \
		echo "--- $$t ---"; \
		./$$t || failures=$$((failures + 1)); \
	done; \
	echo ""; \
	echo "=================="; \
	if [ $$failures -eq 0 ]; then \
		echo "ALL TESTS PASSED"; \
	else \
		echo "$$failures TEST SUITE(S) FAILED"; \
		exit 1; \
	fi

check-asan: CFLAGS += -fsanitize=address -fno-omit-frame-pointer
check-asan: LDFLAGS += -fsanitize=address
check-asan: clean all
	@$(MAKE) check

check-tsan: CFLAGS += -fsanitize=thread
check-tsan: LDFLAGS += -fsanitize=thread
check-tsan: clean all
	@$(MAKE) check

clean:
	rm -f $(TESTS_PORTABLE) $(TESTS_LINUX)

.PHONY: all check check-asan check-tsan clean
