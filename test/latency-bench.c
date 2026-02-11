/*
 * latency-bench.c — Per-second SET/GET latency tracker for Valkey.
 *
 * Runs continuous operations and reports per-second stats:
 *   timestamp, ops, avg_us, p50_us, p99_us, max_us
 *
 * Build:  gcc -O2 -o latency-bench latency-bench.c -lhiredis
 * Run:    ./latency-bench <host> <port> <seconds> <value_size>
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <hiredis/hiredis.h>

#define MAX_SAMPLES_PER_SEC 200000

static int cmp_long(const void *a, const void *b)
{
	long la = *(const long *)a;
	long lb = *(const long *)b;

	return (la > lb) - (la < lb);
}

static long ts_diff_us(struct timespec *a, struct timespec *b)
{
	return (b->tv_sec - a->tv_sec) * 1000000L +
	       (b->tv_nsec - a->tv_nsec) / 1000;
}

int main(int argc, char **argv)
{
	const char *host = argc > 1 ? argv[1] : "127.0.0.1";
	int port = argc > 2 ? atoi(argv[2]) : 6379;
	int seconds = argc > 3 ? atoi(argv[3]) : 60;
	int valsz = argc > 4 ? atoi(argv[4]) : 256;
	redisContext *c;
	redisReply *r;
	char *val;
	long *samples;
	int sec;
	struct timespec t_start, t_op_start, t_op_end, t_now;
	unsigned int seed = 42;

	c = redisConnect(host, port);
	if (!c || c->err) {
		fprintf(stderr, "Connect failed: %s\n",
			c ? c->errstr : "alloc error");
		return 1;
	}

	val = malloc(valsz);
	memset(val, 'X', valsz);
	val[valsz - 1] = '\0';

	samples = malloc(MAX_SAMPLES_PER_SEC * sizeof(long));
	if (!samples) {
		fprintf(stderr, "malloc failed\n");
		return 1;
	}

	printf("# host=%s port=%d seconds=%d value_size=%d\n",
	       host, port, seconds, valsz);
	printf("# sec  ops    avg_us  p50_us  p99_us  max_us\n");

	clock_gettime(CLOCK_MONOTONIC, &t_start);

	for (sec = 0; sec < seconds; sec++) {
		int count = 0;
		long total_us = 0;
		struct timespec t_sec_end;

		t_sec_end.tv_sec = t_start.tv_sec + sec + 1;
		t_sec_end.tv_nsec = t_start.tv_nsec;

		while (1) {
			char key[32];
			long lat;

			clock_gettime(CLOCK_MONOTONIC, &t_now);
			if (t_now.tv_sec > t_sec_end.tv_sec ||
			    (t_now.tv_sec == t_sec_end.tv_sec &&
			     t_now.tv_nsec >= t_sec_end.tv_nsec))
				break;

			/* Alternate SET and GET */
			snprintf(key, sizeof(key), "bench:%d",
				 rand_r(&seed) % 10000);

			clock_gettime(CLOCK_MONOTONIC, &t_op_start);
			if (count % 2 == 0)
				r = redisCommand(c, "SET %s %s", key, val);
			else
				r = redisCommand(c, "GET %s", key);
			clock_gettime(CLOCK_MONOTONIC, &t_op_end);

			if (!r) {
				/* Reconnect on connection loss (e.g. CRIU freeze) */
				redisFree(c);
				c = NULL;
				while (1) {
					struct timespec t_rc;
					clock_gettime(CLOCK_MONOTONIC, &t_rc);
					if (t_rc.tv_sec > t_sec_end.tv_sec + 5)
						goto done;
					c = redisConnect(host, port);
					if (c && !c->err)
						break;
					if (c) redisFree(c);
					c = NULL;
					usleep(1000);
				}
				/* Count reconnect time as one big-latency op */
				clock_gettime(CLOCK_MONOTONIC, &t_op_end);
				lat = ts_diff_us(&t_op_start, &t_op_end);
				total_us += lat;
				if (count < MAX_SAMPLES_PER_SEC)
					samples[count] = lat;
				count++;
				continue;
			}
			freeReplyObject(r);

			lat = ts_diff_us(&t_op_start, &t_op_end);
			total_us += lat;
			if (count < MAX_SAMPLES_PER_SEC)
				samples[count] = lat;
			count++;
		}

		if (count > 0) {
			int n = count < MAX_SAMPLES_PER_SEC ?
				count : MAX_SAMPLES_PER_SEC;

			qsort(samples, n, sizeof(long), cmp_long);
			printf("%4d  %6d  %6ld  %6ld  %6ld  %6ld\n",
			       sec, count,
			       total_us / count,
			       samples[n / 2],
			       samples[(long)(n * 0.99)],
			       samples[n - 1]);
			fflush(stdout);
		}
	}

done:
	free(samples);
	free(val);
	redisFree(c);
	return 0;
}
