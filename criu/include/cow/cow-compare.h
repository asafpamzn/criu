#ifndef __CR_COW_COMPARE_H__
#define __CR_COW_COMPARE_H__

#include <sys/types.h>

/* PRIMARY side: Listen and send state */
int cow_compare_listen(int *out_sk);
int cow_compare_send_state(int sk, pid_t pid);

/* REPLICA side: Connect and verify */
int cow_compare_connect(const char *primary_addr, int *out_sk);
int cow_compare_receive_and_verify(int sk, pid_t pid);

#endif /* __CR_COW_COMPARE_H__ */
