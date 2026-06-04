#ifndef __CR_CLONE_COMPARE_H__
#define __CR_CLONE_COMPARE_H__

#include <sys/types.h>

/* PRIMARY side: Listen and send state */
int clone_compare_listen(int *out_sk);
int clone_compare_send_state(int sk, pid_t pid);

/* REPLICA side: Connect and verify */
int clone_compare_connect(const char *primary_addr, int *out_sk);
int clone_compare_receive_and_verify(int sk, pid_t pid);

#endif /* __CR_CLONE_COMPARE_H__ */
