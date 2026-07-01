#ifndef __CR_ASYNCD_H__
#define __CR_ASYNCD_H__

#include "namespaces.h"

extern int stop_asyncd(void);
extern int start_asyncd(void);
extern int __async_call(const char *func_name, uns_call_t call, int flags, void *arg, size_t arg_size, int fd);

#define async_call(__call, __flags, __arg, __arg_size, __fd) \
	__async_call(__stringify(__call), __call, __flags, __arg, __arg_size, __fd)

#endif /* __CR_ASYNCD_H__ */
