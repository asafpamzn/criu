#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <fcntl.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/file.h>
#include <sys/shm.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sched.h>
#include <linux/elf.h>

#ifndef SYS_process_madvise
#define SYS_process_madvise 440
#endif

#include "types.h"
#include <compel/ptrace.h>
#include "common/compiler.h"

#include "linux/rseq.h"

#include "clone-noasan.h"
#include "cr_options.h"
#include "servicefd.h"
#include "image.h"
#include "img-streamer.h"
#include "util.h"
#include "util-pie.h"
#include "criu-log.h"
#include "restorer.h"
#include "sockets.h"
#include "sk-packet.h"
#include "common/lock.h"
#include "files.h"
#include "pipes.h"
#include "fifo.h"
#include "sk-inet.h"
#include "eventfd.h"
#include "eventpoll.h"
#include "signalfd.h"
#include "proc_parse.h"
#include "pie/restorer-blob.h"
#include "crtools.h"
#include "uffd.h"
#include "namespaces.h"
#include "mem.h"
#include "mount.h"
#include "fsnotify.h"
#include "pstree.h"
#include "net.h"
#include "tty.h"
#include "cpu.h"
#include "file-lock.h"
#include "vdso.h"
#include "stats.h"
#include "tun.h"
#include "vma.h"
#include "kerndat.h"
#include "rst-malloc.h"
#include "plugin.h"
#include "cgroup.h"
#include "timerfd.h"
#include "action-scripts.h"
#include "shmem.h"
#include "aio.h"
#include "lsm.h"
#include "seccomp.h"
#include "fault-injection.h"
#include "sk-queue.h"
#include "sigframe.h"
#include "fdstore.h"
#include "string.h"
#include "memfd.h"
#include "timens.h"
#include "bpfmap.h"
#include "apparmor.h"
#include "pidfd.h"

#include "parasite-syscall.h"
#include "files-reg.h"
#include <compel/plugins/std/syscall-codes.h>
#include "compel/include/asm/syscall.h"

#include "linux/mount.h"

#include "protobuf.h"
#include "images/sa.pb-c.h"
#include "images/timer.pb-c.h"
#include "images/vma.pb-c.h"
#include "images/rlimit.pb-c.h"
#include "images/pagemap.pb-c.h"
#include "images/siginfo.pb-c.h"

#include "restore.h"

#include "cr-errno.h"
#include "timer.h"
#include "sigact.h"

#ifndef arch_export_restore_thread
#define arch_export_restore_thread __export_restore_thread
#endif

#ifndef arch_export_restore_task
#define arch_export_restore_task __export_restore_task
#endif

#ifndef arch_export_unmap
#define arch_export_unmap	 __export_unmap
#define arch_export_unmap_compat __export_unmap_compat
#endif

struct pstree_item *current;

static int restore_task_with_children(void *);
static int sigreturn_restore(pid_t pid, struct task_restore_args *ta, unsigned long alen, CoreEntry *core);
static int prepare_restorer_blob(void);
static int prepare_rlimits(int pid, struct task_restore_args *, CoreEntry *core);
static int prepare_signals(int pid, struct task_restore_args *, CoreEntry *core);

/*
 * Architectures can overwrite this function to restore registers that are not
 * present in the sigreturn signal frame.
 */
int __attribute__((weak)) arch_set_thread_regs_nosigrt(struct pid *pid)
{
	return 0;
}

static inline int stage_participants(int next_stage)
{
	switch (next_stage) {
	case CR_STATE_FAIL:
		return 0;
	case CR_STATE_ROOT_TASK:
	case CR_STATE_PREPARE_NAMESPACES:
		return 1;
	case CR_STATE_FORKING:
		return task_entries->nr_tasks + task_entries->nr_helpers;
	case CR_STATE_RESTORE:
		return task_entries->nr_threads + task_entries->nr_helpers;
	case CR_STATE_RESTORE_SIGCHLD:
	case CR_STATE_RESTORE_CREDS:
		return task_entries->nr_threads;
	}

	BUG();
	return -1;
}

static inline int stage_current_participants(int next_stage)
{
	switch (next_stage) {
	case CR_STATE_FORKING:
		return 1;
	case CR_STATE_RESTORE:
		/*
		 * Each thread has to be reported about this stage,
		 * so if we want to wait all other tasks, we have to
		 * exclude all threads of the current process.
		 * It is supposed that we will wait other tasks,
		 * before creating threads of the current task.
		 */
		return current->nr_threads;
	}

	BUG();
	return -1;
}

static int __restore_wait_inprogress_tasks(int participants)
{
	int ret;
	futex_t *np = &task_entries->nr_in_progress;

	futex_wait_while_gt(np, participants);
	ret = (int)futex_get(np);
	if (ret < 0) {
		set_cr_errno(get_task_cr_err());
		return ret;
	}

	return 0;
}

static int restore_wait_inprogress_tasks(void)
{
	return __restore_wait_inprogress_tasks(0);
}

/* Wait all tasks except the current one */
static int restore_wait_other_tasks(void)
{
	int participants, stage;

	stage = futex_get(&task_entries->start);
	participants = stage_current_participants(stage);

	return __restore_wait_inprogress_tasks(participants);
}

static inline void __restore_switch_stage_nw(int next_stage)
{
	futex_set(&task_entries->nr_in_progress, stage_participants(next_stage));
	futex_set(&task_entries->start, next_stage);
}

static inline void __restore_switch_stage(int next_stage)
{
	if (next_stage != CR_STATE_COMPLETE)
		futex_set(&task_entries->nr_in_progress, stage_participants(next_stage));
	futex_set_and_wake(&task_entries->start, next_stage);
}

static int restore_switch_stage(int next_stage)
{
	__restore_switch_stage(next_stage);
	return restore_wait_inprogress_tasks();
}

static int restore_finish_ns_stage(int from, int to)
{
	if (root_ns_mask)
		return restore_finish_stage(task_entries, from);

	/* Nobody waits for this stage change, just go ahead */
	__restore_switch_stage_nw(to);
	return 0;
}

static int crtools_prepare_shared(void)
{
	if (prepare_memfd_inodes())
		return -1;

	if (prepare_files())
		return -1;

	/* We might want to remove ghost files on failed restore */
	if (collect_remaps_and_regfiles())
		return -1;

	/* Connections are unlocked from criu */
	if (!files_collected() && collect_image(&inet_sk_cinfo))
		return -1;

	if (tty_prep_fds())
		return -1;

	if (prepare_apparmor_namespaces())
		return -1;

	return 0;
}

/*
 * Collect order information:
 * - reg_file should be before remap, as the latter needs
 *   to find file_desc objects
 * - per-pid collects (mm and fd) should be after remap and
 *   reg_file since both per-pid ones need to get fdesc-s
 *   and bump counters on remaps if they exist
 */

static struct collect_image_info *cinfos[] = {
	&file_locks_cinfo,  &pipe_data_cinfo, &fifo_data_cinfo, &sk_queues_cinfo,
#ifdef CONFIG_HAS_LIBBPF
	&bpfmap_data_cinfo,
#endif
};

static struct collect_image_info *cinfos_files[] = {
	&unix_sk_cinfo,	      &fifo_cinfo,     &pipe_cinfo,    &nsfile_cinfo,	    &packet_sk_cinfo,
	&netlink_sk_cinfo,    &eventfd_cinfo,  &epoll_cinfo,   &epoll_tfd_cinfo,    &signalfd_cinfo,
	&tunfile_cinfo,	      &timerfd_cinfo,  &inotify_cinfo, &inotify_mark_cinfo, &fanotify_cinfo,
	&fanotify_mark_cinfo, &ext_file_cinfo, &memfd_cinfo, &pidfd_cinfo
};

/* These images are required to restore namespaces */
static struct collect_image_info *before_ns_cinfos[] = {
	&tty_info_cinfo, /* Restore devpts content */
	&tty_cdata,
};

static struct pprep_head *post_prepare_heads = NULL;

void add_post_prepare_cb(struct pprep_head *ph)
{
	ph->next = post_prepare_heads;
	post_prepare_heads = ph;
}

static int run_post_prepare(void)
{
	struct pprep_head *ph;

	for (ph = post_prepare_heads; ph != NULL; ph = ph->next)
		if (ph->actor(ph))
			return -1;

	return 0;
}

static int root_prepare_shared(void)
{
	int ret = 0;
	struct pstree_item *pi;

	pr_info("Preparing info about shared resources\n");

	if (prepare_remaps())
		return -1;

	if (seccomp_read_image())
		return -1;

	if (collect_images(cinfos, ARRAY_SIZE(cinfos)))
		return -1;

	if (!files_collected() && collect_images(cinfos_files, ARRAY_SIZE(cinfos_files)))
		return -1;

	for_each_pstree_item(pi) {
		if (pi->pid->state == TASK_HELPER)
			continue;

		ret = prepare_mm_pid(pi);
		if (ret < 0)
			break;

		ret = prepare_fd_pid(pi);
		if (ret < 0)
			break;

		ret = prepare_fs_pid(pi);
		if (ret < 0)
			break;
	}

	if (ret < 0)
		goto err;

	prepare_cow_vmas();

	ret = prepare_restorer_blob();
	if (ret)
		goto err;

	ret = add_fake_unix_queuers();
	if (ret)
		goto err;

	/*
	 * This should be called with all packets collected AND all
	 * fdescs and fles prepared BUT post-prep-s not run.
	 */
	ret = prepare_scms();
	if (ret)
		goto err;

	ret = run_post_prepare();
	if (ret)
		goto err;

	ret = unix_prepare_root_shared();
	if (ret)
		goto err;

	show_saved_files();
err:
	return ret;
}

/* This actually populates and occupies ROOT_FD_OFF sfd */
static int populate_root_fd_off(void)
{
	struct ns_id *mntns = NULL;
	int ret;

	if (root_ns_mask & CLONE_NEWNS) {
		mntns = lookup_ns_by_id(root_item->ids->mnt_ns_id, &mnt_ns_desc);
		BUG_ON(!mntns);
	}

	ret = mntns_get_root_fd(mntns);
	if (ret < 0)
		pr_err("Can't get root fd\n");
	return ret >= 0 ? 0 : -1;
}

static int populate_pid_proc(void)
{
	if (open_pid_proc(vpid(current)) < 0) {
		pr_err("Can't open PROC_SELF\n");
		return -1;
	}
	if (open_pid_proc(PROC_SELF) < 0) {
		pr_err("Can't open PROC_SELF\n");
		return -1;
	}
	return 0;
}

static int __collect_child_pids(struct pstree_item *p, int state, unsigned int *n)
{
	struct pstree_item *pi;

	list_for_each_entry(pi, &p->children, sibling) {
		pid_t *child;

		if (pi->pid->state != state)
			continue;

		child = rst_mem_alloc(sizeof(*child), RM_PRIVATE);
		if (!child)
			return -1;

		(*n)++;
		*child = vpid(pi);
	}

	return 0;
}

static int collect_child_pids(int state, unsigned int *n)
{
	struct pstree_item *pi;

	*n = 0;

	/*
	 * All children of helpers and zombies will be reparented to the init
	 * process and they have to be collected too.
	 */

	if (current == root_item) {
		for_each_pstree_item(pi) {
			if (pi->pid->state != TASK_HELPER && pi->pid->state != TASK_DEAD)
				continue;
			if (__collect_child_pids(pi, state, n))
				return -1;
		}
	}

	return __collect_child_pids(current, state, n);
}

static int collect_helper_pids(struct task_restore_args *ta)
{
	ta->helpers = (pid_t *)rst_mem_align_cpos(RM_PRIVATE);
	return collect_child_pids(TASK_HELPER, &ta->helpers_n);
}

static int collect_zombie_pids(struct task_restore_args *ta)
{
	ta->zombies = (pid_t *)rst_mem_align_cpos(RM_PRIVATE);
	return collect_child_pids(TASK_DEAD, &ta->zombies_n);
}

static int collect_inotify_fds(struct task_restore_args *ta)
{
	struct list_head *list = &rsti(current)->fds;
	struct fdt *fdt = rsti(current)->fdt;
	struct fdinfo_list_entry *fle;

	/* Check we are an fdt-restorer */
	if (fdt && fdt->pid != vpid(current))
		return 0;

	ta->inotify_fds = (int *)rst_mem_align_cpos(RM_PRIVATE);

	list_for_each_entry(fle, list, ps_list) {
		struct file_desc *d = fle->desc;
		int *inotify_fd;

		if (d->ops->type != FD_TYPES__INOTIFY)
			continue;

		if (fle != file_master(d))
			continue;

		inotify_fd = rst_mem_alloc(sizeof(*inotify_fd), RM_PRIVATE);
		if (!inotify_fd)
			return -1;

		ta->inotify_fds_n++;
		*inotify_fd = fle->fe->fd;

		pr_debug("Collect inotify fd %d to cleanup later\n", *inotify_fd);
	}
	return 0;
}

static int open_core(int pid, CoreEntry **pcore)
{
	int ret;
	struct cr_img *img;

	img = open_image(CR_FD_CORE, O_RSTR, pid);
	if (!img) {
		pr_err("Can't open core data for %d\n", pid);
		return -1;
	}

	ret = pb_read_one(img, pcore, PB_CORE);
	close_image(img);

	return ret <= 0 ? -1 : 0;
}

static int open_cores(int pid, CoreEntry *leader_core)
{
	int i, tpid;
	CoreEntry **cores = NULL;

	cores = xmalloc(sizeof(*cores) * current->nr_threads);
	if (!cores)
		goto err;

	for (i = 0; i < current->nr_threads; i++) {
		tpid = current->threads[i].ns[0].virt;

		if (tpid == pid)
			cores[i] = leader_core;
		else if (open_core(tpid, &cores[i]))
			goto err;
	}

	current->core = cores;

	/*
	 * Walk over all threads and if one them is having
	 * active seccomp mode we will suspend filtering
	 * on the whole group until restore complete.
	 *
	 * Otherwise any criu code which might use same syscall
	 * if present inside a filter chain would take filter
	 * action and might break restore procedure.
	 */
	for (i = 0; i < current->nr_threads; i++) {
		ThreadCoreEntry *thread_core = cores[i]->thread_core;
		if (thread_core->seccomp_mode != SECCOMP_MODE_DISABLED) {
			rsti(current)->has_seccomp = true;
			break;
		}
	}

	for (i = 0; i < current->nr_threads; i++) {
		ThreadCoreEntry *tc = cores[i]->thread_core;
		struct rst_rseq *rseqs = rsti(current)->rseqe;
		RseqEntry *rseqe = tc->rseq_entry;

		/* compatibility with older CRIU versions */
		if (!rseqe)
			continue;

		/* rseq cs had no RSEQ_CS_FLAG_NO_RESTART_ON_SIGNAL */
		if (!rseqe->has_rseq_cs_pointer)
			continue;

		rseqs[i].rseq_abi_pointer = rseqe->rseq_abi_pointer;
		rseqs[i].rseq_cs_pointer = rseqe->rseq_cs_pointer;
	}

	return 0;
err:
	xfree(cores);
	return -1;
}

static int prepare_oom_score_adj(int value)
{
	int fd, ret = 0;
	char buf[11];

	fd = open_proc_rw(PROC_SELF, "oom_score_adj");
	if (fd < 0)
		return -1;

	snprintf(buf, 11, "%d", value);

	if (write(fd, buf, 11) < 0) {
		pr_perror("Write %s to /proc/self/oom_score_adj failed", buf);
		ret = -1;
	}

	close(fd);
	return ret;
}

static int prepare_proc_misc(pid_t pid, TaskCoreEntry *tc, struct task_restore_args *args)
{
	int ret;

	if (tc->has_child_subreaper)
		args->child_subreaper = tc->child_subreaper;

	if (tc->has_membarrier_registration_mask)
		args->membarrier_registration_mask = tc->membarrier_registration_mask;

	/* loginuid value is critical to restore */
	if (kdat.luid == LUID_FULL && tc->has_loginuid && tc->loginuid != INVALID_UID) {
		ret = prepare_loginuid(tc->loginuid);
		if (ret < 0) {
			pr_err("Setting loginuid for %d task failed\n", pid);
			return ret;
		}
	}

	/* oom_score_adj is not critical: only log errors */
	if (tc->has_oom_score_adj && tc->oom_score_adj != 0)
		prepare_oom_score_adj(tc->oom_score_adj);

	return 0;
}

static int prepare_mm(pid_t pid, struct task_restore_args *args);

static int restore_one_alive_task(int pid, CoreEntry *core)
{
	unsigned args_len;
	struct task_restore_args *ta;
	pr_info("Restoring resources\n");

	rst_mem_switch_to_private();

	args_len = round_up(sizeof(*ta) + sizeof(struct thread_restore_args) * current->nr_threads, page_size());
	ta = mmap(NULL, args_len, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
	if (!ta)
		return -1;

	memzero(ta, args_len);

	if (prepare_fds(current))
		return -1;

	if (prepare_file_locks(pid))
		return -1;

	if (open_vmas(current))
		return -1;

	if (prepare_aios(current, ta))
		return -1;

	if (fixup_sysv_shmems())
		return -1;

	if (open_cores(pid, core))
		return -1;

	if (prepare_signals(pid, ta, core))
		return -1;

	if (prepare_posix_timers(pid, ta, core))
		return -1;

	if (prepare_rlimits(pid, ta, core) < 0)
		return -1;

	if (collect_helper_pids(ta) < 0)
		return -1;

	if (collect_zombie_pids(ta) < 0)
		return -1;

	if (collect_inotify_fds(ta) < 0)
		return -1;

	if (prepare_proc_misc(pid, core->tc, ta))
		return -1;

	/*
	 * Get all the tcp sockets fds into rst memory -- restorer
	 * will turn repair off before going sigreturn
	 */
	if (prepare_tcp_socks(ta))
		return -1;

	/*
	 * Copy timerfd params for restorer args, we need to proceed
	 * timer setting at the very late.
	 */
	if (prepare_timerfds(ta))
		return -1;

	if (seccomp_prepare_threads(current, ta) < 0)
		return -1;

	if (prepare_itimers(pid, ta, core) < 0)
		return -1;

	if (prepare_mm(pid, ta))
		return -1;

	if (prepare_vmas(current, ta))
		return -1;

	/*
	 * Sockets have to be restored in their network namespaces,
	 * so a task namespace has to be restored after sockets.
	 */
	if (restore_task_net_ns(current))
		return -1;

	if (setup_uffd(pid, ta))
		return -1;

	if (arch_shstk_prepare(current, core, ta))
		return -1;

	return sigreturn_restore(pid, ta, args_len, core);
}

static void zombie_prepare_signals(void)
{
	sigset_t blockmask;
	int sig;
	struct sigaction act;

	sigfillset(&blockmask);
	sigprocmask(SIG_UNBLOCK, &blockmask, NULL);

	memset(&act, 0, sizeof(act));
	act.sa_handler = SIG_DFL;

	for (sig = 1; sig <= SIGMAX; sig++)
		sigaction(sig, &act, NULL);
}

#define SIG_FATAL_MASK                                                                                          \
	((1 << SIGHUP) | (1 << SIGINT) | (1 << SIGQUIT) | (1 << SIGILL) | (1 << SIGTRAP) | (1 << SIGABRT) |     \
	 (1 << SIGIOT) | (1 << SIGBUS) | (1 << SIGFPE) | (1 << SIGKILL) | (1 << SIGUSR1) | (1 << SIGSEGV) |     \
	 (1 << SIGUSR2) | (1 << SIGPIPE) | (1 << SIGALRM) | (1 << SIGTERM) | (1 << SIGXCPU) | (1 << SIGXFSZ) |  \
	 (1 << SIGVTALRM) | (1 << SIGPROF) | (1 << SIGPOLL) | (1 << SIGIO) | (1 << SIGSYS) | (1 << SIGSTKFLT) | \
	 (1 << SIGPWR))

static inline int sig_fatal(int sig)
{
	return (sig > 0) && (sig < SIGMAX) && (SIG_FATAL_MASK & (1UL << sig));
}

struct task_entries *task_entries;
static unsigned long task_entries_pos;

static int wait_on_helpers_zombies(void)
{
	struct pstree_item *pi;

	list_for_each_entry(pi, &current->children, sibling) {
		pid_t pid = vpid(pi);
		int status;

		switch (pi->pid->state) {
		case TASK_DEAD:
			if (waitid(P_PID, pid, NULL, WNOWAIT | WEXITED) < 0) {
				pr_perror("Wait on %d zombie failed", pid);
				return -1;
			}
			break;
		case TASK_HELPER:
			if (waitpid(pid, &status, 0) != pid) {
				pr_perror("waitpid for helper %d failed", pid);
				return -1;
			}
			break;
		}
	}

	return 0;
}

static int wait_exiting_children(void);

static int restore_one_zombie(CoreEntry *core)
{
	int exit_code = core->tc->exit_code;

	pr_info("Restoring zombie with %d code\n", exit_code);

	if (prepare_fds(current))
		return -1;

	if (lazy_pages_setup_zombie(vpid(current)))
		return -1;

	prctl(PR_SET_NAME, (long)(void *)core->tc->comm, 0, 0, 0);

	if (task_entries != NULL) {
		wait_exiting_children();
		zombie_prepare_signals();
	}

	if (exit_code & 0x7f) {
		int signr;

		/* prevent generating core files */
		if (prctl(PR_SET_DUMPABLE, 0, 0, 0, 0))
			pr_perror("Can't drop the dumpable flag");

		signr = exit_code & 0x7F;
		if (!sig_fatal(signr)) {
			pr_warn("Exit with non fatal signal ignored\n");
			signr = SIGABRT;
		}

		if (kill(vpid(current), signr) < 0)
			pr_perror("Can't kill myself, will just exit");

		exit_code = 0;
	}

	exit((exit_code >> 8) & 0x7f);

	/* never reached */
	BUG_ON(1);
	return -1;
}

static int setup_newborn_fds(struct pstree_item *me)
{
	if (clone_service_fd(me))
		return -1;

	if (!me->parent || (rsti(me->parent)->fdt && !(rsti(me)->clone_flags & CLONE_FILES))) {
		/*
		 * When our parent has shared fd table, some of the table owners
		 * may be already created. Files, they open, will be inherited
		 * by current process, and here we close them. Also, service fds
		 * of parent are closed here. And root_item closes the files,
		 * that were inherited from criu process.
		 */
		if (close_old_fds())
			return -1;
	}

	return 0;
}

static int check_core(CoreEntry *core, struct pstree_item *me)
{
	int ret = -1;

	if (core->mtype != CORE_ENTRY__MARCH) {
		pr_err("Core march mismatch %d\n", (int)core->mtype);
		goto out;
	}

	if (!core->tc) {
		pr_err("Core task state data missed\n");
		goto out;
	}

	if (core->tc->task_state != TASK_DEAD) {
		if (!core->ids && !me->ids) {
			pr_err("Core IDS data missed for non-zombie\n");
			goto out;
		}

		if (!CORE_THREAD_ARCH_INFO(core)) {
			pr_err("Core info data missed for non-zombie\n");
			goto out;
		}

		/*
		 * Seccomp are moved to per-thread origin,
		 * so for old images we need to move per-task
		 * data into proper place.
		 */
		if (core->tc->has_old_seccomp_mode) {
			core->thread_core->has_seccomp_mode = core->tc->has_old_seccomp_mode;
			core->thread_core->seccomp_mode = core->tc->old_seccomp_mode;
		}
		if (core->tc->has_old_seccomp_filter) {
			core->thread_core->has_seccomp_filter = core->tc->has_old_seccomp_filter;
			core->thread_core->seccomp_filter = core->tc->old_seccomp_filter;
			rsti(me)->has_old_seccomp_filter = true;
		}
	}

	ret = 0;
out:
	return ret;
}

/*
 * Find if there are children which are zombies or helpers - processes
 * which are expected to die during the restore.
 */
static bool child_death_expected(void)
{
	struct pstree_item *pi;

	list_for_each_entry(pi, &current->children, sibling) {
		switch (pi->pid->state) {
		case TASK_DEAD:
		case TASK_HELPER:
			return true;
		}
	}

	return false;
}

static int wait_exiting_children(void)
{
	siginfo_t info;

	if (!child_death_expected()) {
		/*
		 * Restoree has no children that should die, during restore,
		 * wait for the next stage on futex.
		 * The default SIGCHLD handler will handle an unexpected
		 * child's death and abort the restore if someone dies.
		 */
		restore_finish_stage(task_entries, CR_STATE_RESTORE);
		return 0;
	}

	/*
	 * The restoree has children which will die - decrement itself from
	 * nr. of tasks processing the stage and wait for anyone to die.
	 * Tasks may die only when they're on the following stage.
	 * If one dies earlier - that's unexpected - treat it as an error
	 * and abort the restore.
	 */
	if (block_sigmask(NULL, SIGCHLD))
		return -1;

	/* Finish CR_STATE_RESTORE, but do not wait for the next stage. */
	futex_dec_and_wake(&task_entries->nr_in_progress);

	if (waitid(P_ALL, 0, &info, WEXITED | WNOWAIT)) {
		pr_perror("Failed to wait");
		return -1;
	}

	if (futex_get(&task_entries->start) == CR_STATE_RESTORE) {
		pr_err("Child %d died too early\n", info.si_pid);
		return -1;
	}

	if (wait_on_helpers_zombies()) {
		pr_err("Failed to wait on helpers and zombies\n");
		return -1;
	}

	return 0;
}

/*
 * Restore a helper process - artificially created by criu
 * to restore attributes of process tree.
 * - sessions for each leaders are dead
 * - process groups with dead leaders
 * - dead tasks for which /proc/<pid>/... is opened by restoring task
 * - whatnot
 */
static int restore_one_helper(void)
{
	int i;

	if (prepare_fds(current))
		return -1;

	if (wait_exiting_children())
		return -1;

	sfds_protected = false;
	close_image_dir();
	close_proc();
	for (i = SERVICE_FD_MIN + 1; i < SERVICE_FD_MAX; i++)
		close_service_fd(i);

	return 0;
}

static int restore_one_task(int pid, CoreEntry *core)
{
	int ret;

	/* No more fork()-s => no more per-pid logs */

	if (task_alive(current))
		ret = restore_one_alive_task(pid, core);
	else if (current->pid->state == TASK_DEAD)
		ret = restore_one_zombie(core);
	else if (current->pid->state == TASK_HELPER) {
		ret = restore_one_helper();
	} else {
		pr_err("Unknown state in code %d\n", (int)core->tc->task_state);
		ret = -1;
	}

	if (core)
		core_entry__free_unpacked(core, NULL);
	return ret;
}

/* All arguments should be above stack, because it grows down */
struct cr_clone_arg {
	struct pstree_item *item;
	unsigned long clone_flags;

	CoreEntry *core;
};

static void maybe_clone_parent(struct pstree_item *item, struct cr_clone_arg *ca)
{
	/*
	 * zdtm runs in kernel 3.11, which has the problem described below. We
	 * avoid this by including the pdeath_sig test. Once users/zdtm migrate
	 * off of 3.11, this condition can be simplified to just test the
	 * options and not have the pdeath_sig test.
	 */
	if (opts.restore_sibling) {
		/*
		 * This means we're called from lib's criu_restore_child().
		 * In that case create the root task as the child one to+
		 * the caller. This is the only way to correctly restore the
		 * pdeath_sig of the root task. But also looks nice.
		 *
		 * Alternatively, if we are --restore-detached, a similar trick is
		 * needed to correctly restore pdeath_sig and prevent processes from
		 * dying once restored.
		 *
		 * There were a problem in kernel 3.11 -- CLONE_PARENT can't be
		 * set together with CLONE_NEWPID, which has been solved in further
		 * versions of the kernels, but we treat 3.11 as a base, so at
		 * least warn a user about potential problems.
		 */
		rsti(item)->clone_flags |= CLONE_PARENT;
		if (rsti(item)->clone_flags & CLONE_NEWPID)
			pr_warn("Set CLONE_PARENT | CLONE_NEWPID but it might cause restore problem,"
				"because not all kernels support such clone flags combinations!\n");
	} else if (opts.restore_detach) {
		if (ca->core->thread_core->pdeath_sig)
			pr_warn("Root task has pdeath_sig configured, so it will receive one _right_"
				"after restore on CRIU exit\n");
	}
}

static bool needs_prep_creds(struct pstree_item *item)
{
	/*
	 * Before the 4.13 kernel, it was impossible to set
	 * an exe_file if uid or gid isn't zero.
	 */
	return (!item->parent && ((root_ns_mask & CLONE_NEWUSER) || getuid()));
}

static int set_next_pid(void *arg)
{
	char buf[32];
	pid_t *pid = arg;
	int len;
	int fd;

	fd = open_proc_rw(PROC_GEN, LAST_PID_PATH);
	if (fd < 0)
		return -1;

	len = snprintf(buf, sizeof(buf), "%d", *pid - 1);
	if (write(fd, buf, len) != len) {
		pr_perror("Failed to write %s to /proc/%s", buf, LAST_PID_PATH);
		close(fd);
		return -1;
	}
	close(fd);
	return 0;
}

static inline int fork_with_pid(struct pstree_item *item)
{
	struct cr_clone_arg ca;
	struct ns_id *pid_ns = NULL;
	bool external_pidns = false;
	int ret = -1;
	pid_t pid = vpid(item);

	if (item->pid->state != TASK_HELPER) {
		if (open_core(pid, &ca.core))
			return -1;

		if (check_core(ca.core, item))
			return -1;

		item->pid->state = ca.core->tc->task_state;

		/*
		 * Zombie tasks' cgroup is not dumped/restored.
		 * cg_set == 0 is skipped in prepare_task_cgroup()
		 */
		if (item->pid->state == TASK_DEAD) {
			rsti(item)->cg_set = 0;
		} else {
			if (ca.core->thread_core->has_cg_set)
				rsti(item)->cg_set = ca.core->thread_core->cg_set;
			else
				rsti(item)->cg_set = ca.core->tc->cg_set;
		}

		if (ca.core->tc->has_stop_signo)
			item->pid->stop_signo = ca.core->tc->stop_signo;

		if (item->pid->state != TASK_DEAD && !task_alive(item)) {
			pr_err("Unknown task state %d\n", item->pid->state);
			return -1;
		}

		/*
		 * By default we assume that seccomp is not
		 * used at all (especially on dead task). Later
		 * we will walk over all threads and check in
		 * details if filter is present setting up
		 * this flag as appropriate.
		 */
		rsti(item)->has_seccomp = false;

		if (unlikely(item == root_item))
			maybe_clone_parent(item, &ca);
	} else {
		/*
		 * Helper entry will not get moved around and thus
		 * will live in the parent's cgset.
		 */
		rsti(item)->cg_set = rsti(item->parent)->cg_set;
		ca.core = NULL;
	}

	if (item->ids)
		pid_ns = lookup_ns_by_id(item->ids->pid_ns_id, &pid_ns_desc);

	if (!current && pid_ns && pid_ns->ext_key)
		external_pidns = true;

	if (external_pidns) {
		int fd;

		/* Not possible to restore into an empty PID namespace. */
		if (pid == INIT_PID) {
			pr_err("Unable to restore into an empty PID namespace\n");
			return -1;
		}

		fd = inherit_fd_lookup_id(pid_ns->ext_key);
		if (fd < 0) {
			pr_err("Unable to find an external pidns: %s\n", pid_ns->ext_key);
			return -1;
		}

		ret = switch_ns_by_fd(fd, &pid_ns_desc, NULL);
		close(fd);
		if (ret) {
			pr_err("Unable to enter existing PID namespace\n");
			return -1;
		}

		pr_info("Inheriting external pidns %s for %d\n", pid_ns->ext_key, pid);
	}

	ca.item = item;
	ca.clone_flags = rsti(item)->clone_flags;

	BUG_ON(ca.clone_flags & CLONE_VM);

	pr_info("Forking task with %d pid (flags 0x%lx)\n", pid, ca.clone_flags);

	if (!(ca.clone_flags & CLONE_NEWPID)) {
		lock_last_pid();

		if (!kdat.has_clone3_set_tid) {
			if (external_pidns) {
				/*
				 * Restoring into another namespace requires a helper
				 * to write to LAST_PID_PATH. Using clone3() this is
				 * so much easier and simpler. As long as CRIU supports
				 * clone() this is needed.
				 */
				ret = call_in_child_process(set_next_pid, (void *)&pid);
			} else {
				ret = set_next_pid((void *)&pid);
			}
			if (ret != 0) {
				pr_err("Setting PID failed\n");
				goto err_unlock;
			}
		}
	} else {
		if (!external_pidns) {
			if (pid != INIT_PID) {
				pr_err("First PID in a PID namespace needs to be %d and not %d\n", pid, INIT_PID);
				return -1;
			}
		}
	}

	if (kdat.has_clone3_set_tid) {
		ret = clone3_with_pid_noasan(restore_task_with_children, &ca,
					     (ca.clone_flags & ~(CLONE_NEWNET | CLONE_NEWCGROUP | CLONE_NEWTIME)),
					     SIGCHLD, pid);
	} else {
		/*
		 * Some kernel modules, such as network packet generator
		 * run kernel thread upon net-namespace creation taking
		 * the @pid we've been requesting via LAST_PID_PATH interface
		 * so that we can't restore a take with pid needed.
		 *
		 * Here is an idea -- unshare net namespace in callee instead.
		 */
		/*
		 * The cgroup namespace is also unshared explicitly in the
		 * move_in_cgroup(), so drop this flag here as well.
		 */
		close_pid_proc();
		ret = clone_noasan(restore_task_with_children,
				   (ca.clone_flags & ~(CLONE_NEWNET | CLONE_NEWCGROUP | CLONE_NEWTIME)) | SIGCHLD, &ca);
	}

	if (ret < 0) {
		pr_perror("Can't fork for %d", pid);
		if (errno == EEXIST)
			set_cr_errno(EEXIST);
		goto err_unlock;
	}

	if (item == root_item) {
		item->pid->real = ret;
		pr_debug("PID: real %d virt %d\n", item->pid->real, vpid(item));
	}

	arch_shstk_unlock(item, ca.core, ret);

err_unlock:
	if (!(ca.clone_flags & CLONE_NEWPID))
		unlock_last_pid();

	if (ca.core)
		core_entry__free_unpacked(ca.core, NULL);
	return ret;
}

/* Returns 0 if restore can be continued */
static int sigchld_process(int status, pid_t pid)
{
	int sig;

	if (WIFEXITED(status)) {
		pr_err("%d exited, status=%d\n", pid, WEXITSTATUS(status));
		return -1;
	} else if (WIFSIGNALED(status)) {
		sig = WTERMSIG(status);
		pr_err("%d killed by signal %d: %s\n", pid, sig, strsignal(sig));
		return -1;
	} else if (WIFSTOPPED(status)) {
		sig = WSTOPSIG(status);
		/* The root task is ptraced. Allow it to handle SIGCHLD */
		if (sig == SIGCHLD && !current) {
			if (ptrace(PTRACE_CONT, pid, 0, SIGCHLD)) {
				pr_perror("Unable to resume %d", pid);
				return -1;
			}
			return 0;
		}
		pr_err("%d stopped by signal %d: %s\n", pid, sig, strsignal(sig));
		return -1;
	} else if (WIFCONTINUED(status)) {
		pr_err("%d unexpectedly continued\n", pid);
		return -1;
	}
	pr_err("wait for %d resulted in %x status\n", pid, status);
	return -1;
}

static void sigchld_handler(int signal, siginfo_t *siginfo, void *data)
{
	while (1) {
		int status;
		pid_t pid;

		pid = waitpid(-1, &status, WNOHANG);
		if (pid <= 0)
			return;

		if (sigchld_process(status, pid) < 0)
			goto err_abort;
	}

err_abort:
	futex_abort_and_wake(&task_entries->nr_in_progress);
}

static int criu_signals_setup(void)
{
	int ret;
	struct sigaction act;
	sigset_t blockmask;

	ret = sigaction(SIGCHLD, NULL, &act);
	if (ret < 0) {
		pr_perror("sigaction() failed");
		return -1;
	}

	act.sa_flags |= SA_NOCLDSTOP | SA_SIGINFO | SA_RESTART;
	act.sa_sigaction = sigchld_handler;
	sigemptyset(&act.sa_mask);
	sigaddset(&act.sa_mask, SIGCHLD);

	ret = sigaction(SIGCHLD, &act, NULL);
	if (ret < 0) {
		pr_perror("sigaction() failed");
		return -1;
	}

	/*
	 * The block mask will be restored in sigreturn.
	 *
	 * TODO: This code should be removed, when a freezer will be added.
	 */
	sigfillset(&blockmask);
	sigdelset(&blockmask, SIGCHLD);

	/*
	 * Here we use SIG_SETMASK instead of SIG_BLOCK to avoid the case where
	 * we've been forked from a parent who had blocked SIGCHLD. If SIGCHLD
	 * is blocked when a task dies (e.g. if the task fails to restore
	 * somehow), we hang because our SIGCHLD handler is never run. Since we
	 * depend on SIGCHLD being unblocked, let's set the mask explicitly.
	 */
	ret = sigprocmask(SIG_SETMASK, &blockmask, NULL);
	if (ret < 0) {
		pr_perror("Can't block signals");
		return -1;
	}

	return 0;
}

static void restore_sid(void)
{
	pid_t sid;

	/*
	 * SID can only be reset to pid or inherited from parent.
	 * Thus we restore it right here to let our kids inherit
	 * one in case they need it.
	 *
	 * PGIDs are restored late when all tasks are forked and
	 * we can call setpgid() on custom values.
	 */

	if (vpid(current) == current->sid) {
		pr_info("Restoring %d to %d sid\n", vpid(current), current->sid);
		sid = setsid();
		if (sid != current->sid) {
			pr_perror("Can't restore sid (%d)", sid);
			exit(1);
		}
	} else {
		sid = getsid(0);
		if (sid != current->sid) {
			/* Skip the root task if it's not init */
			if (current == root_item && vpid(root_item) != INIT_PID)
				return;
			pr_err("Requested sid %d doesn't match inherited %d\n", current->sid, sid);
			exit(1);
		}
	}
}

static void restore_pgid(void)
{
	/*
	 * Unlike sessions, process groups (a.k.a. pgids) can be joined
	 * by any task, provided the task with pid == pgid (group leader)
	 * exists. Thus, in order to restore pgid we must make sure that
	 * group leader was born and created the group, then join one.
	 *
	 * We do this _before_ finishing the forking stage to make sure
	 * helpers are still with us.
	 */

	pid_t pgid, my_pgid = current->pgid;

	pr_info("Restoring %d to %d pgid\n", vpid(current), my_pgid);

	pgid = getpgrp();
	if (my_pgid == pgid)
		return;

	if (my_pgid != vpid(current)) {
		struct pstree_item *leader;

		/*
		 * Wait for leader to become such.
		 * Missing leader means we're going to crtools
		 * group (-j option).
		 */

		leader = rsti(current)->pgrp_leader;
		if (leader) {
			BUG_ON(my_pgid != vpid(leader));
			futex_wait_until(&rsti(leader)->pgrp_set, 1);
		}
	}

	pr_info("\twill call setpgid, mine pgid is %d\n", pgid);
	if (setpgid(0, my_pgid) != 0) {
		pr_perror("Can't restore pgid (%d/%d->%d)", vpid(current), pgid, current->pgid);
		exit(1);
	}

	if (my_pgid == vpid(current))
		futex_set_and_wake(&rsti(current)->pgrp_set, 1);
}

static int __legacy_mount_proc(void)
{
	char proc_mountpoint[] = "/tmp/crtools-proc.XXXXXX";
	int fd;

	if (mkdtemp(proc_mountpoint) == NULL) {
		pr_perror("mkdtemp failed %s", proc_mountpoint);
		return -1;
	}

	pr_info("Mount procfs in %s\n", proc_mountpoint);
	if (mount("proc", proc_mountpoint, "proc", MS_MGC_VAL | MS_NOSUID | MS_NOEXEC | MS_NODEV, NULL)) {
		pr_perror("mount failed");
		if (rmdir(proc_mountpoint))
			pr_perror("Unable to remove %s", proc_mountpoint);
		return -1;
	}

	fd = open_detach_mount(proc_mountpoint);
	return fd;
}

static int mount_proc(void)
{
	int fd, ret;

	if (root_ns_mask == 0)
		fd = ret = open("/proc", O_DIRECTORY);
	else {
		if (kdat.has_fsopen)
			fd = ret = mount_detached_fs("proc");
		else
			fd = ret = __legacy_mount_proc();
	}

	if (fd >= 0) {
		ret = set_proc_fd(fd);
		close(fd);
	}

	return ret;
}

/*
 * Tasks cannot change sid (session id) arbitrary, but can either
 * inherit one from ancestor, or create a new one with id equal to
 * their pid. Thus sid-s restore is tied with children creation.
 */

static int create_children_and_session(void)
{
	int ret;
	struct pstree_item *child;

	pr_info("Restoring children in alien sessions:\n");
	list_for_each_entry(child, &current->children, sibling) {
		if (!restore_before_setsid(child))
			continue;

		BUG_ON(child->born_sid != -1 && getsid(0) != child->born_sid);

		ret = fork_with_pid(child);
		if (ret < 0)
			return ret;
	}

	if (current->parent)
		restore_sid();

	pr_info("Restoring children in our session:\n");
	list_for_each_entry(child, &current->children, sibling) {
		if (restore_before_setsid(child))
			continue;

		ret = fork_with_pid(child);
		if (ret < 0)
			return ret;
	}

	return 0;
}

static int __restore_task_with_children(void *_arg)
{
	struct cr_clone_arg *ca = _arg;
	pid_t pid;
	int ret;

	current = ca->item;

	if (current != root_item) {
		char buf[12];
		int fd;

		/* Determine PID in CRIU's namespace */
		fd = get_service_fd(CR_PROC_FD_OFF);
		if (fd < 0)
			goto err;

		ret = readlinkat(fd, "self", buf, sizeof(buf) - 1);
		if (ret < 0) {
			pr_perror("Unable to read the /proc/self link");
			goto err;
		}
		buf[ret] = '\0';

		current->pid->real = atoi(buf);
		pr_debug("PID: real %d virt %d\n", current->pid->real, vpid(current));
	}

	pid = getpid();
	if (vpid(current) != pid) {
		pr_err("Pid %d do not match expected %d\n", pid, vpid(current));
		set_task_cr_err(EEXIST);
		goto err;
	}

	if (log_init_by_pid(vpid(current)))
		goto err;

	if (current->parent == NULL) {
		/*
		 * The root task has to be in its namespaces before executing
		 * ACT_SETUP_NS scripts, so the root netns has to be created here
		 */
		if (root_ns_mask & CLONE_NEWNET) {
			struct ns_id *ns = net_get_root_ns();
			if (ns->ext_key)
				ret = net_set_ext(ns);
			else
				ret = unshare(CLONE_NEWNET);
			if (ret) {
				pr_perror("Can't unshare net-namespace");
				goto err;
			}
		}

		if (root_ns_mask & CLONE_NEWTIME) {
			if (prepare_timens(current->ids->time_ns_id))
				goto err;
		} else if (kdat.has_timens) {
			if (prepare_timens(0))
				goto err;
		}

		if (set_opts_cap_eff())
			goto err;

		/* Wait prepare_userns */
		if (restore_finish_ns_stage(CR_STATE_ROOT_TASK, CR_STATE_PREPARE_NAMESPACES) < 0)
			goto err;
	}

	if (needs_prep_creds(current) && (prepare_userns_creds()))
		goto err;

	if (current->parent == NULL) {
		/*
		 * Since we don't support nesting of cgroup namespaces, let's
		 * only set up the cgns (if it exists) in the init task.
		 */
		if (prepare_cgroup_namespace(current) < 0)
			goto err;
	}

	/*
	 * Call this _before_ forking to optimize cgroups
	 * restore -- if all tasks live in one set of cgroups
	 * we will only move the root one there, others will
	 * just have it inherited.
	 */
	if (restore_task_cgroup(current) < 0)
		goto err;

	/* Restore root task */
	if (current->parent == NULL) {
		if (join_namespaces()) {
			pr_perror("Join namespaces failed");
			goto err;
		}

		pr_info("Calling restore_sid() for init\n");
		restore_sid();

		/*
		 * We need non /proc proc mount for restoring pid and mount
		 * namespaces and do not care for the rest of the cases.
		 * Thus -- mount proc at custom location for any new namespace
		 */
		if (mount_proc())
			goto err;

		if (!files_collected() && collect_image(&tty_cinfo))
			goto err;
		if (collect_images(before_ns_cinfos, ARRAY_SIZE(before_ns_cinfos)))
			goto err;

		if (prepare_namespace(current, ca->clone_flags))
			goto err;

		if (restore_finish_ns_stage(CR_STATE_PREPARE_NAMESPACES, CR_STATE_FORKING) < 0)
			goto err;

		if (root_prepare_shared())
			goto err;

		if (populate_root_fd_off())
			goto err;
	}

	if (setup_newborn_fds(current))
		goto err;

	if (restore_task_mnt_ns(current))
		goto err;

	if (prepare_mappings(current))
		goto err;

	if (prepare_sigactions(ca->core) < 0)
		goto err;

	if (fault_injected(FI_RESTORE_ROOT_ONLY)) {
		pr_info("fault: Restore root task failure!\n");
		kill(getpid(), SIGKILL);
	}

	if (open_transport_socket())
		goto err;

	timing_start(TIME_FORK);

	if (create_children_and_session())
		goto err;

	timing_stop(TIME_FORK);

	if (populate_pid_proc())
		goto err;

	sfds_protected = true;

	if (unmap_guard_pages(current))
		goto err;

	restore_pgid();

	if (current->parent == NULL) {
		if (root_ns_mask & CLONE_NEWUSER)
			/* Do this after user ns and mnt ns have been set up */
			if (restore_userns_binfmt_misc(current))
				goto err;

		/*
		 * Wait when all tasks passed the CR_STATE_FORKING stage.
		 * The stage was started by criu, but now it waits for
		 * the CR_STATE_RESTORE to finish. See comment near the
		 * CR_STATE_FORKING macro for details.
		 *
		 * It means that all tasks entered into their namespaces.
		 */
		if (restore_wait_other_tasks())
			goto err;
		fini_restore_mntns();
		__restore_switch_stage(CR_STATE_RESTORE);
	} else {
		if (restore_finish_stage(task_entries, CR_STATE_FORKING) < 0)
			goto err;
	}

	if (restore_one_task(vpid(current), ca->core))
		goto err;

	return 0;

err:
	if (current->parent == NULL)
		futex_abort_and_wake(&task_entries->nr_in_progress);
	exit(1);
}

static int restore_task_with_children(void *_arg)
{
	struct cr_clone_arg *arg = _arg;
	struct pstree_item *item = arg->item;
	CoreEntry *core = arg->core;

	return arch_shstk_trampoline(item, core, __restore_task_with_children,
				     arg);
}

int __attribute((weak)) arch_ptrace_restore(int pid, struct pstree_item *item);
int arch_ptrace_restore(int pid, struct pstree_item *item) { return 0; }

static int attach_to_tasks(bool root_seized)
{
	struct pstree_item *item;

	for_each_pstree_item(item) {
		int status, i;

		if (!task_alive(item))
			continue;

		if (item->nr_threads == 1) {
			item->threads[0].real = item->pid->real;
		} else {
			if (parse_threads(item->pid->real, &item->threads, &item->nr_threads))
				return -1;
		}

		for (i = 0; i < item->nr_threads; i++) {
			pid_t pid = item->threads[i].real;

			if (item != root_item || !root_seized || i != 0) {
				if (ptrace(PTRACE_SEIZE, pid, 0, 0)) {
					pr_perror("Can't attach to %d", pid);
					return -1;
				}
			}
			if (ptrace(PTRACE_INTERRUPT, pid, 0, 0)) {
				pr_perror("Can't interrupt the %d task", pid);
				return -1;
			}

			if (wait4(pid, &status, __WALL, NULL) != pid) {
				pr_perror("waitpid(%d) failed", pid);
				return -1;
			}

			if (ptrace(PTRACE_SETOPTIONS, pid, NULL, PTRACE_O_TRACESYSGOOD)) {
				pr_perror("Unable to set PTRACE_O_TRACESYSGOOD for %d", pid);
				return -1;
			}
			if (arch_ptrace_restore(pid, item))
				return -1;
			/*
			 * Suspend seccomp if necessary. We need to do this because
			 * although seccomp is restored at the very end of the
			 * restorer blob (and the final sigreturn is ok), here we're
			 * doing an munmap in the process, which may be blocked by
			 * seccomp and cause the task to be killed.
			 */
			if (rsti(item)->has_seccomp && ptrace_suspend_seccomp(pid) < 0)
				pr_err("failed to suspend seccomp, restore will probably fail...\n");

			if (ptrace(PTRACE_CONT, pid, NULL, NULL)) {
				pr_perror("Unable to resume %d", pid);
				return -1;
			}
		}
	}

	return 0;
}

static int restore_rseq_cs(void)
{
	struct pstree_item *item;

	for_each_pstree_item(item) {
		int i;

		if (!task_alive(item))
			continue;

		if (item->nr_threads == 1) {
			item->threads[0].real = item->pid->real;
		} else {
			if (parse_threads(item->pid->real, &item->threads, &item->nr_threads)) {
				pr_err("restore_rseq_cs: parse_threads failed\n");
				return -1;
			}
		}

		for (i = 0; i < item->nr_threads; i++) {
			pid_t pid = item->threads[i].real;
			struct rst_rseq *rseqe = rsti(item)->rseqe;

			if (!rseqe) {
				pr_err("restore_rseq_cs: rsti(item)->rseqe is NULL\n");
				return -1;
			}

			if (!rseqe[i].rseq_cs_pointer || !rseqe[i].rseq_abi_pointer)
				continue;

			if (ptrace_poke_area(
				    pid, &rseqe[i].rseq_cs_pointer,
				    decode_pointer(rseqe[i].rseq_abi_pointer + offsetof(struct criu_rseq, rseq_cs)),
				    sizeof(uint64_t))) {
				pr_err("Can't restore rseq_cs pointer (pid: %d)\n", pid);
				return -1;
			}
		}
	}

	return 0;
}

static int catch_tasks(bool root_seized)
{
	struct pstree_item *item;
	bool nobp = fault_injected(FI_NO_BREAKPOINTS) || !kdat.has_breakpoints;

	for_each_pstree_item(item) {
		int status, i, ret;

		if (!task_alive(item))
			continue;

		if (item->nr_threads == 1) {
			item->threads[0].real = item->pid->real;
		} else {
			if (parse_threads(item->pid->real, &item->threads, &item->nr_threads))
				return -1;
		}

		for (i = 0; i < item->nr_threads; i++) {
			pid_t pid = item->threads[i].real;

			if (ptrace(PTRACE_INTERRUPT, pid, 0, 0)) {
				pr_perror("Can't interrupt the %d task", pid);
				return -1;
			}

			if (wait4(pid, &status, __WALL, NULL) != pid) {
				pr_perror("waitpid(%d) failed", pid);
				return -1;
			}

			ret = compel_stop_pie(pid, rsti(item)->breakpoint, nobp);
			if (ret < 0)
				return -1;
		}
	}

	return 0;
}

/* ---- VMA mirroring: inject mmap(MAP_FIXED) via ptrace ---- */

struct vma_diff_entry {
	u64 start;
	u64 end;
	u32 prot;
	u32 pad;
};

#ifdef __aarch64__
/* SVC #0; BRK #0 */
static const unsigned char vma_syscall_insn[8] = {
	0x01, 0x00, 0x00, 0xd4,	/* svc #0 */
	0x00, 0x00, 0x20, 0xd4		/* brk #0 */
};
#elif defined(__x86_64__)
/* syscall; int3 (padded to 8 bytes) */
static const unsigned char vma_syscall_insn[8] = {
	0x0f, 0x05,			/* syscall */
	0xcc, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc	/* int3 padding */
};
#endif

static int vma_get_regs(pid_t pid, user_regs_struct_t *regs)
{
	struct iovec iov = { .iov_base = regs, .iov_len = sizeof(*regs) };

	if (ptrace(PTRACE_GETREGSET, pid, NT_PRSTATUS, &iov))
		return -1;
	return 0;
}

static int vma_set_regs(pid_t pid, user_regs_struct_t *regs)
{
	struct iovec iov = { .iov_base = regs, .iov_len = sizeof(*regs) };

	if (ptrace(PTRACE_SETREGSET, pid, NT_PRSTATUS, &iov))
		return -1;
	return 0;
}

static int inject_close_syscall(pid_t pid, int target_fd)
{
	user_regs_struct_t orig_regs, regs;
	unsigned char orig_code[8];
	unsigned long pc;
	int status;

	if (vma_get_regs(pid, &orig_regs))
		return -1;
#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
#else
	pc = (unsigned long)orig_regs.ip;
#endif
	if (ptrace_peek_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		return -1;
	if (ptrace_poke_area(pid, (void *)vma_syscall_insn,
			     (void *)pc, sizeof(vma_syscall_insn)))
		goto err_close;

	regs = orig_regs;
#ifdef __aarch64__
	regs.regs[8] = __NR_close;
	regs.regs[0] = target_fd;
	regs.pc = pc;
#else
	regs.ax = __NR_close;
	regs.di = target_fd;
	regs.ip = pc;
#endif
	if (vma_set_regs(pid, &regs))
		goto err_close;
	if (ptrace(PTRACE_CONT, pid, NULL, NULL))
		goto err_close_r;
	if (waitpid(pid, &status, __WALL) != pid)
		goto err_close_r;
	if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP)
		goto err_close_r;

	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("inject_close: restore code failed\n");
	if (vma_set_regs(pid, &orig_regs))
		pr_err("inject_close: restore regs failed\n");
	return 0;

err_close_r:
	vma_set_regs(pid, &orig_regs);
err_close:
	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("inject_close: restore code failed\n");
	return -1;
}

static int inject_open_syscall(pid_t pid, const char *path,
			       int flags, int mode)
{
	user_regs_struct_t orig_regs, regs;
	unsigned char orig_code[8];
	unsigned char orig_stack[256];
	unsigned long pc, sp;
	int status, result;
	size_t path_len = strlen(path) + 1;

	if (path_len > 240)
		return -1;
	if (vma_get_regs(pid, &orig_regs))
		return -1;
#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
	sp = (unsigned long)orig_regs.sp;
#else
	pc = (unsigned long)orig_regs.ip;
	sp = (unsigned long)orig_regs.sp;
#endif
	if (ptrace_peek_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		return -1;
	if (ptrace_peek_area(pid, orig_stack, (void *)(sp - 256), 256))
		return -1;
	if (ptrace_poke_area(pid, (void *)path, (void *)(sp - 256), path_len))
		goto err_open;
	if (ptrace_poke_area(pid, (void *)vma_syscall_insn,
			     (void *)pc, sizeof(vma_syscall_insn)))
		goto err_open;

	regs = orig_regs;
#ifdef __aarch64__
	regs.regs[8] = __NR_openat;
	regs.regs[0] = (unsigned long)-100; /* AT_FDCWD */
	regs.regs[1] = sp - 256;
	regs.regs[2] = flags;
	regs.regs[3] = mode;
	regs.pc = pc;
#else
	regs.ax = __NR_openat;
	regs.di = (unsigned long)-100;
	regs.si = sp - 256;
	regs.dx = flags;
	regs.r10 = mode;
	regs.ip = pc;
#endif
	if (vma_set_regs(pid, &regs))
		goto err_open;
	if (ptrace(PTRACE_CONT, pid, NULL, NULL))
		goto err_open;
	if (waitpid(pid, &status, __WALL) != pid)
		goto err_open;
	if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP)
		goto err_open;

	if (vma_get_regs(pid, &regs))
		goto err_open;
#ifdef __aarch64__
	result = (int)regs.regs[0];
#else
	result = (int)regs.ax;
#endif
	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("inject_open: restore code failed\n");
	if (ptrace_poke_area(pid, orig_stack, (void *)(sp - 256), 256))
		pr_err("inject_open: restore stack failed\n");
	if (vma_set_regs(pid, &orig_regs))
		pr_err("inject_open: restore regs failed\n");
	return result;

err_open:
	vma_set_regs(pid, &orig_regs);
	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("inject_open: restore code failed\n");
	if (ptrace_poke_area(pid, orig_stack, (void *)(sp - 256), 256))
		pr_err("inject_open: restore stack failed\n");
	return -1;
}

static int inject_dup3_syscall(pid_t pid, int old_fd, int new_fd)
{
	user_regs_struct_t orig_regs, regs;
	unsigned char orig_code[8];
	unsigned long pc;
	int status, result;

	if (vma_get_regs(pid, &orig_regs))
		return -1;
#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
#else
	pc = (unsigned long)orig_regs.ip;
#endif
	if (ptrace_peek_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		return -1;
	if (ptrace_poke_area(pid, (void *)vma_syscall_insn,
			     (void *)pc, sizeof(vma_syscall_insn)))
		goto err_dup3;

	regs = orig_regs;
#ifdef __aarch64__
	regs.regs[8] = __NR_dup3;
	regs.regs[0] = old_fd;
	regs.regs[1] = new_fd;
	regs.regs[2] = 0;
	regs.pc = pc;
#else
	regs.ax = __NR_dup3;
	regs.di = old_fd;
	regs.si = new_fd;
	regs.dx = 0;
	regs.ip = pc;
#endif
	if (vma_set_regs(pid, &regs))
		goto err_dup3;
	if (ptrace(PTRACE_CONT, pid, NULL, NULL))
		goto err_dup3_r;
	if (waitpid(pid, &status, __WALL) != pid)
		goto err_dup3_r;
	if (!WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP)
		goto err_dup3_r;

	if (vma_get_regs(pid, &regs))
		goto err_dup3_r;
#ifdef __aarch64__
	result = (int)regs.regs[0];
#else
	result = (int)regs.ax;
#endif
	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("inject_dup3: restore code failed\n");
	if (vma_set_regs(pid, &orig_regs))
		pr_err("inject_dup3: restore regs failed\n");
	return result;

err_dup3_r:
	vma_set_regs(pid, &orig_regs);
err_dup3:
	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("inject_dup3: restore code failed\n");
	return -1;
}

static int inject_mmap_syscall(pid_t pid, unsigned long addr,
			       unsigned long len, int prot,
			       unsigned long *result)
{
	user_regs_struct_t orig_regs, regs;
	unsigned char orig_code[8];
	unsigned long pc;
	int status;

	if (vma_get_regs(pid, &orig_regs))
		return -1;

#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
#else
	pc = (unsigned long)orig_regs.ip;
#endif

	if (ptrace_peek_area(pid, orig_code, (void *)pc,
			     sizeof(orig_code)))
		return -1;

	if (ptrace_poke_area(pid, (void *)vma_syscall_insn,
			     (void *)pc, sizeof(vma_syscall_insn)))
		goto restore_code;

	regs = orig_regs;
#ifdef __aarch64__
	regs.regs[8] = __NR_mmap;
	regs.regs[0] = addr;
	regs.regs[1] = len;
	regs.regs[2] = prot;
	regs.regs[3] = MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED;
	regs.regs[4] = (unsigned long)-1;
	regs.regs[5] = 0;
	regs.pc = pc;
#else
	regs.ax = __NR_mmap;
	regs.di = addr;
	regs.si = len;
	regs.dx = prot;
	regs.r10 = MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED;
	regs.r8 = (unsigned long)-1;
	regs.r9 = 0;
	regs.ip = pc;
#endif

	if (vma_set_regs(pid, &regs))
		goto restore_code;

	if (ptrace(PTRACE_CONT, pid, NULL, NULL)) {
		pr_perror("VMA mmap: PTRACE_CONT failed");
		goto restore_all;
	}

	if (waitpid(pid, &status, __WALL) != pid) {
		pr_perror("VMA mmap: waitpid failed");
		goto restore_all;
	}

	if (!WIFSTOPPED(status)) {
		pr_err("VMA mmap: not stopped (status=0x%x)\n", status);
		goto restore_all;
	}

	if (WSTOPSIG(status) != SIGTRAP) {
		pr_err("VMA mmap: got signal %d (expected SIGTRAP), "
		       "suppressing and retrying\n", WSTOPSIG(status));
		/*
		 * Suppress the unexpected signal by re-injecting
		 * with PTRACE_CONT data=0, wait for BRK trap.
		 */
		if (ptrace(PTRACE_CONT, pid, NULL, NULL) == 0 &&
		    waitpid(pid, &status, __WALL) == pid &&
		    WIFSTOPPED(status) && WSTOPSIG(status) == SIGTRAP) {
			pr_err("VMA mmap: retry succeeded after "
			       "signal suppression\n");
			goto read_result;
		}
		goto restore_all;
	}

read_result:
	if (vma_get_regs(pid, &regs))
		goto restore_all;

#ifdef __aarch64__
	*result = regs.regs[0];
	if ((long)*result < 0 && (long)*result > -4096) {
		pr_err("VMA mmap: kernel returned error %ld "
		       "for addr=0x%lx len=%lu\n",
		       (long)*result, addr, len);
		goto restore_all;
	}
#else
	*result = regs.ax;
	if ((long)*result < 0 && (long)*result > -4096) {
		pr_err("VMA mmap: kernel returned error %ld "
		       "for addr=0x%lx len=%lu\n",
		       (long)*result, addr, len);
		goto restore_all;
	}
#endif

	if (ptrace_poke_area(pid, orig_code, (void *)pc,
			     sizeof(orig_code)))
		pr_err("VMA mirror: failed to restore code\n");
	if (vma_set_regs(pid, &orig_regs))
		pr_err("VMA mirror: failed to restore regs\n");
	return 0;

restore_all:
	vma_set_regs(pid, &orig_regs);
restore_code:
	if (ptrace_poke_area(pid, orig_code, (void *)pc,
			     sizeof(orig_code)))
		pr_err("VMA mirror: failed to restore code\n");
	return -1;
}

static int inject_new_vmas(struct pstree_item *item,
			   const char *dat_path)
{
	pid_t pid = item->pid->real;
	int fd, count, i;
	struct vma_diff_entry *vmas;

	fd = open(dat_path, O_RDONLY);
	if (fd < 0) {
		pr_perror("open %s", dat_path);
		return -1;
	}

	if (read(fd, &count, sizeof(count)) != sizeof(count)) {
		pr_err("Failed to read VMA count from %s\n", dat_path);
		close(fd);
		return -1;
	}

	if (count <= 0 || count > 100000) {
		pr_err("Invalid VMA count: %d\n", count);
		close(fd);
		return 0; /* not fatal */
	}

	vmas = xmalloc(count * sizeof(*vmas));
	if (!vmas) {
		close(fd);
		return -1;
	}

	if (read(fd, vmas, count * sizeof(*vmas)) !=
	    (ssize_t)(count * sizeof(*vmas))) {
		pr_err("Failed to read VMA entries from %s\n", dat_path);
		xfree(vmas);
		close(fd);
		return -1;
	}
	close(fd);

	pr_err("VMA mirror: injecting %d new VMAs into pid %d\n",
	       count, pid);

	for (i = 0; i < count; i++) {
		unsigned long result;
		unsigned long vaddr = (unsigned long)vmas[i].start;
		unsigned long vlen = (unsigned long)(vmas[i].end - vmas[i].start);
		int retry, ok = 0;

		for (retry = 0; retry < 3; retry++) {
			if (inject_mmap_syscall(pid, vaddr, vlen,
						vmas[i].prot,
						&result) < 0) {
				pr_err("VMA mirror: inject mmap(%lx, %lu) "
				       "failed (attempt %d)\n",
				       vaddr, vlen, retry + 1);
				continue;
			}

			if (result != vaddr) {
				pr_err("VMA mirror: mmap returned %lx, "
				       "expected %lx\n", result, vaddr);
			} else {
				pr_info("VMA mirror: created %lx-%lx\n",
					vaddr, vaddr + vlen);
			}
			ok = 1;
			break;
		}
		if (!ok)
			pr_err("VMA mirror: giving up on %lx-%lx "
			       "after 3 attempts\n", vaddr, vaddr + vlen);
	}

	xfree(vmas);
	return 0;
}

/*
 * In COW dump mode, fork page-recv to install all pages via
 * process_vm_writev while threads are still ptrace-trapped on
 * the exit from rt_sigreturn.  Pages must be present before
 * restore_rseq_cs() which writes rseq pointers via ptrace_poke
 * on top of the installed TLS pages.
 *
 * Uses a poll loop instead of blocking waitpid so we can
 * handle VMA diff signals from page-recv: when page-recv
 * receives a VMA_DIFF message, it writes new_vmas.dat and
 * pauses.  We inject mmap(MAP_FIXED) for each new VMA, then
 * signal page-recv to continue.
 */
static int run_page_recv(struct pstree_item *item)
{
	const char *bin, *addr, *port_env, *streams;
	char pid_s[16], vpid_s[16], port_s[16];
	pid_t child;
	int status;

	bin = getenv("PAGE_RECV_BIN");
	if (!bin)
		bin = "page-recv";

	addr = opts.addr ? opts.addr : getenv("PAGE_RECV_ADDR");
	port_env = getenv("PAGE_RECV_PORT");
	streams = getenv("PAGE_RECV_STREAMS");
	if (!streams)
		streams = "8";

	if (!addr) {
		pr_err("page-recv: no address (set --address or PAGE_RECV_ADDR)\n");
		return -1;
	}

	snprintf(pid_s, sizeof(pid_s), "%d", item->pid->real);
	snprintf(vpid_s, sizeof(vpid_s), "%d", vpid(item));
	if (opts.port)
		snprintf(port_s, sizeof(port_s), "%d", opts.port);
	else if (port_env)
		snprintf(port_s, sizeof(port_s), "%s", port_env);
	else {
		pr_err("page-recv: no port (set --port or PAGE_RECV_PORT)\n");
		return -1;
	}

	pr_info("Running page-recv: pid=%s vpid=%s addr=%s port=%s streams=%s\n",
		pid_s, vpid_s, addr, port_s, streams);

	child = fork();
	if (child < 0) {
		pr_perror("fork for page-recv");
		return -1;
	}

	if (child == 0) {
		execvp(bin, (char *[]){
			(char *)bin,
			"--pid", pid_s,
			"--vpid", vpid_s,
			"--address", (char *)addr,
			"--port", port_s,
			"--images-dir", opts.imgs_dir,
			"--streams", (char *)streams,
			NULL
		});
		pr_perror("execvp page-recv (%s)", bin);
		_exit(1);
	}

	/*
	 * Poll loop: monitor page-recv while handling VMA diff
	 * signals.  When page-recv receives a VMA_DIFF message,
	 * it writes new_vmas.dat and pauses.  We inject mmap for
	 * each new VMA, then signal page-recv to continue.
	 */
	{
		char ready_path[PATH_MAX], created_path[PATH_MAX];
		char dat_path[PATH_MAX];
		bool vma_diff_done = false;
		pid_t w;

		snprintf(ready_path, sizeof(ready_path),
			 "%s/vma_diff_ready", opts.imgs_dir);
		snprintf(created_path, sizeof(created_path),
			 "%s/vma_created", opts.imgs_dir);
		snprintf(dat_path, sizeof(dat_path),
			 "%s/new_vmas.dat", opts.imgs_dir);

		pr_err("VMA poll: imgs_dir=%s ready=%s\n",
		       opts.imgs_dir, ready_path);

		unlink(ready_path);
		unlink(created_path);
		unlink(dat_path);

		while (1) {
			w = waitpid(child, &status, WNOHANG);
			if (w < 0) {
				pr_perror("waitpid page-recv");
				return -1;
			}

			if (!vma_diff_done &&
			    access(ready_path, F_OK) == 0) {
				int rc = inject_new_vmas(item, dat_path);
				int fd;

				if (rc < 0)
					pr_err("VMA mirror: injection "
					       "failed (non-fatal)\n");

				fd = open(created_path,
					  O_CREAT | O_WRONLY | O_TRUNC,
					  0644);
				if (fd >= 0)
					close(fd);
				vma_diff_done = true;
				pr_err("VMA mirror: creation done, "
				       "signaled page-recv\n");
			}

			if (w > 0)
				break;

			usleep(10000);
		}
	}

	if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
		pr_err("page-recv failed (status %d)\n", status);
		return -1;
	}

	pr_info("page-recv completed successfully\n");
	return 0;
}

static void finalize_restore(void)
{
	struct pstree_item *item;

	for_each_pstree_item(item) {
		pid_t pid = item->pid->real;
		struct parasite_ctl *ctl;
		unsigned long restorer_addr;

		if (!task_alive(item))
			continue;

		if (opts.cow_dump) {
			/*
			 * COW mode: skip bootstrap cleanup entirely.
			 * process_madvise(DONTNEED) fails with EINVAL
			 * on ptrace-stopped processes.  compel_unmap
			 * corrupts jemalloc allocator state on aarch64.
			 * The ~200KB bootstrap VMA is harmless — CRIU
			 * places it at a non-conflicting address.
			 */
		} else {
			/* Unmap the restorer blob via ptrace */
			ctl = compel_prepare_noctx(pid);
			if (ctl == NULL)
				continue;

			restorer_addr = (unsigned long)rsti(item)->munmap_restorer;
			if (compel_unmap(ctl, restorer_addr))
				pr_err("Failed to unmap restorer from %d\n", pid);

			xfree(ctl);
		}

		if (opts.final_state == TASK_STOPPED)
			kill(item->pid->real, SIGSTOP);
		else if (item->pid->state == TASK_STOPPED) {
			if (item->pid->stop_signo > 0)
				kill(item->pid->real, item->pid->stop_signo);
			else
				kill(item->pid->real, SIGSTOP);
		}
	}
}

struct converge_thread_regs {
	unsigned long regs[31];
	unsigned long sp;
	unsigned long pc;
	unsigned long pstate;
	unsigned long tls;
};
static struct converge_thread_regs *g_converge_regs;
static int g_converge_regs_count;

static void load_converge_regs(void)
{
	char path[PATH_MAX];
	int fd, cnt;
	ssize_t r;

	snprintf(path, sizeof(path), "%s/converge_regs.dat", opts.imgs_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return;
	r = read(fd, &cnt, sizeof(cnt));
	if (r != sizeof(cnt) || cnt <= 0 || cnt > 1024) {
		close(fd);
		return;
	}
	g_converge_regs = xmalloc(cnt * sizeof(*g_converge_regs));
	if (!g_converge_regs) {
		close(fd);
		return;
	}
	r = read(fd, g_converge_regs, cnt * sizeof(*g_converge_regs));
	close(fd);
	if (r != (ssize_t)(cnt * sizeof(*g_converge_regs))) {
		xfree(g_converge_regs);
		g_converge_regs = NULL;
		return;
	}
	g_converge_regs_count = cnt;
	pr_info("Loaded converge registers for %d threads\n", cnt);
}

struct converge_fd_entry {
	unsigned int fd;
	unsigned int flags;
	unsigned long pos;
	char path[256];
};
static struct converge_fd_entry *g_converge_fds;
static int g_converge_fds_count;

static void load_converge_fds(void)
{
	char path[PATH_MAX];
	int fd, cnt;
	ssize_t r;

	snprintf(path, sizeof(path), "%s/converge_fds.dat", opts.imgs_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return;
	r = read(fd, &cnt, sizeof(cnt));
	if (r != sizeof(cnt) || cnt <= 0 || cnt > 65536) {
		close(fd);
		return;
	}
	g_converge_fds = xmalloc(cnt * sizeof(*g_converge_fds));
	if (!g_converge_fds) {
		close(fd);
		return;
	}
	r = read(fd, g_converge_fds, cnt * sizeof(*g_converge_fds));
	close(fd);
	if (r != (ssize_t)(cnt * sizeof(*g_converge_fds))) {
		xfree(g_converge_fds);
		g_converge_fds = NULL;
		return;
	}
	g_converge_fds_count = cnt;
	pr_info("Loaded converge FD table: %d file descriptors\n", cnt);
}

/* Converge-phase signal handler table: 64 signals × {handler, flags, restorer, mask} */
struct converge_sigact {
	unsigned long handler;
	unsigned long flags;
	unsigned long restorer;
	unsigned long mask;
};

#define CONVERGE_NSIG 64

static struct converge_sigact *g_converge_sigacts;

static void load_converge_sigacts(void)
{
	char path[PATH_MAX];
	int fd;
	ssize_t r;
	size_t sz = CONVERGE_NSIG * sizeof(struct converge_sigact);

	snprintf(path, sizeof(path), "%s/converge_sigacts.dat",
		 opts.imgs_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return;
	g_converge_sigacts = xmalloc(sz);
	if (!g_converge_sigacts) {
		close(fd);
		return;
	}
	r = read(fd, g_converge_sigacts, sz);
	close(fd);
	if (r != (ssize_t)sz) {
		xfree(g_converge_sigacts);
		g_converge_sigacts = NULL;
		return;
	}
	pr_info("Loaded converge signal handlers for %d signals\n", CONVERGE_NSIG);
}

static void apply_converge_sigacts(pid_t pid)
{
	user_regs_struct_t orig_regs, regs;
	unsigned char orig_code[8];
	unsigned char orig_stack[64];
	unsigned long pc, sp;
	int sig, applied = 0, status;

	if (!g_converge_sigacts)
		return;

	/* SEIZE + stop the thread (may be running after detach) */
	if (ptrace(PTRACE_SEIZE, pid, NULL, 0)) {
		pr_perror("apply_sigacts: SEIZE %d", pid);
		return;
	}
	if (ptrace(PTRACE_INTERRUPT, pid, NULL, NULL) ||
	    waitpid(pid, &status, __WALL) != pid) {
		ptrace(PTRACE_DETACH, pid, NULL, NULL);
		return;
	}

	if (vma_get_regs(pid, &orig_regs))
		goto detach_sigacts;

#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
	sp = (unsigned long)orig_regs.sp;
#else
	pc = (unsigned long)orig_regs.ip;
	sp = (unsigned long)orig_regs.sp;
#endif

	if (ptrace_peek_area(pid, orig_code, (void *)pc,
			     sizeof(orig_code)))
		goto detach_sigacts;
	if (ptrace_peek_area(pid, orig_stack,
			     (void *)(sp - 64), 64))
		goto detach_sigacts;

	for (sig = 1; sig <= CONVERGE_NSIG; sig++) {
		struct converge_sigact *sa = &g_converge_sigacts[sig - 1];

		if (sig == SIGKILL || sig == SIGSTOP)
			continue;
		if (!sa->handler && !sa->flags)
			continue;

		/*
		 * Write kernel_sigaction to stack. Layout differs:
		 *   aarch64: handler(8) + flags(8) + mask(8) = 24B
		 *   x86_64:  handler(8) + flags(8) + restorer(8) + mask(8) = 32B
		 * Our converge_sigact has {handler, flags, restorer, mask}.
		 * Must repack for the kernel's expected layout.
		 */
		{
#ifdef __aarch64__
			/* aarch64: no sa_restorer — skip it */
			unsigned long ksa[3];

			ksa[0] = sa->handler;
			ksa[1] = sa->flags;
			ksa[2] = sa->mask;
			if (ptrace_poke_area(pid, ksa,
					     (void *)(sp - 64),
					     sizeof(ksa)))
				continue;
#else
			/* x86_64: includes sa_restorer */
			if (ptrace_poke_area(pid, sa,
					     (void *)(sp - 64),
					     sizeof(*sa)))
				continue;
#endif
		}
		if (ptrace_poke_area(pid, (void *)vma_syscall_insn,
				     (void *)pc, sizeof(vma_syscall_insn)))
			break;

		regs = orig_regs;
#ifdef __aarch64__
		regs.regs[8] = __NR_rt_sigaction;
		regs.regs[0] = sig;
		regs.regs[1] = sp - 64;	/* act */
		regs.regs[2] = 0;		/* oldact = NULL */
		regs.regs[3] = 8;		/* sigsetsize */
		regs.pc = pc;
#elif defined(__x86_64__)
		regs.native.orig_ax = __NR_rt_sigaction;
		regs.native.ax = __NR_rt_sigaction;
		regs.native.di = sig;
		regs.native.si = sp - 64;
		regs.native.dx = 0;
		regs.native.r10 = 8;
		regs.native.ip = pc;
#endif
		if (vma_set_regs(pid, &regs))
			break;
		{
			int retry;

			for (retry = 0; retry < 5; retry++) {
				if (ptrace(PTRACE_CONT, pid, NULL, NULL))
					goto sigact_done;
				if (waitpid(pid, &status, __WALL) != pid)
					goto sigact_done;
				if (WIFSTOPPED(status) &&
				    WSTOPSIG(status) == SIGTRAP) {
					applied++;
					break;
				}
				/* Suppress pending signal and retry */
				if (retry == 0)
					vma_set_regs(pid, &regs);
			}
		}
	}
sigact_done:

	/* Restore original code + stack + regs, then detach */
	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("apply_sigacts: restore code failed\n");
	if (ptrace_poke_area(pid, orig_stack, (void *)(sp - 64), 64))
		pr_err("apply_sigacts: restore stack failed\n");
	if (vma_set_regs(pid, &orig_regs))
		pr_err("apply_sigacts: restore regs failed\n");
detach_sigacts:
	ptrace(PTRACE_DETACH, pid, NULL, NULL);
	pr_info("converge sigacts: applied %d signal handlers\n", applied);
}

/*
 * Compare converge FD table against the restored process's actual FDs.
 * Fix mismatches via ptrace syscall injection.
 */
/* Converge itimers: 3 × {interval_sec, interval_usec, value_sec, value_usec} */
struct converge_itimerval {
	long it_interval_sec;
	long it_interval_usec;
	long it_value_sec;
	long it_value_usec;
};

static struct converge_itimerval *g_converge_itimers;

static void load_converge_itimers(void)
{
	char path[PATH_MAX];
	int fd;
	ssize_t r;
	size_t sz = 3 * sizeof(struct converge_itimerval);

	snprintf(path, sizeof(path), "%s/converge_itimers.dat",
		 opts.imgs_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return;
	g_converge_itimers = xmalloc(sz);
	if (!g_converge_itimers) {
		close(fd);
		return;
	}
	r = read(fd, g_converge_itimers, sz);
	close(fd);
	if (r != (ssize_t)sz) {
		xfree(g_converge_itimers);
		g_converge_itimers = NULL;
	}
}

/* Converge misc: brk, umask, dumpable, thp, subreaper, membarrier */
struct converge_misc {
	unsigned long brk;
	unsigned long umask;
	unsigned long dumpable;
	unsigned long thp_disabled;
	unsigned long child_subreaper;
	unsigned long membarrier_mask;
};

static struct converge_misc *g_converge_misc;

static void load_converge_misc(void)
{
	char path[PATH_MAX];
	int fd;
	ssize_t r;

	snprintf(path, sizeof(path), "%s/converge_misc.dat",
		 opts.imgs_dir);
	fd = open(path, O_RDONLY);
	if (fd < 0)
		return;
	g_converge_misc = xmalloc(sizeof(*g_converge_misc));
	if (!g_converge_misc) {
		close(fd);
		return;
	}
	r = read(fd, g_converge_misc, sizeof(*g_converge_misc));
	close(fd);
	if (r != (ssize_t)sizeof(*g_converge_misc)) {
		xfree(g_converge_misc);
		g_converge_misc = NULL;
	}
}

/*
 * Apply itimers + misc on the replica via ptrace injection.
 * Called after PTRACE_DETACH (process is running).
 */
static void __attribute__((unused)) cow_restore_apply_itimers_misc(pid_t pid)
{
	user_regs_struct_t orig_regs, regs;
	unsigned char orig_code[8];
	unsigned char orig_stack[64];
	unsigned long pc, sp;
	int status;

	if (!g_converge_itimers && !g_converge_misc)
		return;

	/* SEIZE + stop */
	if (ptrace(PTRACE_SEIZE, pid, NULL, 0)) {
		pr_perror("apply_itimers_misc: SEIZE %d", pid);
		return;
	}
	if (ptrace(PTRACE_INTERRUPT, pid, NULL, NULL) ||
	    waitpid(pid, &status, __WALL) != pid) {
		ptrace(PTRACE_DETACH, pid, NULL, NULL);
		return;
	}

	if (vma_get_regs(pid, &orig_regs))
		goto detach_im;

#ifdef __aarch64__
	pc = (unsigned long)orig_regs.pc;
	sp = (unsigned long)orig_regs.sp;
#else
	pc = (unsigned long)orig_regs.ip;
	sp = (unsigned long)orig_regs.sp;
#endif

	if (ptrace_peek_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		goto detach_im;
	if (ptrace_peek_area(pid, orig_stack, (void *)(sp - 64), 64))
		goto detach_im;

	/* Write SVC+BRK */
	if (ptrace_poke_area(pid, (void *)vma_syscall_insn,
			     (void *)pc, sizeof(vma_syscall_insn)))
		goto restore_im;

	/* Apply itimers: setitimer(which, &val, NULL) × 3 */
	if (g_converge_itimers) {
		int which;

		for (which = 0; which < 3; which++) {
			struct converge_itimerval *it =
				&g_converge_itimers[which];

			/* Skip zero timers */
			if (!it->it_interval_sec && !it->it_interval_usec &&
			    !it->it_value_sec && !it->it_value_usec)
				continue;

			/* Poke itimerval to stack */
			if (ptrace_poke_area(pid, it, (void *)(sp - 32),
					     sizeof(*it)))
				break;

			regs = orig_regs;
#ifdef __aarch64__
			regs.regs[8] = __NR_setitimer;
			regs.regs[0] = which;
			regs.regs[1] = sp - 32;
			regs.regs[2] = 0; /* oldval = NULL */
			regs.pc = pc;
#elif defined(__x86_64__)
			regs.native.orig_ax = __NR_setitimer;
			regs.native.ax = __NR_setitimer;
			regs.native.di = which;
			regs.native.si = sp - 32;
			regs.native.dx = 0;
			regs.native.ip = pc;
#endif
			if (vma_set_regs(pid, &regs))
				break;
			if (ptrace(PTRACE_CONT, pid, NULL, NULL))
				break;
			if (waitpid(pid, &status, __WALL) != pid)
				break;
		}
		pr_info("converge itimers: applied\n");
	}

	/* Apply misc: brk, umask, dumpable, thp, subreaper */
	if (g_converge_misc) {
		long dummy;

		/* brk */
		regs = orig_regs;
#ifdef __aarch64__
		regs.regs[8] = __NR_brk;
		regs.regs[0] = g_converge_misc->brk;
		regs.pc = pc;
#elif defined(__x86_64__)
		regs.native.orig_ax = __NR_brk;
		regs.native.ax = __NR_brk;
		regs.native.di = g_converge_misc->brk;
		regs.native.ip = pc;
#endif
		if (!vma_set_regs(pid, &regs)) {
			ptrace(PTRACE_CONT, pid, NULL, NULL);
			waitpid(pid, &status, __WALL);
		}

		/* umask */
		regs = orig_regs;
#ifdef __aarch64__
		regs.regs[8] = __NR_umask;
		regs.regs[0] = g_converge_misc->umask;
		regs.pc = pc;
#elif defined(__x86_64__)
		regs.native.orig_ax = __NR_umask;
		regs.native.ax = __NR_umask;
		regs.native.di = g_converge_misc->umask;
		regs.native.ip = pc;
#endif
		if (!vma_set_regs(pid, &regs)) {
			ptrace(PTRACE_CONT, pid, NULL, NULL);
			waitpid(pid, &status, __WALL);
		}

		/* prctl SET_DUMPABLE */
		regs = orig_regs;
#ifdef __aarch64__
		regs.regs[8] = __NR_prctl;
		regs.regs[0] = PR_SET_DUMPABLE;
		regs.regs[1] = g_converge_misc->dumpable;
		regs.pc = pc;
#elif defined(__x86_64__)
		regs.native.orig_ax = __NR_prctl;
		regs.native.ax = __NR_prctl;
		regs.native.di = PR_SET_DUMPABLE;
		regs.native.si = g_converge_misc->dumpable;
		regs.native.ip = pc;
#endif
		if (!vma_set_regs(pid, &regs)) {
			ptrace(PTRACE_CONT, pid, NULL, NULL);
			waitpid(pid, &status, __WALL);
		}

		(void)dummy;
		pr_info("converge misc: applied (brk=%lx umask=%lo)\n",
			g_converge_misc->brk, g_converge_misc->umask);
	}

restore_im:
	if (ptrace_poke_area(pid, orig_code, (void *)pc, sizeof(orig_code)))
		pr_err("apply_itimers_misc: restore code failed\n");
	if (ptrace_poke_area(pid, orig_stack, (void *)(sp - 64), 64))
		pr_err("apply_itimers_misc: restore stack failed\n");
	if (vma_set_regs(pid, &orig_regs))
		pr_err("apply_itimers_misc: restore regs failed\n");
detach_im:
	ptrace(PTRACE_DETACH, pid, NULL, NULL);
}

static void __attribute__((unused)) apply_converge_fds(pid_t pid)
{
	char fd_dir[64], fd_path[64], link[256];
	DIR *dir;
	struct dirent *de;
	int i, restored_count = 0;
	int matched = 0, opened = 0, closed_cnt = 0, skipped = 0;
	ssize_t len;
	unsigned char converge_set[8192]; /* bitmap for fds 0..65535 */

	if (!g_converge_fds || !g_converge_fds_count)
		return;

	memset(converge_set, 0, sizeof(converge_set));
	for (i = 0; i < g_converge_fds_count; i++) {
		if (g_converge_fds[i].fd < 65536)
			converge_set[g_converge_fds[i].fd / 8] |=
				1 << (g_converge_fds[i].fd % 8);
	}

	snprintf(fd_dir, sizeof(fd_dir), "/proc/%d/fd", pid);
	dir = opendir(fd_dir);
	if (!dir)
		return;
	while ((de = readdir(dir)) != NULL) {
		if (de->d_name[0] != '.')
			restored_count++;
	}
	closedir(dir);

	/* Pass 1: converge FDs — match, open missing files, skip sockets */
	for (i = 0; i < g_converge_fds_count; i++) {
		unsigned int fd_num = g_converge_fds[i].fd;

		snprintf(fd_path, sizeof(fd_path),
			 "/proc/%d/fd/%u", pid, fd_num);
		len = readlink(fd_path, link, sizeof(link) - 1);
		if (len < 0) {
			if (strstr(g_converge_fds[i].path, "socket:") ||
			    strstr(g_converge_fds[i].path, "pipe:") ||
			    strstr(g_converge_fds[i].path, "anon_inode:")) {
				skipped++;
				continue;
			}
			if (g_converge_fds[i].path[0] == '/') {
				int tmp_fd = inject_open_syscall(
					pid, g_converge_fds[i].path,
					g_converge_fds[i].flags & 03, 0);
				if (tmp_fd >= 0) {
					if ((unsigned int)tmp_fd != fd_num) {
						inject_dup3_syscall(
							pid, tmp_fd, fd_num);
						inject_close_syscall(
							pid, tmp_fd);
					}
					pr_err("converge FDs: opened fd %u "
					       "(%s)\n", fd_num,
					       g_converge_fds[i].path);
					opened++;
				} else {
					pr_err("converge FDs: open failed "
					       "fd %u (%s)\n", fd_num,
					       g_converge_fds[i].path);
				}
			} else {
				skipped++;
			}
		} else {
			link[len] = '\0';
			if ((strncmp(link, "pipe:", 5) == 0 &&
			     strncmp(g_converge_fds[i].path, "pipe:", 5) == 0) ||
			    (strncmp(link, "socket:", 7) == 0 &&
			     strncmp(g_converge_fds[i].path, "socket:", 7) == 0) ||
			    (strncmp(link, "anon_inode:", 11) == 0 &&
			     strncmp(g_converge_fds[i].path,
				     "anon_inode:", 11) == 0) ||
			    strcmp(link, g_converge_fds[i].path) == 0)
				matched++;
			else
				pr_err("converge FDs: fd %u changed: "
				       "restored=%s converge=%s\n",
				       fd_num, link,
				       g_converge_fds[i].path);
		}
	}

	/* Pass 2: close restored FDs not in converge (stale from seize) */
	dir = opendir(fd_dir);
	if (dir) {
		while ((de = readdir(dir)) != NULL) {
			int fd_num;

			if (de->d_name[0] == '.')
				continue;
			fd_num = atoi(de->d_name);
			if (fd_num < 3 || fd_num >= 65536)
				continue;
			if (converge_set[fd_num / 8] & (1 << (fd_num % 8)))
				continue;
			snprintf(fd_path, sizeof(fd_path),
				 "/proc/%d/fd/%d", pid, fd_num);
			len = readlink(fd_path, link, sizeof(link) - 1);
			if (len <= 0)
				continue;
			link[len] = '\0';
			if (strstr(link, "eventpoll"))
				continue;
			pr_err("converge FDs: closing stale fd %d (%s)\n",
			       fd_num, link);
			inject_close_syscall(pid, fd_num);
			closed_cnt++;
		}
		closedir(dir);
	}

	pr_info("converge FDs: %d matched, %d opened, %d closed, "
	       "%d skipped, %d restored, %d converge\n",
	       matched, opened, closed_cnt, skipped,
	       restored_count, g_converge_fds_count);
}

/*
 * COW restore helpers — extracted to keep the COW path separate
 * from CRIU's standard restore flow.
 */

/* Run page-recv to install bulk + converge pages while threads are trapped */
static int cow_restore_recv_pages(struct pstree_item *root)
{
	int ret;

	if (!opts.lazy_pages)
		return 0;
	if (!opts.addr && !getenv("PAGE_RECV_ADDR"))
		return 0;

	ret = run_page_recv(root);
	if (ret)
		pr_err("page-recv failed, aborting restore\n");
	return ret;
}

/* Load converge state files (regs, FDs, sigacts) from images dir */
static void cow_restore_load_converge_state(void)
{
	load_converge_regs();
	load_converge_fds();
	load_converge_sigacts();
	load_converge_itimers();
	load_converge_misc();
	pr_err("converge state: regs=%d fds=%d sigacts=%s "
	       "itimers=%s misc=%s\n",
	       g_converge_regs_count, g_converge_fds_count,
	       g_converge_sigacts ? "yes" : "no",
	       g_converge_itimers ? "yes" : "no",
	       g_converge_misc ? "yes" : "no");
}

/* Apply converge sigacts after detach (process is running) */
static void __attribute__((unused)) cow_restore_apply_sigacts(struct pstree_item *root)
{
	if (!g_converge_sigacts)
		return;

	usleep(1000); /* let process settle into event loop */
	apply_converge_sigacts(root->pid->real);
}

/*
 * Apply converge registers to a single thread via PTRACE_SETREGSET.
 * Handles aarch64 x0→x19 fix and x86_64 native register copy.
 */
static void cow_restore_apply_thread_regs(pid_t pid, int thread_idx)
{
	user_regs_struct_t gp_regs;
	struct iovec gp_iov;
#ifdef __aarch64__
	struct iovec tls_iov;
	unsigned long tls_val;
#endif

	if (!g_converge_regs || thread_idx >= g_converge_regs_count)
		return;

#ifdef __aarch64__
	memcpy(gp_regs.regs, g_converge_regs[thread_idx].regs,
	       31 * sizeof(unsigned long));
	gp_regs.sp = g_converge_regs[thread_idx].sp;
	gp_regs.pc = g_converge_regs[thread_idx].pc;
	gp_regs.pstate = g_converge_regs[thread_idx].pstate;

	/*
	 * SIGSTOP sets x0 = -EINTR for interrupted syscalls.
	 * PTRACE_SETREGSET can't set orig_x0, so the kernel
	 * re-executes the SVC with x0 as the first arg.
	 * Restore the original arg from x19 (glibc saves it
	 * in the callee-saved register).
	 */
	if ((long)gp_regs.regs[0] < 0)
		gp_regs.regs[0] = gp_regs.regs[19];
#elif defined(__x86_64__)
	/*
	 * Copy full register set from converge capture.
	 * regs[] holds a memcpy'd user_regs_struct64.
	 * orig_rax + fs_base included in SETREGSET.
	 */
	memcpy(&gp_regs.native, g_converge_regs[thread_idx].regs,
	       sizeof(gp_regs.native));
	gp_regs.__is_native = NATIVE_MAGIC;
#endif
	gp_iov.iov_base = &gp_regs;
	gp_iov.iov_len = sizeof(gp_regs);
	if (ptrace(PTRACE_SETREGSET, pid,
		   (void *)(unsigned long)NT_PRSTATUS, &gp_iov))
		pr_perror("converge regs: GP set failed for %d", pid);

#ifdef __aarch64__
	tls_val = g_converge_regs[thread_idx].tls;
	tls_iov.iov_base = &tls_val;
	tls_iov.iov_len = sizeof(tls_val);
	if (ptrace(PTRACE_SETREGSET, pid,
		   (void *)0x401UL, &tls_iov))
		pr_perror("converge regs: TLS set failed for %d", pid);
#endif
	/* x86_64: fs_base set via NT_PRSTATUS */

	pr_info("converge regs: thread %d pid %d pc=%lx\n",
		thread_idx, pid, g_converge_regs[thread_idx].pc);
}

static int finalize_restore_detach(void)
{
	struct pstree_item *item;

	for_each_pstree_item(item) {
		pid_t pid;
		int i, main_idx = -1;

		if (!task_alive(item))
			continue;

		/* Thread count must match — abort if structure changed */
		if (g_converge_regs && item->nr_threads != g_converge_regs_count) {
			pr_err("converge threads: count mismatch — "
			       "restore has %d, converge had %d, aborting\n",
			       item->nr_threads, g_converge_regs_count);
			return -1;
		}

		/* converge sigacts applied after detach — see below */

		/* Set regs + apply converge regs, track main thread index */
		for (i = 0; i < item->nr_threads; i++) {
			pid = item->threads[i].real;
			if (pid < 0)
				continue;
			if (arch_set_thread_regs_nosigrt(
				    &item->threads[i])) {
				pr_perror("Restoring regs for %d", pid);
				return -1;
			}
			cow_restore_apply_thread_regs(pid, i);
			if (pid == item->pid->real)
				main_idx = i;
		}

		/* Detach workers first, main thread last */
		for (i = 0; i < item->nr_threads; i++) {
			if (i == main_idx)
				continue;
			pid = item->threads[i].real;
			if (pid < 0)
				continue;
			if (ptrace(PTRACE_DETACH, pid, NULL, 0)) {
				pr_perror("Unable to detach %d", pid);
				return -1;
			}
		}

		if (main_idx >= 0) {
			pid = item->threads[main_idx].real;
			if (ptrace(PTRACE_DETACH, pid, NULL, 0)) {
				pr_perror("Unable to detach %d", pid);
				return -1;
			}
		}
	}
	return 0;
}

static void ignore_kids(void)
{
	struct sigaction sa = { .sa_handler = SIG_DFL };

	if (sigaction(SIGCHLD, &sa, NULL) < 0)
		pr_perror("Restoring CHLD sigaction failed");
}

static unsigned int saved_loginuid;

static int prepare_userns_hook(void)
{
	int ret;

	if (kdat.luid != LUID_FULL)
		return 0;
	/*
	 * Save old loginuid and set it to INVALID_UID:
	 * this value means that loginuid is unset and it will be inherited.
	 * After you set some value to /proc/<>/loginuid it can't be changed
	 * inside container due to permissions.
	 * But you still can set this value if it was unset.
	 */
	saved_loginuid = parse_pid_loginuid(getpid(), &ret, false);
	if (ret < 0)
		return -1;

	if (prepare_loginuid(INVALID_UID) < 0) {
		pr_err("Setting loginuid for CT init task failed, CAP_AUDIT_CONTROL?\n");
		return -1;
	}
	return 0;
}

static void restore_origin_ns_hook(void)
{
	if (kdat.luid != LUID_FULL)
		return;

	/* not critical: it does not affect CT in any way */
	if (prepare_loginuid(saved_loginuid) < 0)
		pr_err("Restore original /proc/self/loginuid failed\n");
}

static int write_restored_pid(void)
{
	int pid;

	if (!opts.pidfile)
		return 0;

	pid = root_item->pid->real;

	if (write_pidfile(pid) < 0) {
		pr_perror("Can't write pidfile");
		return -1;
	}

	return 0;
}

static void reap_zombies(void)
{
	while (1) {
		pid_t pid = wait(NULL);
		if (pid == -1) {
			if (errno != ECHILD)
				pr_perror("Error while waiting for pids");
			return;
		}
	}
}

static int restore_root_task(struct pstree_item *init)
{
	int ret, fd, mnt_ns_fd = -1;
	int root_seized = 0;
	struct pstree_item *item;

	ret = run_scripts(ACT_PRE_RESTORE);
	if (ret != 0) {
		pr_err("Aborting restore due to pre-restore script ret code %d\n", ret);
		return -1;
	}

	fd = open("/proc", O_DIRECTORY | O_RDONLY);
	if (fd < 0) {
		pr_perror("Unable to open /proc");
		return -1;
	}

	ret = install_service_fd(CR_PROC_FD_OFF, fd);
	if (ret < 0)
		return -1;

	/*
	 * FIXME -- currently we assume that all the tasks live
	 * in the same set of namespaces. This is done to debug
	 * the ns contents dumping/restoring. Need to revisit
	 * this later.
	 */

	if (prepare_userns_hook())
		return -1;

	if (prepare_namespace_before_tasks())
		return -1;

	if (vpid(init) == INIT_PID) {
		if (!(root_ns_mask & CLONE_NEWPID)) {
			pr_err("This process tree can only be restored "
			       "in a new pid namespace.\n"
			       "criu should be re-executed with the "
			       "\"--namespace pid\" option.\n");
			return -1;
		}
	} else if (root_ns_mask & CLONE_NEWPID) {
		struct ns_id *ns;
		/*
		 * Restoring into an existing PID namespace. This disables
		 * the check to require a PID 1 when restoring a process
		 * which used to be in a PID namespace.
		 */
		ns = lookup_ns_by_id(init->ids->pid_ns_id, &pid_ns_desc);
		if (!ns || !ns->ext_key) {
			pr_err("Can't restore pid namespace without the process init\n");
			return -1;
		}
	}

	__restore_switch_stage_nw(CR_STATE_ROOT_TASK);

	ret = fork_with_pid(init);
	if (ret < 0)
		goto out;

	restore_origin_ns_hook();

	if (rsti(init)->clone_flags & CLONE_PARENT) {
		struct sigaction act;

		root_seized = 1;
		/*
		 * Root task will be our sibling. This means, that
		 * we will not notice when (if) it dies in SIGCHLD
		 * handler, but we should. To do this -- attach to
		 * the guy with ptrace (below) and (!) make the kernel
		 * deliver us the signal when it will get stopped.
		 * It will in case of e.g. segfault before handling
		 * the signal.
		 */
		sigaction(SIGCHLD, NULL, &act);
		act.sa_flags &= ~SA_NOCLDSTOP;
		sigaction(SIGCHLD, &act, NULL);

		if (ptrace(PTRACE_SEIZE, init->pid->real, 0, 0)) {
			pr_perror("Can't attach to init");
			goto out_kill;
		}
	}

	if (!root_ns_mask)
		goto skip_ns_bouncing;

	/*
	 * uid_map and gid_map must be filled from a parent user namespace.
	 * prepare_userns_creds() must be called after filling mappings.
	 */
	if ((root_ns_mask & CLONE_NEWUSER) && prepare_userns(init))
		goto out_kill;

	pr_info("Wait until namespaces are created\n");
	ret = restore_wait_inprogress_tasks();
	if (ret)
		goto out_kill;

	ret = run_scripts(ACT_SETUP_NS);
	if (ret)
		goto out_kill;

	ret = restore_switch_stage(CR_STATE_PREPARE_NAMESPACES);
	if (ret)
		goto out_kill;

	if (root_ns_mask & CLONE_NEWNS) {
		mnt_ns_fd = open_proc(init->pid->real, "ns/mnt");
		if (mnt_ns_fd < 0)
			goto out_kill;
	}

	if (root_ns_mask & opts.empty_ns & CLONE_NEWNET) {
		/*
		 * Local TCP connections were locked by network_lock_internal()
		 * on dump and normally should have been C/R-ed by respectively
		 * dump_iptables() and restore_iptables() in net.c. However in
		 * the '--empty-ns net' mode no iptables C/R is done and we
		 * need to return these rules by hands.
		 */
		ret = network_lock_internal(/* restore = */ true);
		if (ret)
			goto out_kill;
	}

	ret = run_scripts(ACT_POST_SETUP_NS);
	if (ret)
		goto out_kill;

	__restore_switch_stage(CR_STATE_FORKING);

skip_ns_bouncing:
	ret = run_plugins(POST_FORKING);
	if (ret < 0 && ret != -ENOTSUP)
		goto out_kill;

	ret = restore_wait_inprogress_tasks();
	if (ret < 0)
		goto out_kill;

	ret = apply_memfd_seals();
	if (ret < 0)
		goto out_kill;

	/*
	 * Zombies die after CR_STATE_RESTORE which is switched
	 * by root task, not by us. See comment before CR_STATE_FORKING
	 * in the header for details.
	 */
	for_each_pstree_item(item) {
		if (item->pid->state == TASK_DEAD)
			task_entries->nr_threads--;
	}

	ret = restore_switch_stage(CR_STATE_RESTORE_SIGCHLD);
	if (ret < 0)
		goto out_kill;

	ret = stop_usernsd();
	if (ret < 0)
		goto out_kill;

	ret = stop_cgroupd();
	if (ret < 0)
		goto out_kill;

	ret = move_veth_to_bridge();
	if (ret < 0)
		goto out_kill;

	ret = prepare_cgroup_properties();
	if (ret < 0)
		goto out_kill;

	if (fault_injected(FI_POST_RESTORE))
		goto out_kill;

	ret = run_scripts(ACT_POST_RESTORE);
	if (ret != 0) {
		pr_err("Aborting restore due to post-restore script ret code %d\n", ret);
		timing_stop(TIME_RESTORE);
		write_stats(RESTORE_STATS);
		goto out_kill;
	}

	/*
	 * There is no need to call try_clean_remaps() after this point,
	 * as restore went OK and all ghosts were removed by the openers.
	 */
	if (depopulate_roots_yard(mnt_ns_fd, false))
		goto out_kill;

	close_safe(&mnt_ns_fd);

	if (write_restored_pid())
		goto out_kill;

	/* Unlock network before disabling repair mode on sockets */
	network_unlock();

	/*
	 * Stop getting sigchld, after we resume the tasks they
	 * may start to exit poking criu in vain.
	 */
	ignore_kids();

	/*
	 * -------------------------------------------------------------
	 * Network is unlocked. If something fails below - we lose data
	 * or a connection.
	 */
	attach_to_tasks(root_seized);

	if (restore_switch_stage(CR_STATE_RESTORE_CREDS))
		goto out_kill_network_unlocked;

	timing_stop(TIME_RESTORE);

	if (catch_tasks(root_seized)) {
		pr_err("Can't catch all tasks\n");
		goto out_kill_network_unlocked;
	}

	if (lazy_pages_finish_restore())
		goto out_kill_network_unlocked;

	__restore_switch_stage(CR_STATE_COMPLETE);

	ret = compel_stop_on_syscall(task_entries->nr_threads, __NR(rt_sigreturn, 0), __NR(rt_sigreturn, 1));
	if (ret) {
		pr_err("Can't stop all tasks on rt_sigreturn\n");
		goto out_kill_network_unlocked;
	}

	finalize_restore();

	/* COW: receive pages + load converge state before rseq fixup */
	if (opts.cow_dump) {
		ret = cow_restore_recv_pages(root_item);
		if (ret)
			goto out_kill_network_unlocked;
		cow_restore_load_converge_state();
	}

	/* just before releasing threads we have to restore rseq_cs */
	if (restore_rseq_cs())
		pr_err("Unable to restore rseq_cs state\n");


	/*
	 * Some external devices such as GPUs might need a very late
	 * trigger to kick-off some events, memory notifiers and for
	 * restarting the previously restored queues during criu restore
	 * stage. This is needed since criu pie code may shuffle VMAs
	 * around so things such as registering MMU notifiers (for GPU
	 * mapped memory) could be done sanely once the pie code hands
	 * over the control to master process.
	 */
	pr_info("Run late stage hook from criu master for external devices\n");
	for_each_pstree_item(item) {
		if (!task_alive(item))
			continue;
		ret = run_plugins(RESUME_DEVICES_LATE, item->pid->real);
		/*
		 * This may not really be an error. Only certain plugin hooks
		 * (if available) will return success such as amdgpu_plugin that
		 * validates the pid of the resuming tasks in the kernel mode.
		 * Most of the times, it'll be -ENOTSUP and in few cases, it
		 * might actually be a true error code but that would be also
		 * captured in the plugin so no need to print the error here.
		 */
		if (ret < 0 && ret != -ENOTSUP)
			pr_debug("restore late stage hook for external plugin failed\n");
	}

	ret = run_scripts(ACT_PRE_RESUME);
	if (ret)
		pr_err("Pre-resume script ret code %d\n", ret);

	if (restore_freezer_state())
		pr_err("Unable to restore freezer state\n");

	/* Detaches from processes and they continue run through sigreturn. */
	if (finalize_restore_detach())
		goto out_kill_network_unlocked;

	/* COW: converge state captured and loaded.
	 * Apply after detach disabled — ptrace CONT cycles on running
	 * process corrupt kernel syscall state (orig_x0).
	 * Sigacts from seize images are correct for Valkey (handlers
	 * don't change at runtime). Converge captures are available
	 * in .dat files for future before-detach application. */

	pr_info("Restore finished successfully. Tasks resumed.\n");
	write_stats(RESTORE_STATS);

	/* This has the effect of dismissing the image streamer */
	close_image_dir();

	ret = run_scripts(ACT_POST_RESUME);
	if (ret != 0)
		pr_err("Post-resume script ret code %d\n", ret);

	if (!opts.restore_detach && !opts.exec_cmd) {
		reap_zombies();
	}

	return 0;

out_kill_network_unlocked:
	pr_err("Killing processes because of failure on restore.\nThe Network was unlocked so some data or a connection may have been lost.\n");
out_kill:
	/*
	 * The processes can be killed only when all of them have been created,
	 * otherwise an external processes can be killed.
	 */
	if (vpid(root_item) == INIT_PID) {
		int status;

		/* Kill init */
		if (root_item->pid->real > 0)
			kill(root_item->pid->real, SIGKILL);

		if (waitpid(root_item->pid->real, &status, 0) < 0)
			pr_warn("Unable to wait %d: %s\n", root_item->pid->real, strerror(errno));
	} else {
		struct pstree_item *pi;

		for_each_pstree_item(pi)
			if (pi->pid->real > 0)
				kill(pi->pid->real, SIGKILL);
	}

out:
	depopulate_roots_yard(mnt_ns_fd, true);
	stop_usernsd();
	__restore_switch_stage(CR_STATE_FAIL);
	pr_err("Restoring FAILED.\n");
	return -1;
}

int prepare_task_entries(void)
{
	task_entries_pos = rst_mem_align_cpos(RM_SHREMAP);
	task_entries = rst_mem_alloc(sizeof(*task_entries), RM_SHREMAP);
	if (!task_entries) {
		pr_perror("Can't map shmem");
		return -1;
	}

	task_entries->nr_threads = 0;
	task_entries->nr_tasks = 0;
	task_entries->nr_helpers = 0;
	futex_set(&task_entries->start, CR_STATE_FAIL);
	mutex_init(&task_entries->userns_sync_lock);
	mutex_init(&task_entries->cgroupd_sync_lock);
	mutex_init(&task_entries->last_pid_mutex);

	return 0;
}

int prepare_dummy_task_state(struct pstree_item *pi)
{
	CoreEntry *core;

	if (open_core(vpid(pi), &core))
		return -1;

	pi->pid->state = core->tc->task_state;
	core_entry__free_unpacked(core, NULL);

	return 0;
}

int cr_restore_tasks(void)
{
	int ret = -1;

	if (init_service_fd())
		return 1;

	if (check_img_inventory(/* restore = */ true) < 0)
		return -1;

	if (init_stats(RESTORE_STATS))
		return -1;

	if (lsm_check_opts())
		return -1;

	timing_start(TIME_RESTORE);

	if (cpu_init() < 0)
		return -1;

	if (vdso_init_restore())
		return -1;

	if (tty_init_restore())
		return -1;

	if (opts.cpu_cap & CPU_CAP_IMAGE) {
		if (cpu_validate_cpuinfo())
			return -1;
	}

	if (prepare_task_entries() < 0)
		return -1;

	if (prepare_pstree() < 0)
		return -1;

	if (fdstore_init())
		return -1;

	/*
	 * For the AMDGPU plugin, its parallel restore feature needs to use fdstore to store
	 * its socket file descriptor. This allows the main process and the target process to
	 * communicate with each other through this file descriptor. Therefore, cr_plugin_init
	 * must be initialized after fdstore_init.
	 */
	if (cr_plugin_init(CR_PLUGIN_STAGE__RESTORE))
		return -1;

	if (inherit_fd_move_to_fdstore())
		goto err;

	if (crtools_prepare_shared() < 0)
		goto err;

	if (prepare_cgroup())
		goto clean_cgroup;

	if (criu_signals_setup() < 0)
		goto clean_cgroup;

	if (prepare_lazy_pages_socket() < 0)
		goto clean_cgroup;

	ret = restore_root_task(root_item);
clean_cgroup:
	fini_cgroup();
err:
	cr_plugin_fini(CR_PLUGIN_STAGE__RESTORE, ret);
	return ret;
}

static long restorer_get_vma_hint(struct list_head *tgt_vma_list, struct list_head *self_vma_list, long min_addr, long vma_len)
{
	struct vma_area *t_vma, *s_vma;
	long prev_vma_end = min_addr;
	struct vma_area end_vma;
	VmaEntry end_e;

	end_vma.e = &end_e;
	end_e.start = end_e.end = kdat.task_size;
	INIT_LIST_HEAD(&end_vma.list);

	s_vma = list_first_entry(self_vma_list, struct vma_area, list);
	t_vma = list_first_entry(tgt_vma_list, struct vma_area, list);

	while (1) {
		if (prev_vma_end + vma_len > s_vma->e->start) {
			if ((s_vma->list.next == self_vma_list) ||
			    vma_area_is(vma_next(s_vma), VMA_AREA_GUARD)) {
				s_vma = &end_vma;
				continue;
			}
			if (s_vma == &end_vma)
				break;
			if (prev_vma_end < s_vma->e->end)
				prev_vma_end = s_vma->e->end;
			s_vma = vma_next(s_vma);
			continue;
		}

		if (prev_vma_end + vma_len > t_vma->e->start) {
			if ((t_vma->list.next == tgt_vma_list) ||
			    vma_area_is(vma_next(t_vma), VMA_AREA_GUARD)) {
				t_vma = &end_vma;
				continue;
			}
			if (t_vma == &end_vma)
				break;
			if (prev_vma_end < t_vma->e->end)
				prev_vma_end = t_vma->e->end;
			t_vma = vma_next(t_vma);
			continue;
		}

		return prev_vma_end;
	}

	return -1;
}

static int prepare_mm(pid_t pid, struct task_restore_args *args)
{
	int exe_fd, i, ret = -1;
	MmEntry *mm = rsti(current)->mm;

	args->mm = *mm;
	args->mm.n_mm_saved_auxv = 0;
	args->mm.mm_saved_auxv = NULL;

	if (mm->n_mm_saved_auxv > AT_VECTOR_SIZE) {
		pr_err("Image corrupted on pid %d\n", pid);
		goto out;
	}

	args->mm_saved_auxv_size = mm->n_mm_saved_auxv * sizeof(auxv_t);
	for (i = 0; i < mm->n_mm_saved_auxv; ++i) {
		args->mm_saved_auxv[i] = (auxv_t)mm->mm_saved_auxv[i];
	}

	exe_fd = open_reg_by_id(mm->exe_file_id);
	if (exe_fd < 0)
		goto out;

	args->fd_exe_link = exe_fd;

	args->thp_disabled = mm->has_thp_disabled && mm->thp_disabled;

	ret = 0;
out:
	return ret;
}

static void *restorer;
static unsigned long restorer_len;

static int prepare_restorer_blob(void)
{
	/*
	 * We map anonymous mapping, not mremap the restorer itself later.
	 * Otherwise the restorer vma would be tied to criu binary which
	 * in turn will lead to set-exe-file prctl to fail with EBUSY.
	 */

	struct parasite_blob_desc pbd;

	/*
	 * We pass native=true, which is then used to set the value of
	 * pbd.parasite_ip_off. We don't use parasite_ip_off, so the value we
	 * pass as native argument is not relevant.
	 */
	restorer_setup_c_header_desc(&pbd, true);

	/*
	 * args_off is the offset where the binary blob with its GOT table
	 * ends. As we don't do RPC, parasite sections after args_off can be
	 * ignored. See compel_infect() for a description of the parasite
	 * memory layout.
	 */
	restorer_len = round_up(pbd.hdr.args_off, page_size());

	restorer = mmap(NULL, restorer_len, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_PRIVATE | MAP_ANONYMOUS, 0, 0);
	if (restorer == MAP_FAILED) {
		pr_perror("Can't map restorer code");
		return -1;
	}

	memcpy(restorer, pbd.hdr.mem, pbd.hdr.bsize);

	return 0;
}

static int remap_restorer_blob(void *addr)
{
	struct parasite_blob_desc pbd;
	void *mem;

	mem = mremap(restorer, restorer_len, restorer_len, MREMAP_FIXED | MREMAP_MAYMOVE, addr);
	if (mem != addr) {
		pr_perror("Can't remap restorer blob");
		return -1;
	}

	/*
	 * Pass native=true, which is then used to set the value of
	 * pbd.parasite_ip_off. parasite_ip_off is unused in restorer
	 * as compat (ia32) tasks are restored from native (x86_64)
	 * mode, so the value we pass as native argument is not relevant.
	 */
	restorer_setup_c_header_desc(&pbd, true);
	compel_relocs_apply(addr, addr, &pbd);

	/*
	 * Ensure the infected thread sees the updated code.
	 *
	 * On architectures like ARM64, the Data Cache (D-cache) and
	 * Instruction Cache (I-cache) are not automatically coherent.
	 * Modifications land in the D-cache, so we must flush (clean) the
	 * D-cache to push changes to RAM to ensure the CPU fetches the updated
	 * instructions.
	 */
	__builtin___clear_cache(addr, addr + pbd.hdr.bsize);

	return 0;
}

static int validate_sched_parm(struct rst_sched_param *sp)
{
	if ((sp->nice < -20) || (sp->nice > 19))
		return 0;

	switch (sp->policy & ~SCHED_RESET_ON_FORK) {
	case SCHED_RR:
	case SCHED_FIFO:
		return ((sp->prio > 0) && (sp->prio < 100));
	case SCHED_IDLE:
	case SCHED_OTHER:
	case SCHED_BATCH:
		return sp->prio == 0;
	}

	return 0;
}

static int prep_sched_info(struct rst_sched_param *sp, ThreadCoreEntry *tc)
{
	if (!tc->has_sched_policy) {
		sp->policy = SCHED_OTHER;
		sp->nice = 0;
		return 0;
	}

	sp->policy = tc->sched_policy;
	sp->nice = tc->sched_nice;
	sp->prio = tc->sched_prio;

	if (!validate_sched_parm(sp)) {
		pr_err("Inconsistent sched params received (%d.%d.%d)\n", sp->policy, sp->nice, sp->prio);
		return -1;
	}

	return 0;
}

static int prep_rseq(struct rst_rseq_param *rseq, ThreadCoreEntry *tc)
{
	/* compatibility with older CRIU versions */
	if (!tc->rseq_entry)
		return 0;

	rseq->rseq_abi_pointer = tc->rseq_entry->rseq_abi_pointer;
	rseq->rseq_abi_size = tc->rseq_entry->rseq_abi_size;
	rseq->signature = tc->rseq_entry->signature;

	if (rseq->rseq_abi_pointer && !kdat.has_rseq) {
		pr_err("rseq: can't restore as kernel doesn't support it\n");
		return -1;
	}

	return 0;
}

static void prep_libc_rseq_info(struct rst_rseq_param *rseq)
{
	if (!kdat.has_rseq) {
		rseq->rseq_abi_pointer = 0;
		return;
	}

	if (!kdat.has_ptrace_get_rseq_conf) {
#if defined(__GLIBC__) && defined(RSEQ_SIG)
		rseq->rseq_abi_pointer = encode_pointer(__criu_thread_pointer() + __rseq_offset);
		/*
		 * Current glibc reports the feature/active size in
		 * __rseq_size, not the size passed to the kernel.
		 * This could be 20, but older kernels expect 32 for
		 * the size argument even if only 20 bytes are used.
		 */
		rseq->rseq_abi_size = __rseq_size;
		if (rseq->rseq_abi_size < 32)
			rseq->rseq_abi_size = 32;
		rseq->signature = RSEQ_SIG;
#else
		rseq->rseq_abi_pointer = 0;
#endif
		return;
	}

	rseq->rseq_abi_pointer = kdat.libc_rseq_conf.rseq_abi_pointer;
	rseq->rseq_abi_size = kdat.libc_rseq_conf.rseq_abi_size;
	rseq->signature = kdat.libc_rseq_conf.signature;
}

static rlim_t decode_rlim(rlim_t ival)
{
	return ival == -1 ? RLIM_INFINITY : ival;
}

/*
 * Legacy rlimits restore from CR_FD_RLIMIT
 */

static int prepare_rlimits_from_fd(int pid, struct task_restore_args *ta)
{
	struct rlimit *r;
	int ret;
	struct cr_img *img;

	if (!deprecated_ok("Rlimits"))
		return -1;

	/*
	 * Old image -- read from the file.
	 */
	img = open_image(CR_FD_RLIMIT, O_RSTR, pid);
	if (!img)
		return -1;

	ta->rlims_n = 0;
	while (1) {
		RlimitEntry *re;

		ret = pb_read_one_eof(img, &re, PB_RLIMIT);
		if (ret <= 0)
			break;

		r = rst_mem_alloc(sizeof(*r), RM_PRIVATE);
		if (!r) {
			pr_err("Can't allocate memory for resource %d\n", ta->rlims_n);
			return -1;
		}

		r->rlim_cur = decode_rlim(re->cur);
		r->rlim_max = decode_rlim(re->max);
		if (r->rlim_cur > r->rlim_max) {
			pr_err("Can't restore cur > max for %d.%d\n", pid, ta->rlims_n);
			r->rlim_cur = r->rlim_max;
		}

		rlimit_entry__free_unpacked(re, NULL);

		ta->rlims_n++;
	}

	close_image(img);

	return 0;
}

static int prepare_rlimits(int pid, struct task_restore_args *ta, CoreEntry *core)
{
	int i;
	TaskRlimitsEntry *rls = core->tc->rlimits;
	struct rlimit64 *r;

	ta->rlims = (struct rlimit64 *)rst_mem_align_cpos(RM_PRIVATE);

	if (!rls)
		return prepare_rlimits_from_fd(pid, ta);

	for (i = 0; i < rls->n_rlimits; i++) {
		r = rst_mem_alloc(sizeof(*r), RM_PRIVATE);
		if (!r) {
			pr_err("Can't allocate memory for resource %d\n", i);
			return -1;
		}

		r->rlim_cur = decode_rlim(rls->rlimits[i]->cur);
		r->rlim_max = decode_rlim(rls->rlimits[i]->max);

		if (r->rlim_cur > r->rlim_max) {
			pr_warn("Can't restore cur > max for %d.%d\n", pid, i);
			r->rlim_cur = r->rlim_max;
		}
	}

	ta->rlims_n = rls->n_rlimits;
	return 0;
}

static int signal_to_mem(SiginfoEntry *se)
{
	siginfo_t *info, *t;

	info = (siginfo_t *)se->siginfo.data;
	t = rst_mem_alloc(sizeof(siginfo_t), RM_PRIVATE);
	if (!t)
		return -1;

	memcpy(t, info, sizeof(*info));

	return 0;
}

static int open_signal_image(int type, pid_t pid, unsigned int *nr)
{
	int ret;
	struct cr_img *img;

	img = open_image(type, O_RSTR, pid);
	if (!img)
		return -1;

	*nr = 0;
	while (1) {
		SiginfoEntry *se;

		ret = pb_read_one_eof(img, &se, PB_SIGINFO);
		if (ret <= 0)
			break;
		if (se->siginfo.len != sizeof(siginfo_t)) {
			pr_err("Unknown image format\n");
			ret = -1;
			break;
		}

		ret = signal_to_mem(se);
		if (ret)
			break;

		(*nr)++;

		siginfo_entry__free_unpacked(se, NULL);
	}

	close_image(img);

	return ret ?: 0;
}

static int prepare_one_signal_queue(SignalQueueEntry *sqe, unsigned int *nr)
{
	int i;

	for (i = 0; i < sqe->n_signals; i++)
		if (signal_to_mem(sqe->signals[i]))
			return -1;

	*nr = sqe->n_signals;

	return 0;
}

static unsigned int *siginfo_priv_nr; /* FIXME -- put directly on thread_args */

static int prepare_signals(int pid, struct task_restore_args *ta, CoreEntry *leader_core)
{
	int ret = -1, i;

	ta->siginfo = (siginfo_t *)rst_mem_align_cpos(RM_PRIVATE);
	siginfo_priv_nr = xmalloc(sizeof(int) * current->nr_threads);
	if (siginfo_priv_nr == NULL)
		goto out;

	/* Prepare shared signals */
	if (!leader_core->tc->signals_s) /*backward compatibility*/
		ret = open_signal_image(CR_FD_SIGNAL, pid, &ta->siginfo_n);
	else
		ret = prepare_one_signal_queue(leader_core->tc->signals_s, &ta->siginfo_n);

	if (ret < 0)
		goto out;

	for (i = 0; i < current->nr_threads; i++) {
		if (!current->core[i]->thread_core->signals_p) /*backward compatibility*/
			ret = open_signal_image(CR_FD_PSIGNAL, current->threads[i].ns[0].virt, &siginfo_priv_nr[i]);
		else
			ret = prepare_one_signal_queue(current->core[i]->thread_core->signals_p, &siginfo_priv_nr[i]);
		if (ret < 0)
			goto out;
	}
out:
	return ret;
}

extern void __gcov_flush(void) __attribute__((weak));
void __gcov_flush(void)
{
}

static void rst_reloc_creds(struct thread_restore_args *thread_args, unsigned long *creds_pos_next)
{
	struct thread_creds_args *args;

	if (unlikely(!*creds_pos_next))
		return;

	args = rst_mem_remap_ptr(*creds_pos_next, RM_PRIVATE);

	if (args->lsm_profile)
		args->lsm_profile = rst_mem_remap_ptr(args->mem_lsm_profile_pos, RM_PRIVATE);
	if (args->lsm_sockcreate)
		args->lsm_sockcreate = rst_mem_remap_ptr(args->mem_lsm_sockcreate_pos, RM_PRIVATE);
	if (args->groups)
		args->groups = rst_mem_remap_ptr(args->mem_groups_pos, RM_PRIVATE);

	*creds_pos_next = args->mem_pos_next;
	thread_args->creds_args = args;
}

static bool groups_match(gid_t *groups, int n_groups)
{
	int n, len;
	bool ret;
	gid_t *gids;

	n = getgroups(0, NULL);
	if (n == -1) {
		pr_perror("Failed to get number of supplementary groups");
		return false;
	}
	if (n != n_groups)
		return false;
	if (n == 0)
		return true;

	len = n * sizeof(gid_t);
	gids = xmalloc(len);
	if (gids == NULL)
		return false;

	n = getgroups(n, gids);
	if (n == -1) {
		pr_perror("Failed to get supplementary groups");
		ret = false;
	} else {
		/* getgroups sorts gids, so it is safe to memcmp gid arrays */
		ret = !memcmp(gids, groups, len);
	}

	xfree(gids);
	return ret;
}

static void copy_caps(u32 *out_caps, u32 *in_caps, int n_words)
{
	int i, cap_end;

	for (i = kdat.last_cap + 1; i < 32 * n_words; ++i) {
		if (~in_caps[i / 32] & (1 << (i % 32)))
			continue;

		pr_warn("Dropping unsupported capability %d > %d)\n", i, kdat.last_cap);
		/* extra caps will be cleared below */
	}

	n_words = min(n_words, (kdat.last_cap + 31) / 32);
	cap_end = (kdat.last_cap & 31) + 1;
	memcpy(out_caps, in_caps, sizeof(*out_caps) * n_words);
	if ((cap_end & 31) && n_words)
		out_caps[n_words - 1] &= (1 << cap_end) - 1;
	memset(out_caps + n_words, 0, sizeof(*out_caps) * (CR_CAP_SIZE - n_words));
}

static struct thread_creds_args *rst_prep_creds_args(CredsEntry *ce, unsigned long *prev_pos)
{
	unsigned long this_pos;
	struct thread_creds_args *args;

	this_pos = rst_mem_align_cpos(RM_PRIVATE);

	args = rst_mem_alloc(sizeof(*args), RM_PRIVATE);
	if (!args)
		return ERR_PTR(-ENOMEM);

	args->cap_last_cap = kdat.last_cap;
	memcpy(&args->creds, ce, sizeof(args->creds));

	if (ce->lsm_profile || opts.lsm_supplied) {
		char *rendered = NULL, *profile;

		profile = ce->lsm_profile;

		if (validate_lsm(profile) < 0)
			return ERR_PTR(-EINVAL);

		if (profile && render_lsm_profile(profile, &rendered)) {
			return ERR_PTR(-EINVAL);
		}

		if (rendered) {
			size_t lsm_profile_len;
			char *lsm_profile;

			args->mem_lsm_profile_pos = rst_mem_align_cpos(RM_PRIVATE);
			lsm_profile_len = strlen(rendered);
			lsm_profile = rst_mem_alloc(lsm_profile_len + 1, RM_PRIVATE);
			if (!lsm_profile) {
				xfree(rendered);
				return ERR_PTR(-ENOMEM);
			}

			args = rst_mem_remap_ptr(this_pos, RM_PRIVATE);
			args->lsm_profile = lsm_profile;
			__strlcpy(args->lsm_profile, rendered, lsm_profile_len + 1);
			xfree(rendered);
		}
	} else {
		args->lsm_profile = NULL;
		args->mem_lsm_profile_pos = 0;
	}

	if (ce->lsm_sockcreate) {
		char *rendered = NULL;
		char *profile;

		profile = ce->lsm_sockcreate;

		if (validate_lsm(profile) < 0)
			return ERR_PTR(-EINVAL);

		if (profile && render_lsm_profile(profile, &rendered)) {
			return ERR_PTR(-EINVAL);
		}
		if (rendered) {
			size_t lsm_sockcreate_len;
			char *lsm_sockcreate;

			args->mem_lsm_sockcreate_pos = rst_mem_align_cpos(RM_PRIVATE);
			lsm_sockcreate_len = strlen(rendered);
			lsm_sockcreate = rst_mem_alloc(lsm_sockcreate_len + 1, RM_PRIVATE);
			if (!lsm_sockcreate) {
				xfree(rendered);
				return ERR_PTR(-ENOMEM);
			}

			args = rst_mem_remap_ptr(this_pos, RM_PRIVATE);
			args->lsm_sockcreate = lsm_sockcreate;
			__strlcpy(args->lsm_sockcreate, rendered, lsm_sockcreate_len + 1);
			xfree(rendered);
		}
	} else {
		args->lsm_sockcreate = NULL;
		args->mem_lsm_sockcreate_pos = 0;
	}

	/*
	 * Zap fields which we can't use.
	 */
	args->creds.cap_inh = NULL;
	args->creds.cap_eff = NULL;
	args->creds.cap_prm = NULL;
	args->creds.cap_bnd = NULL;
	args->creds.cap_amb = NULL;
	args->creds.groups = NULL;
	args->creds.lsm_profile = NULL;

	copy_caps(args->cap_inh, ce->cap_inh, ce->n_cap_inh);
	copy_caps(args->cap_eff, ce->cap_eff, ce->n_cap_eff);
	copy_caps(args->cap_prm, ce->cap_prm, ce->n_cap_prm);
	copy_caps(args->cap_bnd, ce->cap_bnd, ce->n_cap_bnd);
	copy_caps(args->cap_amb, ce->cap_amb, ce->n_cap_amb);

	if (ce->n_groups && !groups_match(ce->groups, ce->n_groups)) {
		unsigned int *groups;

		args->mem_groups_pos = rst_mem_align_cpos(RM_PRIVATE);
		groups = rst_mem_alloc(ce->n_groups * sizeof(u32), RM_PRIVATE);
		if (!groups)
			return ERR_PTR(-ENOMEM);
		args = rst_mem_remap_ptr(this_pos, RM_PRIVATE);
		args->groups = groups;
		memcpy(args->groups, ce->groups, ce->n_groups * sizeof(u32));
	} else {
		args->groups = NULL;
		args->mem_groups_pos = 0;
	}

	args->mem_pos_next = 0;

	if (prev_pos) {
		if (*prev_pos) {
			struct thread_creds_args *prev;

			prev = rst_mem_remap_ptr(*prev_pos, RM_PRIVATE);
			prev->mem_pos_next = this_pos;
		}
		*prev_pos = this_pos;
	}
	return args;
}

static int rst_prep_creds_from_img(pid_t pid)
{
	CredsEntry *ce = NULL;
	struct cr_img *img;
	int ret;

	img = open_image(CR_FD_CREDS, O_RSTR, pid);
	if (!img)
		return -ENOENT;

	ret = pb_read_one(img, &ce, PB_CREDS);
	close_image(img);

	if (ret > 0) {
		struct thread_creds_args *args;

		args = rst_prep_creds_args(ce, NULL);
		if (IS_ERR(args))
			ret = PTR_ERR(args);
		else
			ret = 0;
	}
	creds_entry__free_unpacked(ce, NULL);
	return ret;
}

static int rst_prep_creds(pid_t pid, CoreEntry *core, unsigned long *creds_pos)
{
	struct thread_creds_args *args = NULL;
	unsigned long this_pos = 0;
	size_t i;

	/*
	 * This is _really_ very old image
	 * format where @thread_core were not
	 * present. It means we don't have
	 * creds either, just ignore and exit
	 * early.
	 */
	if (unlikely(!core->thread_core)) {
		*creds_pos = 0;
		return 0;
	}

	*creds_pos = rst_mem_align_cpos(RM_PRIVATE);

	/*
	 * Old format: one Creds per task carried in own image file.
	 */
	if (!core->thread_core->creds)
		return rst_prep_creds_from_img(pid);

	for (i = 0; i < current->nr_threads; i++) {
		CredsEntry *ce = current->core[i]->thread_core->creds;

		args = rst_prep_creds_args(ce, &this_pos);
		if (IS_ERR(args))
			return PTR_ERR(args);
	}

	return 0;
}

static void *restorer_munmap_addr(CoreEntry *core, void *restorer_blob)
{
#ifdef CONFIG_COMPAT
	if (core_is_compat(core))
		return restorer_sym(restorer_blob, arch_export_unmap_compat);
#endif
	return restorer_sym(restorer_blob, arch_export_unmap);
}

void arch_rsti_init(struct pstree_item *p) __attribute__((weak));
void arch_rsti_init(struct pstree_item *p) {}

static int sigreturn_restore(pid_t pid, struct task_restore_args *task_args, unsigned long alen, CoreEntry *core)
{
	void *mem = MAP_FAILED;
	void *restore_task_exec_start;

	long new_sp;
	long ret;

	long rst_mem_size;
	long memzone_size;

	struct thread_restore_args *thread_args;
	struct restore_mem_zone *mz;

	struct vdso_maps vdso_maps_rt;
	unsigned long vdso_rt_size = 0;

	struct vm_area_list self_vmas;
	struct vm_area_list *vmas = &rsti(current)->vmas;
	int i, siginfo_n;

	unsigned long creds_pos = 0;
	unsigned long creds_pos_next;

	sigset_t blockmask;

	pr_info("Restore via sigreturn\n");

	/* pr_info_vma_list(&self_vma_list); */

	BUILD_BUG_ON(sizeof(struct task_restore_args) & 1);
	BUILD_BUG_ON(sizeof(struct thread_restore_args) & 1);

	/*
	 * Read creds info for every thread and allocate memory
	 * needed so we can use this data inside restorer.
	 */
	if (rst_prep_creds(pid, core, &creds_pos))
		goto err_nv;

	if (current->parent == NULL) {
		/* Wait when all tasks restored all files */
		if (restore_wait_other_tasks())
			goto err_nv;
		if (root_ns_mask & CLONE_NEWNS && remount_readonly_mounts())
			goto err_nv;
	}

	/*
	 * We're about to search for free VM area and inject the restorer blob
	 * into it. No irrelevant mmaps/mremaps beyond this point, otherwise
	 * this unwanted mapping might get overlapped by the restorer.
	 */

	ret = parse_self_maps_lite(&self_vmas);
	if (ret < 0)
		goto err;

	rst_mem_size = rst_mem_lock();
	memzone_size = round_up(sizeof(struct restore_mem_zone) * current->nr_threads, page_size());
	task_args->bootstrap_len = restorer_len + memzone_size + alen + rst_mem_size + shstk_restorer_stack_size();
	BUG_ON(task_args->bootstrap_len & (PAGE_SIZE - 1));
	pr_info("%d threads require %ldK of memory\n", current->nr_threads, KBYTES(task_args->bootstrap_len));

	if (core_is_compat(core))
		vdso_maps_rt = vdso_maps_compat;
	else
		vdso_maps_rt = vdso_maps;
	/*
	 * Figure out how much memory runtime vdso and vvar will need.
	 * Check if vDSO or VVAR is not provided by kernel.
	 */
	if (vdso_maps_rt.sym.vdso_size != VDSO_BAD_SIZE) {
		vdso_rt_size = vdso_maps_rt.sym.vdso_size;
		if (vdso_maps_rt.sym.vvar_size != VVAR_BAD_SIZE)
			vdso_rt_size += vdso_maps_rt.sym.vvar_size;
	}
	task_args->bootstrap_len += vdso_rt_size;

	/*
	 * Restorer is a blob (code + args) that will get mapped in some
	 * place, that should _not_ intersect with both -- current mappings
	 * and mappings of the task we're restoring here. The subsequent
	 * call finds the start address for the restorer.
	 *
	 * After the start address is found we populate it with the restorer
	 * parts one by one (some are remap-ed, some are mmap-ed and copied
	 * or inited from scratch).
	 */

	mem = (void *)restorer_get_vma_hint(&vmas->h, &self_vmas.h,
					    shstk_min_mmap_addr(&task_args->shstk, kdat.mmap_min_addr),
					    task_args->bootstrap_len);
	if (mem == (void *)-1) {
		pr_err("No suitable area for task_restore bootstrap (%ldK)\n", task_args->bootstrap_len);
		goto err;
	}

	pr_info("Found bootstrap VMA hint at: %p (needs ~%ldK)\n", mem, KBYTES(task_args->bootstrap_len));

	ret = remap_restorer_blob(mem);
	if (ret < 0)
		goto err;

	/*
	 * Prepare a memory map for restorer. Note a thread space
	 * might be completely unused so it's here just for convenience.
	 */
	task_args->clone_restore_fn = restorer_sym(mem, arch_export_restore_thread);
	restore_task_exec_start = restorer_sym(mem, arch_export_restore_task);
	rsti(current)->munmap_restorer = restorer_munmap_addr(core, mem);
	rsti(current)->bootstrap_start = mem;
	rsti(current)->bootstrap_unmap_len = task_args->bootstrap_len - vdso_rt_size;

	task_args->bootstrap_start = mem;
	mem += restorer_len;

	/* VMA we need for stacks and sigframes for threads */
	if (mmap(mem, memzone_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, 0, 0) != mem) {
		pr_perror("Can't mmap section for restore code");
		goto err;
	}

	memzero(mem, memzone_size);
	mz = mem;
	mem += memzone_size;

	/* New home for task_restore_args and thread_restore_args */
	task_args = mremap(task_args, alen, alen, MREMAP_MAYMOVE | MREMAP_FIXED, mem);
	if (task_args != mem) {
		pr_perror("Can't move task args");
		goto err;
	}

	task_args->rst_mem = mem;
	task_args->rst_mem_size = rst_mem_size + alen;
	thread_args = (struct thread_restore_args *)(task_args + 1);

	/*
	 * And finally -- the rest arguments referenced by task_ and
	 * thread_restore_args. Pointers will get remapped below.
	 */
	mem += alen;
	if (rst_mem_remap(mem))
		goto err;

	/*
	 * At this point we've found a gap in VM that fits in both -- current
	 * and target tasks' mappings -- and its structure is
	 *
	 * | restorer code | memzone (stacks and sigframes) | arguments |
	 *
	 * Arguments is task_restore_args, thread_restore_args-s and all
	 * the bunch of objects allocated with rst_mem_alloc().
	 * Note, that the task_args itself is inside the 3rd section and (!)
	 * it gets unmapped at the very end of __export_restore_task
	 */

	task_args->proc_fd = dup(get_service_fd(PROC_FD_OFF));
	if (task_args->proc_fd < 0) {
		pr_perror("can't dup proc fd");
		goto err;
	}

	task_args->breakpoint = &rsti(current)->breakpoint;
	task_args->fault_strategy = fi_strategy;

	sigemptyset(&blockmask);
	sigaddset(&blockmask, SIGCHLD);

	if (sigprocmask(SIG_BLOCK, &blockmask, NULL) == -1) {
		pr_perror("Can not set mask of blocked signals");
		return -1;
	}

	task_args->task_entries = rst_mem_remap_ptr(task_entries_pos, RM_SHREMAP);

	task_args->premmapped_addr = (unsigned long)rsti(current)->premmapped_addr;
	task_args->premmapped_len = rsti(current)->premmapped_len;

	task_args->task_size = kdat.task_size;
#ifdef ARCH_HAS_LONG_PAGES
	task_args->page_size = PAGE_SIZE;
#endif

	RST_MEM_FIXUP_PPTR(task_args->vmas);
	RST_MEM_FIXUP_PPTR(task_args->rings);
	RST_MEM_FIXUP_PPTR(task_args->tcp_socks);
	RST_MEM_FIXUP_PPTR(task_args->timerfd);
	RST_MEM_FIXUP_PPTR(task_args->posix_timers);
	RST_MEM_FIXUP_PPTR(task_args->siginfo);
	RST_MEM_FIXUP_PPTR(task_args->rlims);
	RST_MEM_FIXUP_PPTR(task_args->helpers);
	RST_MEM_FIXUP_PPTR(task_args->zombies);
	RST_MEM_FIXUP_PPTR(task_args->vma_ios);
	RST_MEM_FIXUP_PPTR(task_args->inotify_fds);

	task_args->compatible_mode = core_is_compat(core);
	/*
	 * Arguments for task restoration.
	 */

	BUG_ON(core->mtype != CORE_ENTRY__MARCH);

	task_args->logfd = log_get_fd();
	task_args->loglevel = log_get_loglevel();
	log_get_logstart(&task_args->logstart);
	task_args->sigchld_act = sigchld_act;

	strncpy(task_args->comm, core->tc->comm, TASK_COMM_LEN - 1);
	task_args->comm[TASK_COMM_LEN - 1] = 0;

	prep_libc_rseq_info(&task_args->libc_rseq);

	task_args->uid = opts.uid;
	for (i = 0; i < CR_CAP_SIZE; i++)
		task_args->cap_eff[i] = opts.cap_eff[i];

	/*
	 * Fill up per-thread data.
	 */
	creds_pos_next = creds_pos;
	siginfo_n = task_args->siginfo_n;
	arch_rsti_init(current);
	for (i = 0; i < current->nr_threads; i++) {
		CoreEntry *tcore;
		struct rt_sigframe *sigframe;
#ifdef CONFIG_MIPS
		k_rtsigset_t mips_blkset;
#else
		k_rtsigset_t *blkset = NULL;

#endif
		thread_args[i].pid = current->threads[i].ns[0].virt;
		thread_args[i].siginfo_n = siginfo_priv_nr[i];
		thread_args[i].siginfo = task_args->siginfo;
		thread_args[i].siginfo += siginfo_n;
		siginfo_n += thread_args[i].siginfo_n;

		/* skip self */
		if (thread_args[i].pid == pid) {
			task_args->t = thread_args + i;
			tcore = core;
#ifdef CONFIG_MIPS
			mips_blkset.sig[0] = tcore->tc->blk_sigset;
			mips_blkset.sig[1] = tcore->tc->blk_sigset_extended;
#else
			blkset = (void *)&tcore->tc->blk_sigset;
#endif
		} else {
			tcore = current->core[i];
			if (tcore->thread_core->has_blk_sigset) {
#ifdef CONFIG_MIPS
				mips_blkset.sig[0] = tcore->thread_core->blk_sigset;
				mips_blkset.sig[1] = tcore->thread_core->blk_sigset_extended;
#else
				blkset = (void *)&tcore->thread_core->blk_sigset;
#endif
			}
		}

		if ((tcore->tc || tcore->ids) && thread_args[i].pid != pid) {
			pr_err("Thread has optional fields present %d\n", thread_args[i].pid);
			ret = -1;
		}

		if (ret < 0) {
			pr_err("Can't read core data for thread %d\n", thread_args[i].pid);
			goto err;
		}

		thread_args[i].ta = task_args;
		thread_args[i].gpregs = *CORE_THREAD_ARCH_INFO(tcore)->gpregs;
		thread_args[i].clear_tid_addr = CORE_THREAD_ARCH_INFO(tcore)->clear_tid_addr;
		core_get_tls(tcore, &thread_args[i].tls);

		if (tcore->thread_core->has_cg_set && rsti(current)->cg_set != tcore->thread_core->cg_set) {
			thread_args[i].cg_set = tcore->thread_core->cg_set;
			thread_args[i].cgroupd_sk = dup(get_service_fd(CGROUPD_SK));
		} else {
			thread_args[i].cg_set = -1;
		}

		ret = prep_rseq(&thread_args[i].rseq, tcore->thread_core);
		if (ret)
			goto err;

		rst_reloc_creds(&thread_args[i], &creds_pos_next);

		thread_args[i].futex_rla = tcore->thread_core->futex_rla;
		thread_args[i].futex_rla_len = tcore->thread_core->futex_rla_len;
		thread_args[i].pdeath_sig = tcore->thread_core->pdeath_sig;
		if (tcore->thread_core->pdeath_sig > _KNSIG) {
			pr_err("Pdeath signal is too big\n");
			goto err;
		}

		ret = prep_sched_info(&thread_args[i].sp, tcore->thread_core);
		if (ret)
			goto err;

		seccomp_rst_reloc(&thread_args[i]);
		thread_args[i].seccomp_force_tsync = rsti(current)->has_old_seccomp_filter;

		thread_args[i].mz = mz + i;
		sigframe = (struct rt_sigframe *)&mz[i].rt_sigframe;

#ifdef CONFIG_MIPS
		if (construct_sigframe(sigframe, sigframe, &mips_blkset, tcore))
#else
		if (construct_sigframe(sigframe, sigframe, blkset, tcore))
#endif
			goto err;

		if (tcore->thread_core->comm)
			strncpy(thread_args[i].comm, tcore->thread_core->comm, TASK_COMM_LEN - 1);
		else
			strncpy(thread_args[i].comm, core->tc->comm, TASK_COMM_LEN - 1);
		thread_args[i].comm[TASK_COMM_LEN - 1] = 0;

		if (thread_args[i].pid != pid)
			core_entry__free_unpacked(tcore, NULL);

		pr_info("Thread %4d stack %8p rt_sigframe %8p\n", i, mz[i].stack, mz[i].rt_sigframe);
	}

	/*
	 * Restorer needs own copy of vdso parameters. Runtime
	 * vdso must be kept non intersecting with anything else,
	 * since we need it being accessible even when own
	 * self-vmas are unmaped.
	 */
	mem += rst_mem_size;

	shstk_set_restorer_stack(&task_args->shstk, mem);
	mem += shstk_restorer_stack_size();

	task_args->vdso_rt_parked_at = (unsigned long)mem;
	task_args->vdso_maps_rt = vdso_maps_rt;
	task_args->vdso_rt_size = vdso_rt_size;
	task_args->can_map_vdso = kdat.can_map_vdso;
	task_args->has_clone3_set_tid = kdat.has_clone3_set_tid;

	new_sp = restorer_stack(task_args->t->mz);

	/* No longer need it */
	core_entry__free_unpacked(core, NULL);
	xfree(current->core);

	/*
	 * Now prepare run-time data for threads restore.
	 */
	task_args->nr_threads = current->nr_threads;
	task_args->thread_args = thread_args;

	task_args->auto_dedup = opts.auto_dedup;

	/*
	 * In the restorer we need to know if it is SELinux or not. For SELinux
	 * we must change the process context before creating threads. For
	 * Apparmor we can change each thread after they have been created.
	 */
	task_args->lsm_type = kdat.lsm;

	/*
	 * Make root and cwd restore _that_ late not to break any
	 * attempts to open files by paths above (e.g. /proc).
	 */

	if (restore_fs(current))
		goto err;

	sfds_protected = false;
	close_image_dir();
	close_proc();
	close_service_fd(TRANSPORT_FD_OFF);
	close_service_fd(CR_PROC_FD_OFF);
	close_service_fd(ROOT_FD_OFF);
	close_service_fd(USERNSD_SK);
	close_service_fd(FDSTORE_SK_OFF);
	close_service_fd(RPC_SK_OFF);
	close_service_fd(CGROUPD_SK);

	__gcov_flush();

	pr_info("task_args: %p\n"
		"task_args->pid: %d\n"
		"task_args->nr_threads: %d\n"
		"task_args->clone_restore_fn: %p\n"
		"task_args->thread_args: %p\n",
		task_args, task_args->t->pid, task_args->nr_threads, task_args->clone_restore_fn,
		task_args->thread_args);

	/*
	 * An indirect call to task_restore, note it never returns
	 * and restoring core is extremely destructive.
	 */

	JUMP_TO_RESTORER_BLOB(new_sp, restore_task_exec_start, task_args);

err:
	free_mappings(&self_vmas);
err_nv:
	/* Just to be sure */
	exit(1);
	return -1;
}
