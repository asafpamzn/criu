# 32-bit tasks C/R

## Compatible applications

On x86_64, there are two types of compatibility mode applications:
- ia32: Compiled to run on an i686 target, these can be executed on x86_64 if the `IA32_EMULATION` configuration option is enabled.
- x32: Specially compiled binaries designed to run on x86_64 with the `CONFIG_X86_X32` configuration option enabled.

Both use 4-byte pointers and thus can address no more than 4 GB of virtual memory.
However, x32 uses the full 64-bit register set and therefore cannot be launched natively on an i686 host.
Both require an additional environment on x86_64, such as Glibc, libraries, and compiler support.
x32 is rarely distributed; currently, only the [Debian x32 port](https://wiki.debian.org/X32Port) is easily found.
Currently, CRIU supports ia32 C/R. Support for x32 can be added relatively easily, as the necessary kernel patches for ia32 C/R are already in place.
In this document, the terms *compatible* and *32-bit* refer to ia32 applications unless otherwise specified.

## Difference between native and compatibility mode applications

From the CPU's point of view, 32-bit compatibility mode applications differ from 64-bit applications by the current Code Segment (CS) selector. If the L-bit (Long mode) in the segment descriptor is set, the CPU operates in 64-bit mode when that descriptor is used. There are other differences between 32-bit and 64-bit selectors; for more details, see [the article "The 0x33 Segment Selector (Heavens Gate)"](https://www.malwaretech.com/2014/02/the-0x33-segment-selector-heavens-gate.html). Code selectors for both modes are defined in kernel headers as `__USER32_CS` and `__USER_CS`, corresponding to descriptors in the Global Descriptor Table (GDT). The mode can be switched from 64-bit to compatibility mode by changing the CS value (e.g., using a long jump).

From the Linux kernel's point of view, applications differ based on values set during `exec`, such as `mmap_base` or thread info flags like `TIF_ADDR32`, `TIF_IA32`, or `TIF_X32`.
Both native and compatibility mode applications can perform either 32-bit or 64-bit syscalls.

## Mixed-bitness applications

The current kernel ABI allows for the creation of mixed-bitness applications, which can become quite complex.
For instance, an application could set both 32-bit and 64-bit robust futex list pointers.
Alternatively, a multi-threaded application could have some threads executing 32-bit code while others execute 64-bit code.

If support for such mixed-bitness applications is ever needed, it could be added to CRIU relatively easily. However, this should likely be a compile-time configuration option to avoid adding unnecessary syscalls to standard C/R operations.

Currently, there are no plans to add this support, as such applications are unlikely to be encountered outside of synthetic tests.

## Approaches to C/R for compatibility mode applications

32-bit C/R can be implemented in several ways. This section describes the pros and cons of various approaches and explains why the current implementation was chosen.

### Restore via exec() of a 32-bit dummy binary vs. from 64-bit CRIU

Restoring a 32-bit application could be done using a 32-bit daemon that communicates with the 64-bit CRIU binary or a 32-bit CRIU subprocess.

**Pros**:
- No kernel patches expected (though `vDSO mremap()` would still require support).

**Cons**:
- The CRIU codebase lacks a dedicated restore daemon, requiring significant rework.
- A 64-bit application can have a 32-bit child, which in turn could parent a 64-bit process. This would require re-executing the native 64-bit CRIU from the 32-bit dummy or subprocess.
- It would be necessary to send process properties, open image file descriptors, and shared memory containing the parsed `ps_tree` to the daemon. The volume of IPC calls would slow down the restoration process.
- Restoration becomes more complex, especially when considering user and PID namespaces.
- Task properties that are erased during `exec()` cannot benefit from optimized inheritance.
- A separate daemon would also be needed for x32.

### Restore with a flag to sigreturn() or arch_prctl()

The initial attempt to implement 32-bit C/R was rejected by the LKML community for several reasons. It involved swapping thread info flags (e.g., `TIF_ADDR32`, `TIF_IA32`, `TIF_X32`), unmapping the native 64-bit vDSO, and mapping the 32-bit vDSO based on a bit in the `rt_sigreturn()` sigframe or a dedicated `arch_prctl()` call.

**Pros**:
- Simple for CRIU: just perform a `sigreturn` with the new bit set or call `arch_prctl` before `sigreturn`.

**Cons**:
- If the 32-bit vDSO on the restoration host differs from the dumped image, the task must be intercepted after `sigreturn` to create jump trampolines (this is simpler with `arch_prctl`).
- Too many potential failure points for a single syscall; overly complex.
- Allowing userspace to swap thread info flags could introduce new race conditions and bugs (e.g., since the `TASK_SIZE` macro depends on `TIF_ADDR32`, memory mapping behavior might become unpredictable).

Following LKML discussions, it was decided to separate personality changes from the vDSO mapping API, remove the `TIF_IA32` flag that distinguished 32-bit from 64-bit tasks, and instead rely on the nature of the syscall (compat, x32, or native).

### Seizing with separate 32-bit and 64-bit parasites

**Pros**:
- No 32-bit calls in the 64-bit parasite and vice-versa.
- Since `ptrace` does not allow setting a 32-bit register set on a 64-bit task (and vice versa), using a parasite of the same nature as the task avoids these limitations.

**Cons**:
- Requires maintaining two or three (for x32) separate parasite blobs.
- Requires complex Makefile macros to build multiple parasites.
- Serializing parasite responses is difficult because argument sizes differ between modes, leading to complex and less readable C macros.

### Current approach

CRIU (a 64-bit process) handles 32-bit (ia32) tasks through a series of architecture-specific transitions:

1.  **Architecture Detection**: CRIU uses `ptrace(PTRACE_GETREGSET, pid, NT_PRSTATUS, &iov)` to detect the task's architecture. The kernel returns different register set sizes depending on the mode: `sizeof(user_regs_struct64)` for native 64-bit tasks and `sizeof(user_regs_struct32)` for 32-bit compatibility mode tasks.
2.  **Dumping**: When dumping a 32-bit task, CRIU uses the 64-bit `ptrace` interface. The kernel handles the internal mapping of 32-bit registers into the structure expected by CRIU.
3.  **vDSO Handling**: To ensure the restored task uses a vDSO compatible with the current kernel, CRIU uses the `arch_prctl(ARCH_MAP_VDSO_32, addr)` system call (available since kernel v4.8) to map the 32-bit vDSO into the restored process's address space.
4.  **Restoration via Sigreturn**: The final restoration of 32-bit registers is performed using a 32-bit `rt_sigreturn` call:
    *   CRIU prepares a 32-bit signal frame (`rt_sigframe_ia32`) on the target task's stack.
    *   The CRIU restorer code, running in 64-bit mode, executes a far return (`lretq`) to switch the CPU to 32-bit mode with the `USER32_CS` (0x23) segment selector.
    *   Once in 32-bit mode, it executes `int $0x80` with the `__NR32_rt_sigreturn` syscall number. The kernel then restores all registers from the 32-bit sigframe and resumes the task in 32-bit mode.

## To-Do

### vsyscall page handling

The `vsyscall` page is an emulated, fixed-address page (`0xffffffffff600000`) used for legacy support. It is not a standard VMA and is marked as `VMA_AREA_VSYSCALL` by CRIU, which avoids dumping or restoring its contents. Since its presence in `/proc/<pid>/maps` depends on kernel configuration (`vsyscall=emulate` or `vsyscall=xonly`), it can introduce noise during ZDTM tests that compare memory layouts. Consequently, tests are often run with `vsyscall=none`.

### Error reporting on x32 binary dumping

Currently, CRIU does not support x32 binaries (64-bit registers with 32-bit pointers). While the infrastructure for 32-bit pointers exists, the specific register handling and vDSO mapping for x32 are not implemented. Attempting to dump an x32 binary should result in an explicit error.

### Removal of TIF_IA32 from the kernel

The `TIF_IA32` thread info flag was historically used to distinguish 32-bit tasks. Kernel efforts (merged in v5.11) have moved towards relying on the nature of the syscall (compat vs. native) rather than a persistent thread flag. This unification simplifies how the kernel and CRIU interact, particularly for tracing tools like uprobes.

## External links
- [GitHub issue](https://github.com/checkpoint-restore/criu/issues/43)

