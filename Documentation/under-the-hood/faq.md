# Frequently Asked Questions (FAQ)

This page provides answers to common questions and technical insights into CRIU's behavior and limitations.

## General Questions

### Q: Why does CRIU dump parts of read-only code mappings?
**A**: Even if a mapping (like the code section of `/usr/bin/something`) is marked as read-only, it may still contain "dirty" pages that CRIU must dump. This typically happens due to **Copy-on-Write (COW)** events during dynamic linking, relocation patching, or if the process modified its own code via `mprotect` and `ptrace`.

### Q: How can I verify that my system is ready for CRIU?
**A**: Use the built-in check tool:
```bash
criu check --extra
```
This will verify that your kernel has all the necessary features (like `kcmp`, `ns_last_pid`, etc.) enabled. Additionally, running the [ZDTM Test Suite](zdtm-test-suite.md) is the best way to confirm functional correctness on your specific hardware and software stack.

### Q: Is it possible to change the IP address during live migration?
**A**: Yes, but with caveats. Since TCP connections are identified by their IP/Port 4-tuple, changing the IP will normally break established connections.
- If you can tolerate connection resets, use the `--tcp-close` flag.
- For listening sockets, you can use the `UPDATE_INETSK` plugin hook or the `CRIT` tool to remap addresses.
- For seamless migration, virtual IPs or network-level NAT are required. See [Changing IP Addresses](change-ip-address.md) for more details.

### Q: Why does restore fail with a "PID mismatch" error?
**A**: This occurs because the PID CRIU is trying to restore is already in use by another process on the system.
- **Solution**: The most common way to avoid this is to run the restored process inside a fresh **PID namespace**. This ensures that the PID range is entirely available to CRIU.
- **Internal Note**: CRIU uses the `ns_last_pid` kernel interface or the modern `clone3` system call to request specific PIDs during restoration.

### Q: Why does dump fail with "Cannot dump half of a stream unix connection"?
**A**: This usually happens when one end of a Unix domain socket is held by a process *outside* the process tree being checkpointed. CRIU cannot capture the state of the "external" peer, so it cannot safely restore the connection unless the socket is explicitly marked as external via the `--external unix[ino]` option.

---

## Testing (ZDTM)

### Q: Why do my ZDTM tests fail with "Permission Denied" even when run as root?
**A**: The `zdtm.py` test runner executes many sub-tests as a non-privileged user to verify CRIU's behavior in unprivileged environments. If your specific test requires root privileges, you must add `'flags': 'suid'` to the test's `.desc` file.

---

## Containers and Docker

### Q: Why can't I restore a Docker container onto a different image?
**A**: CRIU checkpoints the state of the processes, but it does **not** checkpoint the underlying filesystem. The process images contain paths to files that must exist exactly as they did during the dump.
- **Solution**: To restore a container, you must ensure the filesystem state is identical. In Docker, this often involves using `docker commit` to create an image of the container's filesystem at the moment of the checkpoint.
- **Modern Tools**: Container engines like Podman or newer versions of Docker/RunC handle this integration more seamlessly by managing the filesystem snapshots alongside the CRIU state.
