# AppArmor Support

CRIU provides support for checkpointing and restoring **AppArmor** security profiles and namespaces. This is a critical feature for containerized environments (like Docker, LXC, or Podman) where each container frequently operates under its own set of specialized security policies.

## How CRIU Handles AppArmor

AppArmor integration in CRIU ensures that restored processes continue to operate under the same security constraints as the original processes, while also managing the temporary permissions needed for the checkpointing process itself.

### 1. Checkpointing (Dumping)
During the dump phase, CRIU detects the AppArmor state of each task:
*   **Profile Identification**: CRIU captures the active profile name for every thread (e.g., `unconfined`, `docker-default`, or a custom user-defined profile).
*   **Namespace and Policy Dumping**: In modern containerized setups, containers often have their own AppArmor namespaces. CRIU walks the `/sys/kernel/security/apparmor/policy/` directory to capture the full hierarchy of namespaces and the raw binary blobs of all loaded policies.
*   **Parasite Profile**: To allow the [Parasite Code](parasite-code.md) to perform its necessary inspections (like opening network sockets or reading memory) without being blocked by the application's strict security policy, CRIU temporarily transitions the task into a special, permissive "parasite profile" while it is infected.

### 2. Restoration
Restoring AppArmor state involves re-establishing the security context before the process resumes:
*   **Policy Loading**: CRIU uses the `apparmor_parser` utility on the destination host to re-load the policy blobs captured in the image files.
*   **Namespace Reconstruction**: It recreates any nested AppArmor namespaces to match the original environment.
*   **Profile Re-attachment**: As each process is restored, CRIU ensures it is transitioned back into its original profile (or stack of profiles) using the `aa_change_profile()` interface before the application code begins executing.

## Support for Stacking

Modern AppArmor implementations support **Profile Stacking**, where multiple security profiles are applied to a single process simultaneously (e.g., a container-wide profile plus a per-application profile). CRIU correctly identifies, dumps, and restores these complex stacked configurations.

## Kernel Requirements

Reliable AppArmor C/R requires:
*   A kernel with `CONFIG_SECURITY_APPARMOR` enabled and active.
*   The `securityfs` filesystem mounted (typically at `/sys/kernel/security`).
*   Support for AppArmor policy introspection and namespaces, which is standard in modern distributions like Ubuntu and Debian.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Parasite Code](parasite-code.md)
* [Kerndat Feature Detection](kerndat.md)
