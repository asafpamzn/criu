# Changing IP Addresses During Migration

When performing a [live migration](live-migration.md) of a process between hosts, a common challenge is handling IP address changes. While the ideal solution often involves using containers with their own network namespaces and virtual IPs, migrating a service to a different physical IP address is sometimes necessary.

## The Core Problem

TCP connections are identified by a 4-tuple: (Source IP, Source Port, Destination IP, Destination Port). If either IP address changes during migration, the TCP stack on the peer will not recognize the migrated connection and will typically respond with a Reset (RST) or simply ignore the packets.

Consequently, there are three scenarios to consider when changing IPs:

### 1. Listening Sockets
If a server is bound to `0.0.0.0` (INADDR_ANY), it will "just work" after migration, as it will listen on all available interfaces on the new host. However, if the server is bound to a specific IP address that does not exist on the destination host, restoration will fail unless the binding is updated.

**Solutions:**
- **CRIT**: Use the [CRIT](../crit.md) tool to manually edit the `inetsk.img` or `files.img` images to update the binding address.
- **Plugins**: Use the `UPDATE_INETSK` plugin hook (see below) to programmatically change the IP address during restoration.

### 2. In-Flight Connections
These are connections that have been initiated but not yet accepted by the application. CRIU provides the `--skip-in-flight` option to ignore these connections during the dump.

### 3. Established Sockets
These are active connections. Changing the IP address of an established socket is technically possible but will usually break the connection unless specialized network-level translation (like NAT) is used.

**CRIU Solutions:**
- **--tcp-close**: This option tells CRIU to dump established connections but restore them in a closed state. This prevents application-level errors caused by "holes" in the file descriptor table while acknowledging that the specific network connection is terminated.
- **--tcp-established**: Used in combination with IP translation mechanisms (like NAT or proxies), this allows the connection to be restored.

## Programmatic IP Remapping (Plugins)

CRIU provides a plugin hook, `UPDATE_INETSK`, specifically for modifying socket attributes during restoration. A plugin can implement this hook to intercept the restoration of an INET socket and change its source or destination IP addresses.

```c
/* Plugin hook signature in criu-plugin.h */
int cr_plugin_update_inetsk(uint32_t family, uint32_t state, uint32_t *src_ip, uint32_t *dst_ip);
```

By modifying `src_ip` and `dst_ip` within the plugin, you can redirect sockets to new addresses as they are being recreated.

## Summary of Options

| Scenario | Recommendation | CRIU Flag / Tool |
| :--- | :--- | :--- |
| **Old IP not on new host** | Remap local binding | `CRIT` or `UPDATE_INETSK` plugin |
| **In-Flight Connection** | Ignore | `--skip-in-flight` |
| **Established Connection** | Terminate gracefully | `--tcp-close` |
| **Established Connection** | Maintain (requires NAT) | `--tcp-established` + remapping |
