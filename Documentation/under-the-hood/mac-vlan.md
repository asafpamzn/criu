# Mac-VLAN

Mac-VLAN is a Linux network driver that allows creating multiple virtual interfaces with unique MAC addresses on top of a single physical interface. These virtual interfaces act as standalone devices on the network, each with its own IP and MAC address.

## Checkpoint and Restore of Mac-VLAN

CRIU identifies Mac-VLAN interfaces by monitoring netlink messages (specifically `RTM_NEWLINK`) and inspecting their attributes.

### 1. Checkpointing
During a dump, CRIU extracts the following attributes for each Mac-VLAN device:
- **Parent Interface**: The physical device (or "upper" link) that the Mac-VLAN is built upon (identified via `IFLA_LINK`).
- **Mode**: The specific Mac-VLAN operational mode (e.g., `bridge`, `private`, `vepa`, `passthru`), extracted from `IFLA_MACVLAN_MODE`.
- **Flags**: Any additional configuration flags associated with the interface (`IFLA_MACVLAN_FLAGS`).
- **MAC Address**: The unique hardware address of the virtual interface.

### 2. Restoration
To recreate a Mac-VLAN interface exactly as it was, CRIU performs the following:
- **Link Creation**: It sends an `RTM_NEWLINK` netlink message with the kind set to `"macvlan"`, specifying the original mode and the link to the parent device.
- **Index Preservation**: To ensure that any application sockets bound to the interface index remain valid, CRIU uses the `IFLA_NEW_IFINDEX` attribute. This allows CRIU to request the exact same interface index that the device possessed before the checkpoint. (This kernel feature was originally developed specifically to support CRIU).
- **Namespace Migration**: Once created, the interface is moved into the target network namespace for the restored process.

## External Interface Mapping

Since the parent physical interface may have a different name or index on the destination host during migration, CRIU provides the `--external` option to map these dependencies:

```bash
# Example mapping of an internal macvlan interface to a host physical interface
criu restore --external macvlan[eth0]:phys0 ...
```

This tells CRIU that the Mac-VLAN interface which was originally attached to `eth0` should now be attached to the physical interface `phys0` on the current host.

## See also
* [Checkpoint/Restore Architecture](checkpointrestore.md)
* [Networking](networking.md)
