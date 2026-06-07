# BPF Maps

BPF maps are kernel objects that store data used by BPF programs, typically in the form of key-value pairs. Applications access these maps via file descriptors. Checkpointing and restoring BPF maps involves serializing both their **metadata** and their **data contents**.

## How CRIU Handles BPF Maps

### Metadata Serialization
CRIU collects essential map attributes from several sources:
- **/proc filesystem**: Essential fields such as `map_type`, `key_size`, `value_size`, `max_entries`, and the `frozen` status are parsed from the task's `fdinfo`.
- **BPF System Call**: CRIU uses the `bpf` system call with the `BPF_OBJ_GET_INFO_BY_FD` command to retrieve additional information, including the map name and interface index (`ifindex`).

### Data Serialization
To preserve the map's contents, CRIU relies on batch operations:
- **Dumping**: During the checkpoint stage, CRIU uses `BPF_MAP_LOOKUP_BATCH` to efficiently read all key-value pairs from the map.
- **Restoring**: During the restore phase, CRIU recreates the map and uses `BPF_MAP_UPDATE_BATCH` to repopulate it with the saved key-value pairs.

### Supported Map Types
CRIU currently supports data serialization for the following BPF map types:
- `BPF_MAP_TYPE_HASH`
- `BPF_MAP_TYPE_ARRAY`

For other map types, CRIU may be able to restore the map itself (metadata) but not its contents, depending on kernel support for batch operations on those types.

### Frozen Maps
If a BPF map was marked as read-only (frozen) using `bpf_map_freeze()`, CRIU detects this state from `fdinfo` and reapplies the freeze during restoration after the data has been repopulated.

## To-Do

- **BTF Support**: Serialization and restoration of BPF Type Format (BTF) information associated with maps.
- **Extended Map Types**: Implementation of data serialization for more BPF map types (e.g., `BPF_MAP_TYPE_PERF_EVENT_ARRAY`, `BPF_MAP_TYPE_LPM_TRIE`).
- **Map Extra Data**: Full support for `map_extra` fields introduced in recent kernels (currently only partially parsed with limited restoration).

## External Links
- [BPF Documentation](https://www.kernel.org/doc/html/latest/bpf/index.html)
- [Notes on BPF](https://blogs.oracle.com/linux/notes-on-bpf-1)
- [An eBPF Overview](https://www.collabora.com/news-and-blog/blog/2019/04/05/an-ebpf-overview-part-1-introduction/)
