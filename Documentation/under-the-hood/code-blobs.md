# Code Blobs and PIE Generation

CRIU and its sub-project **Compel** use specialized binary blobs to execute code in environments where standard libraries and runtime environments are unavailable. These blobs are Position-Independent Executables (PIE) that are converted into C headers for easy integration into the main CRIU binary.

## Why Code Blobs are Necessary

CRIU operates in two primary scenarios that require these specialized environments:

1.  **Parasite Code Execution**: During a checkpoint, CRIU injects code into the target process's address space to extract internal state (like memory contents and credentials). This code must be self-contained and PIE-compiled to run at any address.
2.  **Restorer Context**: During restoration, the process must unmap its current memory (including CRIU's own code) and map the original memory of the checkpointed application. The code performing these operations must exist in a memory region that does not conflict with the target application's layout.

## Building PIE Code Blobs

The generation of these blobs is handled by the **Compel** utility. The process involves compiling C and assembly source files into a single ELF object and then using the `compel hgen` tool to transform that object into a C header.

### The `compel hgen` Tool

The `hgen` (header generator) tool performs the following tasks:
1.  **Relocation Extraction**: It identifies all symbols that require relocation and creates a structured `compel_reloc` array.
2.  **Binary Data Conversion**: It converts the allocated ELF sections (code and data) into a static C byte array.
3.  **Bootstrap Initialization**: It generates a setup function (e.g., `parasite_setup_c_header`) that populates a `parasite_blob_desc` structure, which CRIU uses to manage the blob's lifecycle.

### Example Header Format

The generated header file typically contains:

```c
/* Relocation information */
static const struct compel_reloc parasite_relocs[] = {
    { .offset = 0x0000002c, .type = COMPEL_TYPE_INT, .addend = 0, .value = 0x12345678 },
    ...
};

/* The binary blob itself */
static const char parasite_blob[] = {
    0x48, 0x8d, 0x25, 0x2d, 0x60, 0x00, 0x00, 0x48,
    ...
};

/* Setup function for CRIU integration */
static void parasite_setup_c_header_desc(struct parasite_blob_desc *pbd, bool native)
{
    pbd->parasite_type = COMPEL_BLOB_CHEADER;
    pbd->hdr.mem       = parasite_blob;
    pbd->hdr.bsize     = sizeof(parasite_blob);
    ...
}
```

## Build Procedure

The build system follows these steps to generate the headers:

1.  **Compilation**: Source files (like `parasite.c` or `restorer.c`) are compiled with PIE flags (`-fpie`, `-ffreestanding`, `-nostdlib`).
2.  **Linking**: Object files are linked into a single `.built-in.o` file using a specialized linker script (`compel-pack.lds.S`) that organizes sections into a layout suitable for a standalone blob.
3.  **Header Generation**: The `compel hgen` command is executed on the linked object to produce the final `-blob.h` header.

```bash
# Example Makefile recipe
$(obj)/parasite-blob.h: $(obj)/parasite.built-in.o
    compel hgen -f $< -o $@
```

## See also

* [Parasite Code](parasite-code.md)
* [Restorer Context](restorer-context.md)
* [Compel Sub-project](../compel.md)
