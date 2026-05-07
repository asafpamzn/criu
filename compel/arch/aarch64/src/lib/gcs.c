#include <stdbool.h>
#include <compel/asm/infect-types.h>

/*
 * GCS (Guarded Control Stack) stubs for aarch64.
 * GCS hardware is not yet widely available; return
 * "not supported" so CRIU skips GCS save/restore.
 */
bool __compel_host_supports_gcs(void)
{
	return false;
}

struct parasite_ctl;
int __parasite_setup_shstk(struct parasite_ctl *ctl,
			   user_fpregs_struct_t *ext_regs)
{
	return 0;
}
