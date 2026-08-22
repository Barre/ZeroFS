/*
 * Target-kernel bridge for client helpers not exported through Rust bindings.
 */

#include <linux/sched/mm.h>

unsigned int zerofs_client_memalloc_nofs_save(void);
void zerofs_client_memalloc_nofs_restore(unsigned int flags);

unsigned int zerofs_client_memalloc_nofs_save(void)
{
	return memalloc_nofs_save();
}

void zerofs_client_memalloc_nofs_restore(unsigned int flags)
{
	memalloc_nofs_restore(flags);
}
