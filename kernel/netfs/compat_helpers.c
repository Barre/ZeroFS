/*
 * Target-kernel bridge for netfslib helpers and layout-sensitive operations.
 * The field aliases are inert on legacy headers and expose a stable spelling
 * when newer headers make the size fields private.
 */
#define _remote_i_size remote_i_size
#define _zero_point zero_point

#include <linux/netfs.h>
#include <linux/pagemap.h>

#include "compat.h"

void zerofs_netfs_initialize_inode(struct inode *inode,
				   const struct netfs_request_ops *ops)
{
	netfs_inode_init(netfs_inode(inode), ops, true);
}

struct netfs_group *
zerofs_netfs_retain_writeback_group(struct netfs_io_request *request)
{
	struct netfs_group *group;
	struct folio *folio;
	loff_t position;

	if (!request || !request->mapping)
		return NULL;

	/*
	 * ZeroFS has no local fscache, so begin_writeback() is called for the
	 * request's first dirty folio while that folio is still locked.
	 */
	position = request->start;
	if (position < 0)
		return NULL;
	folio = filemap_get_folio(request->mapping, position >> PAGE_SHIFT);
	if (IS_ERR(folio))
		return NULL;

	group = netfs_folio_group(folio);
	if (!group || group == NETFS_FOLIO_COPY_TO_CACHE)
		group = NULL;
	else
		refcount_inc(&group->ref);
	folio_put(folio);
	return group;
}

loff_t zerofs_netfs_read_remote_size(const struct inode *inode)
{
	const struct netfs_inode *ctx =
		container_of(inode, struct netfs_inode, inode);

	/*
	 * This is netfs_read_remote_i_size() on the supported 64-bit targets.
	 * Legacy kernels have no callable helper, but their aligned 64-bit field
	 * accesses are compatible with this stronger acquire read.
	 */
	return smp_load_acquire(&ctx->remote_i_size);
}

void zerofs_netfs_write_remote_size(struct inode *inode, loff_t size)
{
	struct netfs_inode *ctx = netfs_inode(inode);

	spin_lock(&inode->i_lock);
	smp_store_release(&ctx->remote_i_size, size);
	smp_store_release(&ctx->zero_point, size);
	spin_unlock(&inode->i_lock);
}

void zerofs_netfs_extend_remote_size(struct inode *inode, loff_t end)
{
	struct netfs_inode *ctx = netfs_inode(inode);

	spin_lock(&inode->i_lock);
	if (end > smp_load_acquire(&ctx->remote_i_size))
		smp_store_release(&ctx->remote_i_size, end);
	if (end > smp_load_acquire(&ctx->zero_point))
		smp_store_release(&ctx->zero_point, end);
	spin_unlock(&inode->i_lock);
}

void zerofs_netfs_write_local_and_remote_size(struct inode *inode, loff_t size)
{
	struct netfs_inode *ctx = netfs_inode(inode);

	spin_lock(&inode->i_lock);
	i_size_write(inode, size);
	smp_store_release(&ctx->remote_i_size, size);
	smp_store_release(&ctx->zero_point, size);
	spin_unlock(&inode->i_lock);
}
