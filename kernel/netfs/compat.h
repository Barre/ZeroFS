/* SPDX-License-Identifier: AGPL-3.0-only */
#ifndef ZEROFS_NETFS_COMPAT_H
#define ZEROFS_NETFS_COMPAT_H

#include <linux/types.h>

struct inode;
struct netfs_group;
struct netfs_io_request;
struct netfs_request_ops;

void zerofs_netfs_initialize_inode(struct inode *inode,
				   const struct netfs_request_ops *ops);
struct netfs_group *
zerofs_netfs_retain_writeback_group(struct netfs_io_request *request);
loff_t zerofs_netfs_read_remote_size(const struct inode *inode);
void zerofs_netfs_write_remote_size(struct inode *inode, loff_t size);
void zerofs_netfs_extend_remote_size(struct inode *inode, loff_t end);
void zerofs_netfs_write_local_and_remote_size(struct inode *inode, loff_t size);

#endif /* ZEROFS_NETFS_COMPAT_H */
