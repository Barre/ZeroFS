#ifndef ZEROFS_VFS_COMPAT_H
#define ZEROFS_VFS_COMPAT_H

#include <linux/mm_types.h>
#include <linux/slab.h>
#include <linux/types.h>
#include <linux/uidgid_types.h>

struct file;
struct inode;
struct iov_iter;
struct user_namespace;
struct vm_fault;

void zerofs_vfs_file_accessed(struct file *file);
slab_flags_t zerofs_vfs_inode_slab_flags(void);
kuid_t zerofs_vfs_make_kuid(struct user_namespace *namespace, uid_t uid);
kgid_t zerofs_vfs_make_kgid(struct user_namespace *namespace, gid_t gid);
uid_t zerofs_vfs_from_kuid(struct user_namespace *namespace, kuid_t uid);
gid_t zerofs_vfs_from_kgid(struct user_namespace *namespace, kgid_t gid);
void zerofs_vfs_zero_exposed_eof_tail(struct inode *inode, loff_t from,
				      loff_t to);
vm_fault_t zerofs_vfs_filemap_fault_after_revalidation(struct vm_fault *vmf);
struct file *zerofs_vfs_pin_fault_file_and_unlock(struct vm_fault *vmf);
size_t zerofs_vfs_iov_iter_count(const struct iov_iter *iter);
void zerofs_vfs_iov_iter_truncate(struct iov_iter *iter, size_t count);
void zerofs_vfs_release_pinned_iov_iter(struct iov_iter *iter,
					size_t dirty_bytes);

#endif /* ZEROFS_VFS_COMPAT_H */
