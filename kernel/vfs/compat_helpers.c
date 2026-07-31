/*
 * Target-kernel bridge for VFS helpers and layout-sensitive operations.
 */

#include <linux/bvec.h>
#include <linux/fs.h>
#include <linux/highmem.h>
#include <linux/mm.h>
#include <linux/pagemap.h>
#include <linux/rmap.h>
#include <linux/slab.h>
#include <linux/uidgid.h>
#include <linux/uio.h>

#include "compat.h"

void zerofs_vfs_file_accessed(struct file *file)
{
	file_accessed(file);
}

slab_flags_t zerofs_vfs_inode_slab_flags(void)
{
	return SLAB_ACCOUNT | SLAB_RECLAIM_ACCOUNT;
}

kuid_t zerofs_vfs_make_kuid(struct user_namespace *namespace, uid_t uid)
{
	return make_kuid(namespace, uid);
}

kgid_t zerofs_vfs_make_kgid(struct user_namespace *namespace, gid_t gid)
{
	return make_kgid(namespace, gid);
}

uid_t zerofs_vfs_from_kuid(struct user_namespace *namespace, kuid_t uid)
{
	return from_kuid(namespace, uid);
}

gid_t zerofs_vfs_from_kgid(struct user_namespace *namespace, kgid_t gid)
{
	return from_kgid(namespace, gid);
}

void zerofs_vfs_zero_exposed_eof_tail(struct inode *inode, loff_t from,
				      loff_t to)
{
	struct folio *folio;
	loff_t folio_start;
	size_t offset, end;

	if (from >= to || !(from & (PAGE_SIZE - 1)))
		return;

	folio = filemap_lock_folio(inode->i_mapping, from >> PAGE_SHIFT);
	if (IS_ERR(folio))
		return;

	folio_start = folio_pos(folio);
	offset = from - folio_start;
	end = min_t(loff_t, to - folio_start, folio_size(folio));

	/*
	 * A shared mapping can dirty bytes past EOF without extending i_size.
	 * Once a later write exposes that tail, clear both the PTE dirtiness
	 * and the bytes before writeback can mistake them for file data.
	 */
	if (folio_mkclean(folio))
		folio_mark_dirty(folio);
	if (folio_test_dirty(folio))
		folio_zero_segment(folio, offset, end);

	folio_unlock(folio);
	folio_put(folio);
}

vm_fault_t zerofs_vfs_filemap_fault_after_revalidation(struct vm_fault *vmf)
{
	bool tried = vmf->flags & FAULT_FLAG_TRIED;
	vm_fault_t result;

	vmf->flags &= ~FAULT_FLAG_TRIED;
	result = filemap_fault(vmf);
	/*
	 * The vm_fault itself remains live after a lock-dropping RETRY. Restore
	 * only the core MM's attempt bit so changes made by filemap are retained.
	 */
	if (tried)
		vmf->flags |= FAULT_FLAG_TRIED;
	return result;
}

struct file *zerofs_vfs_pin_fault_file_and_unlock(struct vm_fault *vmf)
{
	struct file *file;

	if (!vmf || !vmf->vma || !vmf->vma->vm_file)
		return NULL;
	if (!(vmf->flags & FAULT_FLAG_ALLOW_RETRY) ||
	    (vmf->flags & FAULT_FLAG_RETRY_NOWAIT))
		return NULL;

	file = get_file(vmf->vma->vm_file);
	release_fault_lock(vmf);
	return file;
}

size_t zerofs_vfs_iov_iter_count(const struct iov_iter *iter)
{
	return iov_iter_count(iter);
}

void zerofs_vfs_iov_iter_truncate(struct iov_iter *iter, size_t count)
{
	iov_iter_truncate(iter, count);
}

void zerofs_vfs_release_pinned_iov_iter(struct iov_iter *iter,
					size_t dirty_bytes)
{
	const struct bio_vec *bvec;
	unsigned long index;

	if (WARN_ON_ONCE(!iter || !iov_iter_is_bvec(iter)))
		return;

	bvec = iter->bvec;
	for (index = 0; index < iter->nr_segs; index++) {
		struct page *page = bvec[index].bv_page;

		if (dirty_bytes) {
			flush_dcache_page(page);
			set_page_dirty_lock(page);
			if (dirty_bytes > bvec[index].bv_len)
				dirty_bytes -= bvec[index].bv_len;
			else
				dirty_bytes = 0;
		}
		unpin_user_page(page);
	}
	kvfree(bvec);
}
