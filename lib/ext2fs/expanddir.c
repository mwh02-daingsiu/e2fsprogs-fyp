/*
 * expand.c --- expand an ext2fs directory
 *
 * Copyright (C) 1993, 1994, 1995, 1996, 1997, 1998, 1999  Theodore Ts'o.
 *
 * %Begin-Header%
 * This file may be redistributed under the terms of the GNU Library
 * General Public License, version 2.
 * %End-Header%
 */

#include "config.h"
#include "ext2fs/ext2_bmpt.h"
#include "ext2fs/ext2_io.h"
#include <stdio.h>
#include <string.h>
#if HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "ext2_fs.h"
#include "ext2fs.h"
#include "ext2fsP.h"

struct expand_dir_struct {
	int		done;
	int		newblocks;
	union {
		blk64_t		goal;
		struct ext2_bmptirec	goalirec;
	};
	errcode_t	err;
	ext2_ino_t	dir;
};

static int expand_dir_proc(ext2_filsys	fs,
			   blk64_t	*blocknr,
			   e2_blkcnt_t	blockcnt,
			   blk64_t	ref_block EXT2FS_ATTR((unused)),
			   int		ref_offset EXT2FS_ATTR((unused)),
			   void		*priv_data)
{
	struct expand_dir_struct *es = (struct expand_dir_struct *) priv_data;
	blk64_t	new_blk;
	char		*block;
	errcode_t	retval;

	if (*blocknr) {
		if (blockcnt >= 0)
			es->goal = *blocknr;
		return 0;
	}
	if (blockcnt &&
	    (EXT2FS_B2C(fs, es->goal) == EXT2FS_B2C(fs, es->goal+1)))
		new_blk = es->goal+1;
	else {
		es->goal &= ~EXT2FS_CLUSTER_MASK(fs);
		retval = ext2fs_new_block2(fs, es->goal, 0, &new_blk);
		if (retval) {
			es->err = retval;
			return BLOCK_ABORT;
		}
		es->newblocks++;
		ext2fs_block_alloc_stats2(fs, new_blk, +1);
	}
	if (blockcnt > 0) {
		retval = ext2fs_new_dir_block(fs, 0, 0, &block);
		if (retval) {
			es->err = retval;
			return BLOCK_ABORT;
		}
		es->done = 1;
		retval = ext2fs_write_dir_block4(fs, new_blk, block, 0,
						 es->dir);
		ext2fs_free_mem(&block);
	} else
		retval = ext2fs_zero_blocks2(fs, new_blk, 1, NULL, NULL);
	if (blockcnt >= 0)
		es->goal = new_blk;
	if (retval) {
		es->err = retval;
		return BLOCK_ABORT;
	}
	*blocknr = new_blk;

	if (es->done)
		return (BLOCK_CHANGED | BLOCK_ABORT);
	else
		return BLOCK_CHANGED;
}

static int expand_bmpt_dir_proc(ext2_filsys	fs,
				int dup_on,
				struct ext2_bmptirec	*block_irec,
				e2_blkcnt_t	blockcnt,
				struct ext2_bmptirec	*ref_block EXT2FS_ATTR((unused)),
				int		ref_offset EXT2FS_ATTR((unused)),
				void		*priv_data)
{
	struct expand_dir_struct *es = (struct expand_dir_struct *) priv_data;
	struct ext2_bmptirec	new_blk;
	char		*block;
	errcode_t	retval;

	if (!ext2_bmpt_irec_is_null(block_irec)) {
		if (blockcnt >= 0)
			es->goalirec = *block_irec;
		return 0;
	}

	if (dup_on) {
		retval = ext2fs_alloc_dup_block(fs, &es->goalirec, 0, &new_blk);
		if (retval) {
			es->err = retval;
			return BLOCK_ABORT;
		}
	} else {
		ext2_bmpt_irec_clear(&new_blk);
		ext2_bmpt_irec_clear(&es->goalirec);
		retval = ext2fs_alloc_block(fs, es->goalirec.b_blocks[0], 0, &new_blk.b_blocks[0]);
		if (retval) {
			es->err = retval;
			return BLOCK_ABORT;
		}
	}
	es->newblocks += fs->super->s_dupinode_dup_cnt;

	if (blockcnt > 0) {
		retval = ext2fs_new_dir_block(fs, 0, 0, &block);
		if (retval) {
			es->err = retval;
			return BLOCK_ABORT;
		}
		es->done = 1;
		retval = ext2fs_write_dir_block4_multiple(fs, &new_blk, block, 0,
							  es->dir);
		ext2fs_free_mem(&block);
	} else {
		retval = ext2fs_get_mem(fs->blocksize, &block);
		if (retval)
			return retval;
		memset(block, 0, fs->blocksize);
		retval = io_channel_write_blk64_multiple(fs->io, &new_blk, 1, fs->super->s_dupinode_dup_cnt, block);
	}
	if (blockcnt >= 0)
		es->goalirec = new_blk;
	if (retval) {
		es->err = retval;
		return BLOCK_ABORT;
	}
	*block_irec = new_blk;

	if (es->done)
		return (BLOCK_CHANGED | BLOCK_ABORT);
	else
		return BLOCK_CHANGED;
}

errcode_t ext2fs_expand_dir(ext2_filsys fs, ext2_ino_t dir)
{
	errcode_t	retval;
	struct expand_dir_struct es;
	struct ext2_inode	inode;

	EXT2_CHECK_MAGIC(fs, EXT2_ET_MAGIC_EXT2FS_FILSYS);

	if (!(fs->flags & EXT2_FLAG_RW))
		return EXT2_ET_RO_FILSYS;

	if (!fs->block_map)
		return EXT2_ET_NO_BLOCK_BITMAP;

	retval = ext2fs_check_directory(fs, dir);
	if (retval)
		return retval;

	retval = ext2fs_read_inode(fs, dir, &inode);
	if (retval)
		return retval;

	es.done = 0;
	es.err = 0;
	if (ext2fs_has_feature_fyp(fs->super))
		ext2_bmpt_irec_clear(&es.goalirec);
	else
		es.goal = ext2fs_find_inode_goal(fs, dir, &inode, 0);
	es.newblocks = 0;
	es.dir = dir;

	if (ext2fs_has_feature_fyp(fs->super)) {
		retval = ext2fs_bmpt_block_iterate(fs, dir, BLOCK_FLAG_APPEND,
					       0, expand_bmpt_dir_proc, &es);
	} else {
		retval = ext2fs_block_iterate3(fs, dir, BLOCK_FLAG_APPEND,
					       0, expand_dir_proc, &es);
	}
	if (retval == EXT2_ET_INLINE_DATA_CANT_ITERATE)
		return ext2fs_inline_data_expand(fs, dir);

	if (es.err)
		return es.err;
	if (!es.done)
		return EXT2_ET_EXPAND_DIR_ERR;

	/*
	 * Update the size and block count fields in the inode.
	 */
	retval = ext2fs_read_inode(fs, dir, &inode);
	if (retval)
		return retval;

	inode.i_size += fs->blocksize;
	ext2fs_iblk_add_blocks(fs, &inode, es.newblocks);

	retval = ext2fs_write_inode(fs, dir, &inode);
	if (retval)
		return retval;

	return 0;
}
