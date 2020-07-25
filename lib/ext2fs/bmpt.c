#include "config.h"
#include "ext2fs/ext2_bmpt.h"
#include "ext2fs/ext2fs.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#if HAVE_UNISTD_H
#include <unistd.h>
#endif
#if HAVE_ERRNO_H
#include <errno.h>
#endif
#if HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#if HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif

#include "ext2_fs.h"
#include "ext2fsP.h"
#include "e2image.h"

static inline int ext2_bmpt_min_numlevels(ext2_filsys fs, blk_t iblk)
{
	int i = 0;
	blk_t addr_per_block = EXT2_BMPT_ADDR_PER_BLOCK(fs->blocksize);

	while (iblk) {
		i++;
		iblk /= addr_per_block;
	}
	return i;
}

static inline int ext2_bmpt_offsets(ext2_filsys fs, int level, blk_t iblk)
{
	int i;
	blk_t addr_per_block = EXT2_BMPT_ADDR_PER_BLOCK(fs->blocksize);

	for (i = 0; i < level; i++)
		iblk /= addr_per_block;
	return iblk % addr_per_block;
}

static inline blk_t ext2_bmpt_boundary(ext2_filsys fs, int level, blk_t iblk)
{
	blk_t addr_per_block = EXT2_BMPT_ADDR_PER_BLOCK(fs->blocksize);
	unsigned int mask = addr_per_block * (level + 1) - 1;
	return iblk | mask;
}

void ext2_bmpt_rec2irec(struct ext2_bmptrec *rec, struct ext2_bmptirec *irec)
{
	int i = 0;
	for (i = 0; i < EXT2_BMPT_N_DUPS; i++)
		irec->b_blocks[i] = ext2fs_le32_to_cpu(rec->b_blocks[i]);
	irec->b_flags = ext2fs_le32_to_cpu(rec->b_flags);
}

void ext2_bmpt_irec2rec(struct ext2_bmptirec *irec, struct ext2_bmptrec *rec)
{
	int i = 0;
	for (i = 0; i < EXT2_BMPT_N_DUPS; i++)
		rec->b_blocks[i] = ext2fs_cpu_to_le32(irec->b_blocks[i]);
	rec->b_flags = ext2fs_cpu_to_le32(irec->b_flags);
}

blk_t ext2_bmpt_find_goal_noiblk(ext2_filsys fs, ext2_ino_t ino, int which)
{
	__u8 log_flex = fs->super->s_log_groups_per_flex;
	dgrp_t group = ext2fs_group_of_ino(fs, ino);
	dgrp_t ngroups = fs->group_desc_count;
	if (log_flex)
		group = group & ~((1 << (log_flex)) - 1);
	group = (group + which) % ngroups;
	return (blk_t)ext2fs_group_first_block2(fs, group);
}

static errcode_t ext2fs_bmpt_build_branch(ext2_filsys fs, ext2_ino_t ino,
					  blk_t iblk, int ninds,
					  struct ext2_bmptirec *irecs)
{
	int i, j;
	char *buf;
	char *block_buf[EXT2_BMPT_MAXLEVELS];
	blk_t goal[EXT2_BMPT_N_DUPS];
	errcode_t retval;

	retval = ext2fs_get_array(ninds, fs->blocksize, &buf);
	if (retval)
		return retval;

	block_buf[0] = buf;
	ext2_bmpt_irec_clear(&irecs[0]);
	for (i = 1; i < ninds; i++) {
		block_buf[i] = block_buf[i - 1] + fs->blocksize;
		ext2_bmpt_irec_clear(&irecs[i]);
	}
	for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
		goal[j] = ext2_bmpt_find_goal_noiblk(fs, ino, j);
	for (i = 0; i < ninds; i++) {
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
			retval = ext2fs_alloc_block(fs, goal[j], block_buf[i],
						    &irecs[i].b_blocks[j]);
			if (retval)
				goto done;
		}
	}

	for (i = 0; i < ninds; i++) {
		blk64_t blks[EXT2_BMPT_N_DUPS];
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
			blks[j] = irecs[i].b_blocks[j];

		memset(block_buf[i], 0, fs->blocksize);
		if (i != ninds - 1) {
			int off = ext2_bmpt_offsets(fs, i, iblk);
			ext2_bmpt_irec2rec(
				&irecs[i + 1],
				&((struct ext2_bmptrec *)block_buf[i])[off]);
		}
		retval = io_channel_write_blk64_multiple(
			fs->io, blks, 1, EXT2_BMPT_N_DUPS, block_buf[i]);
		if (retval)
			goto done;
	}

done:
	if (retval) {
		for (i = 0; i < ninds; i++) {
			for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
				if (irecs[i].b_blocks[j])
					ext2fs_block_alloc_stats2(
						fs, irecs[i].b_blocks[j], -1);
			}
			ext2_bmpt_irec_clear(&irecs[i]);
		}
	}
	ext2fs_free_mem(&buf);
	return retval;
}

errcode_t ext2fs_increase_inds(ext2_filsys fs, ext2_ino_t ino,
			       struct ext2_inode *inode, int add_levels)
{
	struct ext2_bmpthdr *hdr = (struct ext2_bmpthdr *)&inode->i_block[0];
	char *buf;
	int i, j, nr_levels, ninds;
	char *block_buf[EXT2_BMPT_MAXLEVELS];
	struct ext2_bmptirec *irecs;
	blk_t goal[EXT2_BMPT_N_DUPS];
	errcode_t retval;

	nr_levels = ext2fs_le32_to_cpu(hdr->h_levels);
	ninds = add_levels;
	if (ext2_bmpt_rec_is_null(&hdr->h_root))
		ninds = 1;

	retval = ext2fs_get_array(ninds, fs->blocksize, &buf);
	if (retval)
		return retval;
	memset(buf, 0, fs->blocksize * ninds);
	retval = ext2fs_get_array(ninds, sizeof(struct ext2_bmptirec), &irecs);
	if (retval) {
		ext2fs_free_mem(&buf);
		return retval;
	}

	block_buf[0] = buf;
	ext2_bmpt_irec_clear(&irecs[0]);
	for (i = 1; i < ninds; i++) {
		block_buf[i] = block_buf[i - 1] + fs->blocksize;
		ext2_bmpt_irec_clear(&irecs[i]);
	}
	for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
		goal[j] = ext2_bmpt_find_goal_noiblk(fs, ino, j);
	for (i = 0; i < ninds; i++) {
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
			retval = ext2fs_alloc_block(fs, goal[j], block_buf[i],
						    &irecs[i].b_blocks[j]);
			if (retval)
				goto done;
		}
	}

	for (i = 0; i < ninds; i++) {
		blk64_t blks[EXT2_BMPT_N_DUPS];
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
			blks[j] = irecs[i].b_blocks[j];

		if (i != ninds - 1) {
			ext2_bmpt_irec2rec(
				&irecs[i + 1],
				&((struct ext2_bmptrec *)block_buf[i])[0]);

		} else {
			((struct ext2_bmptrec *)block_buf[i])[0] = hdr->h_root;
		}
		retval = io_channel_write_blk64_multiple(
			fs->io, blks, 1, EXT2_BMPT_N_DUPS, block_buf[i]);
		if (retval)
			goto done;
	}

	hdr->h_levels = nr_levels + add_levels;
	ext2_bmpt_irec2rec(&irecs[0], &hdr->h_root);
	retval = ext2fs_write_inode(fs, ino, inode);

done:
	if (retval) {
		for (i = 0; i < ninds; i++) {
			for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
				if (irecs[i].b_blocks[j])
					ext2fs_block_alloc_stats2(
						fs, irecs[i].b_blocks[j], -1);
			}
		}
	}
	ext2fs_free_mem(&buf);
	ext2fs_free_mem(&irecs);
	return retval;
}

static errcode_t ext2fs_bmpt_linear(ext2_filsys fs, ext2_ino_t ino,
				    struct ext2_inode *inode, char *block_buf,
				    int bmap_flags, blk64_t block,
				    int *ret_flags,
				    struct ext2_bmptirec *phys_blk)
{
	struct ext2_bmpthdr *hdr = (struct ext2_bmpthdr *)&inode->i_block[0];
	int can_insert = BMAP_SET | BMAP_ALLOC;
	struct ext2_bmptirec dbirec;
	errcode_t retval = 0;

	if (bmap_flags & BMAP_SET) {
		ext2_bmpt_irec2rec(phys_blk, &hdr->h_root);
		return ext2fs_write_inode(fs, ino, inode);
	}

	if (ext2_bmpt_rec_is_null(&hdr->h_root) && can_insert) {
		blk64_t blks[EXT2_BMPT_N_DUPS];
		blk_t goal[EXT2_BMPT_N_DUPS];

		if (ext2fs_cpu_to_le32(hdr->h_flags) &
		    EXT2_BMPT_HDR_FLAGS_DUP) {
			int j;
			for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
				goal[j] =
					ext2_bmpt_find_goal_noiblk(fs, ino, j);
			for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
				retval =
					ext2fs_alloc_block(fs, goal[j],
							   block_buf,
							   &dbirec.b_blocks[j]);
				if (retval)
					goto done;
			}
		} else {
			goal[0] = ext2_bmpt_find_goal_noiblk(fs, ino, 0);
			retval = ext2fs_alloc_block(fs, goal[0], block_buf,
						    &dbirec.b_blocks[0]);
			if (retval)
				goto done;
		}

		ext2_bmpt_irec2rec(&dbirec, &hdr->h_root);

		retval = ext2fs_write_inode(fs, ino, inode);
		if (retval)
			goto done;
	}

	ext2_bmpt_rec2irec(&hdr->h_root, phys_blk);

done:
	if (retval) {
		int j;
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
			if (dbirec.b_blocks[j])
				ext2fs_block_alloc_stats2(
					fs, dbirec.b_blocks[j], -1);
		}
	}
	return retval;
}

errcode_t ext2fs_bmpt_bmap2(ext2_filsys fs, ext2_ino_t ino,
			    struct ext2_inode *inode, char *block_buf,
			    int bmap_flags, blk64_t block, int *ret_flags,
			    struct ext2_bmptirec *phys_blk)
{
	struct ext2_bmpthdr *hdr = (struct ext2_bmpthdr *)&inode->i_block[0];
	blk_t addr_per_block = EXT2_BMPT_ADDR_PER_BLOCK(fs->blocksize);
	int nr_levels = ext2fs_le32_to_cpu(hdr->h_levels);
	struct ext2_bmptirec irec, dbirec;
	errcode_t retval = 0;
	int can_insert = BMAP_SET | BMAP_ALLOC;
	int i = 0, off;
	struct ext2_bmptirec *ind_irecs = NULL;
	char *ind_blocks[EXT2_BMPT_MAXLEVELS], *buf = NULL;
	blk_t blocks_alloc = 0;
	int ind_new = -1;
	int ind_offs = -1;

	if (ret_flags)
		*ret_flags = 0;
	if (!(bmap_flags & BMAP_SET))
		ext2_bmpt_irec_clear(phys_blk);
	ext2_bmpt_irec_clear(&dbirec);

	if (!(inode->i_flags & EXT2_FYP_BMPT_FL))
		return EXT2_ET_INODE_NOT_EXTENT;

	if (ext2fs_le32_to_cpu(hdr->h_magic) != EXT2_BMPT_HDR_MAGIC) {
		if (!can_insert)
			goto done;
		retval = ext2fs_init_bmpt(fs, ino, inode,
					  inode->i_flags & EXT2_FYP_DUP_RUN_FL);
		if (retval)
			goto done;
	}

	if (!(bmap_flags & can_insert)) {
		if (ext2_bmpt_min_numlevels(fs, (blk_t)block) > nr_levels)
			goto done;
	} else {
		int min_levels = ext2_bmpt_min_numlevels(fs, (blk_t)block);
		if (min_levels > nr_levels) {
			retval = ext2fs_increase_inds(fs, ino, inode,
						      min_levels - nr_levels);
			if (retval)
				goto done;
			blocks_alloc += min_levels - nr_levels;
			nr_levels = min_levels;
		}
	}

	if (!nr_levels)
		return ext2fs_bmpt_linear(fs, ino, inode, block_buf, bmap_flags,
					  block, ret_flags, phys_blk);

	ind_new = nr_levels - 1;

	retval = ext2fs_get_array(nr_levels, fs->blocksize, &buf);
	if (retval)
		goto done;
	retval = ext2fs_get_array(nr_levels,
				  nr_levels * sizeof(struct ext2_bmptirec),
				  &ind_irecs);
	if (retval)
		goto done;
	memset(ind_irecs, 0, nr_levels * sizeof(struct ext2_bmptirec));

	ind_blocks[0] = buf;
	for (i = 1; i < nr_levels; i++)
		ind_blocks[i] = ind_blocks[i - 1] + fs->blocksize;

	ext2_bmpt_rec2irec(&hdr->h_root, &irec);
	for (i = 0; i < nr_levels; i++) {
		retval = io_channel_read_blk64(fs->io, irec.b_blocks[0], 1,
					       ind_blocks[i]);
		if (retval)
			goto done;

		ind_irecs[i] = irec;
		off = ext2_bmpt_offsets(fs, i, block);
		if (ext2_bmpt_rec_is_null(
			    &((struct ext2_bmptrec *)ind_blocks[i])[off]) &&
		    i != nr_levels - 1) {
			blk64_t blks[EXT2_BMPT_N_DUPS];

			if (!can_insert)
				goto done;

			retval = ext2fs_bmpt_build_branch(fs, ino, block, i,
							  &ind_irecs[i]);
			if (retval)
				goto done;
			ind_new = i;
			ind_offs = off;

			irec = ind_irecs[0];
			blocks_alloc += ind_new;
		} else {
			ext2_bmpt_rec2irec(
				&((struct ext2_bmptrec *)ind_blocks[i])[off],
				&irec);
		}
	}

	if (bmap_flags & BMAP_SET) {
		blk64_t blks[EXT2_BMPT_N_DUPS];
		int j;

		ext2_bmpt_irec2rec(phys_blk,
				   &((struct ext2_bmptrec *)
					     ind_blocks[nr_levels - 1])[off]);
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
			blks[j] = ind_irecs[nr_levels - 1].b_blocks[j];
		retval = io_channel_write_blk64_multiple(
			fs->io, blks, 1, EXT2_BMPT_N_DUPS,
			ind_blocks[nr_levels - 1]);
		if (retval)
			goto done;
	} else if (ext2_bmpt_irec_is_null(&irec) && can_insert) {
		blk64_t blks[EXT2_BMPT_N_DUPS];
		blk_t goal[EXT2_BMPT_N_DUPS];
		int j;

		if (ext2fs_cpu_to_le32(hdr->h_flags) &
		    EXT2_BMPT_HDR_FLAGS_DUP) {
			for (j = 0; j < fs->super->s_dupinode_dup_cnt; j++)
				goal[j] =
					ext2_bmpt_find_goal_noiblk(fs, ino, j);
			for (j = 0; j < fs->super->s_dupinode_dup_cnt; j++) {
				retval =
					ext2fs_alloc_block(fs, goal[j],
							   block_buf,
							   &dbirec.b_blocks[j]);
				if (retval)
					goto done;
			}
			blocks_alloc += fs->super->s_dupinode_dup_cnt;
		} else {
			goal[0] = ext2_bmpt_find_goal_noiblk(fs, ino, 0);
			retval = ext2fs_alloc_block(fs, goal[0], block_buf,
						    &dbirec.b_blocks[0]);
			if (retval)
				goto done;
			blocks_alloc++;
		}

		ext2_bmpt_irec2rec(&dbirec,
				   &((struct ext2_bmptrec *)
					     ind_blocks[nr_levels - 1])[off]);

		for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
			blks[j] = ind_irecs[nr_levels - 1].b_blocks[j];
		retval = io_channel_write_blk64_multiple(
			fs->io, blks, 1, EXT2_BMPT_N_DUPS,
			ind_blocks[nr_levels - 1]);
		if (retval)
			goto done;
	}

	/*
	 * We insert any newly created indirection blocks near the end, if any.
	 * This makes reverting failed attempts easier.
	 */
	if (ind_new != nr_levels - 1) {
		blk64_t blks[EXT2_BMPT_N_DUPS];
		int j;

		ext2_bmpt_irec2rec(&ind_irecs[0],
				   &((struct ext2_bmptrec *)
					     ind_blocks[ind_new])[ind_offs]);
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
			blks[j] = ind_irecs[ind_new].b_blocks[j];
		retval = io_channel_write_blk64_multiple(
			fs->io, blks, 1, EXT2_BMPT_N_DUPS, ind_blocks[ind_new]);
		if (retval)
			goto done;
	}

	ext2_bmpt_rec2irec(
		&((struct ext2_bmptrec *)ind_blocks[nr_levels - 1])[off],
		phys_blk);
	if (can_insert)
		ext2fs_iblk_add_blocks(fs, inode, blocks_alloc);

done:
	if (retval) {
		int j;

		for (i = nr_levels - 1; i > ind_new; i--) {
			for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
				if (ind_irecs[i].b_blocks[j])
					ext2fs_block_alloc_stats2(
						fs, ind_irecs[i].b_blocks[j],
						-1);
			}
		}

		for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
			if (dbirec.b_blocks[j])
				ext2fs_block_alloc_stats2(
					fs, dbirec.b_blocks[j], -1);
		}
	}
	ext2fs_free_mem(&buf);
	ext2fs_free_mem(&ind_irecs);
	return retval;
}

/*
 * This function returns 1 if the specified block is all zeros
 */
static int check_zero_block(char *buf, int blocksize)
{
	char *cp = buf;
	int left = blocksize;

	while (left > 0) {
		if (*cp++)
			return 0;
		left--;
	}
	return 1;
}

/*
 * This is originally from ext2fs_punch_ind, modified to support bmpt
 */
static errcode_t bmpt_punch(ext2_filsys fs, struct ext2_inode *inode,
			    char *block_buf, struct ext2_bmptrec *p, int level,
			    blk64_t start, blk64_t count, int max)
{
	errcode_t retval;
	struct ext2_bmptirec b;
	int i, j;
	blk64_t offset, incr;
	int freed = 0;
	blk64_t blks[EXT2_BMPT_N_DUPS];

#ifdef PUNCH_DEBUG
	printf("Entering ind_punch, level %d, start %llu, count %llu, "
	       "max %d\n",
	       level, start, count, max);
#endif
	incr = 1ULL
	       << ((EXT2_BLOCK_SIZE_BITS(fs->super) - EXT2_BMPTREC_SZ_BITS) *
		   level);
	for (i = 0, offset = 0; i < max; i++, p++, offset += incr) {
		if (offset >= start + count)
			break;
		if (ext2_bmpt_rec_is_null(p) || (offset + incr) <= start)
			continue;
		ext2_bmpt_rec2irec(p, &b);
		if (level > 0) {
			blk_t start2;
#ifdef PUNCH_DEBUG
			printf("Reading indirect block %u\n", b);
#endif
			retval = io_channel_read_blk64(fs->io, b.b_blocks[0], 1,
						       block_buf);
			if (retval)
				return retval;
			start2 = (start > offset) ? start - offset : 0;
			retval = bmpt_punch(
				fs, inode, block_buf + fs->blocksize,
				(struct ext2_bmptrec *)block_buf, level - 1,
				start2, count - offset,
				fs->blocksize >> EXT2_BMPTREC_SZ_BITS);
			if (retval)
				return retval;
			for (j = 0; j < EXT2_BMPT_N_DUPS; j++)
				blks[j] = b.b_blocks[j];
			retval = io_channel_write_blk64_multiple(
				fs->io, blks, 1, EXT2_BMPT_N_DUPS, block_buf);
			if (retval)
				return retval;
			if (!check_zero_block(block_buf, fs->blocksize))
				continue;
		}
#ifdef PUNCH_DEBUG
		printf("Freeing block %u (offset %llu)\n", b, offset);
#endif
		for (j = 0; j < EXT2_BMPT_N_DUPS; j++) {
			if (b.b_blocks[j])
				ext2fs_block_alloc_stats2(fs, b.b_blocks[j],
							  -1);
		}
		ext2_bmpt_rec_clear(p);
		freed++;
	}
#ifdef PUNCH_DEBUG
	printf("Freed %d blocks\n", freed);
#endif
	return ext2fs_iblk_sub_blocks(fs, inode, freed);
}

#define BLK_T_MAX ((blk_t)~0ULL)
errcode_t ext2fs_punch_bmpt(ext2_filsys fs, ext2_ino_t ino,
			    struct ext2_inode *inode, char *block_buf,
			    blk64_t start, blk64_t end)
{
	struct ext2_bmpthdr *hdr = (struct ext2_bmpthdr *)&inode->i_block[0];
	errcode_t retval;
	char *buf = 0;
	int nr_levels;
	int i;
	blk_t addr_per_block;
	blk64_t max = 1;
	blk_t count;
	struct ext2_bmptrec root_rec = hdr->h_root;

	/* Check start/end don't overflow the 2^32-1 indirect block limit */
	if (start > BLK_T_MAX)
		return 0;
	if (end >= BLK_T_MAX || end - start + 1 >= BLK_T_MAX)
		count = BLK_T_MAX - start;
	else
		count = end - start + 1;

	if (!block_buf) {
		retval = ext2fs_get_array(EXT2_BMPT_MAXLEVELS, fs->blocksize,
					  &buf);
		if (retval)
			return retval;
		block_buf = buf;
	}

	if (ext2fs_le32_to_cpu(hdr->h_magic) != EXT2_BMPT_HDR_MAGIC) {
		retval = 0;
		goto done;
	}

	addr_per_block = (blk_t)fs->blocksize >> EXT2_BMPTREC_SZ;
	nr_levels = ext2fs_le32_to_cpu(hdr->h_levels);

	for (i = 0; i < nr_levels; i++)
		max *= addr_per_block;

	retval = bmpt_punch(fs, inode, block_buf, &hdr->h_root, nr_levels,
			    start, count, max);
	if (retval)
		goto done;

	if (memcmp(&root_rec, &hdr->h_root, sizeof(root_rec))) {
		if (ext2_bmpt_rec_is_null(&hdr->h_root))
			hdr->h_levels = 0;
		retval = ext2fs_write_inode(fs, ino, inode);
	}

done:
	if (buf)
		ext2fs_free_mem(&buf);
	return retval;
}

struct bmpt_walk_ctx {
	ext2_filsys fs;
	struct ext2_inode *inode;
	unsigned int call_on_index : 1;
	unsigned int aborted : 1;
	int (*callback)(e2_blkcnt_t iblk, const struct ext2_bmptirec *blks,
			int depth, int level, int dup_on, void *priv_data);
	void *priv_data;
};

static errcode_t bmpt_dump_walk(struct bmpt_walk_ctx *ctx, char *block_buf,
				struct ext2_bmptrec *p, int level,
				blk64_t start, blk64_t count, int max,
				blk64_t iblk)
{
	struct ext2_bmpthdr *hdr =
		(struct ext2_bmpthdr *)&ctx->inode->i_block[0];
	errcode_t retval;
	struct ext2_bmptirec b;
	int i;
	blk64_t offset, incr;
	int depth = hdr->h_levels - level - 1;

	incr = 1ULL << ((EXT2_BLOCK_SIZE_BITS(ctx->fs->super) -
			 EXT2_BMPTREC_SZ_BITS) *
			level);
	for (i = 0, offset = 0; i < max;
	     i++, p++, offset += incr, iblk += incr) {
		if (offset >= start + count)
			break;
		if (ext2_bmpt_rec_is_null(p) || (offset + incr) <= start)
			continue;
		ext2_bmpt_rec2irec(p, &b);
		if (level > 0) {
			blk_t start2;
			int flags;

			retval = io_channel_read_blk64(
				ctx->fs->io, b.b_blocks[0], 1, block_buf);
			if (retval)
				return retval;
			if (ctx->call_on_index) {
				flags = ctx->callback(
					iblk, &b, depth, level,
					ctx->inode->i_flags &
						EXT2_FYP_DUP_RUN_FL,
					ctx->priv_data);
				if (flags & BLOCK_ABORT) {
					ctx->aborted = 1;
					break;
				}
			}
			start2 = (start > offset) ? start - offset : 0;
			retval = bmpt_dump_walk(
				ctx, block_buf + ctx->fs->blocksize,
				(struct ext2_bmptrec *)block_buf, level - 1,
				start2, count - offset,
				ctx->fs->blocksize >> EXT2_BMPTREC_SZ_BITS,
				iblk);
			if (retval)
				return retval;
			if (ctx->aborted)
				break;
		} else {
			int flags;

			flags = ctx->callback(iblk, &b, depth, level,
					      ctx->inode->i_flags &
						      EXT2_FYP_DUP_RUN_FL,
					      ctx->priv_data);
			if (flags & BLOCK_ABORT) {
				ctx->aborted = 1;
				break;
			}
		}
	}

	return 0;
}

#define BLK_T_MAX ((blk_t)~0ULL)
errcode_t ext2fs_bmpt_dump(ext2_filsys fs, ext2_ino_t ino, char *block_buf,
			   blk64_t start, blk64_t end, int call_on_index,
			   int (*callback)(e2_blkcnt_t iblk,
					   const struct ext2_bmptirec *blks,
					   int depth, int level, int dup_on,
					   void *priv_data),
			   void *priv_data)
{
	errcode_t retval;
	char *buf = 0;
	int nr_levels;
	int i;
	blk_t addr_per_block;
	blk64_t max = 1;
	blk_t count;
	struct bmpt_walk_ctx ctx;
	struct ext2_inode inode;
	struct ext2_bmpthdr *hdr = (struct ext2_bmpthdr *)&inode.i_block[0];
	struct ext2_bmptrec root_rec;

	retval = ext2fs_read_inode(fs, ino, &inode);
	if (retval)
		return retval;

	ctx.fs = fs;
	ctx.inode = &inode;
	ctx.call_on_index = call_on_index;
	ctx.aborted = 0;
	ctx.callback = callback;
	ctx.priv_data = priv_data;
	root_rec = hdr->h_root;

	/* Check start/end don't overflow the 2^32-1 indirect block limit */
	if (start > BLK_T_MAX)
		return 0;
	if (end >= BLK_T_MAX || end - start + 1 >= BLK_T_MAX)
		count = BLK_T_MAX - start;
	else
		count = end - start + 1;

	if (!block_buf) {
		retval = ext2fs_get_array(EXT2_BMPT_MAXLEVELS, fs->blocksize,
					  &buf);
		if (retval)
			return retval;
		block_buf = buf;
	}

	if (ext2fs_le32_to_cpu(hdr->h_magic) != EXT2_BMPT_HDR_MAGIC) {
		retval = 0;
		goto done;
	}

	addr_per_block = (blk_t)fs->blocksize >> EXT2_BMPTREC_SZ;
	nr_levels = ext2fs_le32_to_cpu(hdr->h_levels);

	for (i = 0; i < nr_levels; i++)
		max *= addr_per_block;

	retval = bmpt_dump_walk(&ctx, block_buf, &hdr->h_root, nr_levels, start,
				count, max, 0);
	if (retval)
		goto done;

done:
	if (buf)
		ext2fs_free_mem(&buf);
	return retval;
}

errcode_t ext2fs_init_bmpt(ext2_filsys fs, ext2_ino_t ino,
			   struct ext2_inode *inode, int dup_on)
{
	struct ext2_bmpthdr *hdr = (struct ext2_bmpthdr *)&inode->i_block[0];
	hdr->h_magic = ext2fs_cpu_to_le32(EXT2_BMPT_HDR_MAGIC);
	hdr->h_flags = dup_on ? ext2fs_cpu_to_le32(EXT2_BMPT_HDR_FLAGS_DUP) : 0;
	hdr->h_levels = 0;
	ext2_bmpt_rec_clear(&hdr->h_root);

	hdr++;
	hdr->h_magic = ext2fs_cpu_to_le32(EXT2_BMPT_HDR_MAGIC);
	hdr->h_flags = 0;
	hdr->h_levels = 0;
	ext2_bmpt_rec_clear(&hdr->h_root);

	return ext2fs_write_inode(fs, ino, inode);
}