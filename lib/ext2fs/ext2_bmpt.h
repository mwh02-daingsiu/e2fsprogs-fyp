#ifndef _BMPT_H_
#define _BMPT_H_

#include <linux/types.h>
#include <stdint.h>

#define EXT2_BMPT_N_DUPS 3
struct ext2_bmptrec {
	__le32 b_blocks[EXT2_BMPT_N_DUPS];
	__le32 b_flags;
};

struct ext2_bmptirec {
	uint32_t b_blocks[EXT2_BMPT_N_DUPS];
	uint32_t b_flags;
};

#define EXT2_BMPT_HDR_MAGIC 0xf5e5c5d5
#define EXT2_BMPT_MAXLEVELS 7

struct ext2_bmpthdr {
	__le32 h_magic;
	__le32 h_levels;
	__le32 h_flags;
	struct ext2_bmptrec h_root;
};

#define EXT2_BMPTREC_SZ (sizeof(struct ext2_bmptrec))
#define EXT2_BMPTREC_SZ_BITS (4)

#define EXT2_BMPT_ADDR_PER_BLOCK(sz) ((sz) >> EXT2_BMPTREC_SZ_BITS)

#define EXT2_BMPT_HDR_FLAGS_DUP 0x00000001

static inline int ext2_bmpt_irec_is_null(struct ext2_bmptirec *rec)
{
	return !rec->b_blocks[0];
}

static inline void ext2_bmpt_irec_clear(struct ext2_bmptirec *rec)
{
	int i;
	for (i = 0; i < EXT2_BMPT_N_DUPS; i++)
		rec->b_blocks[i] = 0;
	rec->b_flags = 0;
}

static inline int ext2_bmpt_rec_is_null(struct ext2_bmptrec *rec)
{
	return !rec->b_blocks[0];
}

static inline void ext2_bmpt_rec_clear(struct ext2_bmptrec *rec)
{
	int i;
	for (i = 0; i < EXT2_BMPT_N_DUPS; i++)
		rec->b_blocks[i] = 0;
	rec->b_flags = 0;
}

extern void ext2_bmpt_rec2irec(struct ext2_bmptrec *rec,
			       struct ext2_bmptirec *irec);
extern void ext2_bmpt_irec2rec(struct ext2_bmptirec *irec,
			       struct ext2_bmptrec *rec);

#endif /* _BMPT_H_ */