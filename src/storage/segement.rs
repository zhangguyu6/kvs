use super::allocater::BitMap;

struct Segement<BMap: BitMap> {
    total_blocks_num: usize,
    used_blocks_num: usize,
    bitmap: BMap,
}

