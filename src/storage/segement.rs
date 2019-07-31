use crate::utils::BitMap;
use crate::transaction::TimeStamp;

struct Segement<BMap: BitMap> {
    cmmit_ts: TimeStamp,
    total_blocks_num: usize,
    used_blocks_num: usize,
    bitmap: BMap,
}
