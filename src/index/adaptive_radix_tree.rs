use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::yield_now;
trait Node {}

const SPLIN_COUNT: usize = 60;
struct OptimisticLock(AtomicU64);

impl OptimisticLock {
    #[inline]
    fn read_lock(&self) -> Option<u64> {
        let version = self.wait_unlock();
        if version & 1 == 1 {
            None
        } else {
            Some(version)
        }
    }
    #[inline]
    fn check_lock(&self, version: u64) -> bool {
        self.read_unlock(version)
    }
    #[inline]
    fn read_unlock(&self, version: u64) -> bool {
        version == self.0.load(Ordering::SeqCst)
    }
    #[inline]
    fn read_unlock_other(&self, version: u64, other: &Self) -> bool {
        if version != self.0.load(Ordering::SeqCst) {
            self.write_unlock();
            return false;
        }
        true
    }
    #[inline]
    fn update_to_write(&self, version: u64) -> bool {
        self.0
            .compare_and_swap(version, version + 2, Ordering::SeqCst)
            == version
    }
    #[inline]
    fn update_to_write_other(&self, version: u64, other: &Self) -> bool {
        if self
            .0
            .compare_and_swap(version, version + 2, Ordering::SeqCst)
            != version
        {
            self.write_unlock();
            false
        } else {
            true
        }
    }
    #[inline]
    fn write_lock(&self) -> bool {
        loop {
            if let Some(version) = self.read_lock() {
                if self.update_to_write(version) {
                    break;
                }
            } else {
                return false;
            }
        }
        true
    }
    #[inline]
    fn write_unlock(&self) {
        self.0.fetch_add(2, Ordering::SeqCst);
    }
    #[inline]
    fn write_unlock_obsolete(&self) {
        self.0.fetch_add(3, Ordering::SeqCst);
    }
    #[inline]
    fn wait_unlock(&self) -> u64 {
        let mut version = self.0.load(Ordering::SeqCst);
        let mut count = 0;
        while version & 2 == 2 {
            if count >= SPLIN_COUNT {
                yield_now();
                count = 0;
            }
            count += 1;
            version = self.0.load(Ordering::SeqCst);
        }
        version
    }
}
