KVS
===
KVS is a pure rust embedded On-Disk B+tree Key Value Store inspired by [Blot project][bolt] and [F2FS filesystem][F2FS]. 
The goal of this project is to learn how to implement a storage engine .

[bolt]:https://github.com/boltdb/bolt
[F2FS]:https://www.usenix.org/conference/fast15/technical-sessions/presentation/lee

**WARNING**: ⚠️ This project still at very early stage, do not use it. ⚠️

## Features
* Pure rust
* Log-structured , copy-on-write B+tree internally
* Indirect pointer table inspired by f2fs to eliminate update propagation("wandering tree" problem)
* MVCC using a single writer and multiple readers
* Transactional support snapshot isolation level
* Keys and values are treated as an arbitrary binary
* Checkpoint and Crash-consistent

## Usage

The basic usage of the library is shown below:

```rust
extern crate kvs;
use std::env;
use kvs::{KVStore,KVWriterKVReader};

fn main() {
        let kv = KVStore::open(env::current_dir().unwrap()).unwrap();

        let mut writer = kv.get_writer();
        assert_eq!(writer.insert(vec![1, 2, 3], vec![3, 2, 1]), Ok(()));
        assert_eq!(writer.commit(), Ok(()));

        let mut reader = kv.get_reader().unwrap();
        assert_eq!(reader.get(&vec![1, 2, 3]), Ok(Some(vec![3, 2, 1])));
}

```

## TODO

- [X] Mvcc support
- [X] Error handling
- [ ] Garbage collector
- [ ] Statics
- [ ] Documentation
- [ ] Tests
- [ ] Benchmark

