use crate::{
    error::TdbError,
    storage::{BlockDeserialize, BlockId, BlockSerialize, BLOCK_SIZE},
    tree::{Node, NodeKind, NodePos},
};
use futures::task::AtomicWaker;
use lazy_static::lazy_static;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

lazy_static! {
    pub static ref G_DEV: BlockDev<FileBlockDev> = { BlockDev::new(FileBlockDev::default()) };
}

pub trait RawBlockDev: Sync + Send {
    // buf must align to block size
    fn read(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError>;
    fn write(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError>;
    fn async_read<F: FnOnce()>(
        &self,
        block_start: u32,
        buf: &mut [u8],
        callback: F,
    ) -> Poll<Result<(), TdbError>>;
    fn async_write<F: FnOnce()>(
        &self,
        block_start: u32,
        buf: &[u8],
        callback: F,
    ) -> Poll<Result<(), TdbError>>;
}

pub struct BlockReader<'a, R: ?Sized + Unpin> {
    reader: &'a R,
    block_start: u32,
    buf: &'a mut [u8],
    done: AtomicBool,
    waker: AtomicWaker,
}

impl<R: ?Sized + Unpin> Unpin for BlockReader<'_, R> {}

impl<R: RawBlockDev + ?Sized + Unpin> Future for BlockReader<'_, R> {
    type Output = Result<(), TdbError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        this.waker.register(cx.waker());
        if this.done.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            let done = &this.done;
            let waker = &this.waker;
            this.reader.async_read(this.block_start, this.buf, move || {
                done.store(true, Ordering::SeqCst);
                waker.wake();
            })
        }
    }
}

pub struct BlockWriter<'a, W: ?Sized + Unpin> {
    writer: &'a W,
    block_start: u32,
    buf: &'a [u8],
    done: AtomicBool,
    waker: AtomicWaker,
}

impl<R: ?Sized + Unpin> Unpin for BlockWriter<'_, R> {}

impl<R: RawBlockDev + ?Sized + Unpin> Future for BlockWriter<'_, R> {
    type Output = Result<(), TdbError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        this.waker.register(cx.waker());
        if this.done.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            let done = &this.done;
            let waker = &this.waker;
            this.writer
                .async_write(this.block_start, this.buf, move || {
                    done.store(true, Ordering::SeqCst);
                    waker.wake();
                })
        }
    }
}

pub struct BlockDev<Dev> {
    dev: Dev,
}

impl<Dev: RawBlockDev + Unpin> BlockDev<Dev> {
    pub fn new(dev: Dev) -> Self {
        Self { dev: dev }
    }
    fn sync_read(&self, block_start: u32, buf: &mut [u8]) -> Result<(), TdbError> {
        assert_eq!(buf.len() % BLOCK_SIZE, 0);
        self.dev.read(block_start, buf)
    }
    fn poll_read<'a>(&'a self, block_start: u32, buf: &'a mut [u8]) -> BlockReader<'a, Dev> {
        assert_eq!(buf.len() % BLOCK_SIZE, 0);
        BlockReader {
            reader: &self.dev,
            block_start: block_start,
            buf: buf,
            done: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }
    fn sync_write(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError> {
        assert_eq!(buf.len() % BLOCK_SIZE, 0);
        self.dev.write(block_start, buf)
    }
    fn poll_write<'a>(&'a self, block_start: u32, buf: &'a [u8]) -> BlockWriter<'a, Dev> {
        assert_eq!(buf.len() % BLOCK_SIZE, 0);
        BlockWriter {
            writer: &self.dev,
            block_start: block_start,
            buf: buf,
            done: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }
    pub fn sync_read_node(
        &self,
        node_pos: &NodePos,
        node_kind: NodeKind,
    ) -> Result<Node, TdbError> {
        let buf_len = node_pos.block_len as usize * BLOCK_SIZE;
        let mut buf = Vec::with_capacity(buf_len);
        self.sync_read(node_pos.block_start, &mut buf)?;
        Node::read(&buf[node_pos.offset as usize..], node_kind)
    }

    pub fn sync_write_node(&self, node_pos: &NodePos, node: &Node) -> Result<(), TdbError> {
        let buf_len = node_pos.block_len as usize * BLOCK_SIZE;
        let mut buf = Vec::with_capacity(buf_len);
        if node_pos.offset != 0 {
            self.sync_read(node_pos.block_start, &mut buf[0..BLOCK_SIZE])?;
        }
        node.write(&mut buf)?;
        self.sync_write(node_pos.block_start, &mut buf)
    }
}

pub struct FileBlockDev {}

impl Default for FileBlockDev {
    fn default() -> Self {
        unimplemented!()
    }
}

impl RawBlockDev for FileBlockDev {
    fn read(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError> {
        unimplemented!()
    }
    fn write(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError> {
        unimplemented!()
    }
    fn async_read<F: FnOnce()>(
        &self,
        block_start: u32,
        buf: &mut [u8],
        callback: F,
    ) -> Poll<Result<(), TdbError>> {
        unimplemented!()
    }
    fn async_write<F: FnOnce()>(
        &self,
        block_start: u32,
        buf: &[u8],
        callback: F,
    ) -> Poll<Result<(), TdbError>> {
        unimplemented!()
    }
}
