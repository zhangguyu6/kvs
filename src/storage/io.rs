use super::{ObjectPos, BLOCK_SIZE};
use crate::{
    error::TdbError,
    object::{Object, ObjectTag},
};
use futures::task::AtomicWaker;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

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
    pub fn sync_read(&self, block_start: u32, buf: &mut [u8]) -> Result<(), TdbError> {
        assert_eq!(buf.len() % BLOCK_SIZE, 0);
        self.dev.read(block_start, buf)
    }
    pub fn poll_read<'a>(&'a self, block_start: u32, buf: &'a mut [u8]) -> BlockReader<'a, Dev> {
        assert_eq!(buf.len() % BLOCK_SIZE, 0);
        BlockReader {
            reader: &self.dev,
            block_start: block_start,
            buf: buf,
            done: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }
    }
    pub fn sync_write(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError> {
        assert_eq!(buf.len() % BLOCK_SIZE, 0);
        self.dev.write(block_start, buf)
    }


    pub fn poll_write<'a>(&'a self, block_start: u32, buf: &'a [u8]) -> BlockWriter<'a, Dev> {
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
        obj_pos: &ObjectPos,
        obj_tag: &ObjectTag,
    ) -> Result<Object, TdbError> {
        let buf_len = obj_pos.get_blk_len();
        let mut buf = Vec::with_capacity(buf_len);
        self.sync_read_node_raw(obj_pos, obj_tag, &mut buf)
    }

    pub fn sync_read_node_raw(
        &self,
        obj_pos: &ObjectPos,
        obj_tag: &ObjectTag,
        buf: &mut [u8],
    ) -> Result<Object, TdbError> {
        assert!(buf.len() >= obj_pos.get_blk_len() as usize * BLOCK_SIZE);
        self.sync_read(obj_pos.get_bid(), buf)?;
        Object::read(&buf[obj_pos.get_inner_offset()..], obj_tag)
    }

    pub fn sync_write_node(&self, obj_pos: &ObjectPos, obj: &Object) -> Result<(), TdbError> {
        let buf_len = obj_pos.get_blk_len();
        let mut buf = Vec::with_capacity(buf_len);
        self.sync_write_node_raw(obj_pos, obj, &mut buf)
    }

    pub fn sync_write_node_raw(
        &self,
        obj_pos: &ObjectPos,
        obj: &Object,
        buf: &mut [u8],
    ) -> Result<(), TdbError> {
        assert!(buf.len() >= obj_pos.get_blk_len());
        if obj_pos.get_inner_offset() != 0 {
            self.sync_read(obj_pos.get_bid(), &mut buf[0..BLOCK_SIZE])?;
        }
        obj.write(buf)?;
        self.sync_write(obj_pos.get_bid(), buf)
    }

    pub async fn async_read_node_raw(
        &self,
        obj_pos: &ObjectPos,
        obj_tag: &ObjectTag,
        buf: &mut [u8],
    ) -> Result<Object, TdbError> {
    assert!(buf.len() >= obj_pos.get_blk_len() );
        self.poll_read(obj_pos.get_bid(), buf).await?;
        Object::read(&buf[obj_pos.get_inner_offset()..], obj_tag)
    }

    pub async fn async_write_node_raw(
        &self,
        obj_pos: &ObjectPos,
        obj: &Object,
        buf: &mut [u8],
    ) -> Result<(), TdbError> {
        assert!(buf.len() >= obj_pos.get_blk_len() );
        if obj_pos.get_inner_offset() != 0 {
            self.poll_write(obj_pos.get_bid(), &mut buf[0..BLOCK_SIZE])
                .await?;
        }
        obj.write(buf)?;
        self.poll_write(obj_pos.get_bid(), buf).await
    }
}

pub struct Dummy {}
impl RawBlockDev for Dummy {
    fn read(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError> {
        Ok(())
    }
    fn write(&self, block_start: u32, buf: &[u8]) -> Result<(), TdbError> {
        Ok(())
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
