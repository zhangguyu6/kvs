use crate::{
    error::TxnError,
    storage::block_layout::{BlockId, RawBlock},
};
use futures::ready;
use lazy_static::lazy_static;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

lazy_static! {
    pub static ref G_DEV: BlockDev<FileBlockDev> = { BlockDev::new(FileBlockDev::default()) };
}

pub trait RawBlockDev {
    fn read(&self, blockid: BlockId, buf: &mut RawBlock) -> Result<(), TxnError>;
    fn poll_read(
        &self,
        cx: &mut Context,
        blockid: BlockId,
        buf: &mut RawBlock,
    ) -> Poll<Result<(), TxnError>>;
    fn write(&self, blockid: BlockId, buf: &RawBlock) -> Result<(), TxnError>;
    fn async_write(
        &self,
        cx: &mut Context,
        blockid: BlockId,
        buf: &RawBlock,
    ) -> Poll<Result<(), TxnError>>;
}

pub struct BlockReader<'a, R: ?Sized + Unpin> {
    reader: &'a R,
    block_id: BlockId,
    buf: &'a mut RawBlock,
}

impl<R: ?Sized + Unpin> Unpin for BlockReader<'_, R> {}

impl<R: RawBlockDev + ?Sized + Unpin> Future for BlockReader<'_, R> {
    type Output = Result<(), TxnError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let result = ready!(this.reader.poll_read(cx, this.block_id, this.buf));
        Poll::Ready(result)
    }
}

pub struct BlockWriter<'a, W: ?Sized + Unpin> {
    writer: &'a W,
    block_id: BlockId,
    buf: &'a RawBlock,
}

impl<R: ?Sized + Unpin> Unpin for BlockWriter<'_, R> {}

impl<R: RawBlockDev + ?Sized + Unpin> Future for BlockWriter<'_, R> {
    type Output = Result<(), TxnError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let result = ready!(this.writer.async_write(cx, this.block_id, this.buf));
        Poll::Ready(result)
    }
}

#[derive(Clone)]
pub struct BlockDev<Dev> {
    dev: Dev,
}

impl<Dev: RawBlockDev + Unpin> BlockDev<Dev> {
    pub fn new(dev: Dev) -> Self {
        Self { dev: dev }
    }
    pub fn sync_read(&self, block_id: BlockId, buf: &mut RawBlock) -> Result<(), TxnError> {
        self.dev.read(block_id, buf)
    }
    pub fn async_read<'a>(
        &'a self,
        block_id: BlockId,
        buf: &'a mut RawBlock,
    ) -> BlockReader<'a, Dev> {
        BlockReader {
            reader: &self.dev,
            block_id: block_id,
            buf: buf,
        }
    }
    pub fn sync_write(&self, block_id: BlockId, buf: &RawBlock) -> Result<(), TxnError> {
        self.dev.write(block_id, buf)
    }
    pub fn async_write<'a>(&'a self, block_id: BlockId, buf: &'a RawBlock) -> BlockWriter<'a, Dev> {
        BlockWriter {
            writer: &self.dev,
            block_id: block_id,
            buf: buf,
        }
    }
}

pub struct FileBlockDev {}

impl Default for FileBlockDev {
    fn default() -> Self {
        unimplemented!()
    }
}

impl RawBlockDev for FileBlockDev {
    fn read(&self, blockid: BlockId, buf: &mut RawBlock) -> Result<(), TxnError> {
        unimplemented!()
    }
    fn poll_read(
        &self,
        cx: &mut Context,
        blockid: BlockId,
        buf: &mut RawBlock,
    ) -> Poll<Result<(), TxnError>> {
        unimplemented!()
    }
    fn write(&self, blockid: BlockId, buf: &RawBlock) -> Result<(), TxnError> {
        unimplemented!()
    }
    fn async_write(
        &self,
        cx: &mut Context,
        blockid: BlockId,
        buf: &RawBlock,
    ) -> Poll<Result<(), TxnError>> {
        unimplemented!()
    }
}
