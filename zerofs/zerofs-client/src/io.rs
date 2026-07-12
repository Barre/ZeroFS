//! `tokio-io` feature: a [`FileCursor`] adapter that turns the positioned
//! [`File`] API into a stateful `AsyncRead + AsyncWrite + AsyncSeek` stream, so
//! `tokio::io::copy` and friends work against a ZeroFS file. Rust-only; never
//! crosses the FFI boundary.

use crate::error::ZeroFsError;
use crate::file::File;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

#[cfg(not(target_arch = "wasm32"))]
type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;
#[cfg(target_arch = "wasm32")]
type BoxFut<T> = Pin<Box<dyn Future<Output = T>>>;

enum State {
    Idle,
    Reading(BoxFut<Result<bytes::Bytes, ZeroFsError>>),
    /// A write in flight, plus how many bytes it covers (added to `pos` on success).
    Writing {
        fut: BoxFut<Result<(), ZeroFsError>>,
        len: u64,
    },
    /// A `SeekFrom::End` in flight: resolves the size, then offsets by `delta`.
    SeekingEnd {
        fut: BoxFut<Result<u64, ZeroFsError>>,
        delta: i64,
    },
}

/// An owned cursor over a [`File`] implementing `AsyncRead + AsyncWrite +
/// AsyncSeek`, built entirely on [`File::read_at`]/[`File::write_at`]. Multiple
/// cursors over one `File` are safe; each carries its own position and the
/// underlying I/O is positioned, so there is no shared seek pointer.
///
/// One operation is in flight at a time per cursor (it is driven by `&mut self`
/// like every async-io adapter); starting a seek while a read or write is
/// pending is rejected with `io::ErrorKind::Other`.
pub struct FileCursor {
    file: Arc<File>,
    pos: u64,
    state: State,
}

impl FileCursor {
    pub(crate) fn new(file: Arc<File>) -> Self {
        Self {
            file,
            pos: 0,
            state: State::Idle,
        }
    }

    /// The cursor's current absolute byte offset.
    pub fn position(&self) -> u64 {
        self.pos
    }
}

impl AsyncRead for FileCursor {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                State::Reading(fut) => {
                    let data = ready!(fut.as_mut().poll(cx));
                    self.state = State::Idle;
                    let data = data.map_err(io::Error::from)?;
                    let n = data.len().min(buf.remaining());
                    buf.put_slice(&data[..n]);
                    self.pos += n as u64;
                    return Poll::Ready(Ok(()));
                }
                State::Idle => {
                    if buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                    // Cap one read at the negotiated chunk so each poll is a
                    // single round trip; a partial fill is normal for AsyncRead.
                    let chunk = self.file.max_read_chunk().max(1) as usize;
                    let len = buf.remaining().min(chunk) as u32;
                    let offset = self.pos;
                    let file = Arc::clone(&self.file);
                    self.state =
                        State::Reading(Box::pin(async move { file.read_at(offset, len).await }));
                }
                _ => return Poll::Ready(Err(busy())),
            }
        }
    }
}

impl AsyncWrite for FileCursor {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            match &mut self.state {
                State::Writing { fut, len } => {
                    let res = ready!(fut.as_mut().poll(cx));
                    let len = *len;
                    self.state = State::Idle;
                    res.map_err(io::Error::from)?;
                    self.pos += len;
                    return Poll::Ready(Ok(len as usize));
                }
                State::Idle => {
                    if buf.is_empty() {
                        return Poll::Ready(Ok(0));
                    }
                    // write_at writes all of `data` (chunked internally), so one
                    // poll consumes the whole buffer.
                    let offset = self.pos;
                    let data = buf.to_vec();
                    let len = data.len() as u64;
                    let file = Arc::clone(&self.file);
                    self.state = State::Writing {
                        fut: Box::pin(async move { file.write_at(offset, &data).await }),
                        len,
                    };
                }
                _ => return Poll::Ready(Err(busy())),
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Drive an in-flight write to completion; there is no client-side write
        // buffer beyond that (each write_at awaits the server), so once idle
        // everything sent is acknowledged.
        match &mut self.state {
            State::Writing { fut, len } => {
                let res = ready!(fut.as_mut().poll(cx));
                let len = *len;
                self.state = State::Idle;
                res.map_err(io::Error::from)?;
                self.pos += len;
                Poll::Ready(Ok(()))
            }
            _ => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

impl AsyncSeek for FileCursor {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        if !matches!(self.state, State::Idle) {
            return Err(busy());
        }
        match position {
            SeekFrom::Start(n) => self.pos = n,
            SeekFrom::Current(delta) => {
                self.pos = self
                    .pos
                    .checked_add_signed(delta)
                    .ok_or_else(negative_seek)?;
            }
            SeekFrom::End(delta) => {
                let file = Arc::clone(&self.file);
                self.state = State::SeekingEnd {
                    fut: Box::pin(async move { file.metadata().await.map(|m| m.size) }),
                    delta,
                };
            }
        }
        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        if let State::SeekingEnd { fut, delta } = &mut self.state {
            let size = ready!(fut.as_mut().poll(cx));
            let delta = *delta;
            self.state = State::Idle;
            let size = size.map_err(io::Error::from)?;
            self.pos = size.checked_add_signed(delta).ok_or_else(negative_seek)?;
        }
        Poll::Ready(Ok(self.pos))
    }
}

fn busy() -> io::Error {
    io::Error::other("zerofs FileCursor: another I/O operation is already in flight")
}

fn negative_seek() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "zerofs FileCursor: seek to a negative or out-of-range offset",
    )
}
