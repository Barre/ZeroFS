//! `stream` feature: a [`futures_core::Stream`] adapter over [`Dir`] listing,
//! so a directory can be consumed with `StreamExt` (`.next()`, `.collect()`,
//! `try_for_each`, ...). Rust-only; never crosses the FFI boundary.

use crate::dir::Dir;
use crate::error::ZeroFsError;
use crate::types::DirEntry;
use futures_core::Stream;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

type BoxFut<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// A pull-based stream of [`DirEntry`] over an open [`Dir`], yielding entries
/// one at a time (fetched a server batch at a time underneath). It holds the
/// `Dir` open for its lifetime; an error ends the stream after it is yielded.
pub struct DirStream {
    dir: Arc<Dir>,
    buf: VecDeque<DirEntry>,
    fetch: Option<BoxFut<Result<Vec<DirEntry>, ZeroFsError>>>,
    done: bool,
}

impl DirStream {
    pub(crate) fn new(dir: Arc<Dir>) -> Self {
        Self {
            dir,
            buf: VecDeque::new(),
            fetch: None,
            done: false,
        }
    }
}

impl Stream for DirStream {
    type Item = Result<DirEntry, ZeroFsError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(entry) = self.buf.pop_front() {
                return Poll::Ready(Some(Ok(entry)));
            }
            if self.done {
                return Poll::Ready(None);
            }
            if self.fetch.is_none() {
                let dir = Arc::clone(&self.dir);
                self.fetch = Some(Box::pin(async move { dir.next_batch(None).await }));
            }
            let batch = ready!(self.fetch.as_mut().unwrap().as_mut().poll(cx));
            self.fetch = None;
            match batch {
                // An empty batch is end-of-directory.
                Ok(batch) if batch.is_empty() => {
                    self.done = true;
                    return Poll::Ready(None);
                }
                Ok(batch) => self.buf.extend(batch),
                Err(e) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }
    }
}
