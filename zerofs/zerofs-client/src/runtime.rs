use std::future::Future;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) use tokio::time::timeout;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn sleep(duration: Duration) {
    let millis = duration.as_millis().min(u32::MAX as u128) as u32;
    gloo_timers::future::TimeoutFuture::new(millis).await;
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, ()>
where
    F: Future,
{
    use std::pin::pin;
    use std::task::Poll;

    let mut future = pin!(future);
    let mut timer = pin!(sleep(duration));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(output) = future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        if let Poll::Ready(()) = timer.as_mut().poll(cx) {
            return Poll::Ready(Err(()));
        }
        Poll::Pending
    })
    .await
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}
