use std::future::Future;
use std::time::Duration;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct Clock(std::time::Instant);

#[cfg(not(target_arch = "wasm32"))]
impl Clock {
    pub(crate) fn now() -> Self {
        Self(std::time::Instant::now())
    }

    pub(crate) fn elapsed_millis(&self) -> u64 {
        self.0.elapsed().as_millis() as u64
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) struct Clock(f64);

#[cfg(target_arch = "wasm32")]
fn monotonic_millis() -> f64 {
    use wasm_bindgen::JsCast;

    js_sys::Reflect::get(
        js_sys::global().as_ref(),
        &wasm_bindgen::JsValue::from_str("performance"),
    )
    .ok()
    .and_then(|value| value.dyn_into::<web_sys::Performance>().ok())
    .map_or_else(js_sys::Date::now, |performance| performance.now())
}

#[cfg(target_arch = "wasm32")]
impl Clock {
    pub(crate) fn now() -> Self {
        Self(monotonic_millis())
    }

    pub(crate) fn elapsed_millis(&self) -> u64 {
        (monotonic_millis() - self.0).max(0.0) as u64
    }
}

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

#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn yield_now() {
    tokio::task::yield_now().await;
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
    use futures::FutureExt;
    use futures::future::{Either, select};

    let future = future.fuse();
    let timer = sleep(duration).fuse();
    futures::pin_mut!(future, timer);
    match select(future, timer).await {
        Either::Left((output, _)) => Ok(output),
        Either::Right(((), _)) => Err(()),
    }
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn spawn<F>(future: F)
where
    F: Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn yield_now() {
    gloo_timers::future::TimeoutFuture::new(0).await;
}
