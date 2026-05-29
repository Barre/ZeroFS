use std::future::Future;
use tokio::task::JoinHandle;

pub fn spawn_named<T, F>(name: &str, future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::Builder::new()
        .name(name)
        .spawn(future)
        .expect("failed to spawn task")
}

pub fn spawn_blocking_named<T, F>(name: &str, f: F) -> std::io::Result<JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::Builder::new().name(name).spawn_blocking(f)
}
