use std::future::Future;
use tokio::runtime::Handle;
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

pub fn spawn_named_on<T, F>(name: &str, future: F, handle: &Handle) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::Builder::new()
        .name(name)
        .spawn_on(future, handle)
        .expect("failed to spawn task")
}
