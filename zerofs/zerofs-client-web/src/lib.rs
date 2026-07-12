use serde::Serialize;
use std::cell::RefCell;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use zerofs_client::{OpenOptions, SetAttrs};

#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

fn js_error(error: zerofs_client::ZeroFsError) -> JsValue {
    let errno = error.to_errno();
    let js_error = js_sys::Error::new(&error.to_string());
    let _ = js_sys::Reflect::set(
        js_error.as_ref(),
        &JsValue::from_str("ecode"),
        &JsValue::from_f64(errno as f64),
    );
    js_error.into()
}

fn serialized<T: Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let serializer =
        serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true);
    value
        .serialize(&serializer)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

#[derive(Serialize)]
struct WebTrafficStats {
    bytes_sent: u64,
    bytes_received: u64,
    operations: u64,
}

/// Browser-facing ZeroFS client backed by the shared Rust implementation.
#[wasm_bindgen(js_name = Client)]
pub struct WebClient {
    inner: Arc<zerofs_client::Client>,
}

#[wasm_bindgen(js_class = Client)]
impl WebClient {
    /// Connect to a `ws://` or `wss://` 9P endpoint.
    #[wasm_bindgen(js_name = connect)]
    pub async fn connect(target: String) -> Result<WebClient, JsValue> {
        let inner = zerofs_client::Client::connect(&target)
            .await
            .map_err(js_error)?;
        Ok(WebClient { inner })
    }

    #[wasm_bindgen(getter, js_name = connected)]
    pub fn connected(&self) -> bool {
        self.inner.is_connected()
    }

    #[wasm_bindgen(js_name = capabilities)]
    pub fn capabilities(&self) -> Result<JsValue, JsValue> {
        serialized(&self.inner.capabilities())
    }

    #[wasm_bindgen(js_name = trafficStats)]
    pub fn traffic_stats(&self) -> Result<JsValue, JsValue> {
        let stats = self.inner.traffic_stats();
        serialized(&WebTrafficStats {
            bytes_sent: stats.bytes_sent,
            bytes_received: stats.bytes_received,
            operations: stats.operations,
        })
    }

    pub async fn close(&self) {
        self.inner.close().await;
    }

    /// Flush all writes made through this client to durable storage.
    pub async fn sync(&self) -> Result<(), JsValue> {
        self.inner.sync().await.map_err(js_error)
    }

    pub async fn stat(&self, path: String) -> Result<JsValue, JsValue> {
        let metadata = self.inner.stat(path).await.map_err(js_error)?;
        serialized(&metadata)
    }

    #[wasm_bindgen(js_name = readDir)]
    pub async fn read_dir(&self, path: String) -> Result<JsValue, JsValue> {
        let entries = self.inner.read_dir(path).await.map_err(js_error)?;
        serialized(&entries)
    }

    #[wasm_bindgen(js_name = openDir)]
    pub async fn open_dir(&self, path: String) -> Result<WebDir, JsValue> {
        let inner = self.inner.open_dir(path).await.map_err(js_error)?;
        Ok(WebDir {
            inner: RefCell::new(Some(inner)),
        })
    }

    pub async fn read(&self, path: String) -> Result<Vec<u8>, JsValue> {
        self.inner
            .read(path)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = readRange)]
    pub async fn read_range(
        &self,
        path: String,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, JsValue> {
        self.inner
            .read_range(path, offset, length)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = openFile)]
    #[allow(clippy::too_many_arguments)]
    pub async fn open_file(
        &self,
        path: String,
        read: bool,
        write: bool,
        create: bool,
        create_new: bool,
        truncate: bool,
        mode: u32,
    ) -> Result<WebFile, JsValue> {
        let options = OpenOptions {
            read,
            write,
            create,
            create_new,
            truncate,
            mode,
        };
        let inner = self.inner.open(path, options).await.map_err(js_error)?;
        Ok(WebFile {
            inner: RefCell::new(Some(inner)),
        })
    }

    #[wasm_bindgen(js_name = createDir)]
    pub async fn create_dir(&self, path: String, mode: u32) -> Result<JsValue, JsValue> {
        let metadata = self.inner.create_dir(path, mode).await.map_err(js_error)?;
        serialized(&metadata)
    }

    #[wasm_bindgen(js_name = removeFile)]
    pub async fn remove_file(&self, path: String) -> Result<(), JsValue> {
        self.inner.remove_file(path).await.map_err(js_error)
    }

    #[wasm_bindgen(js_name = removeDir)]
    pub async fn remove_dir(&self, path: String) -> Result<(), JsValue> {
        self.inner.remove_dir(path).await.map_err(js_error)
    }

    pub async fn rename(&self, from: String, to: String) -> Result<(), JsValue> {
        self.inner.rename(from, to).await.map_err(js_error)
    }

    pub async fn symlink(&self, target: String, link_path: String) -> Result<JsValue, JsValue> {
        let metadata = self
            .inner
            .symlink(target, link_path)
            .await
            .map_err(js_error)?;
        serialized(&metadata)
    }

    #[wasm_bindgen(js_name = readLink)]
    pub async fn read_link(&self, path: String) -> Result<Vec<u8>, JsValue> {
        let target = self.inner.read_link(path).await.map_err(js_error)?;
        Ok(target.to_string_lossy().as_bytes().to_vec())
    }

    #[wasm_bindgen(js_name = setAttr)]
    pub async fn set_attr(
        &self,
        path: String,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> Result<JsValue, JsValue> {
        let metadata = self
            .inner
            .set_attr(
                path,
                SetAttrs {
                    mode,
                    uid,
                    gid,
                    ..SetAttrs::default()
                },
            )
            .await
            .map_err(js_error)?;
        serialized(&metadata)
    }
}

/// Stateful directory cursor used for cancellable, incremental listings.
#[wasm_bindgen(js_name = DirectoryHandle)]
pub struct WebDir {
    inner: RefCell<Option<Arc<zerofs_client::Dir>>>,
}

impl WebDir {
    fn inner(&self) -> Result<Arc<zerofs_client::Dir>, JsValue> {
        self.inner
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| js_error(zerofs_client::ZeroFsError::Closed))
    }
}

#[wasm_bindgen(js_class = DirectoryHandle)]
impl WebDir {
    #[wasm_bindgen(js_name = nextBatch)]
    pub async fn next_batch(&self) -> Result<JsValue, JsValue> {
        let entries = self.inner()?.next_batch(None).await.map_err(js_error)?;
        serialized(&entries)
    }

    pub async fn close(&self) {
        let inner = self.inner.borrow_mut().take();
        if let Some(inner) = inner {
            inner.close().await;
        }
    }
}

/// Stateful shared-library file handle used for streaming browser I/O.
#[wasm_bindgen(js_name = FileHandle)]
pub struct WebFile {
    inner: RefCell<Option<Arc<zerofs_client::File>>>,
}

impl WebFile {
    fn inner(&self) -> Result<Arc<zerofs_client::File>, JsValue> {
        self.inner
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| js_error(zerofs_client::ZeroFsError::Closed))
    }
}

#[wasm_bindgen(js_class = FileHandle)]
impl WebFile {
    #[wasm_bindgen(js_name = readAt)]
    pub async fn read_at(&self, offset: u64, length: u32) -> Result<Vec<u8>, JsValue> {
        self.inner()?
            .read_at(offset, length)
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = writeAt)]
    pub async fn write_at(&self, offset: u64, data: Vec<u8>) -> Result<(), JsValue> {
        self.inner()?
            .write_at(offset, &data)
            .await
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = syncAll)]
    pub async fn sync_all(&self) -> Result<(), JsValue> {
        self.inner()?.sync_all().await.map_err(js_error)
    }

    pub async fn close(&self) {
        let inner = self.inner.borrow_mut().take();
        if let Some(inner) = inner {
            inner.close().await;
        }
    }
}
