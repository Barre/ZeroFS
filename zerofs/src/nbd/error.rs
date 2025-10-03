use thiserror::Error;

#[derive(Error, Debug)]
pub enum NBDError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Device not found: {}", String::from_utf8_lossy(.0))]
    DeviceNotFound(Vec<u8>),

    #[error("Client does not support required features")]
    IncompatibleClient,

    #[error("Deku parsing error: {0}")]
    Deku(#[from] deku::DekuError),

    #[error("Filesystem error: {0}")]
    Filesystem(String),
}

pub type Result<T> = std::result::Result<T, NBDError>;
