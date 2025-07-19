use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use crate::filesystem::SlateDbFs;

// NBD Magic numbers
const NBD_MAGIC: u64 = 0x4e42444d41474943; // "NBDMAGIC"
const NBD_IHAVEOPT: u64 = 0x49484156454F5054; // "IHAVEOPT"  
const NBD_REQUEST_MAGIC: u32 = 0x25609513;
const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;
const NBD_REPLY_MAGIC: u64 = 0x3e889045565a9;

// Handshake flags
const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// Client flags
const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1 << 0;

// Transmission flags
const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
const NBD_FLAG_SEND_FUA: u16 = 1 << 3;
const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;

// Options
const NBD_OPT_EXPORT_NAME: u32 = 1;
const NBD_OPT_ABORT: u32 = 2;
const NBD_OPT_LIST: u32 = 3;
const NBD_OPT_GO: u32 = 7;
const NBD_OPT_STRUCTURED_REPLY: u32 = 8;

// Option replies
const NBD_REP_ACK: u32 = 1;
const NBD_REP_SERVER: u32 = 2;
const NBD_REP_INFO: u32 = 3;
const NBD_REP_ERR_UNSUP: u32 = 0x80000001;
const NBD_REP_ERR_INVALID: u32 = 0x80000003;

// Info types
const NBD_INFO_EXPORT: u16 = 0;

// Commands
const NBD_CMD_READ: u16 = 0;
const NBD_CMD_WRITE: u16 = 1;
const NBD_CMD_DISC: u16 = 2;
const NBD_CMD_FLUSH: u16 = 3;
const NBD_CMD_TRIM: u16 = 4;
const NBD_CMD_WRITE_ZEROES: u16 = 6;

// Errors
const NBD_EIO: u32 = 5;
const NBD_EINVAL: u32 = 22;

#[derive(Clone)]
pub struct NBDDevice {
    pub name: String,
    pub size: u64,
}

pub struct NBDServer {
    filesystem: Arc<SlateDbFs>,
    devices: HashMap<String, NBDDevice>,
    port: u16,
}

impl NBDServer {
    pub fn new(filesystem: Arc<SlateDbFs>, port: u16) -> Self {
        Self {
            filesystem,
            devices: HashMap::new(),
            port,
        }
    }

    pub fn add_device(&mut self, name: String, size: u64) {
        let device = NBDDevice {
            name: name.clone(),
            size,
        };
        self.devices.insert(name, device);
    }

    pub async fn start(&self) -> io::Result<()> {
        // Initialize device files (.nbd directory created in main)
        for device in self.devices.values() {
            self.initialize_device(device).await?;
        }

        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port)).await?;
        info!("NBD server listening on port {}", self.port);

        loop {
            let (stream, addr) = listener.accept().await?;
            info!("NBD client connected from {}", addr);

            let filesystem = Arc::clone(&self.filesystem);
            let devices = self.devices.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, filesystem, devices).await {
                    error!("Error handling NBD client {}: {}", addr, e);
                }
            });
        }
    }

    async fn initialize_device(&self, device: &NBDDevice) -> io::Result<()> {
        use zerofs_nfsserve::nfs::{nfsstring, sattr3, set_mode3};
        use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

        let auth = AuthContext {
            uid: 0,
            gid: 0,
            gids: vec![],
        };
        let nbd_name = nfsstring(b".nbd".to_vec());
        let device_name = nfsstring(device.name.as_bytes().to_vec());

        // Check if device file exists, create it if not
        let nbd_dir_inode = self
            .filesystem
            .lookup(&auth, 0, &nbd_name)
            .await
            .map_err(|e| io::Error::other(format!("Failed to lookup .nbd directory: {e:?}")))?;

        match self
            .filesystem
            .lookup(&auth, nbd_dir_inode, &device_name)
            .await
        {
            Ok(device_inode) => {
                // Device exists, validate that the size matches
                let existing_attr =
                    self.filesystem
                        .getattr(&auth, device_inode)
                        .await
                        .map_err(|e| {
                            io::Error::other(format!("Failed to get device attributes: {e:?}"))
                        })?;

                if existing_attr.size != device.size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "NBD device {} size mismatch: existing size is {} bytes, requested size is {} bytes. Cannot resize existing devices.",
                            device.name, existing_attr.size, device.size
                        ),
                    ));
                }

                debug!(
                    "NBD device file {} already exists with correct size {}",
                    device.name, device.size
                );
                Ok(())
            }
            Err(_) => {
                // Create device file
                debug!(
                    "Creating NBD device file {} with size {}",
                    device.name, device.size
                );
                let attr = sattr3 {
                    mode: set_mode3::mode(0o600),
                    uid: zerofs_nfsserve::nfs::set_uid3::uid(0),
                    gid: zerofs_nfsserve::nfs::set_gid3::gid(0),
                    size: zerofs_nfsserve::nfs::set_size3::Void,
                    atime: zerofs_nfsserve::nfs::set_atime::DONT_CHANGE,
                    mtime: zerofs_nfsserve::nfs::set_mtime::DONT_CHANGE,
                };

                let (device_inode, _) = self
                    .filesystem
                    .create(&auth, nbd_dir_inode, &device_name, attr)
                    .await
                    .map_err(|e| {
                        io::Error::other(format!("Failed to create device file: {e:?}"))
                    })?;

                // Set the file size by writing a zero byte at the end
                if device.size > 0 {
                    let data = vec![0u8; 1];
                    self.filesystem
                        .write(&auth, device_inode, device.size - 1, &data)
                        .await
                        .map_err(|e| {
                            io::Error::other(format!("Failed to set device size: {e:?}"))
                        })?;
                }

                Ok(())
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    filesystem: Arc<SlateDbFs>,
    devices: HashMap<String, NBDDevice>,
) -> io::Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Handshake phase
    perform_handshake(&mut reader, &mut writer, &devices).await?;

    // Get selected device from handshake
    let device = wait_for_export_selection(&mut reader, &mut writer, &devices).await?;

    info!("Client selected device: {}", device.name);

    // Transmission phase
    handle_transmission(reader, writer, filesystem, device).await?;

    Ok(())
}

async fn perform_handshake<R, W>(
    reader: &mut R,
    writer: &mut W,
    _devices: &HashMap<String, NBDDevice>,
) -> io::Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // Send initial handshake
    writer.write_u64(NBD_MAGIC).await?;
    writer.write_u64(NBD_IHAVEOPT).await?;
    writer
        .write_u16(NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES)
        .await?;
    writer.flush().await?;

    // Read client flags
    let client_flags = reader.read_u32().await?;
    debug!("Client flags: 0x{:x}", client_flags);

    if (client_flags & NBD_FLAG_C_FIXED_NEWSTYLE) == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Client does not support fixed newstyle",
        ));
    }

    Ok(())
}

async fn wait_for_export_selection<R, W>(
    reader: &mut R,
    writer: &mut W,
    devices: &HashMap<String, NBDDevice>,
) -> io::Result<NBDDevice>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    loop {
        // Read option header
        let magic = reader.read_u64().await?;
        if magic != NBD_IHAVEOPT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid option magic",
            ));
        }

        let option = reader.read_u32().await?;
        let length = reader.read_u32().await?;

        match option {
            NBD_OPT_LIST => {
                handle_list_option(reader, writer, devices, length).await?;
            }
            NBD_OPT_EXPORT_NAME => {
                return handle_export_name_option(reader, writer, devices, length).await;
            }
            NBD_OPT_GO => {
                return handle_go_option(reader, writer, devices, length).await;
            }
            NBD_OPT_STRUCTURED_REPLY => {
                handle_structured_reply_option(reader, writer, length).await?;
            }
            NBD_OPT_ABORT => {
                send_option_reply(writer, option, NBD_REP_ACK, &[]).await?;
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "Client aborted",
                ));
            }
            _ => {
                // Skip unknown option data
                let mut buf = vec![0u8; length as usize];
                reader.read_exact(&mut buf).await?;
                send_option_reply(writer, option, NBD_REP_ERR_UNSUP, &[]).await?;
            }
        }
    }
}

async fn handle_list_option<R, W>(
    reader: &mut R,
    writer: &mut W,
    devices: &HashMap<String, NBDDevice>,
    length: u32,
) -> io::Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // Skip any data
    if length > 0 {
        let mut buf = vec![0u8; length as usize];
        reader.read_exact(&mut buf).await?;
    }

    // Send device list
    for device in devices.values() {
        let name_bytes = device.name.as_bytes();
        let mut reply_data = Vec::new();
        reply_data.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
        reply_data.extend_from_slice(name_bytes);

        send_option_reply(writer, NBD_OPT_LIST, NBD_REP_SERVER, &reply_data).await?;
    }

    // Send final ACK
    send_option_reply(writer, NBD_OPT_LIST, NBD_REP_ACK, &[]).await?;
    writer.flush().await?;
    Ok(())
}

async fn handle_export_name_option<R, W>(
    reader: &mut R,
    writer: &mut W,
    devices: &HashMap<String, NBDDevice>,
    length: u32,
) -> io::Result<NBDDevice>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut name_buf = vec![0u8; length as usize];
    reader.read_exact(&mut name_buf).await?;
    let name = String::from_utf8_lossy(&name_buf);

    let device = devices.get(name.as_ref()).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Device {name} not found"))
    })?;

    // Send export info
    writer.write_u64(device.size).await?;
    writer
        .write_u16(
            NBD_FLAG_HAS_FLAGS
                | NBD_FLAG_SEND_FLUSH
                | NBD_FLAG_SEND_FUA
                | NBD_FLAG_SEND_TRIM
                | NBD_FLAG_SEND_WRITE_ZEROES,
        )
        .await?;
    writer.flush().await?;
    // No trailing zeroes due to NBD_FLAG_NO_ZEROES

    Ok(device.clone())
}

async fn handle_go_option<R, W>(
    reader: &mut R,
    writer: &mut W,
    devices: &HashMap<String, NBDDevice>,
    length: u32,
) -> io::Result<NBDDevice>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut data = vec![0u8; length as usize];
    reader.read_exact(&mut data).await?;

    if data.len() < 4 {
        send_option_reply(writer, NBD_OPT_GO, NBD_REP_ERR_INVALID, &[]).await?;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid GO option",
        ));
    }

    let name_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + name_len + 2 {
        send_option_reply(writer, NBD_OPT_GO, NBD_REP_ERR_INVALID, &[]).await?;
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid GO option",
        ));
    }

    let name = String::from_utf8_lossy(&data[4..4 + name_len]);
    let device = devices.get(name.as_ref()).ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("Device {name} not found"))
    })?;

    // Send NBD_INFO_EXPORT
    let mut info_data = Vec::new();
    info_data.extend_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
    info_data.extend_from_slice(&device.size.to_be_bytes());
    info_data.extend_from_slice(
        &(NBD_FLAG_HAS_FLAGS
            | NBD_FLAG_SEND_FLUSH
            | NBD_FLAG_SEND_FUA
            | NBD_FLAG_SEND_TRIM
            | NBD_FLAG_SEND_WRITE_ZEROES)
            .to_be_bytes(),
    );

    send_option_reply(writer, NBD_OPT_GO, NBD_REP_INFO, &info_data).await?;
    send_option_reply(writer, NBD_OPT_GO, NBD_REP_ACK, &[]).await?;
    writer.flush().await?;

    Ok(device.clone())
}

async fn handle_structured_reply_option<R, W>(
    reader: &mut R,
    writer: &mut W,
    length: u32,
) -> io::Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    // Skip any data
    if length > 0 {
        let mut buf = vec![0u8; length as usize];
        reader.read_exact(&mut buf).await?;
    }

    // We don't support structured replies for now
    send_option_reply(writer, NBD_OPT_STRUCTURED_REPLY, NBD_REP_ERR_UNSUP, &[]).await?;
    writer.flush().await?;
    Ok(())
}

async fn send_option_reply<W>(
    writer: &mut W,
    option: u32,
    reply_type: u32,
    data: &[u8],
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_u64(NBD_REPLY_MAGIC).await?;
    writer.write_u32(option).await?;
    writer.write_u32(reply_type).await?;
    writer.write_u32(data.len() as u32).await?;
    if !data.is_empty() {
        writer.write_all(data).await?;
    }
    Ok(())
}

async fn handle_transmission<R, W>(
    mut reader: R,
    mut writer: W,
    filesystem: Arc<SlateDbFs>,
    device: NBDDevice,
) -> io::Result<()>
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    use zerofs_nfsserve::nfs::nfsstring;
    use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

    let auth = AuthContext {
        uid: 0,
        gid: 0,
        gids: vec![],
    };
    let nbd_name = nfsstring(b".nbd".to_vec());
    let device_name = nfsstring(device.name.as_bytes().to_vec());

    // Get device inode
    let nbd_dir_inode = filesystem
        .lookup(&auth, 0, &nbd_name)
        .await
        .map_err(|e| io::Error::other(format!("Failed to lookup .nbd directory: {e:?}")))?;

    let device_inode = filesystem
        .lookup(&auth, nbd_dir_inode, &device_name)
        .await
        .map_err(|e| io::Error::other(format!("Failed to lookup device file: {e:?}")))?;

    loop {
        // Read request header
        let magic = reader.read_u32().await?;
        if magic != NBD_REQUEST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid request magic",
            ));
        }

        let flags = reader.read_u16().await?;
        let cmd_type = reader.read_u16().await?;
        let cookie = reader.read_u64().await?;
        let offset = reader.read_u64().await?;
        let length = reader.read_u32().await?;

        debug!(
            "NBD command: type={}, offset={}, length={}",
            cmd_type, offset, length
        );

        let error = match cmd_type {
            NBD_CMD_READ => {
                handle_read_command(
                    &filesystem,
                    device_inode,
                    &mut writer,
                    cookie,
                    offset,
                    length,
                )
                .await
            }
            NBD_CMD_WRITE => {
                handle_write_command(
                    &filesystem,
                    device_inode,
                    &mut reader,
                    &mut writer,
                    cookie,
                    offset,
                    length,
                    flags,
                )
                .await
            }
            NBD_CMD_DISC => {
                info!("Client disconnecting");
                return Ok(());
            }
            NBD_CMD_FLUSH => {
                handle_flush_command(&filesystem, device_inode, &mut writer, cookie).await
            }
            NBD_CMD_TRIM => {
                handle_trim_command(
                    &filesystem,
                    device_inode,
                    &mut writer,
                    cookie,
                    offset,
                    length,
                )
                .await
            }
            NBD_CMD_WRITE_ZEROES => {
                handle_write_zeroes_command(
                    &filesystem,
                    device_inode,
                    &mut writer,
                    cookie,
                    offset,
                    length,
                )
                .await
            }
            _ => {
                let _ = send_simple_reply(&mut writer, cookie, NBD_EINVAL, &[]).await;
                0
            }
        };

        if error != 0 {
            warn!("NBD command failed with error: {}", error);
        }
    }
}

async fn handle_read_command<W>(
    filesystem: &SlateDbFs,
    inode: u64,
    writer: &mut W,
    cookie: u64,
    offset: u64,
    length: u32,
) -> u32
where
    W: AsyncWriteExt + Unpin,
{
    use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

    let auth = AuthContext {
        uid: 0,
        gid: 0,
        gids: vec![],
    };

    match filesystem.read(&auth, inode, offset, length).await {
        Ok((data, _)) => {
            if send_simple_reply(writer, cookie, 0, &data).await.is_err() {
                return NBD_EIO;
            }
            0
        }
        Err(_) => {
            let _ = send_simple_reply(writer, cookie, NBD_EIO, &[]).await;
            NBD_EIO
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_write_command<R, W>(
    filesystem: &SlateDbFs,
    inode: u64,
    reader: &mut R,
    writer: &mut W,
    cookie: u64,
    offset: u64,
    length: u32,
    _flags: u16,
) -> u32
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

    let auth = AuthContext {
        uid: 0,
        gid: 0,
        gids: vec![],
    };

    let mut data = vec![0u8; length as usize];
    if reader.read_exact(&mut data).await.is_err() {
        let _ = send_simple_reply(writer, cookie, NBD_EIO, &[]).await;
        return NBD_EIO;
    }

    match filesystem.write(&auth, inode, offset, &data).await {
        Ok(_) => {
            // Note: FUA (Force Unit Access) is handled by the filesystem layer
            if send_simple_reply(writer, cookie, 0, &[]).await.is_err() {
                return NBD_EIO;
            }
            0
        }
        Err(_) => {
            let _ = send_simple_reply(writer, cookie, NBD_EIO, &[]).await;
            NBD_EIO
        }
    }
}

async fn handle_flush_command<W>(
    filesystem: &SlateDbFs,
    _inode: u64,
    writer: &mut W,
    cookie: u64,
) -> u32
where
    W: AsyncWriteExt + Unpin,
{
    match filesystem.db.flush().await {
        Ok(_) => {
            if send_simple_reply(writer, cookie, 0, &[]).await.is_err() {
                return NBD_EIO;
            }
            0
        }
        Err(e) => {
            error!("NBD flush failed: {}", e);
            let _ = send_simple_reply(writer, cookie, NBD_EIO, &[]).await;
            NBD_EIO
        }
    }
}

async fn handle_trim_command<W>(
    _filesystem: &SlateDbFs,
    _inode: u64,
    writer: &mut W,
    cookie: u64,
    _offset: u64,
    _length: u32,
) -> u32
where
    W: AsyncWriteExt + Unpin,
{
    // Just reply success - ZeroFS handles sparse storage automatically
    if send_simple_reply(writer, cookie, 0, &[]).await.is_err() {
        return NBD_EIO;
    }
    0
}

async fn handle_write_zeroes_command<W>(
    filesystem: &SlateDbFs,
    inode: u64,
    writer: &mut W,
    cookie: u64,
    offset: u64,
    length: u32,
) -> u32
where
    W: AsyncWriteExt + Unpin,
{
    use zerofs_nfsserve::vfs::{AuthContext, NFSFileSystem};

    let auth = AuthContext {
        uid: 0,
        gid: 0,
        gids: vec![],
    };
    let zero_data = vec![0u8; length as usize];

    match filesystem.write(&auth, inode, offset, &zero_data).await {
        Ok(_) => {
            if send_simple_reply(writer, cookie, 0, &[]).await.is_err() {
                return NBD_EIO;
            }
            0
        }
        Err(_) => {
            let _ = send_simple_reply(writer, cookie, NBD_EIO, &[]).await;
            NBD_EIO
        }
    }
}

async fn send_simple_reply<W>(
    writer: &mut W,
    cookie: u64,
    error: u32,
    data: &[u8],
) -> io::Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    writer.write_u32(NBD_SIMPLE_REPLY_MAGIC).await?;
    writer.write_u32(error).await?;
    writer.write_u64(cookie).await?;
    if !data.is_empty() {
        writer.write_all(data).await?;
    }
    writer.flush().await?;
    Ok(())
}
