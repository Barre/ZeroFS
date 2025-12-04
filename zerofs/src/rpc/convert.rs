use crate::checkpoint_manager::CheckpointInfo;
use crate::fs::tracing::{FileAccessEvent, FileOperation};
use crate::rpc::proto;
use prost_types::Timestamp;
use std::fmt;
use uuid::Uuid;

impl fmt::Display for proto::FileOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            proto::FileOperation::Read => "read   ",
            proto::FileOperation::Write => "write  ",
            proto::FileOperation::Create => "create ",
            proto::FileOperation::Remove => "remove ",
            proto::FileOperation::Rename => "rename ",
            proto::FileOperation::Mkdir => "mkdir  ",
            proto::FileOperation::Readdir => "readdir",
            proto::FileOperation::Lookup => "lookup ",
            proto::FileOperation::Setattr => "setattr",
            proto::FileOperation::Link => "link   ",
            proto::FileOperation::Symlink => "symlink",
            proto::FileOperation::Mknod => "mknod  ",
            proto::FileOperation::Trim => "trim   ",
        };
        write!(f, "{}", s)
    }
}

impl From<CheckpointInfo> for proto::CheckpointInfo {
    fn from(info: CheckpointInfo) -> Self {
        proto::CheckpointInfo {
            id: info.id.to_string(),
            name: info.name,
            created_at: Some(Timestamp {
                seconds: info.created_at as i64,
                nanos: 0,
            }),
        }
    }
}

impl TryFrom<proto::CheckpointInfo> for CheckpointInfo {
    type Error = uuid::Error;

    fn try_from(proto: proto::CheckpointInfo) -> Result<Self, Self::Error> {
        Ok(CheckpointInfo {
            id: Uuid::parse_str(&proto.id)?,
            name: proto.name,
            created_at: proto.created_at.map(|t| t.seconds as u64).unwrap_or(0),
        })
    }
}

impl From<FileAccessEvent> for proto::FileAccessEvent {
    fn from(event: FileAccessEvent) -> Self {
        let (operation, params) = match event.operation {
            FileOperation::Read { offset, length } => (
                proto::FileOperation::Read as i32,
                proto::OperationParams {
                    offset: Some(offset),
                    length: Some(length),
                    ..Default::default()
                },
            ),
            FileOperation::Write { offset, length } => (
                proto::FileOperation::Write as i32,
                proto::OperationParams {
                    offset: Some(offset),
                    length: Some(length),
                    ..Default::default()
                },
            ),
            FileOperation::Create { mode } => (
                proto::FileOperation::Create as i32,
                proto::OperationParams {
                    mode: Some(mode),
                    ..Default::default()
                },
            ),
            FileOperation::Remove => (
                proto::FileOperation::Remove as i32,
                proto::OperationParams::default(),
            ),
            FileOperation::Rename { new_path } => (
                proto::FileOperation::Rename as i32,
                proto::OperationParams {
                    new_path: Some(new_path),
                    ..Default::default()
                },
            ),
            FileOperation::Mkdir { mode } => (
                proto::FileOperation::Mkdir as i32,
                proto::OperationParams {
                    mode: Some(mode),
                    ..Default::default()
                },
            ),
            FileOperation::Readdir { count } => (
                proto::FileOperation::Readdir as i32,
                proto::OperationParams {
                    length: Some(count as u64),
                    ..Default::default()
                },
            ),
            FileOperation::Lookup { filename } => (
                proto::FileOperation::Lookup as i32,
                proto::OperationParams {
                    filename: Some(filename),
                    ..Default::default()
                },
            ),
            FileOperation::Setattr { mode } => (
                proto::FileOperation::Setattr as i32,
                proto::OperationParams {
                    mode,
                    ..Default::default()
                },
            ),
            FileOperation::Link { new_path } => (
                proto::FileOperation::Link as i32,
                proto::OperationParams {
                    new_path: Some(new_path),
                    ..Default::default()
                },
            ),
            FileOperation::Symlink { target } => (
                proto::FileOperation::Symlink as i32,
                proto::OperationParams {
                    link_target: Some(target),
                    ..Default::default()
                },
            ),
            FileOperation::Mknod { mode } => (
                proto::FileOperation::Mknod as i32,
                proto::OperationParams {
                    mode: Some(mode),
                    ..Default::default()
                },
            ),
            FileOperation::Trim { offset, length } => (
                proto::FileOperation::Trim as i32,
                proto::OperationParams {
                    offset: Some(offset),
                    length: Some(length),
                    ..Default::default()
                },
            ),
        };

        proto::FileAccessEvent {
            timestamp: Some(Timestamp {
                seconds: event.timestamp as i64,
                nanos: 0,
            }),
            operation,
            path: event.path,
            params: Some(params),
        }
    }
}
