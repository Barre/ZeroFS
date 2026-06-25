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
            proto::FileOperation::Fsync => "fsync  ",
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
            FileOperation::Fsync => (
                proto::FileOperation::Fsync as i32,
                proto::OperationParams::default(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_file_operation_display_is_named_and_padded() {
        use proto::FileOperation as P;
        let cases = [
            (P::Read, "read   "),
            (P::Write, "write  "),
            (P::Create, "create "),
            (P::Remove, "remove "),
            (P::Rename, "rename "),
            (P::Mkdir, "mkdir  "),
            (P::Readdir, "readdir"),
            (P::Lookup, "lookup "),
            (P::Setattr, "setattr"),
            (P::Link, "link   "),
            (P::Symlink, "symlink"),
            (P::Mknod, "mknod  "),
            (P::Trim, "trim   "),
            (P::Fsync, "fsync  "),
        ];
        for (op, want) in cases {
            assert_eq!(format!("{op}"), want);
            assert_eq!(want.len(), 7, "display is padded to a fixed width");
        }
    }

    #[test]
    fn checkpoint_info_round_trips() {
        let id = Uuid::parse_str("12345678-1234-1234-1234-1234567890ab").unwrap();
        let info = CheckpointInfo {
            id,
            name: "snap".to_string(),
            created_at: 1_700_000_000,
        };
        let p = proto::CheckpointInfo::from(info.clone());
        assert_eq!(p.id, id.to_string());
        assert_eq!(p.name, "snap");
        assert_eq!(p.created_at.unwrap().seconds, 1_700_000_000);

        let back = CheckpointInfo::try_from(p).unwrap();
        assert_eq!(back.id, id);
        assert_eq!(back.name, "snap");
        assert_eq!(back.created_at, 1_700_000_000);
    }

    #[test]
    fn checkpoint_info_try_from_rejects_a_bad_uuid() {
        let p = proto::CheckpointInfo {
            id: "not-a-uuid".to_string(),
            name: "x".to_string(),
            created_at: None,
        };
        assert!(CheckpointInfo::try_from(p).is_err());
    }

    #[test]
    fn checkpoint_info_try_from_defaults_a_missing_timestamp() {
        let p = proto::CheckpointInfo {
            id: Uuid::nil().to_string(),
            name: "x".to_string(),
            created_at: None,
        };
        assert_eq!(CheckpointInfo::try_from(p).unwrap().created_at, 0);
    }

    fn proto_event(operation: FileOperation) -> proto::FileAccessEvent {
        proto::FileAccessEvent::from(FileAccessEvent {
            timestamp: 42,
            operation,
            path: "/p".to_string(),
        })
    }

    fn code(operation: FileOperation) -> i32 {
        proto_event(operation).operation
    }

    fn params(operation: FileOperation) -> proto::OperationParams {
        proto_event(operation).params.unwrap()
    }

    #[test]
    fn every_operation_maps_to_its_proto_code() {
        use proto::FileOperation as P;
        assert_eq!(
            code(FileOperation::Read {
                offset: 0,
                length: 0
            }),
            P::Read as i32
        );
        assert_eq!(
            code(FileOperation::Write {
                offset: 0,
                length: 0
            }),
            P::Write as i32
        );
        assert_eq!(code(FileOperation::Create { mode: 0 }), P::Create as i32);
        assert_eq!(code(FileOperation::Remove), P::Remove as i32);
        assert_eq!(
            code(FileOperation::Rename {
                new_path: "x".into()
            }),
            P::Rename as i32
        );
        assert_eq!(code(FileOperation::Mkdir { mode: 0 }), P::Mkdir as i32);
        assert_eq!(code(FileOperation::Readdir { count: 0 }), P::Readdir as i32);
        assert_eq!(
            code(FileOperation::Lookup {
                filename: "x".into()
            }),
            P::Lookup as i32
        );
        assert_eq!(
            code(FileOperation::Setattr { mode: None }),
            P::Setattr as i32
        );
        assert_eq!(
            code(FileOperation::Link {
                new_path: "x".into()
            }),
            P::Link as i32
        );
        assert_eq!(
            code(FileOperation::Symlink { target: "x".into() }),
            P::Symlink as i32
        );
        assert_eq!(code(FileOperation::Mknod { mode: 0 }), P::Mknod as i32);
        assert_eq!(
            code(FileOperation::Trim {
                offset: 0,
                length: 0
            }),
            P::Trim as i32
        );
        assert_eq!(code(FileOperation::Fsync), P::Fsync as i32);
    }

    #[test]
    fn operation_params_carry_the_right_fields() {
        let p = params(FileOperation::Read {
            offset: 10,
            length: 20,
        });
        assert_eq!((p.offset, p.length), (Some(10), Some(20)));
        let p = params(FileOperation::Write {
            offset: 5,
            length: 6,
        });
        assert_eq!((p.offset, p.length), (Some(5), Some(6)));
        let p = params(FileOperation::Trim {
            offset: 1,
            length: 2,
        });
        assert_eq!((p.offset, p.length), (Some(1), Some(2)));

        assert_eq!(
            params(FileOperation::Create { mode: 0o644 }).mode,
            Some(0o644)
        );
        assert_eq!(
            params(FileOperation::Mkdir { mode: 0o755 }).mode,
            Some(0o755)
        );
        assert_eq!(
            params(FileOperation::Mknod { mode: 0o600 }).mode,
            Some(0o600)
        );

        assert_eq!(params(FileOperation::Readdir { count: 7 }).length, Some(7));

        assert_eq!(
            params(FileOperation::Rename {
                new_path: "/n".into()
            })
            .new_path,
            Some("/n".to_string())
        );
        assert_eq!(
            params(FileOperation::Link {
                new_path: "/l".into()
            })
            .new_path,
            Some("/l".to_string())
        );
        assert_eq!(
            params(FileOperation::Lookup {
                filename: "f".into()
            })
            .filename,
            Some("f".to_string())
        );
        assert_eq!(
            params(FileOperation::Symlink {
                target: "/t".into()
            })
            .link_target,
            Some("/t".to_string())
        );

        // Setattr forwards the Option mode verbatim (Some and None).
        assert_eq!(
            params(FileOperation::Setattr { mode: Some(0o640) }).mode,
            Some(0o640)
        );
        assert_eq!(params(FileOperation::Setattr { mode: None }).mode, None);

        // Remove and Fsync carry no params.
        assert_eq!(
            params(FileOperation::Remove),
            proto::OperationParams::default()
        );
        assert_eq!(
            params(FileOperation::Fsync),
            proto::OperationParams::default()
        );
    }

    #[test]
    fn file_access_event_propagates_timestamp_and_path() {
        let e = proto_event(FileOperation::Fsync);
        assert_eq!(e.timestamp.unwrap().seconds, 42);
        assert_eq!(e.path, "/p");
    }
}
