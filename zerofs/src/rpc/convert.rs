use crate::checkpoint_manager::CheckpointInfo;
use crate::rpc::proto;
use prost_types::Timestamp;
use uuid::Uuid;

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
