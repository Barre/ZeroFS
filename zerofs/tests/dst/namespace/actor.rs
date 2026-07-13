use super::{NsModel, NsOp, show_path};
use crate::actor::ActorContext;
use crate::data::{FileOp, pattern};
use crate::{auth, creds};
use bytes::Bytes;
use rand::Rng;
use rand::rngs::StdRng;
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use zerofs::fs::EXTENT_SIZE;
use zerofs::fs::permissions::Credentials;
use zerofs::fs::types::{AuthContext, FileType, SetAttributes, SetSize};

enum NamespaceAction {
    Create,
    IdempotentCreate,
    Mkdir,
    Content,
    Remove,
    Rename,
    Link,
    Symlink,
    Mknod,
    OpenUnlink,
    Fsync,
}

impl NamespaceAction {
    fn draw(rng: &mut StdRng) -> Self {
        match rng.gen_range(0..100u32) {
            0..15 => Self::Create,
            15..18 => Self::IdempotentCreate,
            18..33 => Self::Mkdir,
            33..48 => Self::Content,
            48..60 => Self::Remove,
            60..73 => Self::Rename,
            73..81 => Self::Link,
            81..90 => Self::Symlink,
            90..95 => Self::Mknod,
            95..97 => Self::OpenUnlink,
            _ => Self::Fsync,
        }
    }
}

enum StepOutcome {
    Completed,
    Skipped,
    Crashed,
}

/// Owns one namespace model for a round and executes one fully ordered stream
/// of prepared mutations against it.
pub(super) struct NamespaceActor {
    model: NsModel,
    rng: StdRng,
    context: ActorContext,
    auth: AuthContext,
    creds: Credentials,
    attributes: SetAttributes,
}

impl NamespaceActor {
    pub(super) fn new(model: NsModel, rng: StdRng, context: ActorContext) -> Self {
        Self {
            model,
            rng,
            context,
            auth: auth(),
            creds: creds(),
            attributes: SetAttributes::default(),
        }
    }

    fn publish_ack(&mut self) {
        self.model.acked = self.model.oplog.len();
        self.context.publish_ack(self.model.acked);
    }

    fn acknowledge(&mut self, event: impl Hash + Debug) {
        self.publish_ack();
        self.context.digest.event(event);
    }

    pub(super) async fn run(mut self, ops: usize) -> NsModel {
        for _ in 0..ops {
            if self.context.is_crashed() {
                break;
            }
            let outcome = match NamespaceAction::draw(&mut self.rng) {
                NamespaceAction::Create => self.create().await,
                NamespaceAction::IdempotentCreate => self.idempotent_create().await,
                NamespaceAction::Mkdir => self.mkdir().await,
                NamespaceAction::Content => self.content().await,
                NamespaceAction::Remove => self.remove().await,
                NamespaceAction::Rename => self.rename().await,
                NamespaceAction::Link => self.link().await,
                NamespaceAction::Symlink => self.symlink().await,
                NamespaceAction::Mknod => self.mknod().await,
                NamespaceAction::OpenUnlink => self.open_unlink().await,
                NamespaceAction::Fsync => self.fsync().await,
            };
            match outcome {
                StepOutcome::Crashed => return self.model,
                StepOutcome::Skipped => continue,
                StepOutcome::Completed => {}
            }
            let pause = self.rng.gen_range(1..=3);
            tokio::time::sleep(Duration::from_millis(pause)).await;
        }
        self.model
    }

    async fn create(&mut self) -> StepOutcome {
        let dir = self.model.choose_dir_path(&mut self.rng);
        let name = self.model.fresh_name();
        let mut path = dir.clone();
        path.push(name.clone());
        let dir_inode = self.model.inode_of(&dir);
        self.model.oplog.push(NsOp::Create(path.clone()));
        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.create(&self.creds, dir_inode, &name, &self.attributes) => {
                match result {
                    Ok((id, _)) => {
                        self.model.apply_last_live(Some(id));
                        self.acknowledge(("ncr", show_path(&path)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns create {}: {error:?}", show_path(&path)),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn idempotent_create(&mut self) -> StepOutcome {
        let dir = self.model.choose_dir_path(&mut self.rng);
        let name = self.model.fresh_name();
        let mut path = dir.clone();
        path.push(name.clone());
        let dir_inode = self.model.inode_of(&dir);
        let mut op_id = [0u8; 16];
        op_id[..8].copy_from_slice(&self.rng.r#gen::<u64>().to_le_bytes());
        op_id[8..].copy_from_slice(&self.rng.r#gen::<u64>().to_le_bytes());
        op_id[0] |= 1;
        let concurrent = self.rng.gen_bool(0.5);
        self.model.oplog.push(NsOp::Create(path.clone()));

        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        if concurrent {
            let calls = async {
                tokio::join!(
                    fs.create_idempotent(&self.creds, dir_inode, &name, &self.attributes, op_id),
                    fs.create_idempotent(&self.creds, dir_inode, &name, &self.attributes, op_id),
                )
            };
            tokio::select! {
                biased;
                (first, second) = calls => {
                    let too_late = *crashed.borrow();
                    let (id1, id2) = match (first, second) {
                        (Ok((a, _)), Ok((b, _))) => (a, b),
                        _ if too_late => return StepOutcome::Crashed,
                        (a, b) => panic!(
                            "dedup concurrent create {}: {a:?} / {b:?}",
                            show_path(&path)
                        ),
                    };
                    assert_eq!(
                        id1,
                        id2,
                        "concurrent same-op-id create returned two inodes ({})",
                        show_path(&path)
                    );
                    self.model.apply_last_live(Some(id1));
                    self.acknowledge(("nd2", show_path(&path)));
                    StepOutcome::Completed
                }
                _ = crashed.changed() => StepOutcome::Crashed,
            }
        } else {
            let first = tokio::select! {
                biased;
                result = fs.create_idempotent(&self.creds, dir_inode, &name, &self.attributes, op_id) => result,
                _ = crashed.changed() => return StepOutcome::Crashed,
            };
            let id1 = match first {
                Ok((id, _)) => id,
                Err(_) if *crashed.borrow() => return StepOutcome::Crashed,
                Err(error) => panic!("dedup create {}: {error:?}", show_path(&path)),
            };
            self.model.apply_last_live(Some(id1));
            self.publish_ack();
            let second = tokio::select! {
                biased;
                result = fs.create_idempotent(&self.creds, dir_inode, &name, &self.attributes, op_id) => result,
                _ = crashed.changed() => return StepOutcome::Crashed,
            };
            match second {
                Ok((id2, _)) => assert_eq!(
                    id1,
                    id2,
                    "retried same-op-id create returned a new inode ({})",
                    show_path(&path)
                ),
                Err(_) if *crashed.borrow() => return StepOutcome::Crashed,
                Err(error) => {
                    panic!("dedup create retry {}: {error:?}", show_path(&path))
                }
            }
            self.context.digest.event(("nd1", show_path(&path)));
            StepOutcome::Completed
        }
    }

    async fn mkdir(&mut self) -> StepOutcome {
        let dir = self.model.choose_dir_path(&mut self.rng);
        let name = self.model.fresh_name();
        let mut path = dir.clone();
        path.push(name.clone());
        let dir_inode = self.model.inode_of(&dir);
        self.model.oplog.push(NsOp::Mkdir(path.clone()));
        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.mkdir(&self.creds, dir_inode, &name, &self.attributes) => {
                match result {
                    Ok((id, _)) => {
                        self.model.apply_last_live(Some(id));
                        self.acknowledge(("nmk", show_path(&path)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns mkdir {}: {error:?}", show_path(&path)),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn content(&mut self) -> StepOutcome {
        let Some(path) = self.model.choose_file_path(&mut self.rng) else {
            return StepOutcome::Skipped;
        };
        let inode = self.model.inode_of(&path);
        let op = match self.rng.gen_range(0..10) {
            0..=6 => {
                let len = self.rng.gen_range(0..=3 * EXTENT_SIZE);
                FileOp::Write {
                    offset: 0,
                    len,
                    tag: self.rng.r#gen::<u64>(),
                }
            }
            7..=8 => FileOp::Truncate {
                size: self.rng.gen_range(0..=3 * EXTENT_SIZE),
            },
            _ => FileOp::Trim {
                offset: self.rng.gen_range(0..=3 * EXTENT_SIZE),
                length: self.rng.gen_range(1..=EXTENT_SIZE),
            },
        };
        self.model.oplog.push(NsOp::Content {
            path: path.clone(),
            op: op.clone(),
        });

        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        let operation = async {
            match &op {
                FileOp::Write { offset, len, tag } => {
                    let data = Bytes::from(pattern(*tag, *len));
                    fs.write(&self.auth, inode, *offset as u64, &data)
                        .await
                        .map(|_| ())
                }
                FileOp::Truncate { size } => {
                    let attributes = SetAttributes {
                        size: SetSize::Set(*size as u64),
                        ..Default::default()
                    };
                    fs.setattr(&self.creds, inode, &attributes)
                        .await
                        .map(|_| ())
                }
                FileOp::Trim { offset, length } => {
                    fs.trim(&self.auth, inode, *offset as u64, *length as u64)
                        .await
                }
            }
        };
        tokio::select! {
            biased;
            result = operation => {
                match result {
                    Ok(()) => {
                        self.model.apply_last_live(None);
                        let tag = match op {
                            FileOp::Write { .. } => "nwr",
                            FileOp::Truncate { .. } => "ntr",
                            FileOp::Trim { .. } => "nhp",
                        };
                        self.acknowledge((tag, show_path(&path)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns content {}: {error:?}", show_path(&path)),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn remove(&mut self) -> StepOutcome {
        let Some(path) = self.model.choose_removable_path(&mut self.rng) else {
            return StepOutcome::Skipped;
        };
        let name = path.last().unwrap().clone();
        let parent_inode = self.model.inode_of(&path[..path.len() - 1]);
        self.model.oplog.push(NsOp::Remove(path.clone()));

        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.remove(&self.auth, parent_inode, &name) => {
                match result {
                    Ok(()) => {
                        self.model.apply_last_live(None);
                        self.acknowledge(("nrm", show_path(&path)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns remove {}: {error:?}", show_path(&path)),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn rename(&mut self) -> StepOutcome {
        let Some(from) = self.model.choose_any_path(&mut self.rng) else {
            return StepOutcome::Skipped;
        };
        let source_is_dir = self.model.is_dir(&from);
        let directory = self.model.choose_dir_outside(&from, &mut self.rng);
        let overwrite = !source_is_dir && self.rng.gen_bool(0.3);
        let to = if overwrite {
            if let Some(target) = self
                .model
                .choose_file_child(&directory, &from, &mut self.rng)
            {
                target
            } else {
                let name = self.model.fresh_name();
                let mut path = directory.clone();
                path.push(name);
                path
            }
        } else {
            let name = self.model.fresh_name();
            let mut path = directory;
            path.push(name);
            path
        };
        if to == from {
            return StepOutcome::Skipped;
        }

        let from_parent = self.model.inode_of(&from[..from.len() - 1]);
        let from_name = from.last().unwrap().clone();
        let to_parent = self.model.inode_of(&to[..to.len() - 1]);
        let to_name = to.last().unwrap().clone();
        self.model.oplog.push(NsOp::Rename {
            from: from.clone(),
            to: to.clone(),
        });

        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.rename(&self.auth, from_parent, &from_name, to_parent, &to_name) => {
                match result {
                    Ok(()) => {
                        self.model.apply_last_live(None);
                        self.acknowledge(("nrn", show_path(&from), show_path(&to)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!(
                        "ns rename {} -> {}: {error:?}",
                        show_path(&from),
                        show_path(&to)
                    ),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn link(&mut self) -> StepOutcome {
        let Some(from) = self.model.choose_hardlinkable_path(&mut self.rng) else {
            return StepOutcome::Skipped;
        };
        let directory = self.model.choose_dir_path(&mut self.rng);
        let name = self.model.fresh_name();
        let mut to = directory.clone();
        to.push(name.clone());
        let from_inode = self.model.inode_of(&from);
        let directory_inode = self.model.inode_of(&directory);
        self.model.oplog.push(NsOp::Link {
            from: from.clone(),
            to: to.clone(),
        });

        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.link(&self.auth, from_inode, directory_inode, &name) => {
                match result {
                    Ok(()) => {
                        self.model.apply_last_live(None);
                        self.acknowledge(("nln", show_path(&from), show_path(&to)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!(
                        "ns link {} -> {}: {error:?}",
                        show_path(&from),
                        show_path(&to)
                    ),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn symlink(&mut self) -> StepOutcome {
        let directory = self.model.choose_dir_path(&mut self.rng);
        let name = self.model.fresh_name();
        let mut path = directory.clone();
        path.push(name.clone());
        let target = pattern(self.rng.r#gen::<u64>(), self.rng.gen_range(1..=64));
        let directory_inode = self.model.inode_of(&directory);
        self.model.oplog.push(NsOp::Symlink {
            path: path.clone(),
            target: target.clone(),
        });

        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.symlink(&self.creds, directory_inode, &name, &target, &self.attributes) => {
                match result {
                    Ok((id, _)) => {
                        self.model.apply_last_live(Some(id));
                        self.acknowledge(("nsl", show_path(&path)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns symlink {}: {error:?}", show_path(&path)),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn mknod(&mut self) -> StepOutcome {
        let directory = self.model.choose_dir_path(&mut self.rng);
        let name = self.model.fresh_name();
        let mut path = directory.clone();
        path.push(name.clone());
        let (file_type, rdev) = match self.rng.gen_range(0..4) {
            0 => (FileType::Fifo, None),
            1 => (FileType::Socket, None),
            2 => (
                FileType::CharDevice,
                Some((self.rng.r#gen::<u32>(), self.rng.r#gen::<u32>())),
            ),
            _ => (
                FileType::BlockDevice,
                Some((self.rng.r#gen::<u32>(), self.rng.r#gen::<u32>())),
            ),
        };
        let directory_inode = self.model.inode_of(&directory);
        self.model.oplog.push(NsOp::Mknod {
            path: path.clone(),
            ftype: file_type,
            rdev,
        });

        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.mknod(&self.creds, directory_inode, &name, file_type, &self.attributes, rdev) => {
                match result {
                    Ok((id, _)) => {
                        self.model.apply_last_live(Some(id));
                        self.acknowledge(("nnd", show_path(&path)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns mknod {}: {error:?}", show_path(&path)),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn open_unlink(&mut self) -> StepOutcome {
        let Some(path) = self.model.choose_file_path(&mut self.rng) else {
            return StepOutcome::Skipped;
        };
        let id = self.model.inode_of(&path);
        let name = path.last().unwrap().clone();
        let parent_inode = self.model.inode_of(&path[..path.len() - 1]);
        let fs = self.context.fs.clone();
        let open_handle = {
            // Order the open count increment against remove's
            // defer-versus-delete decision.
            let _guard = fs.lock_manager.acquire(id).await;
            fs.new_open_handle(id)
        };
        self.model.oplog.push(NsOp::Remove(path.clone()));

        let mut crashed = self.context.crashed.clone();
        let removed = tokio::select! {
            biased;
            result = fs.remove(&self.auth, parent_inode, &name) => {
                match result {
                    Ok(()) => {
                        self.model.apply_last_live(None);
                        self.acknowledge(("nou", show_path(&path)));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns open-unlink {}: {error:?}", show_path(&path)),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        };
        if matches!(removed, StepOutcome::Crashed) {
            return removed;
        }

        drop(open_handle);
        tokio::select! {
            biased;
            () = fs.reclaim_if_unreferenced(id) => {
                self.context.digest.event(("ncl", show_path(&path)));
                StepOutcome::Completed
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }

    async fn fsync(&mut self) -> StepOutcome {
        let snapshot = self.context.begin_fsync();
        let fs = self.context.fs.clone();
        let mut crashed = self.context.crashed.clone();
        tokio::select! {
            biased;
            result = fs.client_fsync() => {
                match result {
                    Ok(()) => {
                        self.context.finish_fsync(snapshot);
                        self.model.fsynced = self.model.acked;
                        self.context.digest.event(("nfs", self.model.acked));
                        StepOutcome::Completed
                    }
                    Err(_) if *crashed.borrow() => StepOutcome::Crashed,
                    Err(error) => panic!("ns fsync: {error:?}"),
                }
            }
            _ = crashed.changed() => StepOutcome::Crashed,
        }
    }
}
