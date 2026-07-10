//! The namespace plane: the tree model, the mutation actor, and the whole-tree
//! structural-isomorphism crash oracle.

mod actor;

use crate::actor::ActorContext;
use crate::data::{FileOp, FileSnapshot, apply};
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use std::collections::{BTreeMap, BTreeSet};
use zerofs::fs::ZeroFS;
use zerofs::fs::inode::{Inode, InodeId};
use zerofs::fs::types::FileType;

// A single namespace actor gives mutations a total order. After a crash, the
// observed tree must match an op-log prefix at or beyond the fsync floor. The
// consistency scan checks internal invariants; this model checks intent.

/// A path under the namespace root, one component per `Vec<u8>` name.
type NsPath = Vec<Vec<u8>>;

/// Observable node kind and value (content, symlink target, or device number).
#[derive(Clone, Debug, PartialEq)]
enum NsKind {
    Dir,
    File(Vec<u8>),
    Symlink(Vec<u8>),
    Special(FileType, Option<(u32, u32)>),
}

impl NsKind {
    fn summary(&self) -> String {
        match self {
            Self::Dir => "dir".into(),
            Self::File(content) => format!("file[{} bytes]", content.len()),
            Self::Symlink(target) => {
                format!("symlink->{:?}", String::from_utf8_lossy(target))
            }
            Self::Special(file_type, rdev) => format!("{file_type:?}{rdev:?}"),
        }
    }
}

fn show_path(p: &NsPath) -> String {
    if p.is_empty() {
        return "<root>".into();
    }
    p.iter()
        .map(|c| String::from_utf8_lossy(c))
        .collect::<Vec<_>>()
        .join("/")
}

/// One replayable namespace mutation, expressed purely in paths and content so
/// a prefix of the log reconstructs the intended tree without any live inode
/// ids (a crash may have discarded the ids of un-acked ops).
#[derive(Clone, Debug)]
enum NsOp {
    Mkdir(NsPath),
    Create(NsPath),
    Content {
        path: NsPath,
        op: FileOp,
    },
    Remove(NsPath),
    Rename {
        from: NsPath,
        to: NsPath,
    },
    Link {
        from: NsPath,
        to: NsPath,
    },
    Symlink {
        path: NsPath,
        target: Vec<u8>,
    },
    Mknod {
        path: NsPath,
        ftype: FileType,
        rdev: Option<(u32, u32)>,
    },
}

/// A tree keyed by path, with a hardlink-identity group per node. Two paths in
/// the same group are the same inode. Produced both by replaying the op-log and
/// (with real inode ids for groups) by walking the live filesystem.
#[derive(Clone, Default)]
struct RefTree {
    paths: BTreeMap<NsPath, u64>,
    groups: BTreeMap<u64, NsKind>,
}

#[derive(Default)]
struct TreeChange {
    added: Option<u64>,
    retired: Option<u64>,
}

/// The observed tree, groups keyed by real inode id.
pub(crate) struct NamespaceSnapshot {
    paths: BTreeMap<NsPath, InodeId>,
    kinds: BTreeMap<InodeId, NsKind>,
}

/// Rebuild the intended tree from a prefix of the op-log. Group ids are
/// allocated deterministically in op order, so a given prefix always yields the
/// same tree; identity across ids is what the oracle compares, not the ids.
impl RefTree {
    fn replay(ops: &[NsOp]) -> Self {
        let mut tree = Self::default();
        let mut next_group = 1u64;
        for op in ops {
            tree.apply(op, &mut next_group);
        }
        tree
    }

    /// Apply one modeled mutation. Replay and the live model both use this
    /// path, so the crash oracle cannot drift from acknowledged-operation
    /// bookkeeping when a mutation's semantics change.
    fn apply(&mut self, op: &NsOp, next_group: &mut u64) -> TreeChange {
        let mut change = TreeChange::default();
        let mut add = |tree: &mut Self, path: &NsPath, kind| {
            let group = *next_group;
            *next_group += 1;
            tree.groups.insert(group, kind);
            tree.paths.insert(path.clone(), group);
            group
        };
        match op {
            NsOp::Mkdir(path) => change.added = Some(add(self, path, NsKind::Dir)),
            NsOp::Create(path) => change.added = Some(add(self, path, NsKind::File(Vec::new()))),
            NsOp::Symlink { path, target } => {
                change.added = Some(add(self, path, NsKind::Symlink(target.clone())))
            }
            NsOp::Mknod { path, ftype, rdev } => {
                change.added = Some(add(self, path, NsKind::Special(*ftype, *rdev)))
            }
            NsOp::Content { path, op } => {
                if let Some(&group) = self.paths.get(path)
                    && let Some(NsKind::File(content)) = self.groups.get_mut(&group)
                {
                    apply(content, op);
                }
            }
            NsOp::Remove(path) => change.retired = self.remove(path),
            NsOp::Link { from, to } => {
                if let Some(&group) = self.paths.get(from) {
                    self.paths.insert(to.clone(), group);
                }
            }
            NsOp::Rename { from, to } => {
                change.retired = self.remove(to);
                let moved: Vec<(NsPath, u64)> = self
                    .paths
                    .iter()
                    .filter(|(path, _)| path.starts_with(from.as_slice()))
                    .map(|(path, &group)| (path.clone(), group))
                    .collect();
                for (path, _) in &moved {
                    self.paths.remove(path);
                }
                for (path, group) in moved {
                    let mut destination = to.clone();
                    destination.extend_from_slice(&path[from.len()..]);
                    self.paths.insert(destination, group);
                }
            }
        }
        change
    }

    /// Drop `path`; retire its group only when no other path still references
    /// it (a hardlinked file keeps its group until the last link goes).
    fn remove(&mut self, path: &NsPath) -> Option<u64> {
        if let Some(group) = self.paths.remove(path)
            && !self.paths.values().any(|candidate| *candidate == group)
        {
            self.groups.remove(&group);
            return Some(group);
        }
        None
    }
}

/// Walk the live tree under `root` (exclusive), reading every node's content so
/// the comparison covers bytes, not just structure.
impl NamespaceSnapshot {
    pub(crate) async fn read(fs: &ZeroFS, root: InodeId) -> Self {
        use futures::StreamExt;
        let mut paths: BTreeMap<NsPath, InodeId> = BTreeMap::new();
        let mut kinds: BTreeMap<InodeId, NsKind> = BTreeMap::new();
        let mut stack: Vec<(NsPath, InodeId)> = vec![(Vec::new(), root)];
        while let Some((prefix, dir_id)) = stack.pop() {
            let mut children: Vec<(Vec<u8>, InodeId)> = Vec::new();
            {
                let mut stream = fs.directory_store.list(dir_id).await.expect("ns dir list");
                while let Some(item) = stream.next().await {
                    let entry = item.expect("ns dir entry");
                    if entry.name == b"." || entry.name == b".." {
                        continue;
                    }
                    children.push((entry.name.clone(), entry.inode_id));
                }
            }
            for (name, child_id) in children {
                let mut path = prefix.clone();
                path.push(name);
                let is_dir = if let Some(kind) = kinds.get(&child_id) {
                    matches!(kind, NsKind::Dir)
                } else {
                    let inode = fs.inode_store.get(child_id).await.expect("ns inode get");
                    let is_dir = matches!(inode, Inode::Directory(_));
                    let kind = Self::kind_of(fs, child_id, &inode).await;
                    kinds.insert(child_id, kind);
                    is_dir
                };
                paths.insert(path.clone(), child_id);
                if is_dir {
                    stack.push((path, child_id));
                }
            }
        }
        Self { paths, kinds }
    }

    async fn kind_of(fs: &ZeroFS, id: InodeId, inode: &Inode) -> NsKind {
        match inode {
            Inode::File(_) => NsKind::File(FileSnapshot::read(fs, id).await.into_bytes()),
            Inode::Directory(_) => NsKind::Dir,
            Inode::Symlink(s) => NsKind::Symlink(s.target.clone()),
            Inode::Fifo(_) => NsKind::Special(FileType::Fifo, None),
            Inode::Socket(_) => NsKind::Special(FileType::Socket, None),
            Inode::CharDevice(s) => NsKind::Special(FileType::CharDevice, s.rdev),
            Inode::BlockDevice(s) => NsKind::Special(FileType::BlockDevice, s.rdev),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.paths.len()
    }
}

/// Structural isomorphism: same path set, a consistent group<->inode bijection
/// (so hardlink identity is preserved exactly), and equal node identity (kind +
/// content) at every path.
impl RefTree {
    fn compare(&self, observed: &NamespaceSnapshot) -> Result<(), String> {
        if !self.paths.keys().eq(observed.paths.keys()) {
            let only_model: Vec<String> = self
                .paths
                .keys()
                .filter(|p| !observed.paths.contains_key(*p))
                .map(show_path)
                .collect();
            let only_fs: Vec<String> = observed
                .paths
                .keys()
                .filter(|p| !self.paths.contains_key(*p))
                .map(show_path)
                .collect();
            return Err(format!(
                "path sets differ: only in model {only_model:?}; only in fs {only_fs:?}"
            ));
        }
        let mut group_to_inode: BTreeMap<u64, InodeId> = BTreeMap::new();
        let mut inode_to_group: BTreeMap<InodeId, u64> = BTreeMap::new();
        for (path, &group) in &self.paths {
            let inode = observed.paths[path];
            match group_to_inode.insert(group, inode) {
                Some(previous) if previous != inode => {
                    return Err(format!(
                        "hardlink split: model node {group} maps to inodes {previous} and {inode} \
                         (at {})",
                        show_path(path)
                    ));
                }
                _ => {}
            }
            match inode_to_group.insert(inode, group) {
                Some(previous) if previous != group => {
                    return Err(format!(
                        "hardlink merge: inode {inode} carries model nodes {previous} and {group} \
                         (at {})",
                        show_path(path)
                    ));
                }
                _ => {}
            }
            let model_kind = &self.groups[&group];
            let observed_kind = &observed.kinds[&inode];
            if model_kind != observed_kind {
                let extra = match (model_kind, observed_kind) {
                    (NsKind::File(expected), NsKind::File(observed)) => {
                        let first_difference = expected
                            .iter()
                            .zip(observed)
                            .position(|(x, y)| x != y)
                            .map(|o| format!(", first byte diff at {o}"))
                            .unwrap_or_default();
                        format!(
                            " (len {} vs {}{first_difference})",
                            expected.len(),
                            observed.len()
                        )
                    }
                    _ => String::new(),
                };
                return Err(format!(
                    "node mismatch at {}: model {} vs fs {}{extra}",
                    show_path(path),
                    model_kind.summary(),
                    observed_kind.summary()
                ));
            }
        }
        Ok(())
    }
}

/// The live namespace model: the current intended tree, the real inode id for
/// each node (so ops can name concrete inodes), and the op-log with its ack and
/// fsync floors, mirroring `FileState` for the data plane.
pub(crate) struct NsModel {
    /// Real inode id of the namespace root dir (created + fsynced at setup, so
    /// it always survives; paths are relative to it).
    pub(crate) root: InodeId,
    tree: RefTree,
    real: BTreeMap<u64, InodeId>,
    next_group: u64,
    oplog: Vec<NsOp>,
    pub(crate) acked: usize,
    pub(crate) fsynced: usize,
    /// Monotonic across the whole world so a fresh name never collides with a
    /// crash survivor from an earlier round.
    name_ctr: u64,
}

impl NsModel {
    pub(crate) fn new(root: InodeId) -> Self {
        Self {
            root,
            tree: RefTree::default(),
            real: BTreeMap::new(),
            next_group: 1,
            oplog: Vec::new(),
            acked: 0,
            fsynced: 0,
            name_ctr: 0,
        }
    }

    fn fresh_name(&mut self) -> Vec<u8> {
        let n = self.name_ctr;
        self.name_ctr += 1;
        format!("n{n}").into_bytes()
    }

    /// Real inode id a path resolves to; the empty path is the root.
    fn inode_of(&self, path: &[Vec<u8>]) -> InodeId {
        if path.is_empty() {
            self.root
        } else {
            let group = self.tree.paths.get(path).expect("modeled path");
            self.real[group]
        }
    }

    fn kind_of(&self, path: &[Vec<u8>]) -> Option<&NsKind> {
        self.tree
            .paths
            .get(path)
            .map(|group| &self.tree.groups[group])
    }

    fn is_dir(&self, path: &NsPath) -> bool {
        matches!(self.kind_of(path), Some(NsKind::Dir))
    }

    fn choose_matching(
        &self,
        rng: &mut StdRng,
        predicate: impl Fn(&NsKind) -> bool,
    ) -> Option<NsPath> {
        self.tree
            .paths
            .iter()
            .filter(|(_, group)| predicate(&self.tree.groups[group]))
            .map(|(path, _)| path)
            .choose(rng)
            .cloned()
    }

    /// Choose a directory uniformly, including the namespace root.
    fn choose_dir_path(&self, rng: &mut StdRng) -> NsPath {
        std::iter::once(None)
            .chain(
                self.tree
                    .paths
                    .iter()
                    .filter(|(_, group)| matches!(self.tree.groups[group], NsKind::Dir))
                    .map(|(path, _)| Some(path)),
            )
            .choose(rng)
            .expect("root is always a directory candidate")
            .cloned()
            .unwrap_or_default()
    }

    fn choose_dir_outside(&self, from: &NsPath, rng: &mut StdRng) -> NsPath {
        std::iter::once(None)
            .chain(
                self.tree
                    .paths
                    .iter()
                    .filter(|(path, group)| {
                        matches!(self.tree.groups[group], NsKind::Dir)
                            && !path.starts_with(from.as_slice())
                    })
                    .map(|(path, _)| Some(path)),
            )
            .choose(rng)
            .expect("root is outside every modeled path")
            .cloned()
            .unwrap_or_default()
    }

    fn choose_file_path(&self, rng: &mut StdRng) -> Option<NsPath> {
        self.choose_matching(rng, |kind| matches!(kind, NsKind::File(_)))
    }

    /// Hardlinkable nodes are files and special files. ZeroFS rejects both
    /// directories and symlinks with InvalidArgument.
    fn choose_hardlinkable_path(&self, rng: &mut StdRng) -> Option<NsPath> {
        self.choose_matching(rng, |kind| {
            matches!(kind, NsKind::File(_) | NsKind::Special(..))
        })
    }

    /// Choose any non-directory node or an empty directory. Build the set of
    /// non-empty directory prefixes once, avoiding the former all-paths scan
    /// for every candidate.
    fn choose_removable_path(&self, rng: &mut StdRng) -> Option<NsPath> {
        let nonempty_dirs: BTreeSet<NsPath> = self
            .tree
            .paths
            .keys()
            .flat_map(|path| (1..path.len()).map(|depth| path[..depth].to_vec()))
            .collect();
        self.tree
            .paths
            .iter()
            .filter(|(path, group)| {
                !matches!(self.tree.groups[group], NsKind::Dir) || !nonempty_dirs.contains(*path)
            })
            .map(|(path, _)| path)
            .choose(rng)
            .cloned()
    }

    fn choose_any_path(&self, rng: &mut StdRng) -> Option<NsPath> {
        self.tree.paths.keys().choose(rng).cloned()
    }

    /// Choose an immediate file child as an overwrite target for a rename.
    fn choose_file_child(
        &self,
        dir: &NsPath,
        exclude: &NsPath,
        rng: &mut StdRng,
    ) -> Option<NsPath> {
        self.tree
            .paths
            .iter()
            .filter(|(path, group)| {
                *path != exclude
                    && path.len() == dir.len() + 1
                    && path.starts_with(dir.as_slice())
                    && matches!(self.tree.groups[group], NsKind::File(_))
            })
            .map(|(path, _)| path)
            .choose(rng)
            .cloned()
    }

    fn apply_last_live(&mut self, inode: Option<InodeId>) {
        let op = self.oplog.last().expect("live namespace op").clone();
        let change = self.tree.apply(&op, &mut self.next_group);
        match (change.added, inode) {
            (Some(group), Some(inode)) => {
                assert!(
                    self.real.insert(group, inode).is_none(),
                    "new live namespace group reused {group}"
                );
            }
            (None, None) => {}
            (Some(_), None) => panic!("namespace create applied without a live inode"),
            (None, Some(_)) => panic!("live inode supplied for a non-create namespace op"),
        }
        if let Some(group) = change.retired {
            assert!(
                self.real.remove(&group).is_some(),
                "retired live namespace group {group} had no inode"
            );
        }
    }

    /// After a crash, adopt the matching replay tree as ground truth and repin
    /// each surviving node's real inode id from the observed tree.
    fn adopt(&mut self, k: usize, tree: RefTree, observed: &NamespaceSnapshot) {
        let mut real = BTreeMap::new();
        for (path, &group) in &tree.paths {
            real.insert(group, observed.paths[path]);
        }
        self.next_group = tree.groups.keys().copied().max().map_or(1, |m| m + 1);
        self.tree = tree;
        self.real = real;
        self.oplog.truncate(k);
        self.acked = k;
        self.fsynced = k;
    }

    pub(crate) fn verify_quiescent(&self, observed: &NamespaceSnapshot, seed: u64, round: usize) {
        assert_eq!(
            self.oplog.len(),
            self.acked,
            "clean round left an attempted ns op dangling (seed {seed}, round {round})"
        );
        if let Err(why) = self.tree.compare(observed) {
            panic!("ns live-model mismatch (seed {seed}, round {round}): {why}");
        }
        if let Err(why) = RefTree::replay(&self.oplog).compare(observed) {
            panic!("ns replay mismatch (seed {seed}, round {round}): {why}");
        }
    }
}

/// Run one ordered namespace mutation actor for a round.
impl NsModel {
    pub(crate) async fn run_round(self, rng: StdRng, ops: usize, context: ActorContext) -> Self {
        actor::NamespaceActor::new(self, rng, context)
            .run(ops)
            .await
    }
}

/// Post-crash namespace oracle: the surviving tree must equal the intended tree
/// at some op-prefix at or past the fsync floor. Adopts the matching prefix as
/// the new ground truth (mirrors `FileState::reconcile_after_crash`).
impl NsModel {
    pub(crate) fn reconcile_after_crash(
        &mut self,
        observed: &NamespaceSnapshot,
        seed: u64,
        round: usize,
    ) {
        for k in self.fsynced..=self.oplog.len() {
            let tree = RefTree::replay(&self.oplog[..k]);
            if tree.compare(observed).is_ok() {
                eprintln!(
                    "dst: reconcile ns round {round}: k={k} of {} ops ({} acked, floor {})",
                    self.oplog.len(),
                    self.acked,
                    self.fsynced
                );
                self.adopt(k, tree, observed);
                return;
            }
        }
        let mut detail = String::new();
        for k in self.fsynced..=self.oplog.len() {
            if let Err(why) = RefTree::replay(&self.oplog[..k]).compare(observed) {
                detail.push_str(&format!(
                    "\n  k={k} ({:?}): {why}",
                    self.oplog.get(k.wrapping_sub(1))
                ));
            }
        }
        panic!(
            "DST NAMESPACE ORACLE VIOLATION (seed {seed}, round {round}): the surviving tree \
         ({} paths) matches no op-prefix >= the fsync floor ({} of {} ops, {} acked). \
         An fsync-acked or prefix-consistent namespace state was lost.{detail}",
            observed.len(),
            self.fsynced,
            self.oplog.len(),
            self.acked,
        );
    }
}
