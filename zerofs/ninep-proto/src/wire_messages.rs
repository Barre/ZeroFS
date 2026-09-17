//! 9P message fields in wire order, with their type IDs and mutation-envelope flags.

macro_rules! for_each_message {
    ($emit:ident) => {
        $emit! {
            Tversion, Tversion, TVERSION, false, { msize: u32, version: (str) };
            Rversion, Rversion, RVERSION, false, { msize: u32, version: (str) };
            Tattach, Tattach, TATTACH, false, {
                fid: u32,
                afid: u32,
                uname: (str),
                aname: (str),
                n_uname: u32
            };
            Rattach, Rattach, RATTACH, false, { qid: Qid };
            Twalk, Twalk, TWALK, false, {
                fid: u32,
                newfid: u32,
                nwname: u16,
                wnames: (names nwname)
            };
            Rwalk, Rwalk, RWALK, false, { nwqid: u16, wqids: (qids nwqid) };
            Tlopen, Tlopen, TLOPEN, false, { fid: u32, flags: u32 };
            Rlopen, Rlopen, RLOPEN, false, { qid: Qid, iounit: u32 };
            Tlcreate, Tlcreate, TLCREATE, true, {
                fid: u32,
                name: (str),
                flags: u32,
                mode: u32,
                gid: u32
            };
            Rlcreate, Rlcreate, RLCREATE, false, { qid: Qid, iounit: u32 };
            Tread, Tread, TREAD, false, { fid: u32, offset: u64, count: u32 };
            Rread, Rread, RREAD, false, { count: u32, data: (bytes count) };
            Twrite, Twrite, TWRITE, true, {
                fid: u32,
                offset: u64,
                count: u32,
                data: (bytes count)
            };
            Rwrite, Rwrite, RWRITE, false, { count: u32 };
            Tclunk, Tclunk, TCLUNK, false, { fid: u32 };
            Rclunk, Rclunk, RCLUNK, false, {};
            Treaddir, Treaddir, TREADDIR, false, { fid: u32, offset: u64, count: u32 };
            Rreaddir, Rreaddir, RREADDIR, false, { count: u32, data: (bytes count) };
            Tgetattr, Tgetattr, TGETATTR, false, { fid: u32, request_mask: u64 };
            Rgetattr, Rgetattr, RGETATTR, false, { valid: u64, stat: Stat };
            Tsetattr, Tsetattr, TSETATTR, true, {
                fid: u32,
                valid: u32,
                mode: u32,
                uid: u32,
                gid: u32,
                size: u64,
                atime_sec: u64,
                atime_nsec: u64,
                mtime_sec: u64,
                mtime_nsec: u64
            };
            Rsetattr, Rsetattr, RSETATTR, false, {};
            Tfallocate, Tfallocate, TFALLOCATE, true, {
                fid: u32,
                offset: u64,
                length: u64,
                mode: u32
            };
            Rfallocate, Rfallocate, RFALLOCATE, false, {};
            Tmkdir, Tmkdir, TMKDIR, true, { dfid: u32, name: (str), mode: u32, gid: u32 };
            Rmkdir, Rmkdir, RMKDIR, false, { qid: Qid };
            Tsymlink, Tsymlink, TSYMLINK, true, { dfid: u32, name: (str), symtgt: (str), gid: u32 };
            Rsymlink, Rsymlink, RSYMLINK, false, { qid: Qid };
            Tmknod, Tmknod, TMKNOD, true, {
                dfid: u32,
                name: (str),
                mode: u32,
                major: u32,
                minor: u32,
                gid: u32
            };
            Rmknod, Rmknod, RMKNOD, false, { qid: Qid };
            Treadlink, Treadlink, TREADLINK, false, { fid: u32 };
            Rreadlink, Rreadlink, RREADLINK, false, { target: (str) };
            Tlink, Tlink, TLINK, true, { dfid: u32, fid: u32, name: (str) };
            Rlink, Rlink, RLINK, false, {};
            Trename, Trename, TRENAME, true, { fid: u32, dfid: u32, name: (str) };
            Rrename, Rrename, RRENAME, false, {};
            Trenameat, Trenameat, TRENAMEAT, true, {
                olddirfid: u32,
                oldname: (str),
                newdirfid: u32,
                newname: (str)
            };
            Rrenameat, Rrenameat, RRENAMEAT, false, {};
            Tunlinkat, Tunlinkat, TUNLINKAT, true, { dirfid: u32, name: (str), flags: u32 };
            Runlinkat, Runlinkat, RUNLINKAT, false, {};
            Tfsync, Tfsync, TFSYNC, false, { fid: u32, datasync: u32 };
            Rfsync, Rfsync, RFSYNC, false, {};
            Tfsyncdur, Tfsyncdur, TFSYNCDUR, false, { fid: u32, datasync: u32, token: u64 };
            Tgetlineage, Tgetlineage, TGETLINEAGE, false, {};
            Rgetlineage, Rgetlineage, RGETLINEAGE, false, { token: u64, writer_epoch: u64 };
            Tlock, Tlock, TLOCK, false, {
                fid: u32,
                lock_type: LockType,
                flags: u32,
                start: u64,
                length: u64,
                proc_id: u32,
                client_id: (str)
            };
            Rlock, Rlock, RLOCK, false, { status: u8 };
            Tgetlock, Tgetlock, TGETLOCK, false, {
                fid: u32,
                lock_type: LockType,
                start: u64,
                length: u64,
                proc_id: u32,
                client_id: (str)
            };
            Rgetlock, Rgetlock, RGETLOCK, false, {
                lock_type: LockType,
                start: u64,
                length: u64,
                proc_id: u32,
                client_id: (str)
            };
            Rlerror, Rlerror, RLERROR, false, { ecode: u32 };
            Tflush, Tflush, TFLUSH, false, { oldtag: u16 };
            Rflush, Rflush, RFLUSH, false, {};
            Txattrwalk, Txattrwalk, TXATTRWALK, false, { fid: u32, newfid: u32, name: (str) };
            Rxattrwalk, Rxattrwalk, RXATTRWALK, false, { size: u64 };
            Tstatfs, Tstatfs, TSTATFS, false, { fid: u32 };
            Rstatfs, Rstatfs, RSTATFS, false, {
                r#type: u32,
                bsize: u32,
                blocks: u64,
                bfree: u64,
                bavail: u64,
                files: u64,
                ffree: u64,
                fsid: u64,
                namelen: u32
            };
            Tlopenat, Tlopenat, TLOPENAT, false, { fid: u32, newfid: u32, flags: u32 };
            Rlopenat, Rlopenat, RLOPENAT, false, { qid: Qid, iounit: u32 };
            Tlopenatread, Tlopenatread, TLOPENATREAD, false, {
                fid: u32,
                newfid: u32,
                flags: u32,
                count: u32
            };
            Rlopenatread, Rlopenatread, RLOPENATREAD, false, {
                qid: Qid,
                iounit: u32,
                eof: u8,
                count: u32,
                data: (bytes count)
            };
            Tlcreateattr, Tlcreateattr, TLCREATEATTR, true, {
                dfid: u32,
                newfid: u32,
                name: (str),
                flags: u32,
                mode: u32,
                gid: u32
            };
            Rlcreateattr, Rlcreateattr, RLCREATEATTR, false, { iounit: u32, stat: Stat };
            Tmkdirattr, Tmkdir, TMKDIRATTR, true, { dfid: u32, name: (str), mode: u32, gid: u32 };
            Rmkdirattr, Rmkdirattr, RMKDIRATTR, false, { stat: Stat };
            Tsymlinkattr, Tsymlink, TSYMLINKATTR, true, {
                dfid: u32,
                name: (str),
                symtgt: (str),
                gid: u32
            };
            Rsymlinkattr, Rsymlinkattr, RSYMLINKATTR, false, { stat: Stat };
            Tmknodattr, Tmknod, TMKNODATTR, true, {
                dfid: u32,
                name: (str),
                mode: u32,
                major: u32,
                minor: u32,
                gid: u32
            };
            Rmknodattr, Rmknodattr, RMKNODATTR, false, { stat: Stat };
            Tlinkattr, Tlink, TLINKATTR, true, { dfid: u32, fid: u32, name: (str) };
            Rlinkattr, Rlinkattr, RLINKATTR, false, { stat: Stat };
            Tsetattrattr, Tsetattr, TSETATTRATTR, true, {
                fid: u32,
                valid: u32,
                mode: u32,
                uid: u32,
                gid: u32,
                size: u64,
                atime_sec: u64,
                atime_nsec: u64,
                mtime_sec: u64,
                mtime_nsec: u64
            };
            Rsetattrattr, Rsetattrattr, RSETATTRATTR, false, { stat: Stat };
            Trebind, Trebind, TREBIND, false, {
                fid: u32,
                inode_id: u64,
                root_inode: u64,
                flags: u8,
                uname: (str),
                n_uname: u32
            };
            Rrebind, Rrebind, RREBIND, false, { qid: Qid };
            Twalkgetattr, Twalkgetattr, TWALKGETATTR, false, {
                fid: u32,
                newfid: u32,
                nwname: u16,
                wnames: (names nwname)
            };
            Rwalkgetattr, Rwalkgetattr, RWALKGETATTR, false, {
                nwqid: u16,
                wqids: (qids nwqid),
                stat: Stat
            };
            Treaddirattr, Treaddirattr, TREADDIRATTR, false, { fid: u32, offset: u64, count: u32 };
            Rreaddirattr, Rreaddirattr, RREADDIRATTR, false, { count: u32, data: (bytes count) };
        }
    };
}
pub(crate) use for_each_message;
