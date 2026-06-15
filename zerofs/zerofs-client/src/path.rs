use crate::error::ZeroFsError;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};

/// POSIX caps a single path component at 255 bytes (NAME_MAX); the server
/// enforces the same limit.
pub(crate) const MAX_NAME_LEN: usize = 255;

/// Render a path for error payloads (lossy display; never fed back as input).
pub(crate) fn display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

/// Split a path into validated byte name components. Paths are bytes (on the
/// 9P wire, in POSIX, and in this API), so UTF-8 is never required; rooted at
/// the attach point (a leading `/` is optional), `.` components are ignored,
/// `..` is rejected (no client-side normalization lies).
pub(crate) fn components(path: &Path) -> Result<Vec<&[u8]>, ZeroFsError> {
    let mut out = Vec::new();
    for comp in path.components() {
        match comp {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                return Err(ZeroFsError::InvalidArgument {
                    message: format!("{path:?}: '..' path components are not supported"),
                });
            }
            Component::Normal(name) => {
                validate_name(name.as_bytes(), &name.to_string_lossy())?;
                out.push(name.as_bytes());
            }
            Component::Prefix(_) => unreachable!("no path prefixes on unix"),
        }
    }
    Ok(out)
}

/// Validate one byte-exact name component: no `/`, no NUL, not `.`/`..`/empty,
/// at most 255 bytes.
pub(crate) fn validate_name(name: &[u8], display: &str) -> Result<(), ZeroFsError> {
    if name.is_empty() || name == b"." || name == b".." {
        return Err(ZeroFsError::InvalidArgument {
            message: format!("invalid name component: {display:?}"),
        });
    }
    if name.contains(&0) || name.contains(&b'/') {
        return Err(ZeroFsError::InvalidArgument {
            message: format!("name contains '/' or NUL: {display:?}"),
        });
    }
    if name.len() > MAX_NAME_LEN {
        return Err(ZeroFsError::NameTooLong {
            name: display.to_string(),
        });
    }
    Ok(())
}

/// Split validated components into (parent components, final name); errors on
/// the root (no final name to operate on). The name borrows the underlying
/// path data, so it outlives the component vector.
pub(crate) fn split_parent<'a, 'b>(
    names: &'b [&'a [u8]],
    path: &str,
) -> Result<(&'b [&'a [u8]], &'a [u8]), ZeroFsError> {
    names
        .split_last()
        .map(|(last, parents)| (parents, *last))
        .ok_or_else(|| ZeroFsError::InvalidArgument {
            message: format!("{path:?}: operation requires a path with a final name component"),
        })
}

/// Render byte components as a lossy display path for error context.
pub(crate) fn display_path(names: &[&[u8]]) -> String {
    let mut out = String::new();
    for n in names {
        out.push('/');
        out.push_str(&String::from_utf8_lossy(n));
    }
    if out.is_empty() { "/".to_string() } else { out }
}
