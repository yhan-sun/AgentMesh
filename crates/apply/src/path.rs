//! Apply path security (Phase 13, section 6).
//!
//! Untracked files move from the workspace to the source repository. Both the
//! source path and the destination path must be validated so that no `..`,
//! absolute path or symlink escape can write outside the two repository roots.

use std::path::{Component, Path, PathBuf};

use crate::error::ApplyError;

/// Expand one untracked entry from the workspace diff into concrete files.
///
/// `git status --porcelain=v1` collapses untracked directories to a single
/// `dir/` entry; walk such directories recursively so every file is copied.
/// A plain file (or symlink) entry is returned as-is.
pub fn expand_untracked(rel: &Path, workspace_root: &Path) -> Vec<PathBuf> {
    let source = workspace_root.join(rel);
    let mut out = Vec::new();
    if source.is_file() {
        out.push(source);
    } else if source.is_dir() {
        walk_files(&source, &mut out);
    }
    out
}

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    let mut files: Vec<PathBuf> = Vec::new();
    let mut dirs: Vec<PathBuf> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            dirs.push(path);
        } else {
            files.push(path);
        }
    }
    // Deterministic order: files first, then nested dirs, each sorted.
    files.sort();
    dirs.sort();
    out.extend(files);
    for dir in dirs {
        walk_files(&dir, out);
    }
}

/// Validate one untracked file for copy: path shape, source existence within
/// the workspace root, destination absence within the source root, and no
/// symlink escape on either side.
pub fn validate_untracked_file(
    rel: &Path,
    workspace_root: &Path,
    source_root: &Path,
) -> Result<(), ApplyError> {
    let rel = sanitize_relative(rel)?;

    // Source must exist and resolve (after symlinks) inside the workspace.
    let source = workspace_root.join(&rel);
    if !source.exists() {
        return Err(ApplyError::SourceFileMissing(rel.display().to_string()));
    }
    if !source.is_file() {
        return Err(ApplyError::UnsafeApplyPath(format!(
            "source is not a regular file: {}",
            rel.display()
        )));
    }
    ensure_resolves_within(&source, workspace_root, "workspace", &rel)?;

    // Destination must not exist and must resolve inside the source repo.
    let target = source_root.join(&rel);
    if target.exists() {
        return Err(ApplyError::ApplyConflict(rel.display().to_string()));
    }
    ensure_within_source(target.parent().unwrap_or(source_root), source_root, &rel)?;

    Ok(())
}

/// Validate an untracked file's path shape without touching the filesystem:
/// relative, no `..`, no `.`, no absolute prefix.
fn sanitize_relative(rel: &Path) -> Result<PathBuf, ApplyError> {
    let mut out = PathBuf::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {
                return Err(ApplyError::UnsafeApplyPath(rel.display().to_string()));
            }
            Component::ParentDir => {
                return Err(ApplyError::UnsafeApplyPath(rel.display().to_string()));
            }
            _ => {
                // RootDir / Prefix / verbatim paths are never relative.
                return Err(ApplyError::UnsafeApplyPath(rel.display().to_string()));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(ApplyError::UnsafeApplyPath("<empty>".to_string()));
    }
    Ok(out)
}

/// `path` (which exists) must canonicalize to somewhere inside `root`.
fn ensure_resolves_within(
    path: &Path,
    root: &Path,
    what: &str,
    rel: &Path,
) -> Result<(), ApplyError> {
    let canonical = path
        .canonicalize()
        .map_err(|err| ApplyError::Internal(format!("cannot canonicalize {what} path: {err}")))?;
    let canonical_root = root
        .canonicalize()
        .map_err(|err| ApplyError::Internal(format!("cannot canonicalize {what} root: {err}")))?;
    if !canonical.starts_with(&canonical_root) {
        return Err(ApplyError::UnsafeApplyPath(format!(
            "{} path escapes {}: {}",
            what,
            canonical_root.display(),
            rel.display()
        )));
    }
    Ok(())
}

/// The deepest existing ancestor of `path` must canonicalize inside `root`.
///
/// The destination may not exist yet (its parents may not either), so walk up
/// to the first existing ancestor. A symlinked parent pointing outside the
/// repository is caught here — the copy would otherwise write outside.
fn ensure_within_source(path: &Path, root: &Path, rel: &Path) -> Result<(), ApplyError> {
    let canonical_root = root
        .canonicalize()
        .map_err(|err| ApplyError::Internal(format!("cannot canonicalize source root: {err}")))?;
    let mut ancestor = path;
    loop {
        if ancestor.exists() {
            let canonical = ancestor.canonicalize().map_err(|err| {
                ApplyError::Internal(format!("cannot canonicalize target ancestor: {err}"))
            })?;
            if !canonical.starts_with(&canonical_root) {
                return Err(ApplyError::UnsafeApplyPath(format!(
                    "destination escapes source repository: {}",
                    rel.display()
                )));
            }
            return Ok(());
        }
        match ancestor.parent() {
            Some(parent) => ancestor = parent,
            None => {
                // Reached the filesystem root without finding an existing
                // ancestor (should not happen for a valid source root).
                return Err(ApplyError::Internal(format!(
                    "no existing ancestor for {}",
                    rel.display()
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_dotdot_and_absolute_and_root() {
        let bad: &[&str] = &[
            "../escape.txt",
            "a/../../escape.txt",
            "/etc/passwd",
            ".",
            "./a.txt",
            "",
        ];
        for rel in bad {
            assert!(
                sanitize_relative(Path::new(rel)).is_err(),
                "must reject {rel:?}"
            );
        }
        // Hidden files are normal relative paths and stay allowed.
        assert!(sanitize_relative(Path::new(".hidden")).is_ok());
        assert!(sanitize_relative(Path::new("a/b.txt")).is_ok());
        assert!(sanitize_relative(Path::new("a/b/c.txt")).is_ok());
    }
}
