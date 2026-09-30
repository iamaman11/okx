use std::{
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Command, Output},
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{HostControlError, HostControlResult};

const QUARANTINE_ROOT: &str = r"C:\okx-control\workspace-quarantine";
const MAX_CHANGES: usize = 16;
const MAX_PATH_BYTES: usize = 512;
const MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Debug)]
struct Candidate {
    relative: PathBuf,
    local_sha256: String,
    target_sha256: String,
    quarantine: PathBuf,
}

pub fn reconcile(repo_root: &Path) -> HostControlResult<Value> {
    if !repo_root.join(".git").is_dir() {
        return Err(HostControlError::RepositoryMissing);
    }

    git(repo_root, &["fetch", "--prune", "origin", "main"])?;
    if git(repo_root, &["rev-parse", "--abbrev-ref", "HEAD"])? != "main" {
        return Err(HostControlError::BranchMismatch);
    }

    let head = git(repo_root, &["rev-parse", "HEAD"])?;
    let origin_main = git(repo_root, &["rev-parse", "origin/main"])?;
    let divergence = git(
        repo_root,
        &["rev-list", "--left-right", "--count", "HEAD...origin/main"],
    )?;
    let (ahead, _behind) = parse_divergence(&divergence)?;
    if ahead != 0 {
        return Err(HostControlError::WorkspaceReconcileUnsafe);
    }

    let status = git(repo_root, &["status", "--porcelain=v1", "--untracked-files=all"])?;
    let lines = status
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect::<Vec<_>>();

    if lines.len() > MAX_CHANGES {
        return Err(HostControlError::WorkspaceReconcileUnsafe);
    }

    if lines.is_empty() {
        if head != origin_main {
            git(repo_root, &["merge", "--ff-only", "origin/main"])?;
        }
        return final_status(repo_root, Vec::new());
    }

    let quarantine_root = Path::new(QUARANTINE_ROOT).join(&origin_main);
    let mut candidates = Vec::with_capacity(lines.len());

    // Full preflight before the first filesystem mutation.
    for line in lines {
        let relative = parse_untracked_path(line)?;
        let local = repo_root.join(&relative);
        let metadata = fs::symlink_metadata(&local)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_FILE_BYTES
        {
            return Err(HostControlError::WorkspaceReconcileUnsafe);
        }

        let spec = format!("origin/main:{}", git_path(&relative)?);
        let target_size = git(repo_root, &["cat-file", "-s", &spec])?
            .parse::<u64>()
            .map_err(|_| HostControlError::WorkspaceReconcileUnsafe)?;
        if target_size > MAX_FILE_BYTES {
            return Err(HostControlError::WorkspaceReconcileUnsafe);
        }

        let target = git_bytes(repo_root, &["show", &spec])?;
        if target.len() as u64 != target_size {
            return Err(HostControlError::WorkspaceReconcileUnsafe);
        }

        let quarantine = quarantine_root.join(&relative);
        if quarantine.exists() {
            return Err(HostControlError::WorkspaceQuarantineConflict);
        }

        candidates.push(Candidate {
            relative,
            local_sha256: sha256_file(&local)?,
            target_sha256: sha256_bytes(&target),
            quarantine,
        });
    }

    let mut moved = Vec::<(PathBuf, PathBuf)>::new();
    for candidate in &candidates {
        let source = repo_root.join(&candidate.relative);
        if let Some(parent) = candidate.quarantine.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Err(error) = fs::rename(&source, &candidate.quarantine) {
            rollback_moves(&moved);
            return Err(error.into());
        }
        moved.push((source, candidate.quarantine.clone()));
    }

    if let Err(error) = git(repo_root, &["merge", "--ff-only", "origin/main"]) {
        rollback_moves(&moved);
        return Err(error);
    }

    let current_head = git(repo_root, &["rev-parse", "HEAD"])?;
    let final_status_text = git(repo_root, &["status", "--porcelain=v1", "--untracked-files=all"])?;
    if current_head != origin_main || !final_status_text.trim().is_empty() {
        return Err(HostControlError::WorkspaceReconcileInvariant);
    }

    final_status(repo_root, candidates)
}

fn final_status(repo_root: &Path, candidates: Vec<Candidate>) -> HostControlResult<Value> {
    let head = git(repo_root, &["rev-parse", "HEAD"])?;
    let status = git(repo_root, &["status", "--porcelain=v1", "--untracked-files=all"])?;
    let quarantined = candidates
        .into_iter()
        .map(|candidate| {
            json!({
                "path": git_path(&candidate.relative).unwrap_or_else(|_| "<invalid>".to_owned()),
                "local_sha256": candidate.local_sha256,
                "origin_main_sha256": candidate.target_sha256,
                "content_equal": candidate.local_sha256 == candidate.target_sha256,
                "quarantine_path": candidate.quarantine
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "schema": "okx.host-control.workspace-reconcile/v1",
        "head": head,
        "branch": git(repo_root, &["rev-parse", "--abbrev-ref", "HEAD"])?,
        "clean": status.trim().is_empty(),
        "quarantined_count": quarantined.len(),
        "quarantined": quarantined,
        "strategy": "PREFLIGHT_QUARANTINE_THEN_FF_ONLY"
    }))
}

fn parse_untracked_path(line: &str) -> HostControlResult<PathBuf> {
    let path = line
        .strip_prefix("?? ")
        .ok_or(HostControlError::WorkspaceReconcileUnsafe)?;
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.starts_with('"')
        || path.ends_with('"')
        || path.contains('\0')
        || path.contains('\\')
        || path.contains(':')
    {
        return Err(HostControlError::WorkspaceReconcileUnsafe);
    }

    let relative = PathBuf::from(path);
    if relative.is_absolute()
        || relative.components().any(|component| {
            !matches!(component, Component::Normal(_))
        })
    {
        return Err(HostControlError::WorkspaceReconcileUnsafe);
    }
    Ok(relative)
}

fn git_path(path: &Path) -> HostControlResult<String> {
    let text = path
        .to_str()
        .ok_or(HostControlError::WorkspaceReconcileUnsafe)?
        .replace('\\', "/");
    if text.is_empty() || text.len() > MAX_PATH_BYTES {
        return Err(HostControlError::WorkspaceReconcileUnsafe);
    }
    Ok(text)
}

fn parse_divergence(value: &str) -> HostControlResult<(u64, u64)> {
    let mut parts = value.split_whitespace();
    let ahead = parts
        .next()
        .ok_or(HostControlError::WorkspaceReconcileUnsafe)?
        .parse::<u64>()
        .map_err(|_| HostControlError::WorkspaceReconcileUnsafe)?;
    let behind = parts
        .next()
        .ok_or(HostControlError::WorkspaceReconcileUnsafe)?
        .parse::<u64>()
        .map_err(|_| HostControlError::WorkspaceReconcileUnsafe)?;
    if parts.next().is_some() {
        return Err(HostControlError::WorkspaceReconcileUnsafe);
    }
    Ok((ahead, behind))
}

fn rollback_moves(moved: &[(PathBuf, PathBuf)]) {
    for (source, quarantine) in moved.iter().rev() {
        if quarantine.exists() && !source.exists() {
            if let Some(parent) = source.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let _ = fs::rename(quarantine, source);
        }
    }
}

fn git(repo_root: &Path, args: &[&str]) -> HostControlResult<String> {
    let output = git_output(repo_root, args)?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_bytes(repo_root: &Path, args: &[&str]) -> HostControlResult<Vec<u8>> {
    Ok(git_output(repo_root, args)?.stdout)
}

fn git_output(repo_root: &Path, args: &[&str]) -> HostControlResult<Output> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(HostControlError::CommandFailed("git"));
    }
    Ok(output)
}

fn sha256_file(path: &Path) -> HostControlResult<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex(&digest.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_untracked_relative_paths() {
        assert_eq!(
            parse_untracked_path("?? Cargo.lock").expect("path"),
            PathBuf::from("Cargo.lock")
        );
        assert_eq!(
            parse_untracked_path("?? nested/file.txt").expect("path"),
            PathBuf::from("nested/file.txt")
        );
        for invalid in [
            " M Cargo.lock",
            "?? ../Cargo.lock",
            "?? C:\\Cargo.lock",
            "?? \"quoted path\"",
        ] {
            assert!(parse_untracked_path(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn divergence_requires_exact_two_numbers() {
        assert_eq!(parse_divergence("0\t606").expect("divergence"), (0, 606));
        assert!(parse_divergence("1").is_err());
        assert!(parse_divergence("0 1 2").is_err());
    }

    #[test]
    fn quarantine_is_outside_mutable_workspace() {
        assert!(Path::new(QUARANTINE_ROOT).starts_with(r"C:\okx-control"));
        assert!(!Path::new(QUARANTINE_ROOT).starts_with(r"C:\okx\"));
    }

    #[test]
    fn no_reset_or_clean_execution_path_exists() {
        let source = include_str!("workspace.rs");
        let reset = ["git", " reset"].concat();
        let clean = ["git", " clean"].concat();
        assert!(!source.contains(&reset));
        assert!(!source.contains(&clean));
    }
}
