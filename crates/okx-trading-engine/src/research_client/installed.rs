use okx_ipc_contracts::research_ipc::{PROTOCOL_VERSION, STORAGE_VERSION};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{BufReader, Read},
    path::{Path, PathBuf},
};
const REPOSITORY_ID: u64 = 1_388_071_566;
const MAX_BINARY_BYTES: u64 = 128 * 1024 * 1024;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstalledResearch {
    schema: String,
    repository_id: u64,
    run_id: u64,
    artifact_id: u64,
    source_head_sha: String,
    source_tree: String,
    worker_sha256: String,
    protocol_version: u32,
    storage_version: u32,
}
fn hex(s: &str, n: usize) -> bool {
    s.len() == n
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn plain(path: &Path, directory: bool) -> Result<fs::Metadata, String> {
    let m = fs::symlink_metadata(path).map_err(|_| "RESEARCH_INSTALL_MISSING")?;
    if m.file_type().is_symlink() || (directory && !m.is_dir()) || (!directory && !m.is_file()) {
        return Err("RESEARCH_INSTALL_INVALID_PATH".into());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if m.file_attributes() & 0x400 != 0 {
            return Err("RESEARCH_INSTALL_INVALID_PATH".into());
        }
    }
    Ok(m)
}
fn open_plain(path: &Path) -> Result<fs::File, String> {
    plain(path, false)?;
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(0x0020_0000);
    }
    let file = options
        .open(path)
        .map_err(|_| "RESEARCH_INSTALL_INVALID_PATH")?;
    let meta = file
        .metadata()
        .map_err(|_| "RESEARCH_INSTALL_INVALID_PATH")?;
    if !meta.is_file() {
        return Err("RESEARCH_INSTALL_INVALID_PATH".into());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if meta.file_attributes() & 0x400 != 0 {
            return Err("RESEARCH_INSTALL_INVALID_PATH".into());
        }
    }
    Ok(file)
}
fn directory_chain(path: &Path) -> Result<(), String> {
    if !path.is_absolute() {
        return Err("RESEARCH_INSTALL_INVALID_PATH".into());
    }
    for p in path.ancestors() {
        plain(p, true)?;
    }
    Ok(())
}
/// Read the bounded pointer and validate its content-addressed path without hashing the image.
pub(super) fn candidate(runtime: &Path) -> Result<PathBuf, String> {
    directory_chain(runtime)?;
    let pointer = runtime.join("installed-research.json");
    let meta = plain(&pointer, false)?;
    if meta.len() == 0 || meta.len() > 64 * 1024 {
        return Err("RESEARCH_INSTALL_INVALID_POINTER".into());
    }
    let mut pointer_bytes = Vec::new();
    open_plain(&pointer)?
        .take(64 * 1024 + 1)
        .read_to_end(&mut pointer_bytes)
        .map_err(|_| "RESEARCH_INSTALL_INVALID_POINTER")?;
    if pointer_bytes.len() > 64 * 1024 {
        return Err("RESEARCH_INSTALL_INVALID_POINTER".into());
    }
    let record: InstalledResearch =
        serde_json::from_slice(&pointer_bytes).map_err(|_| "RESEARCH_INSTALL_INVALID_POINTER")?;
    if record.schema != "okx.host-control.installed-research/v1"
        || record.repository_id != REPOSITORY_ID
        || record.run_id == 0
        || record.artifact_id == 0
        || !hex(&record.source_head_sha, 40)
        || !hex(&record.source_tree, 40)
        || !hex(&record.worker_sha256, 64)
        || record.protocol_version != PROTOCOL_VERSION
        || record.storage_version != STORAGE_VERSION
    {
        return Err("RESEARCH_INSTALL_INVALID_PROVENANCE".into());
    }
    let version = runtime
        .join("research")
        .join("versions")
        .join(&record.worker_sha256);
    directory_chain(&version)?;
    // Existing installations remain valid across the product-name migration.
    // A newly staged image always uses the Service name, but a previously
    // verified pointer can still reference the historical Worker filename.
    let service = version.join("okx-research-service.exe");
    let binary = match fs::symlink_metadata(&service) {
        Ok(_) => service,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            version.join("okx-research-worker.exe")
        }
        Err(_) => return Err("RESEARCH_INSTALL_MISSING".into()),
    };
    let meta = plain(&binary, false)?;
    if meta.len() == 0 || meta.len() > MAX_BINARY_BYTES {
        return Err("RESEARCH_INSTALL_INVALID_BINARY".into());
    }
    Ok(binary)
}
/// Fully verify the selected image immediately before starting a child.
pub(super) fn resolve(runtime: &Path) -> Result<PathBuf, String> {
    let binary = candidate(runtime)?;
    let expected = binary
        .parent()
        .and_then(Path::file_name)
        .and_then(|s| s.to_str())
        .ok_or("RESEARCH_INSTALL_INVALID_PATH")?;
    let mut file = BufReader::new(open_plain(&binary)?);
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    let mut total = 0u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| "RESEARCH_INSTALL_INVALID_BINARY")?;
        if count == 0 {
            break;
        }
        total += count as u64;
        if total > MAX_BINARY_BYTES {
            return Err("RESEARCH_INSTALL_INVALID_BINARY".into());
        }
        hash.update(&buffer[..count]);
    }
    if format!("{:x}", hash.finalize()) != expected {
        return Err("RESEARCH_INSTALL_HASH_MISMATCH".into());
    }
    Ok(binary)
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn setup() -> (PathBuf, String) {
        let root = std::env::temp_dir().join(format!(
            "okx-research-install-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let digest = format!("{:x}", Sha256::digest(b"test worker"));
        (root, digest)
    }
    fn pointer(hash: &str) -> serde_json::Value {
        json!({"schema":"okx.host-control.installed-research/v1","repository_id":REPOSITORY_ID,"run_id":1,"artifact_id":2,"source_head_sha":"a".repeat(40),"source_tree":"b".repeat(40),"worker_sha256":hash,"protocol_version":PROTOCOL_VERSION,"storage_version":STORAGE_VERSION})
    }
    #[test]
    fn verifies_pointer_provenance_path_and_hash_without_starting_binary() {
        let (root, hash) = setup();
        assert!(resolve(&root).is_err());
        fs::write(root.join("installed-research.json"), b"{}").unwrap();
        assert!(resolve(&root).is_err());
        let mut data = pointer(&hash);
        data["run_id"] = 0.into();
        fs::write(root.join("installed-research.json"), data.to_string()).unwrap();
        assert!(resolve(&root).is_err());
        data = pointer(&hash);
        data["protocol_version"] = (PROTOCOL_VERSION + 1).into();
        fs::write(root.join("installed-research.json"), data.to_string()).unwrap();
        assert!(resolve(&root).is_err());
        data = pointer("../../bad");
        fs::write(root.join("installed-research.json"), data.to_string()).unwrap();
        assert!(resolve(&root).is_err());
        fs::write(
            root.join("installed-research.json"),
            pointer(&hash).to_string(),
        )
        .unwrap();
        assert!(resolve(&root).is_err());
        let dir = root.join("research/versions").join(&hash);
        fs::create_dir_all(&dir).unwrap();
        let binary = dir.join("okx-research-service.exe");
        fs::write(&binary, b"wrong").unwrap();
        assert_eq!(candidate(&root).unwrap(), binary);
        assert_eq!(
            resolve(&root).unwrap_err(),
            "RESEARCH_INSTALL_HASH_MISMATCH"
        );
        fs::write(&binary, b"test worker").unwrap();
        assert_eq!(resolve(&root).unwrap(), binary);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn existing_verified_worker_image_remains_usable_after_service_rename() {
        let (root, hash) = setup();
        fs::write(
            root.join("installed-research.json"),
            pointer(&hash).to_string(),
        )
        .unwrap();
        let dir = root.join("research/versions").join(&hash);
        fs::create_dir_all(&dir).unwrap();
        let legacy = dir.join("okx-research-worker.exe");
        fs::write(&legacy, b"test worker").unwrap();
        assert_eq!(resolve(&root).unwrap(), legacy);

        // Prefer a present new-name path, without silently running the old
        // image if the new file fails content verification.
        let preferred = dir.join("okx-research-service.exe");
        fs::write(&preferred, b"wrong").unwrap();
        assert_eq!(
            resolve(&root).unwrap_err(),
            "RESEARCH_INSTALL_HASH_MISMATCH"
        );
        fs::write(&preferred, b"test worker").unwrap();
        assert_eq!(resolve(&root).unwrap(), preferred);
        fs::remove_dir_all(root).unwrap();
    }
}
