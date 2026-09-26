use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{HostControlError, HostControlResult};

pub const DESIRED_STATE_SCHEMA_V1: &str = "okx.host.desired/v1";
const DEFAULT_DESIRED_PATH: &str = r"C:\okx-control\desired.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentDesired {
    Running,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredFile {
    schema: String,
    agent: AgentDesired,
}

#[derive(Debug, Clone)]
pub struct DesiredStateStore {
    path: PathBuf,
}

impl DesiredStateStore {
    pub fn canonical() -> Self {
        Self {
            path: PathBuf::from(DEFAULT_DESIRED_PATH),
        }
    }

    #[cfg(test)]
    fn at(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn load(&self) -> HostControlResult<AgentDesired> {
        if !self.path.exists() {
            return Ok(AgentDesired::Stopped);
        }

        let bytes = fs::read(&self.path)?;
        let state: DesiredFile =
            serde_json::from_slice(&bytes).map_err(|_| HostControlError::DesiredStateCorrupt)?;

        if state.schema != DESIRED_STATE_SCHEMA_V1 {
            return Err(HostControlError::DesiredStateCorrupt);
        }

        Ok(state.agent)
    }

    pub fn save(&self, desired: AgentDesired) -> HostControlResult<()> {
        let parent = self
            .path
            .parent()
            .ok_or(HostControlError::DesiredStateCorrupt)?;
        fs::create_dir_all(parent)?;

        let payload = serde_json::to_vec_pretty(&DesiredFile {
            schema: DESIRED_STATE_SCHEMA_V1.to_owned(),
            agent: desired,
        })?;

        let temp = temp_path(&self.path);
        let mut file = File::create(&temp)?;
        file.write_all(&payload)?;
        file.sync_all()?;
        drop(file);

        atomic_replace(&temp, &self.path)?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn temp_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".tmp");
    PathBuf::from(value)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    let source_w = wide(source);
    let destination_w = wide(destination);
    let ok = unsafe {
        MoveFileExW(
            source_w.as_ptr(),
            destination_w.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };

    if ok == 0 {
        Err(std::io::Error::last_os_error().into())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    if destination.exists() {
        fs::remove_file(destination)?;
    }
    fs::rename(source, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_state_fails_closed_to_stopped() {
        let root = std::env::temp_dir().join(format!("okx-desired-missing-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let store = DesiredStateStore::at(root.join("desired.json"));

        assert_eq!(store.load().expect("load"), AgentDesired::Stopped);
    }

    #[test]
    fn desired_state_round_trips() {
        let root =
            std::env::temp_dir().join(format!("okx-desired-roundtrip-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let store = DesiredStateStore::at(root.join("desired.json"));

        store.save(AgentDesired::Running).expect("save running");
        assert_eq!(store.load().expect("load running"), AgentDesired::Running);

        store.save(AgentDesired::Stopped).expect("save stopped");
        assert_eq!(store.load().expect("load stopped"), AgentDesired::Stopped);

        let _ = fs::remove_dir_all(root);
    }
}
