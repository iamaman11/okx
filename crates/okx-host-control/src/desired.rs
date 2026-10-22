//! Process lifecycle state only. No account access or financial authority.
use crate::{HostControlError, HostControlResult};
use okx_protocol::host_state_proto::{self, HOST_STATE_VERSION, HostStateWire};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentDesired {
    Running,
    Stopped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentDesiredState {
    pub agent: AgentDesired,
}
impl AgentDesiredState {
    pub const fn stopped() -> Self {
        Self {
            agent: AgentDesired::Stopped,
        }
    }
    pub const fn running() -> Self {
        Self {
            agent: AgentDesired::Running,
        }
    }
}
#[derive(Debug, Clone)]
pub struct DesiredStateStore {
    path: PathBuf,
}
impl DesiredStateStore {
    pub fn canonical() -> Self {
        Self {
            path: PathBuf::from(r"C:\okx-control\engine-state.pb"),
        }
    }
    pub fn load(&self) -> HostControlResult<AgentDesiredState> {
        let bytes = match fs::read(&self.path) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(AgentDesiredState::stopped());
            }
            Err(e) => return Err(e.into()),
        };
        let state = host_state_proto::decode_host_state(&bytes)
            .ok_or(HostControlError::DesiredStateCorrupt)?;
        Ok(AgentDesiredState {
            agent: match state.desired_agent {
                1 => AgentDesired::Running,
                2 => AgentDesired::Stopped,
                _ => return Err(HostControlError::DesiredStateCorrupt),
            },
        })
    }
    pub fn save(&self, state: AgentDesiredState) -> HostControlResult<()> {
        let bytes = host_state_proto::encode_host_state(&HostStateWire {
            version: HOST_STATE_VERSION,
            desired_agent: match state.agent {
                AgentDesired::Running => 1,
                AgentDesired::Stopped => 2,
            },
        })
        .ok_or(HostControlError::DesiredStateCorrupt)?;
        let parent = self
            .path
            .parent()
            .ok_or(HostControlError::DesiredStateCorrupt)?;
        fs::create_dir_all(parent)?;
        let temp = temp_path(&self.path);
        let mut writer = File::create(&temp)?;
        writer.write_all(&bytes)?;
        writer.sync_all()?;
        drop(writer);
        atomic_replace(&temp, &self.path)?;
        #[cfg(not(windows))]
        File::open(parent)?.sync_all()?;
        Ok(())
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
}
pub(crate) fn temp_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".tmp");
    PathBuf::from(value)
}

#[cfg(windows)]
pub(crate) fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
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
pub(crate) fn atomic_replace(source: &Path, destination: &Path) -> HostControlResult<()> {
    fs::rename(source, destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn process_restart_preserves_desired_without_financial_flags() {
        let root = std::env::temp_dir().join(format!("okx-engine-desired-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let store = DesiredStateStore {
            path: root.join("engine-state.pb"),
        };
        assert_eq!(store.load().unwrap(), AgentDesiredState::stopped());
        store.save(AgentDesiredState::running()).unwrap();
        assert_eq!(store.load().unwrap(), AgentDesiredState::running());
        fs::write(&store.path, b"{\"schema\":\"okx.host.desired/v3\"}").unwrap();
        assert!(store.load().is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
