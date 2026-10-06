use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{HostControlError, HostControlResult};

pub const DESIRED_STATE_SCHEMA_V1: &str = "okx.host.desired/v1";
pub const DESIRED_STATE_SCHEMA_V2: &str = "okx.host.desired/v2";
const DEFAULT_DESIRED_PATH: &str = r"C:\okx-control\desired.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentDesired {
    Running,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentProfile {
    #[default]
    Production,
    DemoAcceptance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentDesiredState {
    pub agent: AgentDesired,
    pub profile: AgentProfile,
}

impl AgentDesiredState {
    pub const fn stopped() -> Self {
        Self {
            agent: AgentDesired::Stopped,
            profile: AgentProfile::Production,
        }
    }

    pub const fn production_running() -> Self {
        Self {
            agent: AgentDesired::Running,
            profile: AgentProfile::Production,
        }
    }

    pub const fn demo_acceptance_running() -> Self {
        Self {
            agent: AgentDesired::Running,
            profile: AgentProfile::DemoAcceptance,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct DesiredHeader {
    schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredFileV1 {
    schema: String,
    agent: AgentDesired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DesiredFileV2 {
    schema: String,
    agent: AgentDesired,
    profile: AgentProfile,
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

    pub fn load(&self) -> HostControlResult<AgentDesiredState> {
        if !self.path.exists() {
            return Ok(AgentDesiredState::stopped());
        }

        let bytes = fs::read(&self.path)?;
        let header: DesiredHeader =
            serde_json::from_slice(&bytes).map_err(|_| HostControlError::DesiredStateCorrupt)?;
        match header.schema.as_str() {
            DESIRED_STATE_SCHEMA_V1 => {
                let state: DesiredFileV1 = serde_json::from_slice(&bytes)
                    .map_err(|_| HostControlError::DesiredStateCorrupt)?;
                Ok(AgentDesiredState {
                    agent: state.agent,
                    profile: AgentProfile::Production,
                })
            }
            DESIRED_STATE_SCHEMA_V2 => {
                let state: DesiredFileV2 = serde_json::from_slice(&bytes)
                    .map_err(|_| HostControlError::DesiredStateCorrupt)?;
                Ok(AgentDesiredState {
                    agent: state.agent,
                    profile: state.profile,
                })
            }
            _ => Err(HostControlError::DesiredStateCorrupt),
        }
    }

    pub fn save(&self, state: AgentDesiredState) -> HostControlResult<()> {
        let parent = self
            .path
            .parent()
            .ok_or(HostControlError::DesiredStateCorrupt)?;
        fs::create_dir_all(parent)?;

        let payload = serde_json::to_vec_pretty(&DesiredFileV2 {
            schema: DESIRED_STATE_SCHEMA_V2.to_owned(),
            agent: state.agent,
            profile: state.profile,
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

    fn test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("okx-desired-{label}-{}", std::process::id()))
    }

    #[test]
    fn missing_state_fails_closed_to_stopped_production() {
        let root = test_root("missing");
        let _ = fs::remove_dir_all(&root);
        let store = DesiredStateStore::at(root.join("desired.json"));

        assert_eq!(store.load().expect("load"), AgentDesiredState::stopped());
    }

    #[test]
    fn v1_running_state_migrates_to_production_profile() {
        let root = test_root("v1");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("root");
        let path = root.join("desired.json");
        fs::write(
            &path,
            br#"{
  "schema": "okx.host.desired/v1",
  "agent": "RUNNING"
}"#,
        )
        .expect("write v1");
        let store = DesiredStateStore::at(path);

        assert_eq!(
            store.load().expect("load v1"),
            AgentDesiredState::production_running()
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn v2_demo_profile_round_trips_without_second_state_owner() {
        let root = test_root("v2-demo");
        let _ = fs::remove_dir_all(&root);
        let store = DesiredStateStore::at(root.join("desired.json"));

        store
            .save(AgentDesiredState::demo_acceptance_running())
            .expect("save demo");
        assert_eq!(
            store.load().expect("load demo"),
            AgentDesiredState::demo_acceptance_running()
        );

        store
            .save(AgentDesiredState::production_running())
            .expect("save production");
        assert_eq!(
            store.load().expect("load production"),
            AgentDesiredState::production_running()
        );

        let _ = fs::remove_dir_all(root);
    }
}
