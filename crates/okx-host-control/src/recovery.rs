use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
};

use serde_json::{Value, json};

use crate::{HostControlError, HostControlResult, service::is_service_mode};

const RECOVERY_ROOT: &str = r"C:\ProgramData\iamaman11\okx\recovery";
const CONTROLLER_CRASH_MARKER: &str = "controller-crash.request";

pub fn controller_crash_probe(request_id: &str) -> HostControlResult<Value> {
    if !is_service_mode() {
        return Err(HostControlError::RecoveryProbeRequiresService);
    }

    let root = PathBuf::from(RECOVERY_ROOT);
    fs::create_dir_all(&root)?;
    let marker = root.join(CONTROLLER_CRASH_MARKER);

    if marker.exists() {
        let existing = fs::read_to_string(&marker)?;
        if existing.trim() != request_id {
            return Err(HostControlError::RecoveryMarkerConflict);
        }

        fs::remove_file(&marker)?;
        return Ok(json!({
            "recovered": true,
            "request_id": request_id,
            "mechanism": "windows_scm_failure_action"
        }));
    }

    let mut file = File::create(&marker)?;
    file.write_all(request_id.as_bytes())?;
    file.sync_all()?;

    std::process::abort();
}
