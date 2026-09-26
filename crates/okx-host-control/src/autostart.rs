use std::{fs, path::PathBuf, process::Command};

use chrono::{Duration as ChronoDuration, Local};
use serde_json::{Value, json};

use crate::{HostControlError, HostControlResult};

const TASK_NAME: &str = r"\iamaman11-okx-host-control";
const CONTROLLER_PATH: &str = r"C:\okx-control\okx-host-control.exe";
const TASK_XML_PATH: &str = r"C:\okx-control\okx-host-control-task.xml";

pub fn install() -> HostControlResult<Value> {
    #[cfg(not(windows))]
    {
        return Err(HostControlError::UnsupportedPlatform);
    }

    #[cfg(windows)]
    {
        let account = current_account()?;
        let start_boundary = (Local::now() + ChronoDuration::seconds(10))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        let xml = task_xml(&account, &start_boundary);
        let xml_path = PathBuf::from(TASK_XML_PATH);

        if let Some(parent) = xml_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&xml_path, xml.as_bytes())?;

        let status = Command::new("schtasks.exe")
            .args(["/Create", "/TN", TASK_NAME, "/XML", TASK_XML_PATH, "/F"])
            .status()?;

        if !status.success() {
            return Err(HostControlError::CommandFailed("schtasks create"));
        }

        status_value()
    }
}

pub fn run_now() -> HostControlResult<Value> {
    #[cfg(not(windows))]
    {
        return Err(HostControlError::UnsupportedPlatform);
    }

    #[cfg(windows)]
    {
        ensure_policy_valid()?;
        let status = Command::new("schtasks.exe")
            .args(["/Run", "/TN", TASK_NAME])
            .status()?;

        if !status.success() {
            return Err(HostControlError::CommandFailed("schtasks run"));
        }

        Ok(json!({
            "task_name": TASK_NAME,
            "run_requested": true
        }))
    }
}

pub fn ensure_policy_valid() -> HostControlResult<()> {
    let status = status_value()?;
    if status
        .get("policy_valid")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(HostControlError::AutostartPolicyInvalid)
    }
}

pub fn status_value() -> HostControlResult<Value> {
    #[cfg(not(windows))]
    {
        return Ok(json!({
            "installed": false,
            "policy_valid": false,
            "platform": "non-windows"
        }));
    }

    #[cfg(windows)]
    {
        let output = Command::new("schtasks.exe")
            .args(["/Query", "/TN", TASK_NAME, "/XML"])
            .output()?;

        if !output.status.success() {
            return Ok(json!({
                "installed": false,
                "policy_valid": false,
                "task_name": TASK_NAME
            }));
        }

        let xml = String::from_utf8_lossy(&output.stdout);
        let policy_valid = exported_policy_valid(&xml);

        Ok(json!({
            "installed": true,
            "policy_valid": policy_valid,
            "task_name": TASK_NAME,
            "controller_path": CONTROLLER_PATH
        }))
    }
}

fn exported_policy_valid(xml: &str) -> bool {
    xml.contains(CONTROLLER_PATH)
        && xml.contains("<Arguments>run --poll-seconds 2</Arguments>")
        && xml.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>")
        && xml.contains("<TimeTrigger>")
        && xml.contains("<StartBoundary>")
        && xml.contains("<Repetition>")
        && xml.contains("<Interval>PT1M</Interval>")
        && !xml.contains("<Duration>")
        && !xml.contains("<RestartOnFailure>")
        && !xml.contains("<LogonTrigger>")
        && xml.contains("<LogonType>InteractiveToken</LogonType>")
}

fn current_account() -> HostControlResult<String> {
    let output = Command::new("whoami.exe").output()?;
    if !output.status.success() {
        return Err(HostControlError::CommandFailed("whoami"));
    }

    let account = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if account.is_empty() || account.len() > 256 || account.chars().any(|ch| ch.is_control()) {
        return Err(HostControlError::WindowsIdentityUnavailable);
    }

    Ok(account)
}

fn task_xml(account: &str, start_boundary: &str) -> String {
    let account = xml_escape(account);
    let start_boundary = xml_escape(start_boundary);
    format!(
        r#"<?xml version="1.0" ?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Author>{account}</Author>
    <Description>OKX native host control plane</Description>
  </RegistrationInfo>
  <Triggers>
    <TimeTrigger>
      <StartBoundary>{start_boundary}</StartBoundary>
      <Enabled>true</Enabled>
      <Repetition>
        <Interval>PT1M</Interval>
      </Repetition>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{account}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{controller}</Command>
      <Arguments>run --poll-seconds 2</Arguments>
      <WorkingDirectory>C:\okx-control</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"#,
        account = account,
        start_boundary = start_boundary,
        controller = xml_escape(CONTROLLER_PATH),
    )
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_xml_is_fixed_and_single_instance() {
        let xml = task_xml(r"HOST\User", "2026-09-27T02:30:00");
        assert!(xml.starts_with(r#"<?xml version="1.0" ?>"#));
        assert!(!xml.contains("encoding="));
        assert!(xml.contains(CONTROLLER_PATH));
        assert!(xml.contains("run --poll-seconds 2"));
        assert!(xml.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"));
        assert!(xml.contains("<TimeTrigger>"));
        assert!(xml.contains("<StartBoundary>2026-09-27T02:30:00</StartBoundary>"));
        assert!(!xml.contains("<LogonTrigger>"));
        assert!(xml.contains("<Repetition>"));
        assert!(xml.contains("<Interval>PT1M</Interval>"));
        assert!(!xml.contains("<Duration>"));
        assert!(!xml.contains("<RestartOnFailure>"));
        assert!(xml.contains("<LogonType>InteractiveToken</LogonType>"));
    }

    #[test]
    #[test]
    fn exported_policy_rejects_bounded_or_wrong_repetition() {
        let bounded = task_xml(r"HOST\User", "2026-09-27T02:30:00").replace(
            "<Interval>PT1M</Interval>",
            "<Interval>PT1M</Interval><Duration>PT1H</Duration>",
        );
        assert!(!exported_policy_valid(&bounded));

        let wrong_interval = task_xml(r"HOST\User", "2026-09-27T02:30:00")
            .replace("<Interval>PT1M</Interval>", "<Interval>PT5M</Interval>");
        assert!(!exported_policy_valid(&wrong_interval));
    }

    #[test]
    fn exported_policy_rejects_duplicate_recovery_authority() {
        let duplicate = task_xml(r"HOST\User", "2026-09-27T02:30:00").replace(
            "<Priority>7</Priority>",
            "<Priority>7</Priority><RestartOnFailure><Interval>PT1M</Interval><Count>32</Count></RestartOnFailure>",
        );
        assert!(!exported_policy_valid(&duplicate));
    }

    #[test]
    fn xml_escape_covers_special_characters() {
        assert_eq!(xml_escape("A&B<C>\"'"), "A&amp;B&lt;C&gt;&quot;&apos;");
    }

    #[test]
    fn task_xml_path_is_outside_mutable_repo() {
        assert!(std::path::Path::new(TASK_XML_PATH).starts_with(r"C:\okx-control"));
    }
}
