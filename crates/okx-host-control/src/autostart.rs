use std::{fs, path::PathBuf, process::Command};

use chrono::{Duration as ChronoDuration, Local};
use serde_json::{Value, json};

use crate::{HostControlError, HostControlResult};

const TASK_NAME: &str = r"\iamaman11-okx-host-control";
const CONTROLLER_PATH: &str = r"C:\okx-control\okx-host-control.exe";
const TASK_XML_PATH: &str = r"C:\okx-control\okx-host-control-task.xml";
const UPDATE_CONTROLLER_PATH: &str = r"C:\okx-control\update\okx-host-control.exe.staged";
const CANONICAL_WORKING_DIRECTORY: &str = r"C:\okx-control";

pub fn install() -> HostControlResult<Value> {
    install_action(CONTROLLER_PATH, "run", r"C:\okx-control")?;
    status_value()
}

pub fn install_controller_update_activation() -> HostControlResult<Value> {
    ensure_policy_valid()?;
    change_action(UPDATE_CONTROLLER_PATH, "activate-controller-update")?;
    if let Err(error) = ensure_activation_policy_valid() {
        let _ = change_action(CONTROLLER_PATH, "run");
        return Err(error);
    }
    Ok(json!({
        "changed": true,
        "task_name": TASK_NAME,
        "controller_path": UPDATE_CONTROLLER_PATH,
        "arguments": "activate-controller-update",
        "working_directory": CANONICAL_WORKING_DIRECTORY
    }))
}

pub fn restore_controller_action() -> HostControlResult<Value> {
    change_action(CONTROLLER_PATH, "run")?;
    status_value()
}

fn change_action(command: &str, arguments: &str) -> HostControlResult<()> {
    #[cfg(not(windows))]
    {
        let _ = (command, arguments);
        return Err(HostControlError::UnsupportedPlatform);
    }

    #[cfg(windows)]
    {
        let task_run = format!("{command} {arguments}");
        let status = Command::new("schtasks.exe")
            .args(["/Change", "/TN", TASK_NAME, "/TR", &task_run])
            .status()?;

        if !status.success() {
            return Err(HostControlError::CommandFailed("schtasks change"));
        }
        Ok(())
    }
}

fn install_action(
    command: &str,
    arguments: &str,
    working_directory: &str,
) -> HostControlResult<()> {
    #[cfg(not(windows))]
    {
        let _ = (command, arguments, working_directory);
        return Err(HostControlError::UnsupportedPlatform);
    }

    #[cfg(windows)]
    {
        let account = current_account()?;
        let start_boundary = (Local::now() + ChronoDuration::seconds(10))
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        let xml = task_xml(
            &account,
            &start_boundary,
            command,
            arguments,
            working_directory,
        );
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
        Ok(())
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

pub fn ensure_activation_policy_valid() -> HostControlResult<()> {
    #[cfg(not(windows))]
    {
        return Err(HostControlError::UnsupportedPlatform);
    }

    #[cfg(windows)]
    {
        let output = Command::new("schtasks.exe")
            .args(["/Query", "/TN", TASK_NAME, "/XML"])
            .output()?;
        if output.status.success()
            && exported_activation_policy_valid(&String::from_utf8_lossy(&output.stdout))
        {
            Ok(())
        } else {
            Err(HostControlError::AutostartPolicyInvalid)
        }
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
    exported_policy_shape_valid(xml)
        && xml.contains(CONTROLLER_PATH)
        && xml.contains("<Arguments>run</Arguments>")
}

fn exported_activation_policy_valid(xml: &str) -> bool {
    exported_policy_shape_valid(xml)
        && xml.contains(UPDATE_CONTROLLER_PATH)
        && xml.contains("<Arguments>activate-controller-update</Arguments>")
}

fn exported_policy_shape_valid(xml: &str) -> bool {
    xml.contains("<WorkingDirectory>C:\\okx-control</WorkingDirectory>")
        && xml.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>")
        && xml.matches("<TimeTrigger>").count() == 1
        && xml.contains("<StartBoundary>")
        && xml.contains("<Repetition>")
        && xml.contains("<Interval>PT1M</Interval>")
        && !xml.contains("<Duration>")
        && xml.contains("<StartWhenAvailable>true</StartWhenAvailable>")
        && xml.contains("<LogonType>InteractiveToken</LogonType>")
        && !xml.contains("<RestartOnFailure>")
        && !xml.contains("<LogonTrigger>")
        && !xml.contains("<RegistrationTrigger>")
        && !xml.contains("<BootTrigger>")
        && !xml.contains("<CalendarTrigger>")
        && !xml.contains("<EventTrigger>")
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

fn task_xml(
    account: &str,
    start_boundary: &str,
    command: &str,
    arguments: &str,
    working_directory: &str,
) -> String {
    let account = xml_escape(account);
    let start_boundary = xml_escape(start_boundary);
    let command = xml_escape(command);
    let arguments = xml_escape(arguments);
    let working_directory = xml_escape(working_directory);
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
      <Command>{command}</Command>
      <Arguments>{arguments}</Arguments>
      <WorkingDirectory>{working_directory}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
"#,
        account = account,
        start_boundary = start_boundary,
        command = command,
        arguments = arguments,
        working_directory = working_directory,
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

    const TEST_START: &str = "2026-09-27T02:30:00";

    #[test]
    fn task_xml_has_one_time_trigger_supervisor() {
        let xml = task_xml(
            r"HOST\User",
            TEST_START,
            CONTROLLER_PATH,
            "run",
            r"C:\okx-control",
        );

        assert!(xml.starts_with(r#"<?xml version="1.0" ?>"#));
        assert!(!xml.contains("encoding="));
        assert!(xml.contains(CONTROLLER_PATH));
        assert!(xml.contains("<Arguments>run</Arguments>"));
        assert!(xml.contains("<WorkingDirectory>C:\\okx-control</WorkingDirectory>"));
        assert!(xml.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>"));
        assert_eq!(xml.matches("<TimeTrigger>").count(), 1);
        assert!(xml.contains("<StartBoundary>2026-09-27T02:30:00</StartBoundary>"));
        assert!(xml.contains("<Repetition>"));
        assert!(xml.contains("<Interval>PT1M</Interval>"));
        assert!(!xml.contains("<Duration>"));
        assert!(xml.contains("<StartWhenAvailable>true</StartWhenAvailable>"));
        assert!(xml.contains("<LogonType>InteractiveToken</LogonType>"));

        assert!(!xml.contains("<RestartOnFailure>"));
        assert!(!xml.contains("<LogonTrigger>"));
        assert!(!xml.contains("<RegistrationTrigger>"));
    }

    #[test]
    fn exported_policy_accepts_exact_time_trigger_contract() {
        assert!(exported_policy_valid(&task_xml(
            r"HOST\User",
            TEST_START,
            CONTROLLER_PATH,
            "run",
            r"C:\okx-control",
        )));
    }

    #[test]
    fn exported_policy_rejects_bounded_or_wrong_repetition() {
        let bounded = task_xml(
            r"HOST\User",
            TEST_START,
            CONTROLLER_PATH,
            "run",
            r"C:\okx-control",
        )
        .replace(
            "<Interval>PT1M</Interval>",
            "<Interval>PT1M</Interval><Duration>PT1H</Duration>",
        );
        assert!(!exported_policy_valid(&bounded));

        let wrong_interval = task_xml(
            r"HOST\User",
            TEST_START,
            CONTROLLER_PATH,
            "run",
            r"C:\okx-control",
        )
        .replace("<Interval>PT1M</Interval>", "<Interval>PT5M</Interval>");
        assert!(!exported_policy_valid(&wrong_interval));
    }

    #[test]
    fn exported_policy_rejects_duplicate_recovery_authority() {
        let duplicate = task_xml(
            r"HOST\User",
            TEST_START,
            CONTROLLER_PATH,
            "run",
            r"C:\okx-control",
        ).replace(
            "<Priority>7</Priority>",
            "<Priority>7</Priority><RestartOnFailure><Interval>PT1M</Interval><Count>32</Count></RestartOnFailure>",
        );
        assert!(!exported_policy_valid(&duplicate));
    }

    #[test]
    fn exported_policy_rejects_non_time_trigger() {
        let logon = task_xml(
            r"HOST\User",
            TEST_START,
            CONTROLLER_PATH,
            "run",
            r"C:\okx-control",
        )
        .replace("<TimeTrigger>", "<LogonTrigger>")
        .replace("</TimeTrigger>", "</LogonTrigger>");
        assert!(!exported_policy_valid(&logon));
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
