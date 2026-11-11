use std::{fs, path::PathBuf};

use chrono::{Duration as ChronoDuration, Local};
use serde_json::{Value, json};

use crate::{HostControlError, HostControlResult, background_process::hidden_command};

const TASK_NAME: &str = r"\iamaman11-okx-host-control";
const ENTRYPOINT_PATH: &str = okx_host_launcher::ENTRYPOINT_PATH;
const TASK_XML_PATH: &str = r"C:\okx\control\okx-host-control-task.xml";

pub fn install() -> HostControlResult<Value> {
    install_action(ENTRYPOINT_PATH, "run", okx_host_launcher::CONTROL_ROOT)?;
    status_value()
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

        let status = hidden_command("schtasks.exe")
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
        ensure_run_allowed()?;
        let status = hidden_command("schtasks.exe")
            .args(["/Run", "/TN", TASK_NAME])
            .status()?;

        if !status.success() {
            return Err(HostControlError::CommandFailed("schtasks run"));
        }

        Ok(json!({
            "task_name": TASK_NAME,
            "run_requested": true,
            "entrypoint_path": ENTRYPOINT_PATH
        }))
    }
}

pub fn run_legacy_now() -> HostControlResult<Value> {
    run_now()
}

pub fn ensure_policy_valid() -> HostControlResult<()> {
    ensure_expected_policy(ENTRYPOINT_PATH)
}

pub fn ensure_legacy_policy_valid() -> HostControlResult<()> {
    ensure_policy_valid()
}

/// Running an explicitly disabled task is never a recovery mechanism.
pub fn ensure_run_allowed() -> HostControlResult<()> {
    let xml = exported_task_xml()?;
    if exported_policy_valid(&xml, ENTRYPOINT_PATH) && task_settings_enabled(&xml) {
        Ok(())
    } else {
        Err(HostControlError::AutostartPolicyInvalid)
    }
}

fn task_settings_enabled(xml: &str) -> bool {
    xml.split_once("<Settings>")
        .and_then(|(_, settings)| settings.split_once("</Settings>"))
        .is_some_and(|(settings, _)| {
            settings.contains("<Enabled>true</Enabled>")
                && !settings.contains("<Enabled>false</Enabled>")
        })
}

fn ensure_expected_policy(command: &str) -> HostControlResult<()> {
    let xml = exported_task_xml()?;
    if exported_policy_valid(&xml, command) {
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
        let output = hidden_command("schtasks.exe")
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
        Ok(json!({
            "installed": true,
            "policy_valid": exported_policy_valid(&xml, ENTRYPOINT_PATH),
            "task_name": TASK_NAME,
            "entrypoint_path": ENTRYPOINT_PATH
        }))
    }
}

fn exported_task_xml() -> HostControlResult<String> {
    #[cfg(not(windows))]
    {
        return Err(HostControlError::UnsupportedPlatform);
    }

    #[cfg(windows)]
    {
        let output = hidden_command("schtasks.exe")
            .args(["/Query", "/TN", TASK_NAME, "/XML"])
            .output()?;
        if !output.status.success() {
            return Err(HostControlError::AutostartPolicyInvalid);
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

fn exported_policy_valid(xml: &str, command: &str) -> bool {
    xml.contains(command)
        && xml.contains("<Arguments>run</Arguments>")
        && exported_policy_shape_valid(xml)
}

fn exported_policy_shape_valid(xml: &str) -> bool {
    xml.contains(&format!(
        "<WorkingDirectory>{}</WorkingDirectory>",
        okx_host_launcher::CONTROL_ROOT
    )) && xml.contains("<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>")
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
    let output = hidden_command("whoami.exe").output()?;
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

    const TEST_START: &str = "2026-09-29T20:00:00";

    #[test]
    fn canonical_task_keeps_one_fixed_entrypoint_forever() {
        let xml = task_xml(
            r"HOST\User",
            TEST_START,
            ENTRYPOINT_PATH,
            "run",
            okx_host_launcher::CONTROL_ROOT,
        );
        assert!(exported_policy_valid(&xml, ENTRYPOINT_PATH));
        assert_eq!(xml.matches("<TimeTrigger>").count(), 1);
        assert!(xml.contains("<Interval>PT1M</Interval>"));
        assert!(xml.contains("<LogonType>InteractiveToken</LogonType>"));
        assert!(!xml.contains("<RestartOnFailure>"));
    }

    #[test]
    fn disabled_task_cannot_be_explicitly_started() {
        let xml = task_xml(
            r"HOST\User",
            TEST_START,
            ENTRYPOINT_PATH,
            "run",
            okx_host_launcher::CONTROL_ROOT,
        );
        assert!(task_settings_enabled(&xml));
        let disabled = xml.replacen(
            "<Settings>\n    <MultipleInstancesPolicy>",
            "<Settings>\n    <MultipleInstancesPolicy>",
            1,
        ).replace(
            "    <Enabled>true</Enabled>\n    <Hidden>false</Hidden>",
            "    <Enabled>false</Enabled>\n    <Hidden>false</Hidden>",
        );
        assert!(exported_policy_valid(&disabled, ENTRYPOINT_PATH));
        assert!(!task_settings_enabled(&disabled));
    }

    #[test]
    fn scheduler_rejects_old_root_and_optional_workspace() {
        for root in [
            r"C:\okx-control",
            okx_host_launcher::WORKSPACE_ROOT,
            okx_host_launcher::RUNTIME_ROOT,
        ] {
            let xml = task_xml(r"HOST\User", TEST_START, ENTRYPOINT_PATH, "run", root);
            assert!(!exported_policy_valid(&xml, ENTRYPOINT_PATH));
        }
        let old_entrypoint = task_xml(
            r"HOST\User",
            TEST_START,
            r"C:\okx-control\okx-host-control.exe",
            "run",
            okx_host_launcher::CONTROL_ROOT,
        );
        assert!(!exported_policy_valid(&old_entrypoint, ENTRYPOINT_PATH));
    }

    #[test]
    fn scheduler_change_is_not_part_of_update_contract() {
        let source = include_str!("autostart.rs");
        let forbidden = [r#""/"#, r#"Change""#].concat();
        assert!(!source.contains(&forbidden));
    }
}
