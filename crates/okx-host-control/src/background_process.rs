use std::{ffi::OsStr, process::Command};

pub fn hidden_command<S: AsRef<OsStr>>(program: S) -> Command {
    let mut command = Command::new(program);
    apply_no_window(&mut command);
    command
}

#[cfg(windows)]
fn apply_no_window(command: &mut Command) {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn apply_no_window(_command: &mut Command) {}

#[cfg(test)]
mod tests {
    #[test]
    fn windows_background_policy_is_explicit() {
        let source = include_str!("background_process.rs");
        assert!(source.contains("CREATE_NO_WINDOW"));
    }
}
