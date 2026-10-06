use std::{ffi::OsStr, process::Command};

pub fn hidden_command<S: AsRef<OsStr>>(program: S) -> Command {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        let mut command = Command::new(program);
        command.creation_flags(CREATE_NO_WINDOW);
        command
    }
    #[cfg(not(windows))]
    {
        Command::new(program)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn windows_background_policy_is_explicit() {
        let source = include_str!("background_process.rs");
        assert!(source.contains("CREATE_NO_WINDOW"));
    }
}
