//! Process-owned standard-stream capture. No launcher process must stay alive.
#[cfg(windows)]
mod platform {
    use std::{
        fs::File,
        io::{self, Write},
        os::windows::io::{AsRawHandle, FromRawHandle},
        path::Path,
        thread::JoinHandle,
        time::{Duration, Instant},
    };
    use windows_sys::Win32::{
        Foundation::HANDLE,
        System::{
            Console::{GetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE, SetStdHandle},
            Pipes::CreatePipe,
        },
    };
    pub struct ProcessLogCapture {
        original: [usize; 2],
        writers: [Option<File>; 2],
        readers: Vec<JoinHandle<()>>,
        installed: [bool; 2],
    }
    fn pipe() -> io::Result<(File, File)> {
        let mut read = std::ptr::null_mut();
        let mut write = std::ptr::null_mut();
        if unsafe { CreatePipe(&mut read, &mut write, std::ptr::null(), 0) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { (File::from_raw_handle(read), File::from_raw_handle(write)) })
    }
    impl ProcessLogCapture {
        pub fn install(root: impl AsRef<Path>) -> io::Result<Self> {
            let out =
                crate::logging::RotatingLog::open(root.as_ref().join("controller.stdout.log"))?;
            let err =
                crate::logging::RotatingLog::open(root.as_ref().join("controller.stderr.log"))?;
            let (out_read, out_write) = pipe()?;
            let (err_read, err_write) = pipe()?;
            let mut capture = Self {
                original: [
                    unsafe { GetStdHandle(STD_OUTPUT_HANDLE) } as usize,
                    unsafe { GetStdHandle(STD_ERROR_HANDLE) } as usize,
                ],
                writers: [Some(out_write), Some(err_write)],
                readers: Vec::with_capacity(2),
                installed: [false; 2],
            };
            capture.readers.push(
                std::thread::Builder::new()
                    .name("controller-stdout-log".into())
                    .spawn(move || {
                        crate::logging::drain(
                            out_read,
                            std::sync::Arc::new(std::sync::Mutex::new(out)),
                        )
                    })?,
            );
            capture.readers.push(
                std::thread::Builder::new()
                    .name("controller-stderr-log".into())
                    .spawn(move || {
                        crate::logging::drain(
                            err_read,
                            std::sync::Arc::new(std::sync::Mutex::new(err)),
                        )
                    })?,
            );
            for (index, kind) in [STD_OUTPUT_HANDLE, STD_ERROR_HANDLE]
                .into_iter()
                .enumerate()
            {
                let handle = capture.writers[index]
                    .as_ref()
                    .ok_or_else(|| io::Error::other("capture writer missing"))?
                    .as_raw_handle();
                if unsafe { SetStdHandle(kind, handle) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                capture.installed[index] = true;
            }
            Ok(capture)
        }
    }
    impl Drop for ProcessLogCapture {
        fn drop(&mut self) {
            let _ = std::io::stdout().flush();
            let _ = std::io::stderr().flush();
            for (index, kind) in [STD_OUTPUT_HANDLE, STD_ERROR_HANDLE]
                .into_iter()
                .enumerate()
            {
                if self.installed[index] {
                    unsafe { SetStdHandle(kind, self.original[index] as HANDLE) };
                }
            }
            for writer in &mut self.writers {
                writer.take();
            }
            // No pipe handles are inheritable. A bounded wait also covers a sink stuck
            // in filesystem I/O; shutdown must not wait indefinitely on diagnostics.
            let until = Instant::now() + Duration::from_secs(2);
            while self.readers.iter().any(|reader| !reader.is_finished()) && Instant::now() < until
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            for reader in self.readers.drain(..) {
                if reader.is_finished() {
                    let _ = reader.join();
                }
            }
        }
    }
}
#[cfg(windows)]
pub use platform::ProcessLogCapture;
#[cfg(not(windows))]
pub struct ProcessLogCapture;
#[cfg(not(windows))]
impl ProcessLogCapture {
    pub fn install(_root: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        Ok(Self)
    }
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use std::{
        fs,
        io::Write,
        path::PathBuf,
        process::{Command, Stdio},
        time::{Duration, Instant},
    };
    const ROLE: &str = "OKX_LOG_CAPTURE_TEST_ROLE";
    const ROOT: &str = "OKX_LOG_CAPTURE_TEST_ROOT";
    fn child(role: &str, root: &std::path::Path) -> Command {
        use std::os::windows::process::CommandExt;
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "--exact",
            "stdio_capture::tests::controller_logs_survive_launcher_parent_exit",
            "--nocapture",
        ])
        .env(ROLE, role)
        .env(ROOT, root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(0x08000000);
        cmd
    }
    #[test]
    fn controller_logs_survive_launcher_parent_exit() {
        match std::env::var(ROLE).ok().as_deref() {
            Some("parent") => {
                let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
                // Windows fixture deliberately exits this parent first; the
                // outer test waits for the child's durable completion marker.
                #[allow(clippy::zombie_processes)]
                let _child = child("child", &root).spawn().unwrap();
            }
            Some("child") => {
                use windows_sys::Win32::System::Console::{
                    GetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE,
                };
                let originals = unsafe {
                    [
                        GetStdHandle(STD_OUTPUT_HANDLE),
                        GetStdHandle(STD_ERROR_HANDLE),
                    ]
                };
                let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
                let invalid = root.join("not-a-directory");
                fs::write(&invalid, b"fixture").unwrap();
                assert!(ProcessLogCapture::install(&invalid).is_err());
                assert_eq!(
                    unsafe {
                        [
                            GetStdHandle(STD_OUTPUT_HANDLE),
                            GetStdHandle(STD_ERROR_HANDLE),
                        ]
                    },
                    originals
                );
                {
                    let _capture = ProcessLogCapture::install(&root).unwrap();
                    let until = Instant::now() + Duration::from_secs(30);
                    while !root.join("parent.exited").exists() {
                        assert!(Instant::now() < until, "fixture parent did not exit");
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    let mut line = [b'x'; 4096];
                    line[4095] = b'\n';
                    for _ in 0..3000 {
                        std::io::stdout().write_all(&line).unwrap();
                    }
                    std::io::stdout()
                        .write_all(b"child wrote after parent exit\n")
                        .unwrap();
                    std::io::stderr()
                        .write_all(b"apiKey=NEVER_PERSIST_CAPTURE\nchild stderr alive\n")
                        .unwrap();
                }
                assert_eq!(
                    unsafe {
                        [
                            GetStdHandle(STD_OUTPUT_HANDLE),
                            GetStdHandle(STD_ERROR_HANDLE),
                        ]
                    },
                    originals
                );
                fs::write(root.join("child.finished"), b"complete").unwrap();
            }
            _ => {
                let root = std::env::temp_dir().join(format!(
                    "okx-process-capture-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                ));
                fs::create_dir_all(&root).unwrap();
                assert!(child("parent", &root).status().unwrap().success());
                fs::write(root.join("parent.exited"), b"exited").unwrap();
                let until = Instant::now() + Duration::from_secs(45);
                while !root.join("child.finished").exists() {
                    assert!(
                        Instant::now() < until,
                        "logging child failed after launcher-equivalent parent exited"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
                let stdout = fs::read_to_string(root.join("controller.stdout.log")).unwrap();
                assert!(stdout.contains("child wrote after parent exit"));
                assert!(root.join("controller.stdout.log.1").exists());
                for entry in fs::read_dir(&root).unwrap() {
                    let entry = entry.unwrap();
                    if entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with("controller.")
                    {
                        assert!(entry.metadata().unwrap().len() <= crate::logging::LOG_MAX_BYTES);
                    }
                }
                let stderr = fs::read_to_string(root.join("controller.stderr.log")).unwrap();
                assert!(stderr.contains("child stderr alive"));
                assert!(!stderr.contains("NEVER_PERSIST_CAPTURE"));
            }
        }
    }
}
