//! Bounded process-pipe logging. Log failures never stop draining the child.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
pub const LOG_MAX_BYTES: u64 = 10 * 1024 * 1024;
pub const LOG_FILES: usize = 10;
const LINE_BYTES: usize = 8192;
#[derive(Clone, Debug)]
pub struct LogHealth(Arc<AtomicBool>);
impl Default for LogHealth {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }
}
impl LogHealth {
    pub fn complete(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
    fn partial(&self) {
        self.0.store(false, Ordering::Release);
    }
}
pub struct RotatingLog {
    path: PathBuf,
    file: Option<File>,
    size: u64,
    limit: u64,
    pending: [u8; LINE_BYTES],
    used: usize,
    overflow: bool,
    health: LogHealth,
}
impl RotatingLog {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::with_limit(path.as_ref(), LOG_MAX_BYTES)
    }
    fn with_limit(path: &Path, limit: u64) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        let mut log = Self {
            path: path.into(),
            file: Some(file),
            size,
            limit,
            pending: [0; LINE_BYTES],
            used: 0,
            overflow: false,
            health: LogHealth::default(),
        };
        if size >= limit {
            if size > limit {
                OpenOptions::new().write(true).open(path)?.set_len(limit)?;
                log.size = limit;
            }
            log.rotate()?;
        }
        Ok(log)
    }
    pub fn offset(&self) -> u64 {
        self.size
    }
    pub fn health(&self) -> LogHealth {
        self.health.clone()
    }
    fn archive(&self, n: usize) -> PathBuf {
        let mut p = self.path.as_os_str().to_os_string();
        p.push(format!(".{n}"));
        p.into()
    }
    fn rotate(&mut self) -> io::Result<()> {
        self.health.partial();
        self.file.take();
        for n in (1..LOG_FILES).rev() {
            let destination = self.archive(n);
            if destination.exists() {
                fs::remove_file(&destination)?;
            }
            let source = if n == 1 {
                self.path.clone()
            } else {
                self.archive(n - 1)
            };
            if source.exists() {
                fs::rename(source, destination)?;
            }
        }
        self.file = Some(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
        );
        self.size = 0;
        Ok(())
    }
    fn record(&mut self) -> io::Result<()> {
        let content = if self.overflow {
            self.health.partial();
            b"[oversize log line omitted]".as_slice()
        } else {
            let value = &self.pending[..self.used];
            if [
                "credential",
                "secret",
                "password",
                "passphrase",
                "api_key",
                "api-key",
                "apikey",
                "authorization",
                "bearer",
                "token",
                "private_key",
                "ok-access-",
            ]
            .iter()
            .any(|label| {
                value
                    .windows(label.len())
                    .any(|window| window.eq_ignore_ascii_case(label.as_bytes()))
            }) {
                self.health.partial();
                b"[credential-labelled log line redacted]".as_slice()
            } else {
                value
            }
        };
        let mut record = [0u8; LINE_BYTES + 1];
        let count = content.len();
        record[..count].copy_from_slice(content);
        record[count] = b'\n';
        self.used = 0;
        self.overflow = false;
        if self.size + (count as u64) + 1 > self.limit {
            self.rotate()?;
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("log unavailable"))?;
        file.write_all(&record[..count + 1])?;
        self.size += count as u64 + 1;
        Ok(())
    }
    pub fn append(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if *byte == b'\n' {
                if self.record().is_err() {
                    self.health.partial();
                    self.used = 0;
                    self.overflow = false;
                }
            } else if self.used < LINE_BYTES {
                self.pending[self.used] = *byte;
                self.used += 1;
            } else {
                self.overflow = true;
                self.health.partial();
            }
        }
    }
    pub fn finish(&mut self) {
        if (self.used > 0 || self.overflow) && self.record().is_err() {
            self.health.partial();
        }
        if let Some(file) = &mut self.file
            && file.flush().is_err()
        {
            self.health.partial();
        }
    }
}
pub fn drain<R: Read>(mut reader: R, log: Arc<Mutex<RotatingLog>>) {
    let mut block = [0u8; 4096];
    loop {
        match reader.read(&mut block) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if let Ok(mut writer) = log.lock() {
                    writer.append(&block[..n]);
                }
            }
        }
    }
    if let Ok(mut writer) = log.lock() {
        writer.finish();
    }
}
pub fn spawn_drain<R: Read + Send + 'static>(
    reader: R,
    log: RotatingLog,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || drain(reader, Arc::new(Mutex::new(log))))
}
#[cfg(test)]
mod tests {
    use super::*;
    fn path(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "okx-logs-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&p).unwrap();
        p.join("process.log")
    }
    #[test]
    fn rotating_files_are_bounded_and_retain_only_ten() {
        let path = path("rotation");
        let mut log = RotatingLog::with_limit(&path, 128).unwrap();
        for i in 0..300 {
            log.append(format!("line-{i:04} payload data\n").as_bytes());
        }
        log.finish();
        let files = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(files.len(), 10);
        for f in files {
            assert!(f.metadata().unwrap().len() <= 128);
        }
        assert!(fs::read_to_string(&path).unwrap().contains("line-0299"));
        assert!(!log.health().complete());
    }
    #[test]
    fn credential_lines_and_unbounded_lines_never_leak() {
        let path = path("redaction");
        let mut log = RotatingLog::open(&path).unwrap();
        log.append(b"healthy event\nAPI_");
        log.append(b"KEY=do-not-persist\ncredential_ref: sensitive-value\n");
        for _ in 0..3000 {
            log.append(b"long-payload");
        }
        log.append(b"password=oversized-secret\nnext healthy event\n");
        log.finish();
        let text = fs::read_to_string(path).unwrap();
        assert!(text.contains("healthy event"));
        assert!(text.contains("redacted"));
        assert!(text.contains("oversize"));
        assert!(!text.contains("do-not-persist"));
        assert!(!text.contains("sensitive-value"));
        assert!(!text.contains("oversized-secret"));
        assert!(text.len() < 256);
        assert!(!log.health().complete());
    }
    #[test]
    fn log_failure_does_not_stop_pipe_drain() {
        use std::sync::atomic::AtomicUsize;
        struct Input {
            left: usize,
            read: Arc<AtomicUsize>,
        }
        impl Read for Input {
            fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
                let n = self.left.min(b.len());
                if n == 0 {
                    return Ok(0);
                }
                b[..n].fill(b'x');
                b[n - 1] = b'\n';
                self.left -= n;
                self.read.fetch_add(n, Ordering::Relaxed);
                Ok(n)
            }
        }
        let path = path("failed-sink");
        let mut writer = RotatingLog::open(path).unwrap();
        writer.file.take();
        let health = writer.health();
        let read = Arc::new(AtomicUsize::new(0));
        drain(
            Input {
                left: 1024 * 1024,
                read: read.clone(),
            },
            Arc::new(Mutex::new(writer)),
        );
        assert_eq!(read.load(Ordering::Relaxed), 1024 * 1024);
        assert!(!health.complete());
    }
    #[test]
    fn venue_headers_and_bearer_queries_are_redacted() {
        let path = path("headers");
        let mut log = RotatingLog::open(&path).unwrap();
        for label in [
            "api_key",
            "api-key",
            "apiKey",
            "secret_key",
            "secretKey",
            "passphrase",
            "Authorization",
            "Bearer",
            "OK-ACCESS-KEY",
            "OK-ACCESS-SIGN",
            "OK-ACCESS-PASSPHRASE",
            "https://host/path?token=",
        ] {
            log.append(format!("{label}: NEVER_PERSIST_THIS\n").as_bytes());
        }
        log.finish();
        let text = fs::read_to_string(path).unwrap();
        assert!(!text.contains("NEVER_PERSIST_THIS"));
        assert_eq!(text.lines().count(), 12);
    }
    #[test]
    fn default_cap_bounds_existing_oversized_log_before_archiving() {
        let path = path("default-cap");
        File::create(&path)
            .unwrap()
            .set_len(LOG_MAX_BYTES + 1024)
            .unwrap();
        let mut log = RotatingLog::open(&path).unwrap();
        log.append(b"fresh process event\n");
        log.finish();
        assert_eq!(LOG_MAX_BYTES, 10 * 1024 * 1024);
        assert_eq!(LOG_FILES, 10);
        assert_eq!(fs::metadata(log.archive(1)).unwrap().len(), LOG_MAX_BYTES);
        assert!(fs::metadata(path).unwrap().len() <= LOG_MAX_BYTES);
    }
}
