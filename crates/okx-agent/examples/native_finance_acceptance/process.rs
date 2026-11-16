//! Bounded capture of one owned build/test child; wait includes process teardown.
use super::Result;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::Command,
    task::JoinHandle,
};

const CAP: usize = 8 * 1024 * 1024;
#[derive(Default)]
struct Capture {
    bytes: Vec<u8>,
    failure: Option<&'static str>,
}
pub(super) struct Output {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: Option<i32>,
    pub success: bool,
    pub failure: Option<&'static str>,
    pub failures: Vec<&'static str>,
}
async fn read(mut pipe: impl AsyncRead + Unpin) -> Capture {
    let mut capture = Capture::default();
    let mut chunk = [0_u8; 8192];
    loop {
        match pipe.read(&mut chunk).await {
            Ok(0) => return capture,
            Ok(count) => {
                let remaining = CAP - capture.bytes.len();
                capture
                    .bytes
                    .extend_from_slice(&chunk[..count.min(remaining)]);
                if count > remaining {
                    capture.failure = Some("CHILD_OUTPUT_LIMIT_EXCEEDED");
                    return capture;
                }
            }
            Err(_) => {
                capture.failure = Some("CHILD_OUTPUT_READ_FAILED");
                return capture;
            }
        }
    }
}
async fn finish(mut reader: JoinHandle<Capture>, deadline: tokio::time::Instant) -> Capture {
    match tokio::time::timeout_at(deadline, &mut reader).await {
        Ok(Ok(capture)) => capture,
        Ok(Err(_)) => Capture {
            failure: Some("CHILD_READER_JOIN_FAILED"),
            ..Default::default()
        },
        Err(_) => {
            reader.abort();
            let _ = reader.await;
            Capture {
                failure: Some("CHILD_READER_TEARDOWN_TIMEOUT"),
                ..Default::default()
            }
        }
    }
}
pub(super) async fn command(mut command: Command, guard: Duration) -> Result<Output> {
    let deadline = tokio::time::Instant::now() + guard;
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| "CHILD_SPAWN_FAILED")?;
    let stdout = child.stdout.take().ok_or("CHILD_STDOUT_MISSING")?;
    let stderr = child.stderr.take().ok_or("CHILD_STDERR_MISSING")?;
    let out = tokio::spawn(read(stdout));
    let err = tokio::spawn(read(stderr));
    let mut failures = Vec::new();
    let status = match tokio::time::timeout_at(deadline, child.wait()).await {
        Ok(Ok(status)) => Some(status),
        waited => {
            failures.push(if waited.is_err() {
                "CHILD_TIMEOUT_GUARD"
            } else {
                "CHILD_WAIT_FAILED"
            });
            if child.kill().await.is_err() {
                failures.push("CHILD_KILL_FAILED");
            }
            match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
                Ok(Ok(status)) => Some(status),
                other => {
                    failures.push(if other.is_err() {
                        "CHILD_TEARDOWN_TIMEOUT"
                    } else {
                        "CHILD_TEARDOWN_FAILED"
                    });
                    None
                }
            }
        }
    };
    // After timeout/exit, inherited pipes get a bounded cleanup window. Both
    // reader tasks are joined, including when abortion is necessary.
    let cleanup_deadline = if !failures.is_empty() {
        tokio::time::Instant::now() + Duration::from_secs(5)
    } else {
        deadline
    };
    let (out, err) = tokio::join!(finish(out, cleanup_deadline), finish(err, cleanup_deadline));
    failures.extend(out.failure);
    failures.extend(err.failure);
    let failure = failures.first().copied();
    Ok(Output {
        stdout: out.bytes,
        stderr: err.bytes,
        exit_code: status.and_then(|s| s.code()),
        success: status.is_some_and(|s| s.success()) && failure.is_none(),
        failure,
        failures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn captured_output_limit_is_finite_and_failure_is_explicit() {
        let bytes = vec![b'x'; CAP + 1];
        let capture = read(bytes.as_slice()).await;
        assert_eq!(capture.bytes.len(), CAP);
        assert_eq!(capture.failure, Some("CHILD_OUTPUT_LIMIT_EXCEEDED"));
    }
    #[tokio::test]
    async fn reader_teardown_is_joined_and_failure_is_explicit() {
        let pending = tokio::spawn(std::future::pending::<Capture>());
        let capture = finish(pending, tokio::time::Instant::now()).await;
        assert_eq!(capture.failure, Some("CHILD_READER_TEARDOWN_TIMEOUT"));
    }
}
