//! Research bearer syntax and the restricted child's disposable sandbox proof.
//! Research has no runtime Credential Manager store/read API: CONTROL owns
//! provisioning and only transmits a one-shot token after child attestation.

pub fn valid_token(token: &[u8]) -> bool {
    (32..=1024).contains(&token.len()) && token.iter().all(u8::is_ascii_graphic)
}

#[cfg(any(test, windows))]
fn valid_sandbox_canary(
    nonce: &str,
    credential_target: &str,
    finance_canary: &std::path::Path,
) -> bool {
    nonce.len() == 32
        && nonce
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && credential_target == format!("finance-sandbox-canary.{nonce}.iamaman11.okx-agent")
        && finance_canary.is_absolute()
        && finance_canary.file_name()
            == Some(std::ffi::OsStr::new(&format!(
                "finance-sandbox-canary.{nonce}"
            )))
}

#[cfg(any(test, windows))]
fn sandbox_ready_line(nonce: &str) -> Result<Vec<u8>, String> {
    let ready = serde_json::json!({
        "schema": "okx.research.sandbox-ready/v2",
        "nonce": nonce,
        "credman_denied": true,
        "finance_file_denied": true,
        "image_write_dac_denied": true,
        "image_write_owner_denied": true,
        "catalog_write_dac_denied": true,
        "catalog_write_owner_denied": true,
    })
    .to_string();
    if ready.len() + 1 > 384 {
        return Err("RESEARCH_SANDBOX_ATTESTATION_INVALID".into());
    }
    let mut ready = ready.into_bytes();
    ready.push(b'\n');
    Ok(ready)
}

/// A real Windows sandbox witness, performed IN the Research child before a
/// bearer is ever sent by CONTROL. CONTROL first proves it can read the
/// disposable synthetic credential and file, then resumes the restricted
/// child and requires this attestation before writing the startup secret.
/// No real financial credential or financial filesystem path is inspected.
#[cfg(windows)]
pub fn attest_startup_sandbox(catalog_root: &std::path::Path) -> Result<(), String> {
    use std::{ffi::OsStr, io::Write as _, os::windows::ffi::OsStrExt as _};
    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, ERROR_ACCESS_DENIED, ERROR_NO_SUCH_LOGON_SESSION, ERROR_NOT_FOUND,
            GENERIC_READ, GENERIC_WRITE, GetLastError, INVALID_HANDLE_VALUE,
        },
        Security::Credentials::{
            CRED_TYPE_GENERIC, CREDENTIALW, CredEnumerateW, CredFree, CredReadW,
        },
        Storage::FileSystem::{
            CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
        },
    };

    // Standard NT access-mask bits (not exported from Win32::Foundation by
    // the pinned windows-sys build). Keep these exact and independently
    // requested so a broad-mask failure cannot hide a granted right.
    const DELETE: u32 = 0x0001_0000;
    const WRITE_DAC: u32 = 0x0004_0000;
    const WRITE_OWNER: u32 = 0x0008_0000;

    fn denied_or_missing(error: u32) -> bool {
        matches!(
            error,
            ERROR_ACCESS_DENIED | ERROR_NOT_FOUND | ERROR_NO_SUCH_LOGON_SESSION
        )
    }
    fn to_wide(s: &OsStr) -> Vec<u16> {
        s.encode_wide().chain(std::iter::once(0)).collect()
    }
    fn require_access_denied(
        path: &std::path::Path,
        access: u32,
        flags: u32,
        exposed_code: &str,
        unexpected_code: &str,
    ) -> Result<(), String> {
        let wide = to_wide(path.as_os_str());
        // SAFETY: a NUL-terminated, owned path is kept live for CreateFileW;
        // a successful handle is closed before any failure is returned.
        let handle = unsafe {
            CreateFileW(
                wide.as_ptr(),
                access,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null(),
                OPEN_EXISTING,
                flags,
                std::ptr::null_mut(),
            )
        };
        if handle != INVALID_HANDLE_VALUE {
            unsafe { CloseHandle(handle) };
            return Err(exposed_code.into());
        }
        if unsafe { GetLastError() } != ERROR_ACCESS_DENIED {
            // Sharing violations and missing paths are not ACL evidence.
            return Err(unexpected_code.into());
        }
        Ok(())
    }

    let nonce = std::env::var("OKX_RESEARCH_SANDBOX_NONCE")
        .map_err(|_| "RESEARCH_SANDBOX_ATTESTATION_INVALID")?;
    let target = std::env::var("OKX_RESEARCH_CRED_CANARY_TARGET")
        .map_err(|_| "RESEARCH_SANDBOX_ATTESTATION_INVALID")?;
    let finance_canary = std::env::var_os("OKX_RESEARCH_FINANCE_CANARY_PATH")
        .ok_or("RESEARCH_SANDBOX_ATTESTATION_INVALID")?;
    let finance_canary = std::path::PathBuf::from(finance_canary);
    if !valid_sandbox_canary(&nonce, &target, &finance_canary) {
        return Err("RESEARCH_SANDBOX_ATTESTATION_INVALID".into());
    }

    if !catalog_root.is_absolute() || !catalog_root.is_dir() {
        return Err("RESEARCH_SANDBOX_ATTESTATION_INVALID".into());
    }
    let image = std::env::current_exe().map_err(|_| "RESEARCH_SANDBOX_ATTESTATION_INVALID")?;
    if !image.is_file() {
        return Err("RESEARCH_SANDBOX_ATTESTATION_INVALID".into());
    }

    let target_wide = to_wide(OsStr::new(&target));
    let mut credential: *mut CREDENTIALW = std::ptr::null_mut();
    // SAFETY: all pointers refer to owned, NUL-terminated UTF-16 buffers or
    // writable pointer outputs. CredFree handles any successful allocation.
    let credential_read =
        unsafe { CredReadW(target_wide.as_ptr(), CRED_TYPE_GENERIC, 0, &mut credential) };
    if credential_read != 0 {
        unsafe { CredFree(credential.cast()) };
        return Err("RESEARCH_SANDBOX_CREDMAN_EXPOSED".into());
    }
    let read_error = unsafe { GetLastError() };
    if !denied_or_missing(read_error) {
        return Err("RESEARCH_SANDBOX_CREDMAN_READ_UNEXPECTED".into());
    }

    let mut count = 0u32;
    let mut credentials: *mut *mut CREDENTIALW = std::ptr::null_mut();
    let enumerated =
        unsafe { CredEnumerateW(target_wide.as_ptr(), 0, &mut count, &mut credentials) };
    if enumerated != 0 {
        unsafe { CredFree(credentials.cast()) };
        return Err("RESEARCH_SANDBOX_CREDMAN_EXPOSED".into());
    }
    let enumerate_error = unsafe { GetLastError() };
    if !denied_or_missing(enumerate_error) {
        return Err("RESEARCH_SANDBOX_CREDMAN_ENUM_UNEXPECTED".into());
    }

    // Research has no write/delete Credential Manager APIs. The parent owns
    // the synthetic credential and verifies its contents both before and
    // after this child's read/enumerate denial checks. Write/delete denial
    // remains NOT_PROVEN until measured without weakening that ownership gate.

    for access in [GENERIC_READ, GENERIC_WRITE, DELETE, WRITE_DAC, WRITE_OWNER] {
        require_access_denied(
            &finance_canary,
            access,
            FILE_ATTRIBUTE_NORMAL,
            "RESEARCH_SANDBOX_FINANCE_FILE_EXPOSED",
            "RESEARCH_SANDBOX_FINANCE_PROBE_UNEXPECTED",
        )?;
    }
    for access in [WRITE_DAC, WRITE_OWNER] {
        require_access_denied(
            &image,
            access,
            FILE_ATTRIBUTE_NORMAL,
            "RESEARCH_SANDBOX_IMAGE_ACL_EXPOSED",
            "RESEARCH_SANDBOX_IMAGE_PROBE_UNEXPECTED",
        )?;
        require_access_denied(
            catalog_root,
            access,
            FILE_FLAG_BACKUP_SEMANTICS,
            "RESEARCH_SANDBOX_CATALOG_ACL_EXPOSED",
            "RESEARCH_SANDBOX_CATALOG_PROBE_UNEXPECTED",
        )?;
    }

    let ready = sandbox_ready_line(&nonce)?;
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(&ready)
        .and_then(|_| stdout.flush())
        .map_err(|_| "RESEARCH_SANDBOX_ATTESTATION_INVALID".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn research_token_validation_is_bounded_and_ascii_only() {
        assert!(valid_token(&[b'A'; 32]));
        assert!(valid_token(&[b'F'; 1024]));
        let mut newline = vec![b'x'; 32];
        newline.push(b'\n');
        let mut zero = vec![b'x'; 32];
        zero.push(0);
        let mut invalid_utf8 = vec![b'x'; 32];
        invalid_utf8.push(0xc0);
        for invalid in [
            vec![],
            vec![b'x'; 31],
            vec![b'x'; 1025],
            newline,
            zero,
            invalid_utf8,
        ] {
            assert!(!valid_token(&invalid));
        }
    }

    #[test]
    fn sandbox_ready_attests_only_disposable_canaries_without_secret_material() {
        let nonce = "a".repeat(32);
        let target = format!("finance-sandbox-canary.{nonce}.iamaman11.okx-agent");
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join(format!("finance-sandbox-canary.{nonce}"));
        assert!(valid_sandbox_canary(&nonce, &target, &path));
        assert!(!valid_sandbox_canary(&"A".repeat(32), &target, &path));
        assert!(!valid_sandbox_canary(
            &nonce,
            "runtime-token.iamaman11.okx-agent",
            &path
        ));
        assert!(!valid_sandbox_canary(
            &nonce,
            &target,
            &root.path().join("finance.sqlite")
        ));
        let ready = sandbox_ready_line(&nonce).unwrap();
        assert!(ready.len() <= 384);
        assert_eq!(ready.last(), Some(&b'\n'));
        assert_eq!(ready.iter().filter(|&&byte| byte == b'\n').count(), 1);
        let value: serde_json::Value = serde_json::from_slice(&ready).unwrap();
        assert_eq!(value["schema"], "okx.research.sandbox-ready/v2");
        assert_eq!(value["nonce"], nonce);
        assert_eq!(value["credman_denied"], true);
        assert_eq!(value["finance_file_denied"], true);
        for field in [
            "image_write_dac_denied",
            "image_write_owner_denied",
            "catalog_write_dac_denied",
            "catalog_write_owner_denied",
        ] {
            assert_eq!(value[field], true);
        }
        assert!(value.get("bearer").is_none());
        assert!(value.get("canary").is_none());
        assert!(value.get("path").is_none());
    }
}
