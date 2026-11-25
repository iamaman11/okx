//! Fail-closed Windows AppContainer Research launch. The caller owns the exact
//! verified Research binary and a fresh kill-on-close Job before calling here.
//! No Engine process, finance database, or installed ACL is modified.

#[cfg(any(windows, test))]
use serde::{Deserialize, Serialize};
#[cfg(any(windows, test))]
use zeroize::Zeroizing;

#[cfg(any(windows, test))]
const READY_SCHEMA: &str = "okx.research.sandbox-ready/v1";
#[cfg(any(windows, test))]
const STARTUP_SCHEMA: &str = "okx.research.startup/v1";
#[cfg(any(windows, test))]
const MAX_READY_BYTES: usize = 256;
#[cfg(any(windows, test))]
const MAX_STARTUP_BYTES: usize = 1536;

#[cfg(any(windows, test))]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxReady {
    schema: String,
    nonce: String,
    credman_denied: bool,
    finance_file_denied: bool,
}

/// A forged, missing, repeated or oversized attestation must never unlock the
/// bearer. This is an additional *child-executed* check, not a substitute for
/// checking the child's token, DACLs and inherited handles in the parent.
#[cfg(any(windows, test))]
fn verify_ready(bytes: &[u8], nonce: &str) -> bool {
    if bytes.len() > MAX_READY_BYTES
        || !bytes.ends_with(b"\n")
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return false;
    }
    let Ok(ready) = serde_json::from_slice::<SandboxReady>(&bytes[..bytes.len() - 1]) else {
        return false;
    };
    ready.schema == READY_SCHEMA
        && ready.nonce == nonce
        && ready.credman_denied
        && ready.finance_file_denied
}

#[cfg(any(windows, test))]
fn startup_frame(bearer: &str) -> Option<Zeroizing<Vec<u8>>> {
    if !(32..=1024).contains(&bearer.len()) || !bearer.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
        return None;
    }
    #[derive(Serialize)]
    struct StartupFrame<'a> {
        schema: &'static str,
        job_assigned: bool,
        bearer: &'a str,
    }
    let bytes = Zeroizing::new(
        serde_json::to_vec(&StartupFrame {
            schema: STARTUP_SCHEMA,
            job_assigned: true,
            bearer,
        })
        .ok()?,
    );
    (bytes.len() <= MAX_STARTUP_BYTES && !bytes.contains(&b'\n')).then_some(bytes)
}

#[cfg(windows)]
mod native {
    use std::{
        ffi::{OsStr, c_void},
        fs::{self, File},
        io::Write,
        mem::{size_of, zeroed},
        os::windows::{
            ffi::OsStrExt,
            io::{AsRawHandle, FromRawHandle, OwnedHandle},
        },
        path::{Path, PathBuf},
        ptr::{null, null_mut},
        time::{Duration, Instant},
    };

    use keyring::api::CredentialStoreApi;
    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, ERROR_ALREADY_EXISTS, ERROR_NOT_FOUND, GetLastError, HANDLE,
            HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, LocalFree, SetHandleInformation,
            WAIT_OBJECT_0, WAIT_TIMEOUT,
        },
        Security::{
            ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, AccessCheck,
            Authorization::{ConvertStringSidToSidW, GetNamedSecurityInfoW, SE_FILE_OBJECT},
            Credentials::{
                CRED_PERSIST_SESSION, CRED_TYPE_GENERIC, CREDENTIALW, CredDeleteW, CredFree,
                CredReadW, CredWriteW,
            },
            DACL_SECURITY_INFORMATION, DeriveCapabilitySidsFromName, DuplicateTokenEx, EqualSid,
            FreeSid, GENERIC_MAPPING, GROUP_SECURITY_INFORMATION, GetAce,
            GetSecurityDescriptorControl, GetSidSubAuthority, GetSidSubAuthorityCount,
            GetTokenInformation,
            Isolation::{CreateAppContainerProfile, DeriveAppContainerSidFromAppContainerName},
            MapGenericMask, OWNER_SECURITY_INFORMATION, PRIVILEGE_SET, PSID, SE_DACL_PROTECTED,
            SECURITY_ATTRIBUTES, SECURITY_CAPABILITIES, SID_AND_ATTRIBUTES, SecurityImpersonation,
            TOKEN_APPCONTAINER_INFORMATION, TOKEN_DUPLICATE, TOKEN_GROUPS, TOKEN_MANDATORY_LABEL,
            TOKEN_QUERY, TokenAppContainerSid, TokenCapabilities, TokenImpersonation,
            TokenIntegrityLevel, TokenIsAppContainer,
        },
        Storage::FileSystem::{
            CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_EXECUTE, FILE_GENERIC_READ,
            FILE_GENERIC_WRITE, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
        },
        System::{
            Pipes::{CreatePipe, PeekNamedPipe},
            Threading::{
                CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
                DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess,
                InitializeProcThreadAttributeList, OpenProcessToken,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES,
                PROCESS_INFORMATION, ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW,
                TerminateProcess, UpdateProcThreadAttribute, WaitForSingleObject,
            },
            WindowsProgramming::PROCESS_CREATION_ALL_APPLICATION_PACKAGES_OPT_OUT,
        },
    };
    use zeroize::{Zeroize, Zeroizing};

    use super::{MAX_READY_BYTES, startup_frame, verify_ready};
    use crate::{HostControlError, HostControlResult, job::AgentJob};

    const PROFILE_NAME: &str = "iamaman11.okx.research.appcontainer.v1";
    const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

    fn denied(stage: &'static str) -> HostControlError {
        HostControlError::ResearchSandboxDenied(stage)
    }

    fn os_error() -> HostControlError {
        std::io::Error::last_os_error().into()
    }

    fn wide(value: &OsStr) -> Vec<u16> {
        value.encode_wide().chain([0]).collect()
    }

    fn handle(value: HANDLE) -> HostControlResult<OwnedHandle> {
        if value.is_null() || value == INVALID_HANDLE_VALUE {
            return Err(os_error());
        }
        // The Win32 API returned exclusive ownership of this handle.
        Ok(unsafe { OwnedHandle::from_raw_handle(value.cast()) })
    }

    fn raw(value: &OwnedHandle) -> HANDLE {
        value.as_raw_handle().cast()
    }

    pub struct ResearchChild {
        process: OwnedHandle,
        pid: u32,
        #[cfg(test)]
        test_child: Option<std::process::Child>,
    }

    impl ResearchChild {
        pub fn id(&self) -> u32 {
            self.pid
        }

        pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            #[cfg(test)]
            if let Some(child) = &mut self.test_child {
                return child.try_wait();
            }
            match unsafe { WaitForSingleObject(raw(&self.process), 0) } {
                WAIT_TIMEOUT => Ok(None),
                WAIT_OBJECT_0 => self.exit_status().map(Some),
                _ => Err(std::io::Error::last_os_error()),
            }
        }

        fn exit_status(&self) -> std::io::Result<std::process::ExitStatus> {
            use std::os::windows::process::ExitStatusExt;
            let mut code = 0;
            if unsafe { GetExitCodeProcess(raw(&self.process), &mut code) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(std::process::ExitStatus::from_raw(code))
        }

        pub fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
            #[cfg(test)]
            if let Some(child) = &mut self.test_child {
                return child.wait();
            }
            if unsafe { WaitForSingleObject(raw(&self.process), u32::MAX) } != WAIT_OBJECT_0 {
                return Err(std::io::Error::last_os_error());
            }
            self.exit_status()
        }

        pub fn kill(&mut self) -> std::io::Result<()> {
            #[cfg(test)]
            if let Some(child) = &mut self.test_child {
                return child.kill();
            }
            if unsafe { TerminateProcess(raw(&self.process), 1) } == 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        }

        #[cfg(test)]
        pub fn from_test_child(child: std::process::Child) -> Self {
            use std::os::windows::io::AsHandle;
            let borrowed = child.as_handle();
            // Test-only: the native handle is borrowed from the real Child.
            // Keep a distinct duplicated handle to make both owners valid.
            let process = borrowed
                .try_clone_to_owned()
                .expect("duplicate test handle");
            Self {
                pid: child.id(),
                process,
                test_child: Some(child),
            }
        }
    }

    struct ProfileSid(PSID);
    impl Drop for ProfileSid {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    FreeSid(self.0);
                }
            }
        }
    }
    struct LocalSid(PSID);
    impl Drop for LocalSid {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0);
                }
            }
        }
    }

    fn profile_sid() -> HostControlResult<ProfileSid> {
        let name = wide(OsStr::new(PROFILE_NAME));
        let display = wide(OsStr::new("OKX isolated Research worker"));
        let mut sid = null_mut();
        let result = unsafe {
            CreateAppContainerProfile(
                name.as_ptr(),
                display.as_ptr(),
                display.as_ptr(),
                null(),
                0,
                &mut sid,
            )
        };
        if result == 0 && !sid.is_null() {
            return Ok(ProfileSid(sid));
        }
        // 0x800700B7 is HRESULT_FROM_WIN32(ERROR_ALREADY_EXISTS).
        if result as u32 != (0x8007_0000u32 | ERROR_ALREADY_EXISTS) {
            return Err(denied("appcontainer_profile"));
        }
        sid = null_mut();
        if unsafe { DeriveAppContainerSidFromAppContainerName(name.as_ptr(), &mut sid) } != 0
            || sid.is_null()
        {
            return Err(denied("appcontainer_sid"));
        }
        Ok(ProfileSid(sid))
    }

    fn capability_sid() -> HostControlResult<LocalSid> {
        let name = wide(OsStr::new("internetClient"));
        let (mut groups, mut group_count, mut caps, mut cap_count) =
            (null_mut(), 0u32, null_mut(), 0u32);
        if unsafe {
            DeriveCapabilitySidsFromName(
                name.as_ptr(),
                &mut groups,
                &mut group_count,
                &mut caps,
                &mut cap_count,
            )
        } == 0
            || cap_count != 1
            || caps.is_null()
        {
            return Err(denied("capability_sid"));
        }
        let sid = unsafe { *caps };
        // Returned SID memory has LocalAlloc ownership; group/cap arrays too.
        if !groups.is_null() {
            for i in 0..group_count {
                unsafe {
                    LocalFree(*groups.add(i as usize));
                }
            }
            unsafe {
                LocalFree(groups.cast());
            }
        }
        unsafe {
            LocalFree(caps.cast());
        }
        if sid.is_null() {
            return Err(denied("capability_sid"));
        }
        Ok(LocalSid(sid))
    }

    fn token_info(token: HANDLE, class: i32) -> HostControlResult<Vec<usize>> {
        let mut bytes = 0u32;
        unsafe {
            GetTokenInformation(token, class, null_mut(), 0, &mut bytes);
        }
        if bytes == 0 || bytes > 64 * 1024 {
            return Err(denied("token_info_size"));
        }
        let mut data = vec![0usize; (bytes as usize).div_ceil(size_of::<usize>())];
        if unsafe { GetTokenInformation(token, class, data.as_mut_ptr().cast(), bytes, &mut bytes) }
            == 0
        {
            return Err(denied("token_info"));
        }
        Ok(data)
    }

    fn verify_token(
        process: HANDLE,
        package_sid: PSID,
        internet_sid: PSID,
    ) -> HostControlResult<OwnedHandle> {
        let mut token = null_mut();
        if unsafe { OpenProcessToken(process, TOKEN_QUERY | TOKEN_DUPLICATE, &mut token) } == 0 {
            return Err(denied("open_child_token"));
        }
        let token = handle(token)?;
        let is_ac = token_info(raw(&token), TokenIsAppContainer)?;
        if is_ac[0] as u32 != 1 {
            return Err(denied("token_not_appcontainer"));
        }
        let package = token_info(raw(&token), TokenAppContainerSid)?;
        let actual = unsafe {
            (*(package.as_ptr().cast::<TOKEN_APPCONTAINER_INFORMATION>())).TokenAppContainer
        };
        if actual.is_null() || unsafe { EqualSid(actual, package_sid) } == 0 {
            return Err(denied("token_package_sid"));
        }
        let integrity = token_info(raw(&token), TokenIntegrityLevel)?;
        let label = unsafe { &*(integrity.as_ptr().cast::<TOKEN_MANDATORY_LABEL>()) };
        let count = unsafe { GetSidSubAuthorityCount(label.Label.Sid) };
        if count.is_null() || unsafe { *count } == 0 {
            return Err(denied("token_integrity"));
        }
        let sub = unsafe { GetSidSubAuthority(label.Label.Sid, (*count - 1) as u32) };
        if sub.is_null() || unsafe { *sub } > 0x1000 {
            return Err(denied("token_integrity"));
        }
        let capabilities = token_info(raw(&token), TokenCapabilities)?;
        let groups = unsafe { &*(capabilities.as_ptr().cast::<TOKEN_GROUPS>()) };
        let records = groups.GroupCount as usize;
        if records == 0
            || records > 32
            || size_of::<TOKEN_GROUPS>() + (records - 1) * size_of::<SID_AND_ATTRIBUTES>()
                > capabilities.len() * size_of::<usize>()
        {
            return Err(denied("token_capabilities"));
        }
        if records != 1
            || !unsafe { std::slice::from_raw_parts(groups.Groups.as_ptr(), records) }
                .iter()
                .all(|cap| !cap.Sid.is_null() && unsafe { EqualSid(cap.Sid, internet_sid) } != 0)
        {
            return Err(denied("token_internet_capability"));
        }
        Ok(token)
    }

    fn can_access(token: HANDLE, path: &Path, mask: u32) -> HostControlResult<bool> {
        let path = wide(path.as_os_str());
        let mut descriptor = null_mut();
        let result = unsafe {
            GetNamedSecurityInfoW(
                path.as_ptr(),
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                null_mut(),
                null_mut(),
                &mut descriptor,
            )
        };
        if result != 0 || descriptor.is_null() {
            return Err(denied("file_security_descriptor"));
        }
        struct Descriptor(*mut c_void);
        impl Drop for Descriptor {
            fn drop(&mut self) {
                unsafe {
                    LocalFree(self.0);
                }
            }
        }
        let _descriptor = Descriptor(descriptor);

        // AccessCheck accepts an impersonation token, never a child primary
        // token; API success and its separate AccessStatus must both be read.
        let mut impersonation = null_mut();
        if unsafe {
            DuplicateTokenEx(
                token,
                TOKEN_QUERY,
                null(),
                SecurityImpersonation,
                TokenImpersonation,
                &mut impersonation,
            )
        } == 0
        {
            return Err(denied("duplicate_impersonation_token"));
        }
        let impersonation = handle(impersonation)?;
        let mapping = GENERIC_MAPPING {
            GenericRead: FILE_GENERIC_READ,
            GenericWrite: FILE_GENERIC_WRITE,
            GenericExecute: FILE_GENERIC_EXECUTE,
            GenericAll: 0x001f_01ff,
        };
        let mut desired = mask;
        unsafe {
            MapGenericMask(&mut desired, &mapping);
        }
        let mut privileges = vec![0usize; 1024];
        let mut privilege_bytes = (privileges.len() * size_of::<usize>()) as u32;
        let (mut granted, mut permitted) = (0u32, 0i32);
        if unsafe {
            AccessCheck(
                descriptor,
                raw(&impersonation),
                desired,
                &mapping,
                privileges.as_mut_ptr().cast::<PRIVILEGE_SET>(),
                &mut privilege_bytes,
                &mut granted,
                &mut permitted,
            )
        } == 0
        {
            return Err(denied("effective_access_check"));
        }
        Ok(permitted != 0 && granted & desired == desired)
    }

    fn sid_from_string(value: &str) -> HostControlResult<LocalSid> {
        let name = wide(OsStr::new(value));
        let mut sid = null_mut();
        if unsafe { ConvertStringSidToSidW(name.as_ptr(), &mut sid) } == 0 || sid.is_null() {
            return Err(denied("acl_sid_parse"));
        }
        Ok(LocalSid(sid))
    }

    /// Refuse broad AppContainer groups even where the current token's
    /// AccessCheck happens to deny. Research assets additionally require an
    /// explicit, protected DACL grant for *this* AppContainer package SID.
    fn check_specific_dacl(path: &Path, package_sid: Option<PSID>) -> HostControlResult<()> {
        let name = wide(path.as_os_str());
        let (mut descriptor, mut dacl) = (null_mut(), null_mut());
        if unsafe {
            GetNamedSecurityInfoW(
                name.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                null_mut(),
                &mut descriptor,
            )
        } != 0
            || descriptor.is_null()
            || dacl.is_null()
        {
            return Err(denied("acl_descriptor_missing"));
        }
        struct Descriptor(*mut c_void);
        impl Drop for Descriptor {
            fn drop(&mut self) {
                unsafe {
                    LocalFree(self.0);
                }
            }
        }
        let _descriptor = Descriptor(descriptor);
        if package_sid.is_some() {
            let (mut control, mut revision) = (0u16, 0u32);
            if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
                || control & SE_DACL_PROTECTED == 0
            {
                return Err(denied("research_dacl_not_protected"));
            }
        }
        let broad_aap = sid_from_string("S-1-15-2-1")?;
        let broad_restricted = sid_from_string("S-1-15-2-2")?;
        let mut exact_package_granted = false;
        let count = unsafe { (*dacl.cast::<ACL>()).AceCount } as usize;
        if count > 1024 {
            return Err(denied("acl_ace_budget"));
        }
        for index in 0..count {
            let mut opaque = null_mut();
            if unsafe { GetAce(dacl, index as u32, &mut opaque) } == 0 || opaque.is_null() {
                return Err(denied("acl_ace_malformed"));
            }
            let header = unsafe { &*(opaque.cast::<ACE_HEADER>()) };
            // Deny and mandatory ACEs cannot grant access. Reject any
            // unfamiliar ALLOW variant rather than guessing its SID offset.
            if header.AceType == 0 {
                if (header.AceSize as usize) < size_of::<ACCESS_ALLOWED_ACE>() {
                    return Err(denied("acl_ace_size"));
                }
                let allow = unsafe { &*(opaque.cast::<ACCESS_ALLOWED_ACE>()) };
                let sid = (&allow.SidStart as *const u32).cast_mut().cast();
                if unsafe { EqualSid(sid, broad_aap.0) } != 0
                    || unsafe { EqualSid(sid, broad_restricted.0) } != 0
                {
                    return Err(denied("acl_all_application_packages"));
                }
                if let Some(expected) = package_sid
                    && unsafe { EqualSid(sid, expected) } != 0
                    && header.AceFlags & 0x10 == 0
                    && allow.Mask != 0
                {
                    exact_package_granted = true;
                }
            } else if matches!(
                header.AceType,
                1 | 2 | 3 | 6 | 7 | 8 | 10 | 12 | 13 | 14 | 15 | 16 | 17
            ) {
                // Known deny/audit/mandatory-label ACEs do not grant file
                // read/write access. Unknown types (including compound and
                // future ALLOW ACEs) are rejected rather than interpreted.
            } else {
                return Err(denied("acl_unknown_or_complex_allow_ace"));
            }
        }
        if package_sid.is_some() && !exact_package_granted {
            return Err(denied("research_package_ace_missing"));
        }
        Ok(())
    }

    fn checked_regular_path(path: &Path) -> HostControlResult<()> {
        use std::os::windows::fs::MetadataExt;
        for component in path.ancestors() {
            let info = fs::symlink_metadata(component)
                .map_err(|_| denied("path_ancestor_missing_or_inaccessible"))?;
            if info.file_attributes() & 0x400 != 0 || info.file_type().is_symlink() {
                return Err(denied("reparse_point_ancestor"));
            }
        }
        Ok(())
    }

    fn check_finance_metadata(token: HANDLE, runtime_root: &Path) -> HostControlResult<()> {
        // These are ONLY metadata/ACL queries. Never open SQLite contents or
        // rewrite a live finance path. The runtime root must not permit the
        // AppContainer to create/traverse new finance objects either.
        checked_regular_path(runtime_root)?;
        check_specific_dacl(runtime_root, None)?;
        for right in [
            0x1u32, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x10000, 0x20000, 0x40000, 0x80000,
            0x100000,
        ] {
            if can_access(token, runtime_root, right)? {
                return Err(denied("runtime_root_finance_access"));
            }
        }
        for directory in ["finance", "accounts", "risk"] {
            let path = runtime_root.join(directory);
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    checked_regular_path(&path)?;
                    check_specific_dacl(&path, None)?;
                    for right in [
                        0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x10000, 0x20000, 0x40000,
                        0x80000, 0x100000,
                    ] {
                        if can_access(token, &path, right)? {
                            return Err(denied("finance_directory_access"));
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(denied("finance_path_metadata")),
            }
        }
        for name in [
            "finance.sqlite",
            "finance.sqlite-wal",
            "finance.sqlite-shm",
            "finance.sqlite.writer.lock",
            "authority-mode.lock",
        ] {
            let path = runtime_root.join("finance").join(name);
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    checked_regular_path(&path)?;
                    check_specific_dacl(&path, None)?;
                    for right in [
                        0x1, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x10000, 0x20000, 0x40000,
                        0x80000, 0x100000,
                    ] {
                        if can_access(token, &path, right)? {
                            return Err(denied("finance_sidecar_access"));
                        }
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(denied("finance_sidecar_metadata")),
            }
        }
        // Finance installations may contain per-account and risk-book SQLite
        // files at dynamic names. Enumerate *filenames and descriptors only*,
        // never database bytes. Any unreadable, reparse, over-budget or
        // excessive-depth subtree is NOT_PROVEN and vetoes Research startup.
        let mut examined = 0usize;
        for name in ["finance", "accounts", "risk"] {
            let mut pending = vec![(runtime_root.join(name), 0usize)];
            while let Some((path, depth)) = pending.pop() {
                match fs::symlink_metadata(&path) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound && depth == 0 => {
                        continue;
                    }
                    Err(_) => return Err(denied("finance_tree_metadata")),
                    Ok(metadata) => {
                        examined += 1;
                        if examined > 4096 || depth > 8 {
                            return Err(denied("finance_tree_budget"));
                        }
                        checked_regular_path(&path)?;
                        check_specific_dacl(&path, None)?;
                        for right in [
                            0x1u32, 0x2, 0x4, 0x8, 0x10, 0x20, 0x40, 0x80, 0x10000, 0x20000,
                            0x40000, 0x80000, 0x100000,
                        ] {
                            if can_access(token, &path, right)? {
                                return Err(denied("finance_tree_effective_access"));
                            }
                        }
                        if metadata.is_dir() {
                            for entry in
                                fs::read_dir(&path).map_err(|_| denied("finance_tree_listing"))?
                            {
                                pending.push((
                                    entry.map_err(|_| denied("finance_tree_entry"))?.path(),
                                    depth + 1,
                                ));
                            }
                        } else if !metadata.is_file() {
                            return Err(denied("finance_tree_special_file"));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn check_research_files(
        token: HANDLE,
        package_sid: PSID,
        executable: &Path,
        catalog_root: &Path,
    ) -> HostControlResult<()> {
        checked_regular_path(executable)?;
        checked_regular_path(executable.parent().ok_or_else(|| denied("image_parent"))?)?;
        checked_regular_path(catalog_root)?;
        check_specific_dacl(executable, Some(package_sid))?;
        check_specific_dacl(
            executable.parent().ok_or_else(|| denied("image_parent"))?,
            Some(package_sid),
        )?;
        check_specific_dacl(catalog_root, Some(package_sid))?;
        if !can_access(token, executable, 0x1)? || !can_access(token, executable, 0x20)? {
            return Err(denied("research_executable_acl"));
        }
        // Research may execute its accepted SHA, but must never be able to
        // replace it or change ownership/ACL of the image or image directory.
        for path in [
            executable,
            executable.parent().ok_or_else(|| denied("image_parent"))?,
        ] {
            for right in [0x2, 0x4, 0x8, 0x10, 0x40, 0x10000, 0x40000, 0x80000] {
                if can_access(token, path, right)? {
                    return Err(denied("research_installed_image_mutable"));
                }
            }
        }
        if !can_access(token, catalog_root, 0x1)? || !can_access(token, catalog_root, 0x2)? {
            return Err(denied("research_catalog_acl"));
        }
        Ok(())
    }

    fn fresh_nonce() -> HostControlResult<String> {
        let mut value = [0u8; 16];
        getrandom::fill(&mut value).map_err(|_| denied("random_nonce"))?;
        Ok(value.iter().map(|b| format!("{b:02x}")).collect())
    }

    struct FinanceCanary {
        path: PathBuf,
        directory: PathBuf,
    }

    impl FinanceCanary {
        fn create(nonce: &str) -> HostControlResult<Self> {
            let directory = std::env::temp_dir().join(format!("okx-research-sandbox-{nonce}"));
            fs::create_dir(&directory).map_err(|_| denied("finance_canary_directory"))?;
            let path = directory.join(format!("finance-sandbox-canary.{nonce}"));
            let proof = (|| -> HostControlResult<()> {
                let mut bytes = Zeroizing::new([0u8; 32]);
                getrandom::fill(bytes.as_mut()).map_err(|_| denied("finance_canary_random"))?;
                let mut file =
                    File::create_new(&path).map_err(|_| denied("finance_canary_create"))?;
                file.write_all(bytes.as_ref())
                    .map_err(|_| denied("finance_canary_write"))?;
                file.flush().map_err(|_| denied("finance_canary_flush"))?;
                drop(file);
                let readback = Zeroizing::new(
                    fs::read(&path).map_err(|_| denied("finance_canary_parent_read"))?,
                );
                if readback.as_slice() != bytes.as_slice() {
                    return Err(denied("finance_canary_parent_mismatch"));
                }
                Ok(())
            })();
            if let Err(error) = proof {
                let _ = fs::remove_file(&path);
                let _ = fs::remove_dir(&directory);
                return Err(error);
            }
            Ok(Self { path, directory })
        }

        fn delete(&self) -> HostControlResult<()> {
            fs::remove_file(&self.path).map_err(|_| denied("finance_canary_cleanup"))?;
            fs::remove_dir(&self.directory).map_err(|_| denied("finance_canary_cleanup"))?;
            Ok(())
        }
    }

    impl Drop for FinanceCanary {
        fn drop(&mut self) {
            if self.path.exists() {
                let _ = fs::remove_file(&self.path);
            }
            if self.directory.exists() {
                let _ = fs::remove_dir(&self.directory);
            }
        }
    }

    struct CredCanary {
        target: String,
        secret: Zeroizing<[u8; 32]>,
        deleted: bool,
    }

    impl CredCanary {
        fn create(nonce: &str) -> HostControlResult<Self> {
            let target = format!("finance-sandbox-canary.{nonce}.iamaman11.okx-agent");
            let mut name = wide(OsStr::new(&target));
            let mut previous = null_mut();
            if unsafe { CredReadW(name.as_ptr(), CRED_TYPE_GENERIC, 0, &mut previous) } != 0 {
                if !previous.is_null() {
                    unsafe {
                        CredFree(previous.cast());
                    }
                }
                return Err(denied("credential_canary_collision"));
            }
            if unsafe { GetLastError() } != ERROR_NOT_FOUND {
                return Err(denied("credential_canary_preflight"));
            }
            let mut bytes = Zeroizing::new([0u8; 32]);
            getrandom::fill(bytes.as_mut()).map_err(|_| denied("credential_canary_random"))?;
            let mut user = wide(OsStr::new("sandbox-negative-control"));
            let credential = CREDENTIALW {
                Type: CRED_TYPE_GENERIC,
                TargetName: name.as_mut_ptr(),
                CredentialBlob: bytes.as_mut_ptr(),
                CredentialBlobSize: 32,
                Persist: CRED_PERSIST_SESSION,
                UserName: user.as_mut_ptr(),
                ..Default::default()
            };
            if unsafe { CredWriteW(&credential, 0) } == 0 {
                return Err(denied("credential_canary_write"));
            }
            let guard = Self {
                target,
                secret: bytes,
                deleted: false,
            };
            guard.verify_present()?;
            Ok(guard)
        }

        fn verify_present(&self) -> HostControlResult<()> {
            let name = wide(OsStr::new(&self.target));
            let mut readback = null_mut();
            if unsafe { CredReadW(name.as_ptr(), CRED_TYPE_GENERIC, 0, &mut readback) } == 0
                || readback.is_null()
            {
                return Err(denied("credential_canary_parent_read"));
            }
            let matching = unsafe {
                (*readback).CredentialBlobSize == 32
                    && !(*readback).CredentialBlob.is_null()
                    && std::slice::from_raw_parts((*readback).CredentialBlob, 32)
                        == self.secret.as_slice()
            };
            unsafe {
                CredFree(readback.cast());
            }
            if !matching {
                return Err(denied("credential_canary_parent_mismatch"));
            }
            Ok(())
        }

        fn delete(&mut self) -> HostControlResult<()> {
            if !self.deleted {
                let name = wide(OsStr::new(&self.target));
                if unsafe { CredDeleteW(name.as_ptr(), CRED_TYPE_GENERIC, 0) } == 0 {
                    return Err(denied("credential_canary_cleanup"));
                }
                self.deleted = true;
            }
            Ok(())
        }
    }

    impl Drop for CredCanary {
        fn drop(&mut self) {
            let _ = self.delete();
        }
    }

    fn load_research_bearer() -> HostControlResult<Zeroizing<String>> {
        let store = windows_native_keyring_store::Store::new()
            .map_err(|_| denied("research_credential_store"))?;
        let entry = store
            .build(
                "iamaman11.okx-research-service.cloudflare",
                "runtime-token",
                None,
            )
            .map_err(|_| denied("research_credential_entry"))?;
        let secret = Zeroizing::new(
            entry
                .get_secret()
                .map_err(|_| denied("research_credential_read"))?,
        );
        let text =
            std::str::from_utf8(&secret).map_err(|_| denied("research_credential_format"))?;
        if startup_frame(text).is_none() {
            return Err(denied("research_credential_format"));
        }
        Ok(Zeroizing::new(text.to_owned()))
    }

    struct Attributes {
        storage: Vec<usize>,
        list: windows_sys::Win32::System::Threading::LPPROC_THREAD_ATTRIBUTE_LIST,
    }

    impl Attributes {
        fn new() -> HostControlResult<Self> {
            let mut bytes = 0usize;
            unsafe {
                InitializeProcThreadAttributeList(null_mut(), 3, 0, &mut bytes);
            }
            if bytes == 0 || bytes > 64 * 1024 {
                return Err(denied("process_attributes_size"));
            }
            let mut storage = vec![0usize; bytes.div_ceil(size_of::<usize>())];
            let list = storage.as_mut_ptr().cast();
            if unsafe { InitializeProcThreadAttributeList(list, 3, 0, &mut bytes) } == 0 {
                return Err(denied("process_attributes_initialize"));
            }
            Ok(Self { storage, list })
        }

        fn set<T>(&mut self, attribute: u32, value: &T) -> HostControlResult<()> {
            if unsafe {
                UpdateProcThreadAttribute(
                    self.list,
                    0,
                    attribute as usize,
                    (value as *const T).cast(),
                    size_of::<T>(),
                    null_mut(),
                    null(),
                )
            } == 0
            {
                return Err(denied("process_attributes_update"));
            }
            Ok(())
        }
    }

    impl Drop for Attributes {
        fn drop(&mut self) {
            unsafe {
                DeleteProcThreadAttributeList(self.list);
            }
            let _ = self.storage.len();
        }
    }

    fn pipe() -> HostControlResult<(OwnedHandle, OwnedHandle)> {
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        let (mut read, mut write) = (null_mut(), null_mut());
        if unsafe { CreatePipe(&mut read, &mut write, &attributes, 0) } == 0 {
            return Err(denied("startup_pipe"));
        }
        let read = handle(read)?;
        let write = handle(write)?;
        Ok((read, write))
    }

    fn nul_stderr() -> HostControlResult<OwnedHandle> {
        let attributes = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: null_mut(),
            bInheritHandle: 1,
        };
        let nul = wide(OsStr::new("NUL"));
        let nul_handle = unsafe {
            CreateFileW(
                nul.as_ptr(),
                0x4000_0000,
                FILE_SHARE_READ | FILE_SHARE_WRITE,
                &attributes,
                OPEN_EXISTING,
                FILE_ATTRIBUTE_NORMAL,
                null_mut(),
            )
        };
        handle(nul_handle)
    }

    fn environment_block(entries: &[(&str, String)]) -> HostControlResult<Vec<u16>> {
        let mut variables = entries.to_vec();
        variables.sort_by(|a, b| a.0.cmp(b.0));
        let mut result = Vec::new();
        for (name, value) in variables {
            if name.is_empty() || name.contains('=') || name.contains('\0') || value.contains('\0')
            {
                return Err(denied("startup_environment"));
            }
            result.extend(name.encode_utf16());
            result.push(b'=' as u16);
            result.extend(value.encode_utf16());
            result.push(0);
        }
        result.push(0);
        Ok(result)
    }

    fn quoted_path(path: &Path) -> HostControlResult<String> {
        let text = path
            .to_str()
            .ok_or_else(|| denied("research_path_unicode"))?;
        if text.contains(['\0', '"', '\r', '\n']) {
            return Err(denied("research_path_characters"));
        }
        Ok(format!("\"{text}\""))
    }

    fn receive_ready(pipe: &OwnedHandle, child: HANDLE, nonce: &str) -> HostControlResult<()> {
        let started = Instant::now();
        let mut data = Vec::new();
        while started.elapsed() < PROBE_TIMEOUT {
            if unsafe { WaitForSingleObject(child, 0) } != WAIT_TIMEOUT {
                return Err(denied("worker_terminated_before_ready"));
            }
            let mut available = 0u32;
            if unsafe {
                PeekNamedPipe(
                    raw(pipe),
                    null_mut(),
                    0,
                    null_mut(),
                    &mut available,
                    null_mut(),
                )
            } == 0
            {
                return Err(denied("worker_ready_pipe"));
            }
            if available as usize > MAX_READY_BYTES.saturating_sub(data.len()) {
                return Err(denied("worker_ready_oversize"));
            }
            if available == 0 {
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            let mut bytes = vec![0u8; available as usize];
            let mut count = 0u32;
            if unsafe {
                windows_sys::Win32::Storage::FileSystem::ReadFile(
                    raw(pipe),
                    bytes.as_mut_ptr(),
                    available,
                    &mut count,
                    null_mut(),
                )
            } == 0
                || count == 0
            {
                return Err(denied("worker_ready_read"));
            }
            data.extend_from_slice(&bytes[..count as usize]);
            if data.contains(&b'\n') {
                if !verify_ready(&data, nonce) {
                    return Err(denied("worker_ready_invalid"));
                }
                // Reject any currently buffered extra output. The child must
                // emit EXACTLY one bounded proof and then block on stdin.
                let mut extra = 0u32;
                if unsafe {
                    PeekNamedPipe(raw(pipe), null_mut(), 0, null_mut(), &mut extra, null_mut())
                } == 0
                    || extra != 0
                {
                    return Err(denied("worker_ready_extra"));
                }
                return Ok(());
            }
        }
        Err(denied("worker_ready_timeout"))
    }

    struct KillOnFailure(HANDLE);
    impl Drop for KillOnFailure {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    TerminateProcess(self.0, 1);
                    WaitForSingleObject(self.0, 5000);
                }
            }
        }
    }

    /// The only production Research spawn path. CreateProcessW is suspended,
    /// the child token/DACL/canaries are checked, and assignment to CONTROL's
    /// kill-on-close Job must succeed before ResumeThread. No `Command::spawn`
    /// fallback exists, including on API failure.
    #[allow(clippy::too_many_arguments)]
    pub fn launch_research(
        job: &AgentJob,
        executable: &Path,
        catalog_root: &Path,
        runtime_root: &Path,
        scope: &str,
        host_scope: &str,
        incarnation: &str,
        edge_wss: &str,
        existing_catalog: bool,
    ) -> HostControlResult<ResearchChild> {
        // The bearer must never enter argv, environment or child handles.
        let bearer = load_research_bearer()?;
        let frame = startup_frame(&bearer).ok_or_else(|| denied("bearer_frame"))?;
        let nonce = fresh_nonce()?;
        let finance = FinanceCanary::create(&nonce)?;
        let mut credential = CredCanary::create(&nonce)?;
        let package = profile_sid()?;
        let internet = capability_sid()?;
        let internet_capability = SID_AND_ATTRIBUTES {
            Sid: internet.0,
            Attributes: 0x0000_0004,
        };
        let capabilities = SECURITY_CAPABILITIES {
            AppContainerSid: package.0,
            Capabilities: &internet_capability as *const _ as *mut _,
            CapabilityCount: 1,
            Reserved: 0,
        };

        // The only inheritable child handles are the gate stdin reader,
        // READY stdout writer and an explicit NUL stderr. Parent private
        // handles (even marked inheritable) are absent from HANDLE_LIST.
        let (stdin_read, stdin_write) = pipe()?;
        let (stdout_read, stdout_write) = pipe()?;
        let stderr_nul = nul_stderr()?;
        for parent_end in [&stdin_write, &stdout_read] {
            if unsafe { SetHandleInformation(raw(parent_end), HANDLE_FLAG_INHERIT, 0) } == 0 {
                return Err(denied("parent_pipe_inheritance"));
            }
        }
        let child_handles = [raw(&stdin_read), raw(&stdout_write), raw(&stderr_nul)];
        let policy = PROCESS_CREATION_ALL_APPLICATION_PACKAGES_OPT_OUT;
        let mut attributes = Attributes::new()?;
        attributes.set(PROC_THREAD_ATTRIBUTE_SECURITY_CAPABILITIES, &capabilities)?;
        attributes.set(PROC_THREAD_ATTRIBUTE_HANDLE_LIST, &child_handles)?;
        attributes.set(windows_sys::Win32::System::Threading::PROC_THREAD_ATTRIBUTE_ALL_APPLICATION_PACKAGES_POLICY, &policy)?;

        let cwd = executable
            .parent()
            .ok_or_else(|| denied("research_executable_parent"))?;
        let mut arguments = format!(
            "{} --edge-runtime --root {} --scope {scope}",
            quoted_path(executable)?,
            quoted_path(catalog_root)?
        );
        if existing_catalog {
            arguments.push_str(" --existing-catalog");
        }
        if !scope.bytes().all(|b| b.is_ascii_hexdigit())
            || scope.len() != 64
            || !host_scope.bytes().all(|b| b.is_ascii_hexdigit())
            || host_scope.len() != 64
        {
            return Err(denied("research_scope"));
        }
        let mut commandline = wide(OsStr::new(&arguments));
        let application = wide(executable.as_os_str());
        let cwd_wide = wide(cwd.as_os_str());
        let mut environment = environment_block(&[
            (
                "SystemRoot",
                std::env::var("SystemRoot").map_err(|_| denied("system_root"))?,
            ),
            ("OKX_RESEARCH_EDGE_WSS_URL", edge_wss.to_owned()),
            ("OKX_RESEARCH_RUNTIME_ID", "windows-primary".to_owned()),
            ("OKX_RESEARCH_HOST_SCOPE", host_scope.to_owned()),
            ("OKX_RESEARCH_INCAR", incarnation.to_owned()),
            ("OKX_RESEARCH_CRED_CANARY_TARGET", credential.target.clone()),
            (
                "OKX_RESEARCH_FINANCE_CANARY_PATH",
                finance
                    .path
                    .to_str()
                    .ok_or_else(|| denied("finance_canary_unicode"))?
                    .to_owned(),
            ),
            ("OKX_RESEARCH_SANDBOX_NONCE", nonce.clone()),
        ])?;
        let mut startup: STARTUPINFOEXW = unsafe { zeroed() };
        startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
        startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
        startup.StartupInfo.hStdInput = raw(&stdin_read);
        startup.StartupInfo.hStdOutput = raw(&stdout_write);
        startup.StartupInfo.hStdError = raw(&stderr_nul);
        startup.lpAttributeList = attributes.list;
        let mut information: PROCESS_INFORMATION = unsafe { zeroed() };
        let created = unsafe {
            CreateProcessW(
                application.as_ptr(),
                commandline.as_mut_ptr(),
                null(),
                null(),
                1,
                CREATE_SUSPENDED
                    | EXTENDED_STARTUPINFO_PRESENT
                    | CREATE_UNICODE_ENVIRONMENT
                    | CREATE_NO_WINDOW,
                environment.as_mut_ptr().cast(),
                cwd_wide.as_ptr(),
                &startup.StartupInfo,
                &mut information,
            )
        };
        // No other copy of this non-secret environment is retained.
        environment.zeroize();
        commandline.zeroize();
        if created == 0 {
            return Err(denied("create_suspended_appcontainer"));
        }
        if information.hProcess.is_null()
            || information.hThread.is_null()
            || information.hProcess == INVALID_HANDLE_VALUE
            || information.hThread == INVALID_HANDLE_VALUE
        {
            unsafe {
                if !information.hProcess.is_null() && information.hProcess != INVALID_HANDLE_VALUE {
                    TerminateProcess(information.hProcess, 1);
                    CloseHandle(information.hProcess);
                }
                if !information.hThread.is_null() && information.hThread != INVALID_HANDLE_VALUE {
                    CloseHandle(information.hThread);
                }
            }
            return Err(denied("suspended_process_handle_invalid"));
        }
        let process = handle(information.hProcess)?;
        let thread = handle(information.hThread)?;
        let mut kill = KillOnFailure(raw(&process));
        drop(stdin_read);
        drop(stdout_write);

        let token = verify_token(raw(&process), package.0, internet.0)?;
        check_finance_metadata(raw(&token), runtime_root)?;
        check_research_files(raw(&token), package.0, executable, catalog_root)?;
        if can_access(raw(&token), &finance.path, 0x1)? {
            return Err(denied("synthetic_finance_canary_readable"));
        }
        // The RAII OwnedHandle remains valid and KillOnFailure owns the
        // suspended process until the Job assignment succeeds.
        unsafe {
            job.assign_raw_process(raw(&process))?;
        }
        if unsafe { ResumeThread(raw(&thread)) } == u32::MAX {
            return Err(denied("resume_research_thread"));
        }
        drop(thread);
        receive_ready(&stdout_read, raw(&process), &nonce)?;
        // Recheck the actual canary after the child's negative probe: absence
        // or replacement must never masquerade as a CredMan isolation proof.
        credential.verify_present()?;
        // The worker has already demonstrated BOTH negative probes. Only now
        // does the authenticated runtime bearer cross the inherited pipe.
        {
            let mut stdin = unsafe { File::from_raw_handle(stdin_write.as_raw_handle()) };
            // Retain a single handle owner by suppressing `OwnedHandle` drop.
            std::mem::forget(stdin_write);
            stdin
                .write_all(&frame)
                .map_err(|_| denied("startup_frame_write"))?;
            stdin.flush().map_err(|_| denied("startup_frame_flush"))?;
        }
        credential.delete()?;
        finance.delete()?;
        kill.0 = null_mut();
        Ok(ResearchChild {
            process,
            pid: information.dwProcessId,
            #[cfg(test)]
            test_child: None,
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn explicit_environment_never_inherits_finance_or_parent_secrets() {
            // Construct the exact allowlist, with an intentionally unrelated
            // parent secret. It must not appear in the child's environment.
            let environment = environment_block(&[
                ("SystemRoot", r"C:\Windows".into()),
                ("OKX_RESEARCH_SANDBOX_NONCE", "a".repeat(32)),
            ])
            .unwrap();
            let visible = String::from_utf16_lossy(&environment);
            assert!(!visible.contains("FINANCE"));
            assert!(!visible.contains("GITHUB_TOKEN"));
            assert!(!visible.contains("CREDENTIAL"));
            assert!(visible.ends_with("\0\0"));
            assert!(environment_block(&[("TEST", "value\0secret".into())]).is_err());
        }

        #[test]
        fn reject_untrusted_commandline_quote_or_newline() {
            assert!(quoted_path(Path::new(r"C:\isolated\worker.exe")).is_ok());
            assert!(quoted_path(Path::new("bad\"quote.exe")).is_err());
            assert!(quoted_path(Path::new("bad\nline.exe")).is_err());
        }

        #[test]
        #[ignore = "runs only on an explicitly disposable Windows CI identity"]
        fn synthetic_finance_credential_and_file_canaries_have_parent_positive_controls() {
            assert_eq!(
                std::env::var("OKX_DISPOSABLE_WINDOWS_RUNNER").as_deref(),
                Ok("1")
            );
            let nonce = fresh_nonce().unwrap();
            let finance = FinanceCanary::create(&nonce).unwrap();
            let mut credential = CredCanary::create(&nonce).unwrap();
            assert!(finance.path.is_file());
            credential.delete().unwrap();
            finance.delete().unwrap();
        }
    }
}

#[cfg(windows)]
pub use native::{ResearchChild, launch_research};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attestation_rejects_false_missing_duplicate_oversized_and_wrong_nonce() {
        let nonce = "a".repeat(32);
        let proof = format!(
            "{{\"schema\":\"{READY_SCHEMA}\",\"nonce\":\"{nonce}\",\"credman_denied\":true,\"finance_file_denied\":true}}\n"
        );
        assert!(verify_ready(proof.as_bytes(), &nonce));
        assert!(!verify_ready(proof.as_bytes(), &"b".repeat(32)));
        assert!(!verify_ready(proof.trim_end().as_bytes(), &nonce));
        assert!(!verify_ready(
            [proof.as_bytes(), proof.as_bytes()].concat().as_slice(),
            &nonce
        ));
        assert!(!verify_ready(&vec![b'x'; MAX_READY_BYTES + 1], &nonce));
        assert!(!verify_ready(
            proof.replace("true", "false").as_bytes(),
            &nonce
        ));
        assert!(!verify_ready(
            proof
                .replace("\"nonce\":", "\"extra\":true,\"nonce\":")
                .as_bytes(),
            &nonce,
        ));
        assert!(!verify_ready(
            proof
                .replace("\"nonce\":", "\"nonce\":\"bad\",\"nonce\":")
                .as_bytes(),
            &nonce,
        ));
    }

    #[test]
    fn bearer_gate_accepts_only_bounded_ascii_one_shot_json() {
        let frame = startup_frame(&"a".repeat(1024)).unwrap();
        assert!(frame.len() <= MAX_STARTUP_BYTES);
        assert!(!frame.contains(&b'\n'));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&frame).unwrap()["schema"],
            STARTUP_SCHEMA
        );
        assert!(startup_frame(&"a".repeat(31)).is_none());
        assert!(startup_frame(&"a".repeat(1025)).is_none());
        assert!(startup_frame(&format!("{}\n", "a".repeat(31))).is_none());
        assert!(startup_frame(&format!("{}é", "a".repeat(31))).is_none());
    }
}
