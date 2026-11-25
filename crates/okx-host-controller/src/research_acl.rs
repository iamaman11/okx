//! Narrow Windows ACL preparation for the independent Research image/catalog.
//!
//! This code never changes the runtime root, Finance, Engine, or the legacy
//! analytics catalog. Every mutation is confined to a verified Research image
//! or the independent Research catalog, and an unproved ACL denies startup.

#[cfg(windows)]
mod native {
    use std::{
        ffi::{OsStr, c_void},
        fs,
        mem::{size_of, zeroed},
        os::windows::{ffi::OsStrExt, io::FromRawHandle},
        path::Path,
        ptr::{null, null_mut},
    };

    use sha2::{Digest, Sha256};
    use windows_sys::Win32::{
        Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, HANDLE, INVALID_HANDLE_VALUE, LocalFree},
        Security::{
            ACCESS_ALLOWED_ACE, ACE_HEADER, ACL,
            Authorization::{
                ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
                GetSecurityInfo, SE_FILE_OBJECT, SetSecurityInfo,
            },
            DACL_SECURITY_INFORMATION, EqualSid, FreeSid, GetAce, GetSecurityDescriptorControl,
            GetSecurityDescriptorDacl, GetSecurityDescriptorOwner, GetSecurityDescriptorSacl,
            GetTokenInformation,
            Isolation::DeriveAppContainerSidFromAppContainerName,
            LABEL_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION,
            PROTECTED_DACL_SECURITY_INFORMATION, PSID, SE_DACL_PROTECTED, SECURITY_ATTRIBUTES,
            SYSTEM_MANDATORY_LABEL_ACE, TOKEN_QUERY, TOKEN_USER, TokenUser,
        },
        Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateDirectoryW, CreateFileW,
            FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, GetFileInformationByHandle,
            OPEN_EXISTING,
        },
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };

    use crate::{HostControlError, HostControlResult};

    const IMAGE_PACKAGE_RX: u32 = 0x0012_00a9;
    // Read/write/execute/traverse/delete within the Research-only catalog,
    // excluding WRITE_DAC and WRITE_OWNER.
    const CATALOG_PACKAGE_MODIFY: u32 = 0x0013_01ff;
    const CONTROL_FULL: u32 = 0x001f_01ff;
    const READ_CONTROL: u32 = 0x0002_0000;
    const WRITE_DAC: u32 = 0x0004_0000;
    const WRITE_OWNER: u32 = 0x0008_0000;
    const FILE_READ_DATA: u32 = 0x0000_0001;
    const FILE_WRITE_DATA: u32 = 0x0000_0002;
    const INHERITED_ACE: u8 = 0x10;
    const OBJECT_INHERIT_ACE: u8 = 0x01;
    const CONTAINER_INHERIT_ACE: u8 = 0x02;
    const SYSTEM_MANDATORY_LABEL_NO_WRITE_UP: u32 = 0x1;
    const CATALOG_ENTRY_BUDGET: usize = 1024;
    const CATALOG_DEPTH_BUDGET: usize = 8;

    fn denied(stage: &'static str) -> HostControlError {
        HostControlError::ResearchSandboxDenied(stage)
    }

    fn wide(value: &OsStr) -> Vec<u16> {
        value.encode_wide().chain(std::iter::once(0)).collect()
    }

    // Parent directory handles are held without FILE_SHARE_DELETE until the
    // leaf handle closes. A path component cannot be swapped to a junction
    // between our reparse check and the handle-based ACL mutation.
    struct Handle {
        raw: HANDLE,
        _parents: Vec<Handle>,
    }
    impl Drop for Handle {
        fn drop(&mut self) {
            if !self.raw.is_null() && self.raw != INVALID_HANDLE_VALUE {
                unsafe { CloseHandle(self.raw) };
            }
        }
    }

    struct LocalDescriptor(*mut c_void);
    impl Drop for LocalDescriptor {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { LocalFree(self.0) };
            }
        }
    }

    struct PackageSid(PSID);
    impl Drop for PackageSid {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe { FreeSid(self.0) };
            }
        }
    }

    struct Identity {
        user: String,
        package: String,
    }

    fn sid_string(sid: PSID) -> HostControlResult<String> {
        let mut text = null_mut();
        if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 || text.is_null() {
            return Err(denied("research_acl_sid_string"));
        }
        let local = LocalDescriptor(text.cast());
        let len = unsafe { (0..256).find(|&i| *text.add(i) == 0) }
            .ok_or_else(|| denied("research_acl_sid_length"))?;
        let value = String::from_utf16(unsafe { std::slice::from_raw_parts(text, len) })
            .map_err(|_| denied("research_acl_sid_utf16"))?;
        drop(local);
        Ok(value)
    }

    fn token_user_sid() -> HostControlResult<String> {
        let mut token = null_mut();
        if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
            return Err(denied("research_acl_control_token"));
        }
        let token = Handle {
            raw: token,
            _parents: Vec::new(),
        };
        let mut bytes = 0u32;
        unsafe { GetTokenInformation(token.raw, TokenUser, null_mut(), 0, &mut bytes) };
        if bytes < size_of::<TOKEN_USER>() as u32 || bytes > 4096 {
            return Err(denied("research_acl_token_user_size"));
        }
        // Vec<usize> has the alignment required for TOKEN_USER.
        let mut data = vec![0usize; (bytes as usize).div_ceil(size_of::<usize>())];
        if unsafe {
            GetTokenInformation(
                token.raw,
                TokenUser,
                data.as_mut_ptr().cast(),
                bytes,
                &mut bytes,
            )
        } == 0
        {
            return Err(denied("research_acl_token_user"));
        }
        let user = unsafe { &*(data.as_ptr().cast::<TOKEN_USER>()) };
        if user.User.Sid.is_null() {
            return Err(denied("research_acl_token_user_missing"));
        }
        sid_string(user.User.Sid)
    }

    fn identity() -> HostControlResult<Identity> {
        // Read-only lookups (including StatusResearch) must NEVER create the
        // OS profile. A missing profile is therefore an unproven identity.
        let name = wide(OsStr::new(
            crate::research_sandbox::RESEARCH_APP_CONTAINER_PROFILE_NAME,
        ));
        let mut package = null_mut();
        if unsafe { DeriveAppContainerSidFromAppContainerName(name.as_ptr(), &mut package) } != 0
            || package.is_null()
        {
            return Err(denied("research_acl_package_sid"));
        }
        let package = PackageSid(package);
        Ok(Identity {
            user: token_user_sid()?,
            package: sid_string(package.0)?,
        })
    }

    fn provisioning_identity() -> HostControlResult<Identity> {
        // Profile creation is explicitly part of DeployResearch/StartResearch,
        // never StatusResearch. Microsoft's documented creation flow registers
        // the per-user profile before deriving the installed package SID.
        crate::research_sandbox::ensure_appcontainer_profile()?;
        identity()
    }

    #[derive(Clone, Copy)]
    enum Kind {
        Image,
        Catalog,
    }

    fn descriptor(identity: &Identity, kind: Kind) -> HostControlResult<LocalDescriptor> {
        // Explicit Package SID grants are mandatory on every protected object.
        // A User ACE is also necessary: AppContainer uses both ordinary-user
        // and package-SID access checks, intersecting their effective rights.
        let (flags, package_mask, label) = match kind {
            Kind::Image => ("", IMAGE_PACKAGE_RX, ""),
            Kind::Catalog => ("OICI", CATALOG_PACKAGE_MODIFY, "S:(ML;OICI;NW;;;LW)"),
        };
        let sddl = format!(
            "O:{user}D:P(A;{flags};0x{full:08x};;;SY)\
             (A;{flags};0x{full:08x};;;BA)\
             (A;{flags};0x{full:08x};;;{user})\
             (A;{flags};0x{package_mask:08x};;;{package}){label}",
            user = identity.user,
            package = identity.package,
            full = CONTROL_FULL,
        );
        let mut raw = null_mut();
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide(OsStr::new(&sddl)).as_ptr(),
                1, // SDDL_REVISION_1
                &mut raw,
                null_mut(),
            )
        } == 0
            || raw.is_null()
        {
            return Err(denied("research_acl_descriptor_create"));
        }
        Ok(LocalDescriptor(raw))
    }

    fn descriptor_parts(raw: *mut c_void) -> HostControlResult<(PSID, *mut ACL, *mut ACL)> {
        let mut owner = null_mut();
        let mut defaulted = 0;
        let mut present = 0;
        let mut dacl = null_mut();
        let mut sacl = null_mut();
        if unsafe { GetSecurityDescriptorOwner(raw, &mut owner, &mut defaulted) } == 0
            || unsafe { GetSecurityDescriptorDacl(raw, &mut present, &mut dacl, &mut defaulted) }
                == 0
            || present == 0
            || owner.is_null()
            || dacl.is_null()
        {
            return Err(denied("research_acl_descriptor_components"));
        }
        // Image descriptors intentionally have no low-integrity label; the
        // immutable EXE needs read/execute only. Catalog needs a Low label.
        if unsafe { GetSecurityDescriptorSacl(raw, &mut present, &mut sacl, &mut defaulted) } == 0 {
            return Err(denied("research_acl_descriptor_sacl"));
        }
        Ok((owner, dacl, sacl))
    }

    fn secure_directory_creation(path: &Path, expected: &LocalDescriptor) -> HostControlResult<()> {
        let parent = path
            .parent()
            .ok_or_else(|| denied("research_acl_create_parent"))?;
        let _held_parent = open_with_share(parent, true, false, true)?;
        let attrs = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: expected.0,
            bInheritHandle: 0,
        };
        if unsafe { CreateDirectoryW(wide(path.as_os_str()).as_ptr(), &attrs) } == 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_ALREADY_EXISTS as i32) {
                return Err(denied("research_acl_create_directory"));
            }
        }
        Ok(())
    }

    fn checked_ancestors(path: &Path) -> HostControlResult<()> {
        use std::os::windows::fs::MetadataExt;
        for ancestor in path.ancestors() {
            let metadata = fs::symlink_metadata(ancestor)
                .map_err(|_| denied("research_acl_ancestor_missing"))?;
            if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
                || metadata.file_type().is_symlink()
            {
                return Err(denied("research_acl_reparse_ancestor"));
            }
        }
        Ok(())
    }

    fn checked_handle(
        path: &Path,
        directory: bool,
        mutation: bool,
        share_write: bool,
        parents: Vec<Handle>,
    ) -> HostControlResult<Handle> {
        let access =
            READ_CONTROL | FILE_READ_DATA | if mutation { WRITE_DAC | WRITE_OWNER } else { 0 };
        let flags = FILE_FLAG_OPEN_REPARSE_POINT
            | if directory {
                FILE_FLAG_BACKUP_SEMANTICS
            } else {
                FILE_ATTRIBUTE_NORMAL
            };
        let raw = unsafe {
            CreateFileW(
                wide(path.as_os_str()).as_ptr(),
                access,
                FILE_SHARE_READ
                    | if share_write {
                        windows_sys::Win32::Storage::FileSystem::FILE_SHARE_WRITE
                    } else {
                        0
                    },
                null(),
                OPEN_EXISTING,
                flags,
                null_mut(),
            )
        };
        if raw == INVALID_HANDLE_VALUE {
            return Err(denied("research_acl_open_handle"));
        }
        let handle = Handle {
            raw,
            _parents: parents,
        };
        let mut information: BY_HANDLE_FILE_INFORMATION = unsafe { zeroed() };
        if unsafe { GetFileInformationByHandle(handle.raw, &mut information) } == 0 {
            return Err(denied("research_acl_file_identity"));
        }
        if information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
            || (information.dwFileAttributes & 0x10 != 0) != directory
            || (!directory && information.nNumberOfLinks != 1)
        {
            return Err(denied("research_acl_file_identity"));
        }
        Ok(handle)
    }

    fn open_with_share(
        path: &Path,
        directory: bool,
        mutation: bool,
        share_write: bool,
    ) -> HostControlResult<Handle> {
        checked_ancestors(path)?;
        let mut parents = Vec::new();
        // Acquire from filesystem root towards the target. A protected parent
        // prevents a later path substitution while the child is opened.
        let ancestors: Vec<_> = path
            .ancestors()
            .skip(1)
            .filter(|p| !p.as_os_str().is_empty())
            .collect();
        for ancestor in ancestors.into_iter().rev() {
            parents.push(checked_handle(ancestor, true, false, true, Vec::new())?);
        }
        checked_handle(path, directory, mutation, share_write, parents)
    }

    fn open(path: &Path, directory: bool, mutation: bool) -> HostControlResult<Handle> {
        open_with_share(path, directory, mutation, false)
    }

    fn verify_owner(handle: &Handle, expected: &LocalDescriptor) -> HostControlResult<()> {
        let mut actual_owner = null_mut();
        let mut raw = null_mut();
        if unsafe {
            GetSecurityInfo(
                handle.raw,
                SE_FILE_OBJECT,
                OWNER_SECURITY_INFORMATION,
                &mut actual_owner,
                null_mut(),
                null_mut(),
                null_mut(),
                &mut raw,
            )
        } != 0
            || raw.is_null()
        {
            return Err(denied("research_acl_owner_query"));
        }
        let _actual = LocalDescriptor(raw);
        let (expected_owner, _, _) = descriptor_parts(expected.0)?;
        if actual_owner.is_null() || unsafe { EqualSid(actual_owner, expected_owner) } == 0 {
            return Err(denied("research_acl_unexpected_owner"));
        }
        Ok(())
    }

    fn verify_exact(
        handle: &Handle,
        expected: &LocalDescriptor,
        kind: Kind,
    ) -> HostControlResult<()> {
        let mut raw = null_mut();
        let mut dacl = null_mut();
        let mut sacl = null_mut();
        let mask = OWNER_SECURITY_INFORMATION
            | DACL_SECURITY_INFORMATION
            | if matches!(kind, Kind::Catalog) {
                LABEL_SECURITY_INFORMATION
            } else {
                0
            };
        if unsafe {
            GetSecurityInfo(
                handle.raw,
                SE_FILE_OBJECT,
                mask,
                null_mut(),
                null_mut(),
                &mut dacl,
                &mut sacl,
                &mut raw,
            )
        } != 0
            || raw.is_null()
            || dacl.is_null()
        {
            return Err(denied("research_acl_readback"));
        }
        let _actual = LocalDescriptor(raw);
        let mut control = 0u16;
        let mut revision = 0u32;
        if unsafe { GetSecurityDescriptorControl(raw, &mut control, &mut revision) } == 0
            || control & SE_DACL_PROTECTED == 0
        {
            return Err(denied("research_acl_not_protected"));
        }
        verify_owner(handle, expected)?;
        let (_, expected_dacl, _) = descriptor_parts(expected.0)?;
        let actual_count = unsafe { (*dacl).AceCount } as u32;
        if actual_count != unsafe { (*expected_dacl).AceCount } as u32 || actual_count != 4 {
            return Err(denied("research_acl_ace_count"));
        }
        // Compare exact mask, flags, type and SID, not merely effective access;
        // a broad AppContainer ACE or inherited ACE must not be accepted.
        for index in 0..actual_count {
            let (mut actual, mut template) = (null_mut(), null_mut());
            if unsafe { GetAce(dacl, index, &mut actual) } == 0
                || unsafe { GetAce(expected_dacl, index, &mut template) } == 0
                || actual.is_null()
                || template.is_null()
            {
                return Err(denied("research_acl_ace_readback"));
            }
            let a = unsafe { &*(actual.cast::<ACE_HEADER>()) };
            let b = unsafe { &*(template.cast::<ACE_HEADER>()) };
            if a.AceType != 0
                || b.AceType != 0
                || a.AceSize != b.AceSize
                || a.AceFlags != b.AceFlags
                || a.AceFlags & INHERITED_ACE != 0
                || (a.AceSize as usize) < size_of::<ACCESS_ALLOWED_ACE>()
            {
                return Err(denied("research_acl_ace_layout"));
            }
            let a = unsafe { &*(actual.cast::<ACCESS_ALLOWED_ACE>()) };
            let b = unsafe { &*(template.cast::<ACCESS_ALLOWED_ACE>()) };
            if a.Mask != b.Mask
                || unsafe {
                    EqualSid(
                        (&a.SidStart as *const u32).cast_mut().cast(),
                        (&b.SidStart as *const u32).cast_mut().cast(),
                    )
                } == 0
            {
                return Err(denied("research_acl_ace_mismatch"));
            }
        }
        if matches!(kind, Kind::Catalog) {
            verify_low_label(sacl, true)?;
        }
        Ok(())
    }

    fn verify_low_label(sacl: *mut ACL, is_directory: bool) -> HostControlResult<()> {
        if sacl.is_null() || unsafe { (*sacl).AceCount } != 1 {
            return Err(denied("research_acl_low_label_missing"));
        }
        let mut ace = null_mut();
        if unsafe { GetAce(sacl, 0, &mut ace) } == 0 || ace.is_null() {
            return Err(denied("research_acl_low_label_readback"));
        }
        let header = unsafe { &*(ace.cast::<ACE_HEADER>()) };
        if header.AceType != 17
            || (header.AceSize as usize) < size_of::<SYSTEM_MANDATORY_LABEL_ACE>()
            || header.AceFlags & !(OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE | INHERITED_ACE) != 0
            || (is_directory
                && header.AceFlags & (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE)
                    != OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE)
        {
            return Err(denied("research_acl_low_label_mismatch"));
        }
        let label = unsafe { &*(ace.cast::<SYSTEM_MANDATORY_LABEL_ACE>()) };
        if label.Mask != SYSTEM_MANDATORY_LABEL_NO_WRITE_UP {
            return Err(denied("research_acl_low_label_mismatch"));
        }
        let low = identity_low_sid()?;
        if unsafe { EqualSid((&label.SidStart as *const u32).cast_mut().cast(), low.0) } == 0 {
            return Err(denied("research_acl_low_label_sid"));
        }
        Ok(())
    }

    // ConvertStringSidToSidW produces LocalFree-owned memory, unlike the
    // FreeSid-owned SID returned by DeriveAppContainerSidFromAppContainerName.
    struct LocalSid(PSID);
    impl Drop for LocalSid {
        fn drop(&mut self) {
            unsafe { LocalFree(self.0) };
        }
    }

    fn identity_low_sid() -> HostControlResult<LocalSid> {
        use windows_sys::Win32::Security::Authorization::ConvertStringSidToSidW;
        let mut sid = null_mut();
        if unsafe { ConvertStringSidToSidW(wide(OsStr::new("S-1-16-4096")).as_ptr(), &mut sid) }
            == 0
            || sid.is_null()
        {
            return Err(denied("research_acl_low_sid"));
        }
        Ok(LocalSid(sid))
    }

    fn seal(handle: &Handle, descriptor: &LocalDescriptor, kind: Kind) -> HostControlResult<()> {
        verify_owner(handle, descriptor)?;
        let (_, dacl, sacl) = descriptor_parts(descriptor.0)?;
        if unsafe {
            SetSecurityInfo(
                handle.raw,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                dacl,
                null(),
            )
        } != 0
        {
            return Err(denied("research_acl_set_protected_dacl"));
        }
        if matches!(kind, Kind::Catalog)
            && (sacl.is_null()
                || unsafe {
                    SetSecurityInfo(
                        handle.raw,
                        SE_FILE_OBJECT,
                        LABEL_SECURITY_INFORMATION,
                        null_mut(),
                        null_mut(),
                        null(),
                        sacl,
                    )
                } != 0)
        {
            return Err(denied("research_acl_set_low_label"));
        }
        verify_exact(handle, descriptor, kind)
    }

    fn hash_file(handle: &Handle, expected: &str) -> HostControlResult<()> {
        // The no-share-write/delete handle stays open through hash and ACL
        // mutation, preventing a path-swap between verification and seal.
        use std::io::{Read, Seek, SeekFrom};
        use std::mem::ManuallyDrop;
        let mut file = ManuallyDrop::new(unsafe { fs::File::from_raw_handle(handle.raw) });
        file.seek(SeekFrom::Start(0))?;
        let mut hasher = Sha256::new();
        let mut buffer = [0u8; 8192];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        if format!("{:x}", hasher.finalize()) != expected {
            return Err(denied("research_acl_binary_sha"));
        }
        Ok(())
    }

    fn verify_version_inventory(binary: &Path) -> HostControlResult<()> {
        let parent = binary
            .parent()
            .ok_or_else(|| denied("research_acl_image_parent"))?;
        verify_version_scope(parent)?;
        let expected = binary
            .file_name()
            .ok_or_else(|| denied("research_acl_image_name"))?;
        if expected != OsStr::new("okx-research-service.exe")
            && expected != OsStr::new("okx-research-worker.exe")
        {
            return Err(denied("research_acl_unexpected_image_name"));
        }
        let mut count = 0usize;
        for item in fs::read_dir(parent)? {
            let item = item?;
            count += 1;
            if count != 1 || item.file_name().as_os_str() != expected {
                // A foreign DLL, alternate executable, interrupted stage file,
                // reparse point or other sidecar must not share the executable
                // search directory with the accepted SHA image.
                return Err(denied("research_acl_version_inventory"));
            }
        }
        if count != 1 {
            return Err(denied("research_acl_version_inventory"));
        }
        Ok(())
    }

    fn verify_version_scope(version: &Path) -> HostControlResult<()> {
        let sha = version
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| denied("research_acl_version_scope"))?;
        if sha.len() != 64
            || !sha
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || version.parent().and_then(Path::file_name) != Some(OsStr::new("versions"))
            || version
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                != Some(OsStr::new("research"))
        {
            return Err(denied("research_acl_version_scope"));
        }
        Ok(())
    }

    fn verify_catalog_scope(catalog: &Path) -> HostControlResult<()> {
        if catalog.file_name() != Some(OsStr::new("edge-catalog"))
            || catalog.parent().and_then(Path::file_name) != Some(OsStr::new("research"))
        {
            return Err(denied("research_acl_catalog_scope"));
        }
        Ok(())
    }

    pub(super) fn preflight_image(binary: &Path) -> HostControlResult<()> {
        verify_version_inventory(binary)?;
        let expected = descriptor(&identity()?, Kind::Image)?;
        let binary_handle = open(binary, false, false)?;
        verify_owner(&binary_handle, &expected)?;
        let parent = binary
            .parent()
            .ok_or_else(|| denied("research_acl_image_parent"))?;
        let parent_handle = open(parent, true, false)?;
        verify_owner(&parent_handle, &expected)
    }

    pub(super) fn write_staged_image_file(
        directory: &Path,
        bytes: &[u8],
    ) -> HostControlResult<std::path::PathBuf> {
        use std::io::Write;
        let expected = descriptor(&identity()?, Kind::Image)?;
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).map_err(|_| denied("research_acl_stage_random"))?;
        let path = directory.join(format!(
            ".research-stage-{:032x}.tmp",
            u128::from_le_bytes(nonce)
        ));
        checked_ancestors(directory)?;
        let _held_directory = open_with_share(directory, true, false, true)?;
        let attrs = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: expected.0,
            bInheritHandle: 0,
        };
        let handle = unsafe {
            CreateFileW(
                wide(path.as_os_str()).as_ptr(),
                FILE_WRITE_DATA | READ_CONTROL | WRITE_DAC,
                FILE_SHARE_READ,
                &attrs,
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
                null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(denied("research_acl_stage_create"));
        }
        let mut file = unsafe { fs::File::from_raw_handle(handle) };
        let result = (|| -> HostControlResult<()> {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        })();
        drop(file);
        if result.is_err() {
            let _ = fs::remove_file(&path);
        }
        result.map(|()| path)
    }

    pub(super) fn prepare_version_directory(version: &Path) -> HostControlResult<()> {
        verify_version_scope(version)?;
        // The two parent directories are verified by the artifact installer;
        // only this SHA-named Research-only directory receives an ACL.
        checked_ancestors(
            version
                .parent()
                .ok_or_else(|| denied("research_acl_version_parent"))?,
        )?;
        let id = provisioning_identity()?;
        let expected = descriptor(&id, Kind::Image)?;
        secure_directory_creation(version, &expected)?;
        let handle = open(version, true, false)?;
        verify_exact(&handle, &expected, Kind::Image)?;
        Ok(())
    }

    pub(super) fn verify_version_image(binary: &Path, sha: &str) -> HostControlResult<()> {
        verify_version_inventory(binary)?;
        let expected = descriptor(&identity()?, Kind::Image)?;
        let binary_handle = open(binary, false, false)?;
        hash_file(&binary_handle, sha)?;
        let parent = binary
            .parent()
            .ok_or_else(|| denied("research_acl_image_parent"))?;
        let parent_handle = open(parent, true, false)?;
        verify_exact(&parent_handle, &expected, Kind::Image)?;
        verify_exact(&binary_handle, &expected, Kind::Image)
    }

    pub(super) fn ensure_independent_catalog(catalog: &Path) -> HostControlResult<()> {
        verify_catalog_scope(catalog)?;
        let parent = catalog
            .parent()
            .ok_or_else(|| denied("research_acl_catalog_parent"))?;
        checked_ancestors(parent)?;
        let expected = descriptor(&provisioning_identity()?, Kind::Catalog)?;
        let existed = match fs::symlink_metadata(catalog) {
            Ok(metadata) => {
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(denied("research_acl_catalog_existing_type"));
                }
                true
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => return Err(denied("research_acl_catalog_metadata")),
        };
        if !existed {
            secure_directory_creation(catalog, &expected)?;
        }
        let handle = open(catalog, true, true)?;
        if existed {
            // An existing nonempty catalog must already have the full sealed
            // contract, including its Low label. Do not silently grant new
            // package rights over arbitrary preexisting files.
            let mut entries = fs::read_dir(catalog)?;
            if entries.next().is_some() {
                verify_exact(&handle, &expected, Kind::Catalog)?;
                return verify_catalog_children(catalog, &identity()?);
            }
        }
        seal(&handle, &expected, Kind::Catalog)
    }

    fn verify_catalog_children(root: &Path, id: &Identity) -> HostControlResult<()> {
        let mut pending = vec![(root.to_path_buf(), 0usize)];
        let mut visited = 0usize;
        while let Some((directory, depth)) = pending.pop() {
            if depth > CATALOG_DEPTH_BUDGET {
                return Err(denied("research_acl_catalog_depth"));
            }
            for entry in fs::read_dir(directory)? {
                visited += 1;
                if visited > CATALOG_ENTRY_BUDGET {
                    return Err(denied("research_acl_catalog_budget"));
                }
                let path = entry?.path();
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.is_file() && !metadata.is_dir() {
                    return Err(denied("research_acl_catalog_special"));
                }
                let item = open(&path, metadata.is_dir(), false)?;
                verify_catalog_child(&item, id, metadata.is_dir())?;
                if metadata.is_dir() {
                    pending.push((path, depth + 1));
                }
            }
        }
        Ok(())
    }

    fn verify_catalog_child(handle: &Handle, id: &Identity, is_dir: bool) -> HostControlResult<()> {
        // Existing descendants can inherit their package grant from the
        // protected catalog root. All allow ACEs must remain scoped to the
        // owner/SYSTEM/Admin/package and never grant WRITE_DAC to the package.
        let expected_root = descriptor(id, Kind::Catalog)?;
        verify_owner(handle, &expected_root)?;
        let mut raw = null_mut();
        let mut dacl = null_mut();
        let mut sacl = null_mut();
        if unsafe {
            GetSecurityInfo(
                handle.raw,
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | LABEL_SECURITY_INFORMATION,
                null_mut(),
                null_mut(),
                &mut dacl,
                &mut sacl,
                &mut raw,
            )
        } != 0
            || raw.is_null()
            || dacl.is_null()
        {
            return Err(denied("research_acl_catalog_child_dacl"));
        }
        let _actual = LocalDescriptor(raw);
        verify_low_label(sacl, is_dir)?;
        let (_, template, _) = descriptor_parts(expected_root.0)?;
        let count = unsafe { (*dacl).AceCount };
        if count != 4 {
            return Err(denied("research_acl_catalog_child_aces"));
        }
        for index in 0..4u32 {
            let (mut actual, mut expected) = (null_mut(), null_mut());
            if unsafe { GetAce(dacl, index, &mut actual) } == 0
                || unsafe { GetAce(template, index, &mut expected) } == 0
                || actual.is_null()
                || expected.is_null()
            {
                return Err(denied("research_acl_catalog_child_ace_read"));
            }
            let a = unsafe { &*(actual.cast::<ACE_HEADER>()) };
            let b = unsafe { &*(expected.cast::<ACE_HEADER>()) };
            if a.AceType != 0
                || a.AceSize != b.AceSize
                || a.AceFlags & !(INHERITED_ACE | OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) != 0
                || (is_dir
                    && a.AceFlags & (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE)
                        != OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE)
                || (!is_dir && a.AceFlags & (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) != 0)
            {
                return Err(denied("research_acl_catalog_child_ace_type"));
            }
            let a = unsafe { &*(actual.cast::<ACCESS_ALLOWED_ACE>()) };
            let b = unsafe { &*(expected.cast::<ACCESS_ALLOWED_ACE>()) };
            if a.Mask != b.Mask
                || unsafe {
                    EqualSid(
                        (&a.SidStart as *const u32).cast_mut().cast(),
                        (&b.SidStart as *const u32).cast_mut().cast(),
                    )
                } == 0
            {
                return Err(denied("research_acl_catalog_child_ace_mismatch"));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::{
            fs::OpenOptions,
            io::{Read, Write},
            os::windows::fs::MetadataExt,
            path::PathBuf,
        };

        // These are CONTROL-user Win32 descriptor/inheritance witnesses. They
        // are deliberately separate from the physical LPAC-child AccessCheck,
        // file-operation and CredMan witnesses in research_sandbox tests.
        // Parent-created SQLite fixtures alone must not be reported as an
        // LPAC child write proof.

        #[derive(Debug, PartialEq, Eq)]
        struct AclSnapshot {
            owner: String,
            protected: bool,
            aces: Vec<(u8, u8, u32, String)>,
        }

        fn snapshot_acl(path: &Path, directory: bool) -> AclSnapshot {
            let handle = open(path, directory, false).unwrap();
            let (mut descriptor, mut owner, mut dacl) = (null_mut(), null_mut(), null_mut());
            assert_eq!(
                unsafe {
                    GetSecurityInfo(
                        handle.raw,
                        SE_FILE_OBJECT,
                        OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                        &mut owner,
                        null_mut(),
                        &mut dacl,
                        null_mut(),
                        &mut descriptor,
                    )
                },
                0,
                "GetSecurityInfo failed for {}",
                path.display()
            );
            assert!(!descriptor.is_null());
            assert!(!owner.is_null());
            assert!(!dacl.is_null());
            let _guard = LocalDescriptor(descriptor);
            let mut control = 0u16;
            let mut revision = 0u32;
            assert_ne!(
                unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) },
                0
            );
            let mut aces = Vec::new();
            for index in 0..unsafe { (*dacl).AceCount } {
                let mut ptr = null_mut();
                assert_ne!(unsafe { GetAce(dacl, index as u32, &mut ptr) }, 0);
                assert!(!ptr.is_null());
                let header = unsafe { &*(ptr.cast::<ACE_HEADER>()) };
                assert_eq!(
                    header.AceType,
                    0,
                    "unexpected ACE type on {}",
                    path.display()
                );
                assert!((header.AceSize as usize) >= size_of::<ACCESS_ALLOWED_ACE>());
                let allow = unsafe { &*(ptr.cast::<ACCESS_ALLOWED_ACE>()) };
                let sid = (&allow.SidStart as *const u32).cast_mut().cast();
                aces.push((
                    header.AceType,
                    header.AceFlags,
                    allow.Mask,
                    sid_string(sid).unwrap(),
                ));
            }
            AclSnapshot {
                owner: sid_string(owner).unwrap(),
                protected: control & SE_DACL_PROTECTED != 0,
                aces,
            }
        }

        fn assert_package_mask(path: &Path, directory: bool, expected_mask: u32) {
            let acl = snapshot_acl(path, directory);
            let package = identity().unwrap().package;
            assert_ne!(
                acl.owner, package,
                "package SID unexpectedly owns an object"
            );
            let grants: Vec<_> = acl
                .aces
                .iter()
                .filter(|(_, _, _, sid)| sid == &package)
                .collect();
            assert_eq!(grants.len(), 1, "exactly one package grant required");
            assert_eq!(grants[0].2, expected_mask);
            assert_eq!(grants[0].2 & (WRITE_DAC | WRITE_OWNER), 0);
            assert!(
                acl.aces
                    .iter()
                    .all(|(_, flags, _, _)| flags & INHERITED_ACE == 0)
                    || !acl.protected,
                "protected image/catalog root has inherited ACEs"
            );
        }

        fn assert_no_exact_package_grant(path: &Path, directory: bool) {
            let acl = snapshot_acl(path, directory);
            let package = identity().unwrap().package;
            assert!(
                acl.aces.iter().all(|(_, _, _, sid)| sid != &package),
                "Research package was granted access to synthetic finance {}",
                path.display()
            );
        }

        struct DisposableRoot(std::path::PathBuf);

        impl DisposableRoot {
            fn new() -> Self {
                assert_eq!(
                    std::env::var("OKX_DISPOSABLE_WINDOWS_RUNNER").as_deref(),
                    Ok("1"),
                    "Windows ACL physical probes require explicit opt-in"
                );
                let runner_temp = PathBuf::from(
                    std::env::var_os("RUNNER_TEMP").expect("RUNNER_TEMP must be set"),
                );
                let base = PathBuf::from(
                    std::env::var_os("OKX_DISPOSABLE_WINDOWS_ROOT")
                        .expect("OKX_DISPOSABLE_WINDOWS_ROOT must be set"),
                );
                assert!(runner_temp.is_absolute() && base.is_absolute());
                assert!(
                    runner_temp.file_name().is_some(),
                    "RUNNER_TEMP must not be a volume root"
                );
                assert!(
                    !runner_temp.components().any(|component| component
                        .as_os_str()
                        .to_str()
                        .is_some_and(|name| name.eq_ignore_ascii_case("okx"))),
                    "never use an installed OKX tree as RUNNER_TEMP"
                );
                assert_ne!(
                    base, runner_temp,
                    "never use RUNNER_TEMP itself as test root"
                );
                assert!(
                    base.starts_with(&runner_temp),
                    "disposable root must be below RUNNER_TEMP"
                );
                assert!(
                    base.file_name()
                        .and_then(|s| s.to_str())
                        .is_some_and(|name| name.starts_with("okx-s2-")),
                    "disposable root must have okx-s2- prefix"
                );
                fs::create_dir_all(&base).unwrap();
                checked_ancestors(&base).unwrap();
                let canonical_runner = runner_temp.canonicalize().unwrap();
                let canonical_base = base.canonicalize().unwrap();
                assert_ne!(canonical_base, canonical_runner);
                assert!(
                    canonical_base.starts_with(canonical_runner),
                    "disposable root escaped RUNNER_TEMP"
                );
                let mut nonce = [0u8; 16];
                getrandom::fill(&mut nonce).unwrap();
                let path = base.join(format!(
                    "okx-research-acl-disposable-{:032x}",
                    u128::from_le_bytes(nonce)
                ));
                fs::create_dir(&path).unwrap();
                Self(path)
            }
        }

        impl Drop for DisposableRoot {
            fn drop(&mut self) {
                // Only this unique test-owned directory may be removed.
                let _ = fs::remove_dir_all(&self.0);
            }
        }

        #[test]
        fn scope_masks_never_grant_package_acl_mutation() {
            assert_eq!(IMAGE_PACKAGE_RX & 0x000d_0016, 0);
            assert_eq!(CATALOG_PACKAGE_MODIFY & (WRITE_DAC | WRITE_OWNER), 0);
            assert_eq!(
                CATALOG_PACKAGE_MODIFY & (FILE_READ_DATA | FILE_WRITE_DATA),
                3
            );
        }

        #[test]
        fn descriptor_is_explicit_protected_and_scope_separated() {
            let fake = Identity {
                user: "S-1-5-21-1-2-3-1001".into(),
                package: "S-1-15-2-123-456-789-10-11-12-13".into(),
            };
            for kind in [Kind::Image, Kind::Catalog] {
                let parsed = descriptor(&fake, kind).unwrap();
                let (_, dacl, sacl) = descriptor_parts(parsed.0).unwrap();
                assert_eq!(unsafe { (*dacl).AceCount }, 4);
                assert_eq!(sacl.is_null(), matches!(kind, Kind::Image));
            }
        }

        #[test]
        #[ignore = "manual disposable Windows ACL proof; not installed PHYSICAL_DENY"]
        fn disposable_windows_acl_image_package_rx_and_inventory() {
            let root = DisposableRoot::new();
            let research = root.0.join("research");
            let versions = research.join("versions");
            fs::create_dir(&research).unwrap();
            fs::create_dir(&versions).unwrap();
            let bytes = b"MZ disposable Research ACL canary, never executed";
            let sha = format!("{:x}", Sha256::digest(bytes));
            let version = versions.join(&sha);
            prepare_version_directory(&version).unwrap();
            let staged = write_staged_image_file(&version, bytes).unwrap();
            let binary = version.join("okx-research-service.exe");
            fs::rename(staged, &binary).unwrap();
            verify_version_image(&binary, &sha).unwrap();
            assert_package_mask(&version, true, IMAGE_PACKAGE_RX);
            assert_package_mask(&binary, false, IMAGE_PACKAGE_RX);
            assert!(snapshot_acl(&version, true).protected);
            assert!(snapshot_acl(&binary, false).protected);
            // An extra image-side DLL violates the exact version inventory.
            fs::write(version.join("unauthorized.dll"), b"synthetic-only").unwrap();
            assert!(verify_version_image(&binary, &sha).is_err());
            println!("OKX_S2_WITNESS=disposable_windows_acl_image_package_rx_and_inventory");
        }

        #[test]
        #[ignore = "manual disposable Windows negative ACL proof"]
        fn disposable_windows_acl_unsealed_catalog_is_rejected() {
            let root = DisposableRoot::new();
            let research = root.0.join("research");
            let catalog = research.join("edge-catalog");
            fs::create_dir(&research).unwrap();
            fs::create_dir(&catalog).unwrap();
            fs::write(catalog.join("research-catalog.sqlite"), b"unsealed").unwrap();
            assert!(ensure_independent_catalog(&catalog).is_err());
            println!("OKX_S2_WITNESS=disposable_windows_acl_unsealed_catalog_is_rejected");
        }

        #[test]
        #[ignore = "manual disposable Windows hardlink negative"]
        fn disposable_windows_acl_reparse_and_hardlink_rejected() {
            let root = DisposableRoot::new();
            let research = root.0.join("research");
            let versions = research.join("versions");
            fs::create_dir(&research).unwrap();
            fs::create_dir(&versions).unwrap();
            let bytes = b"MZ disposable hardlink canary";
            let sha = format!("{:x}", Sha256::digest(bytes));
            let version = versions.join(&sha);
            prepare_version_directory(&version).unwrap();
            let staged = write_staged_image_file(&version, bytes).unwrap();
            let binary = version.join("okx-research-service.exe");
            fs::rename(staged, &binary).unwrap();
            fs::hard_link(&binary, root.0.join("linked-image-canary")).unwrap();
            assert!(verify_version_image(&binary, &sha).is_err());

            // A symlink must be an actual Windows reparse point. Failure to
            // construct this fixture is NOT_PROVEN and fails the test; no
            // feature/privilege-based skip is allowed in the physical lane.
            let catalog = research.join("edge-catalog");
            let target = root.0.join("synthetic-catalog-target");
            fs::create_dir(&target).unwrap();
            std::os::windows::fs::symlink_dir(&target, &catalog)
                .expect("Windows reparse witness unavailable");
            assert!(ensure_independent_catalog(&catalog).is_err());

            let finance = root.0.join("finance");
            fs::create_dir(&finance).unwrap();
            let target = finance.join("finance.sqlite");
            fs::write(&target, b"synthetic finance data").unwrap();
            let link = finance.join("finance.sqlite-wal");
            std::os::windows::fs::symlink_file(&target, &link)
                .expect("Windows file reparse witness unavailable");
            assert_ne!(
                fs::symlink_metadata(&link).unwrap().file_attributes()
                    & FILE_ATTRIBUTE_REPARSE_POINT,
                0,
            );
            assert!(checked_ancestors(&link).is_err());
            println!("OKX_S2_WITNESS=disposable_windows_acl_reparse_and_hardlink_rejected");
        }

        #[test]
        #[ignore = "manual disposable Windows catalog sidecar/Low IL witness"]
        fn disposable_windows_acl_catalog_sqlite_sidecars_inherit_low_il() {
            let root = DisposableRoot::new();
            let research = root.0.join("research");
            fs::create_dir(&research).unwrap();
            let catalog = research.join("edge-catalog");
            ensure_independent_catalog(&catalog).unwrap();
            assert_package_mask(&catalog, true, CATALOG_PACKAGE_MODIFY);
            assert!(snapshot_acl(&catalog, true).protected);
            let id = identity().unwrap();
            for name in [
                "research-catalog.sqlite",
                "research-catalog.sqlite-wal",
                "research-catalog.sqlite-shm",
            ] {
                let file = catalog.join(name);
                fs::write(&file, b"synthetic SQLite sidecar only").unwrap();
                let mut output = OpenOptions::new().append(true).open(&file).unwrap();
                output.write_all(b"\nappend/delete canary").unwrap();
                output.sync_all().unwrap();
                drop(output);
                let handle = open(&file, false, false).unwrap();
                verify_catalog_child(&handle, &id, false).unwrap();
                drop(handle);
                let acl = snapshot_acl(&file, false);
                assert_eq!(acl.aces.len(), 4);
                assert!(!acl.protected, "children should inherit the catalog DACL");
                let package: Vec<_> = acl
                    .aces
                    .iter()
                    .filter(|(_, _, _, sid)| sid == &id.package)
                    .collect();
                assert_eq!(package.len(), 1);
                assert_eq!(package[0].2, CATALOG_PACKAGE_MODIFY);
                assert_ne!(package[0].1 & INHERITED_ACE, 0);
                assert_eq!(package[0].2 & (WRITE_DAC | WRITE_OWNER), 0);
                let mut bytes = Vec::new();
                fs::File::open(&file)
                    .unwrap()
                    .read_to_end(&mut bytes)
                    .unwrap();
                assert!(bytes.ends_with(b"\nappend/delete canary"));
            }
            ensure_independent_catalog(&catalog).unwrap();
            for name in [
                "research-catalog.sqlite",
                "research-catalog.sqlite-wal",
                "research-catalog.sqlite-shm",
            ] {
                fs::remove_file(catalog.join(name)).unwrap();
            }
            ensure_independent_catalog(&catalog).unwrap();
            println!(
                "OKX_S2_WITNESS=disposable_windows_acl_catalog_sqlite_sidecars_inherit_low_il"
            );
        }

        #[test]
        #[ignore = "manual disposable Windows synthetic Finance ACL nonmutation witness"]
        fn disposable_windows_acl_finance_sqlite_locks_untouched() {
            let root = DisposableRoot::new();
            provisioning_identity().unwrap();
            let finance = root.0.join("finance");
            fs::create_dir(&finance).unwrap();
            let mut paths = vec![(finance.clone(), true)];
            for name in [
                "finance.sqlite",
                "finance.sqlite-wal",
                "finance.sqlite-shm",
                "finance.sqlite.writer.lock",
                "authority-mode.lock",
            ] {
                let file = finance.join(name);
                fs::write(&file, format!("synthetic-only:{name}")).unwrap();
                paths.push((file, false));
            }
            let snapshots: Vec<_> = paths
                .iter()
                .map(|(path, directory)| {
                    assert_no_exact_package_grant(path, *directory);
                    snapshot_acl(path, *directory)
                })
                .collect();

            // Exercise the production Research-only provisioner while keeping
            // a synthetic Finance tree adjacent to it. No real Finance root,
            // account state, exchange or installed directory is opened.
            let research = root.0.join("research");
            fs::create_dir(&research).unwrap();
            ensure_independent_catalog(&research.join("edge-catalog")).unwrap();

            for ((path, directory), baseline) in paths.iter().zip(snapshots.iter()) {
                assert_eq!(&snapshot_acl(path, *directory), baseline);
                assert_no_exact_package_grant(path, *directory);
                if !directory {
                    assert!(
                        fs::read_to_string(path)
                            .unwrap()
                            .starts_with("synthetic-only:")
                    );
                }
            }
            println!("OKX_S2_WITNESS=disposable_windows_acl_finance_sqlite_locks_untouched");
        }
    }
}

#[cfg(windows)]
pub(crate) fn prepare_version_directory(version: &std::path::Path) -> crate::HostControlResult<()> {
    native::prepare_version_directory(version)
}

#[cfg(windows)]
pub(crate) fn preflight_image(binary: &std::path::Path) -> crate::HostControlResult<()> {
    native::preflight_image(binary)
}

#[cfg(windows)]
pub(crate) fn write_staged_image_file(
    directory: &std::path::Path,
    bytes: &[u8],
) -> crate::HostControlResult<std::path::PathBuf> {
    native::write_staged_image_file(directory, bytes)
}

#[cfg(windows)]
pub(crate) fn verify_version_image(
    binary: &std::path::Path,
    sha: &str,
) -> crate::HostControlResult<()> {
    native::verify_version_image(binary, sha)
}

#[cfg(windows)]
pub(crate) fn ensure_independent_catalog(
    catalog: &std::path::Path,
) -> crate::HostControlResult<()> {
    native::ensure_independent_catalog(catalog)
}
