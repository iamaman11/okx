//! Native process isolation for Research only. Admission permits still govern
//! concurrent jobs; the Windows Job Object independently fences Research memory
//! and CPU so one expensive Worker cannot monopolize the host's cores.
//! A commit fence is NOT a sampled RSS measurement or a per-job reservation.
//! A Windows named kernel mutex excludes concurrent legacy Engine-owned and
//! standalone CONTROL-owned Research Workers before either gets a CPU quota.

pub const RESEARCH_MEMORY_LIMIT_MIB: u32 = 4096;
/// Windows Job CPU rates are units of 1/10,000 of the host's processor cycles.
/// A single Research process tree receives at most half, leaving capacity
/// for Engine, CONTROL and the OS. A singleton forbids multiple Research quotas.
pub const RESEARCH_CPU_RATE_BASIS_POINTS: u32 = 5_000;

#[cfg(windows)]
const RESEARCH_SINGLETON_MUTEX: &str = r"Global\iamaman11-okx-research-single-worker-v1";

// An accepted release must never allow caller-controlled CPU quota namespaces.
// Test Workers are compiled separately and cannot be built with a release profile.
#[cfg(all(feature = "test-isolated-singleton", not(debug_assertions)))]
compile_error!("test-isolated-singleton is forbidden in release Research binaries");

#[cfg(windows)]
fn runtime_worker_mutex_name() -> Result<String, String> {
    #[cfg(all(feature = "test-isolated-singleton", debug_assertions))]
    if let Some(namespace) = std::env::var_os("OKX_RESEARCH_TEST_SINGLETON_NAMESPACE") {
        let namespace = namespace
            .into_string()
            .map_err(|_| "INVALID_RESEARCH_TEST_SINGLETON_NAMESPACE")?;
        return test_worker_mutex_name(&namespace);
    }
    Ok(RESEARCH_SINGLETON_MUTEX.into())
}

#[cfg(all(windows, feature = "test-isolated-singleton", debug_assertions))]
fn test_worker_mutex_name(namespace: &str) -> Result<String, String> {
    if namespace.len() != 64
        || !namespace
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("INVALID_RESEARCH_TEST_SINGLETON_NAMESPACE".into());
    }
    Ok(format!(
        "Global\\iamaman11-okx-research-single-worker-test-v1-{namespace}"
    ))
}

pub fn limit_bytes() -> u64 {
    u64::from(RESEARCH_MEMORY_LIMIT_MIB) * 1024 * 1024
}

#[cfg(windows)]
pub struct JobMemoryGuard {
    handle: windows_sys::Win32::Foundation::HANDLE,
    _single_worker: ResearchInstanceLease,
}

#[cfg(windows)]
pub struct ResearchInstanceLease {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl ResearchInstanceLease {
    /// Both a running Worker and credential provisioning must be exclusive.
    /// This prevents rotating the Research bearer behind the active process.
    pub fn acquire() -> Result<Self, String> {
        Self::acquire_named(RESEARCH_SINGLETON_MUTEX)
    }

    fn acquire_named(name: &str) -> Result<Self, String> {
        use windows_sys::Win32::{
            Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError},
            System::Threading::CreateMutexW,
        };

        let name = name.encode_utf16().chain(Some(0)).collect::<Vec<_>>();
        let handle = unsafe { CreateMutexW(std::ptr::null(), 0, name.as_ptr()) };
        if handle.is_null() {
            return Err(format!(
                "RESEARCH_SINGLE_WORKER_MUTEX_CREATE: {}",
                std::io::Error::last_os_error()
            ));
        }
        // CreateMutex is atomic across unrelated processes. If an installed
        // legacy Worker already owns the name, this process must not obtain a
        // second independent 50% CPU allowance under another catalog root.
        if unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
            unsafe { CloseHandle(handle) };
            return Err("RESEARCH_WORKER_ALREADY_ACTIVE".into());
        }
        Ok(Self { handle })
    }
}

#[cfg(windows)]
impl Drop for ResearchInstanceLease {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
        }
    }
}

#[cfg(windows)]
impl JobMemoryGuard {
    /// Establish one Research Worker and its nested resource Job before serving
    /// IPC. Failure is fatal: Research cannot bypass the singleton or quotas.
    pub fn enforce() -> Result<Self, String> {
        use std::{ffi::c_void, mem::size_of};
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_CPU_RATE_CONTROL_ENABLE,
            JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP, JOB_OBJECT_LIMIT_JOB_MEMORY,
            JOBOBJECT_CPU_RATE_CONTROL_INFORMATION, JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectCpuRateControlInformation,
            JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;

        let mutex_name = runtime_worker_mutex_name()?;
        let single_worker = ResearchInstanceLease::acquire_named(&mutex_name)?;
        let bytes = usize::try_from(limit_bytes())
            .map_err(|_| "RESEARCH_MEMORY_LIMIT_UNSUPPORTED_ARCH".to_owned())?;
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(format!(
                "RESEARCH_MEMORY_JOB_CREATE: {}",
                std::io::Error::last_os_error()
            ));
        }
        let guard = Self {
            handle,
            _single_worker: single_worker,
        };
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_JOB_MEMORY;
        info.JobMemoryLimit = bytes;
        if unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const c_void,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(format!(
                "RESEARCH_MEMORY_JOB_CONFIGURE: {}",
                std::io::Error::last_os_error()
            ));
        }
        let cpu_flags = JOB_OBJECT_CPU_RATE_CONTROL_ENABLE | JOB_OBJECT_CPU_RATE_CONTROL_HARD_CAP;
        let cpu = JOBOBJECT_CPU_RATE_CONTROL_INFORMATION {
            ControlFlags: cpu_flags,
            Anonymous: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION_0 {
                CpuRate: RESEARCH_CPU_RATE_BASIS_POINTS,
            },
        };
        if unsafe {
            SetInformationJobObject(
                handle,
                JobObjectCpuRateControlInformation,
                &cpu as *const _ as *const c_void,
                size_of::<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION>() as u32,
            )
        } == 0
        {
            return Err(format!(
                "RESEARCH_CPU_JOB_CONFIGURE: {}",
                std::io::Error::last_os_error()
            ));
        }
        // Windows 8+ nests an empty job under the inherited host job. The
        // host's financial process is NOT assigned to this nested Research job.
        if unsafe { AssignProcessToJobObject(handle, GetCurrentProcess()) } == 0 {
            return Err(format!(
                "RESEARCH_MEMORY_JOB_ASSIGN: {}",
                std::io::Error::last_os_error()
            ));
        }
        let mut observed: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        if unsafe {
            QueryInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &mut observed as *mut _ as *mut c_void,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        } == 0
            || observed.BasicLimitInformation.LimitFlags & JOB_OBJECT_LIMIT_JOB_MEMORY == 0
            || observed.JobMemoryLimit != bytes
        {
            return Err("RESEARCH_MEMORY_JOB_VERIFY_FAILED".into());
        }
        let mut observed_cpu: JOBOBJECT_CPU_RATE_CONTROL_INFORMATION =
            unsafe { std::mem::zeroed() };
        if unsafe {
            QueryInformationJobObject(
                handle,
                JobObjectCpuRateControlInformation,
                &mut observed_cpu as *mut _ as *mut c_void,
                size_of::<JOBOBJECT_CPU_RATE_CONTROL_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        } == 0
            || observed_cpu.ControlFlags & cpu_flags != cpu_flags
            // The union is safe to read as CpuRate because HARD_CAP selects it.
            || unsafe { observed_cpu.Anonymous.CpuRate } != RESEARCH_CPU_RATE_BASIS_POINTS
        {
            return Err("RESEARCH_CPU_JOB_VERIFY_FAILED".into());
        }
        Ok(guard)
    }
}

#[cfg(windows)]
impl Drop for JobMemoryGuard {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
        }
    }
}

#[cfg(not(windows))]
pub struct JobMemoryGuard;

#[cfg(not(windows))]
pub struct ResearchInstanceLease;

#[cfg(not(windows))]
impl ResearchInstanceLease {
    pub fn acquire() -> Result<Self, String> {
        Ok(Self)
    }
}

#[cfg(not(windows))]
impl JobMemoryGuard {
    /// Linux test builds are not Windows installed-Worker acceptance evidence.
    pub fn enforce() -> Result<Self, String> {
        Ok(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t10_commit_fence_matches_shared_research_admission_ceiling() {
        assert_eq!(RESEARCH_MEMORY_LIMIT_MIB, 4096);
        assert_eq!(limit_bytes(), 4_294_967_296);
        assert_eq!(
            RESEARCH_MEMORY_LIMIT_MIB,
            crate::analytics::RESEARCH_MEMORY_ADMISSION_MIB
        );
    }

    #[test]
    fn research_cpu_budget_reserves_half_of_windows_processor_cycles_for_other_owners() {
        assert_eq!(RESEARCH_CPU_RATE_BASIS_POINTS, 5_000);
        assert!((1..10_000).contains(&RESEARCH_CPU_RATE_BASIS_POINTS));
    }

    #[cfg(windows)]
    #[test]
    fn research_singleton_is_atomic_and_released_after_owner_exits() {
        let name = format!(
            r"Local\iamaman11-okx-research-single-worker-test-{}",
            std::process::id()
        );
        let first = ResearchInstanceLease::acquire_named(&name).unwrap();
        assert_eq!(
            ResearchInstanceLease::acquire_named(&name).err().as_deref(),
            Some("RESEARCH_WORKER_ALREADY_ACTIVE")
        );
        drop(first);
        let second = ResearchInstanceLease::acquire_named(&name).unwrap();
        drop(second);
    }

    #[cfg(all(windows, feature = "test-isolated-singleton", debug_assertions))]
    #[test]
    fn test_namespace_requires_an_exact_digest_and_never_aliases_production() {
        let test = test_worker_mutex_name(&"a".repeat(64)).unwrap();
        assert!(test.ends_with(&"a".repeat(64)));
        assert_ne!(test, RESEARCH_SINGLETON_MUTEX);
        for invalid in ["", "A", "a/../../", "f"] {
            assert!(test_worker_mutex_name(invalid).is_err());
        }
        assert!(test_worker_mutex_name(&"f".repeat(63)).is_err());
        assert!(test_worker_mutex_name(&"F".repeat(64)).is_err());
    }
}
