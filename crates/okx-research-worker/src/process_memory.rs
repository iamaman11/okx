//! Native process isolation for Research only. Admission permits still govern
//! concurrent jobs; the Windows Job Object is the independent OS commit fence.
//! A commit fence is NOT a sampled RSS measurement or a per-job reservation.

pub const RESEARCH_MEMORY_LIMIT_MIB: u32 = 4096;

pub fn limit_bytes() -> u64 {
    u64::from(RESEARCH_MEMORY_LIMIT_MIB) * 1024 * 1024
}

#[cfg(windows)]
pub struct JobMemoryGuard {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl JobMemoryGuard {
    /// Establish a nested Research-only Job Object before serving IPC. Failure
    /// is fatal: the Worker must never continue without its configured fence.
    pub fn enforce() -> Result<Self, String> {
        use std::{ffi::c_void, mem::size_of};
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_JOB_MEMORY,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            QueryInformationJobObject, SetInformationJobObject,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;

        let bytes = usize::try_from(limit_bytes())
            .map_err(|_| "RESEARCH_MEMORY_LIMIT_UNSUPPORTED_ARCH".to_owned())?;
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(format!("RESEARCH_MEMORY_JOB_CREATE: {}", std::io::Error::last_os_error()));
        }
        let guard = Self { handle };
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
        } == 0 {
            return Err(format!("RESEARCH_MEMORY_JOB_CONFIGURE: {}", std::io::Error::last_os_error()));
        }
        // Windows 8+ nests an empty job under the inherited host job. The
        // host's financial process is NOT assigned to this nested Research job.
        if unsafe { AssignProcessToJobObject(handle, GetCurrentProcess()) } == 0 {
            return Err(format!("RESEARCH_MEMORY_JOB_ASSIGN: {}", std::io::Error::last_os_error()));
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
        } == 0 || observed.BasicLimitInformation.LimitFlags & JOB_OBJECT_LIMIT_JOB_MEMORY == 0
            || observed.JobMemoryLimit != bytes
        {
            return Err("RESEARCH_MEMORY_JOB_VERIFY_FAILED".into());
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
}
