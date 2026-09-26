use std::process::Child;

use crate::{HostControlError, HostControlResult};

#[cfg(windows)]
pub struct AgentJob {
    handle: windows_sys::Win32::Foundation::HANDLE,
}

#[cfg(windows)]
impl AgentJob {
    pub fn new() -> HostControlResult<Self> {
        use std::{ffi::c_void, mem::size_of};
        use windows_sys::Win32::System::JobObjects::{
            CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };

        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(std::io::Error::last_os_error().into());
        }

        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

        let ok = unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const c_void,
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };

        if ok == 0 {
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(handle);
            }
            return Err(std::io::Error::last_os_error().into());
        }

        Ok(Self { handle })
    }

    pub fn assign(&self, child: &mut Child) -> HostControlResult<()> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::JobObjects::AssignProcessToJobObject;

        let ok = unsafe {
            AssignProcessToJobObject(self.handle, child.as_raw_handle() as _)
        };
        if ok == 0 {
            let _ = child.kill();
            let _ = child.wait();
            return Err(HostControlError::JobAssignment(
                std::io::Error::last_os_error().to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(windows)]
impl Drop for AgentJob {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                windows_sys::Win32::Foundation::CloseHandle(self.handle);
            }
        }
    }
}

#[cfg(not(windows))]
pub struct AgentJob;

#[cfg(not(windows))]
impl AgentJob {
    pub fn new() -> HostControlResult<Self> {
        Ok(Self)
    }

    pub fn assign(&self, _child: &mut Child) -> HostControlResult<()> {
        Ok(())
    }
}
