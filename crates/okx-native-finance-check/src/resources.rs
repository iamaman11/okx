//! Diagnostic measurements only: direct child, never fabricated tree totals.
//! Windows kernel counters include all threads of that process, not descendants.
use serde_json::{Value, json};
use tokio::{process::Child, sync::oneshot, task::JoinHandle};

pub(super) struct Measurement {
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Value>>,
}
impl Measurement {
    pub async fn finish(self) -> Value {
        if let Some(stop) = self.stop {
            let _ = stop.send(());
        }
        match self.task {
            Some(task) => task
                .await
                .unwrap_or_else(|_| json!({"status":"UNAVAILABLE"})),
            None => json!({"status":"UNAVAILABLE","scope":"direct_process_only"}),
        }
    }
}

pub(super) fn start(child: &Child) -> Measurement {
    #[cfg(windows)]
    if let Some(handle) = windows::duplicate(child) {
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut peak = None;
            let mut ticks = tokio::time::interval(std::time::Duration::from_millis(20));
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = ticks.tick() => {
                        if let Some(value) = windows::peak(&handle) {
                            peak = Some(peak.map_or(value, |p: usize| p.max(value)));
                        }
                    }
                }
            }
            windows::finish(&handle, peak)
        });
        return Measurement {
            stop: Some(stop),
            task: Some(task),
        };
    }
    #[cfg(not(windows))]
    let _ = child;
    Measurement {
        stop: None,
        task: None,
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::{
        ffi::c_void,
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    };
    #[repr(C)]
    #[derive(Default)]
    struct FileTime {
        low: u32,
        high: u32,
    }
    #[repr(C)]
    #[derive(Default)]
    struct Io {
        read_ops: u64,
        write_ops: u64,
        other_ops: u64,
        read_bytes: u64,
        write_bytes: u64,
        other_bytes: u64,
    }
    #[repr(C)]
    #[derive(Default)]
    struct Memory {
        size: u32,
        faults: u32,
        peak: usize,
        working: usize,
        paged_peak: usize,
        paged: usize,
        nonpaged_peak: usize,
        nonpaged: usize,
        pagefile: usize,
        pagefile_peak: usize,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn DuplicateHandle(
            source: *mut c_void,
            handle: *mut c_void,
            target: *mut c_void,
            duplicated: *mut *mut c_void,
            access: u32,
            inherit: i32,
            options: u32,
        ) -> i32;
        fn GetProcessTimes(
            handle: *mut c_void,
            created: *mut FileTime,
            exited: *mut FileTime,
            kernel: *mut FileTime,
            user: *mut FileTime,
        ) -> i32;
        fn GetProcessIoCounters(handle: *mut c_void, counters: *mut Io) -> i32;
    }
    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(handle: *mut c_void, counters: *mut Memory, size: u32) -> i32;
    }
    pub fn duplicate(child: &Child) -> Option<OwnedHandle> {
        let handle = child.raw_handle()?;
        let mut duplicated = std::ptr::null_mut();
        // SAFETY: Tokio owns a valid process handle until child.wait(); duplicate
        // it now so our independent RAII handle remains valid after reaping.
        unsafe {
            let current = GetCurrentProcess();
            if DuplicateHandle(current, handle, current, &mut duplicated, 0, 0, 2) == 0 {
                return None;
            }
            Some(OwnedHandle::from_raw_handle(duplicated))
        }
    }
    pub fn peak(handle: &OwnedHandle) -> Option<usize> {
        let size = std::mem::size_of::<Memory>() as u32;
        let mut memory = Memory {
            size,
            ..Default::default()
        };
        // SAFETY: RAII handle and correctly sized repr(C) output live throughout call.
        (unsafe { GetProcessMemoryInfo(handle.as_raw_handle(), &mut memory, size) } != 0)
            .then_some(memory.peak)
    }
    pub fn finish(handle: &OwnedHandle, sampled: Option<usize>) -> Value {
        let mut created = FileTime::default();
        let mut exited = FileTime::default();
        let mut kernel = FileTime::default();
        let mut user = FileTime::default();
        let mut io = Io::default();
        // SAFETY: all output pointers refer to initialized repr(C) structures.
        let (time_ok, io_ok) = unsafe {
            (
                GetProcessTimes(
                    handle.as_raw_handle(),
                    &mut created,
                    &mut exited,
                    &mut kernel,
                    &mut user,
                ) != 0,
                GetProcessIoCounters(handle.as_raw_handle(), &mut io) != 0,
            )
        };
        let ticks = |t: FileTime| (u64::from(t.high) << 32) | u64::from(t.low);
        let peak = peak(handle).or(sampled);
        json!({"status":if time_ok && io_ok && peak.is_some(){"MEASURED"}else{"INCOMPLETE"},
            "scope":"direct_process_only", "descendants_included":false,
            "cpu_100ns":time_ok.then(||ticks(kernel)+ticks(user)),
            "read_operations":io_ok.then_some(io.read_ops), "write_operations":io_ok.then_some(io.write_ops),
            "read_bytes":io_ok.then_some(io.read_bytes), "write_bytes":io_ok.then_some(io.write_bytes),
            "peak_working_set_bytes_observed":peak,"memory_sampling_interval_ms":20,
            "io_semantics":"Windows process IO; includes pipes, not disk-only"})
    }
}
