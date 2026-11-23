//! Process-scoped memory witnesses. These are measurements, never permit
//! enforcement or an attribution of shared-process RSS to a single research job.
//! Unknown measurements remain null: they cannot silently pass a memory SLO.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProcessMemory {
    pub(super) resident_bytes: Option<u64>,
    pub(super) peak_resident_bytes: Option<u64>,
    pub(super) physical_memory_bytes: Option<u64>,
    pub(super) source: &'static str,
}

#[cfg(target_os = "linux")]
fn parse_kib_field(input: &str, name: &str) -> Option<u64> {
    let value = input.lines().find_map(|line| line.strip_prefix(name))?;
    let mut tokens = value.split_whitespace();
    let kib = tokens.next()?.parse::<u64>().ok()?;
    if tokens.next()? != "kB" || tokens.next().is_some() {
        return None;
    }
    kib.checked_mul(1024)
}

#[cfg(target_os = "linux")]
pub(super) fn observe() -> ProcessMemory {
    let status = std::fs::read_to_string("/proc/self/status").ok();
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok();
    ProcessMemory {
        resident_bytes: status.as_deref().and_then(|s| parse_kib_field(s, "VmRSS:")),
        peak_resident_bytes: status.as_deref().and_then(|s| parse_kib_field(s, "VmHWM:")),
        physical_memory_bytes: meminfo
            .as_deref()
            .and_then(|s| parse_kib_field(s, "MemTotal:")),
        source: "linux_procfs_process_rss_and_hwm",
    }
}

#[cfg(windows)]
pub(super) fn observe() -> ProcessMemory {
    #[repr(C)]
    struct Counters {
        cb: u32,
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
    #[repr(C)]
    struct Status {
        length: u32,
        load: u32,
        total_phys: u64,
        avail_phys: u64,
        total_pagefile: u64,
        avail_pagefile: u64,
        total_virtual: u64,
        avail_virtual: u64,
        avail_extended: u64,
    }
    #[link(name = "psapi")]
    unsafe extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut core::ffi::c_void,
            counters: *mut Counters,
            size: u32,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetCurrentProcess() -> *mut core::ffi::c_void;
        fn GlobalMemoryStatusEx(status: *mut Status) -> i32;
    }
    let mut counters = Counters {
        cb: std::mem::size_of::<Counters>() as u32,
        faults: 0,
        peak: 0,
        working: 0,
        paged_peak: 0,
        paged: 0,
        nonpaged_peak: 0,
        nonpaged: 0,
        pagefile: 0,
        pagefile_peak: 0,
    };
    let mut status = Status {
        length: std::mem::size_of::<Status>() as u32,
        load: 0,
        total_phys: 0,
        avail_phys: 0,
        total_pagefile: 0,
        avail_pagefile: 0,
        total_virtual: 0,
        avail_virtual: 0,
        avail_extended: 0,
    };
    let measured = unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<Counters>() as u32,
        ) != 0
    };
    let physical = unsafe { GlobalMemoryStatusEx(&mut status) != 0 };
    ProcessMemory {
        resident_bytes: measured.then_some(counters.working as u64),
        peak_resident_bytes: measured.then_some(counters.peak as u64),
        physical_memory_bytes: physical.then_some(status.total_phys),
        source: "windows_psapi_process_working_set",
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
pub(super) fn observe() -> ProcessMemory {
    ProcessMemory {
        resident_bytes: None,
        peak_resident_bytes: None,
        physical_memory_bytes: None,
        source: "unsupported_platform_not_measured",
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn t10_procfs_kib_units_and_overflow_are_strict() {
        let fixture = "Name:\tokx\nVmRSS:\t 1234 kB\nVmHWM:\t4096 kB\n";
        assert_eq!(parse_kib_field(fixture, "VmRSS:"), Some(1_263_616));
        assert_eq!(parse_kib_field(fixture, "VmHWM:"), Some(4_194_304));
        assert_eq!(parse_kib_field(fixture, "VmPeak:"), None);
        assert_eq!(parse_kib_field("VmRSS: 10 MB", "VmRSS:"), None);
        assert_eq!(parse_kib_field("VmRSS: garbage kB", "VmRSS:"), None);
        assert_eq!(parse_kib_field("VmRSS: 10 kB extra", "VmRSS:"), None);
        assert_eq!(
            parse_kib_field("VmRSS: 18446744073709551615 kB", "VmRSS:"),
            None
        );
        assert_eq!(parse_kib_field("NotVmRSS: 10 kB", "VmRSS:"), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn t10_linux_real_process_rss_is_observed_not_inferred() {
        let memory = observe();
        assert_eq!(memory.source, "linux_procfs_process_rss_and_hwm");
        let rss = memory
            .resident_bytes
            .expect("Linux /proc/self/status VmRSS");
        let peak = memory
            .peak_resident_bytes
            .expect("Linux /proc/self/status VmHWM");
        assert!(rss > 0 && rss <= peak);
        assert!(memory.physical_memory_bytes.unwrap() >= peak);
    }
}
