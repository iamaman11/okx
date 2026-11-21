use serde_json::{Value, json};

/// Optional evidence for a separately invoked Research workload. This must be
/// created after build and before spawning any workload processes. It never
/// changes or contributes to the accepted financial resource budget.
pub struct ResearchProcessTree {
    #[cfg(windows)]
    job: Result<windows_job::Measurement, &'static str>,
    #[cfg(target_os = "linux")]
    procfs: linux_proc::Measurement,
}
impl ResearchProcessTree {
    pub fn begin() -> Self {
        Self {
            #[cfg(windows)]
            job: windows_job::Measurement::begin(),
            #[cfg(target_os = "linux")]
            procfs: linux_proc::Measurement::begin(),
        }
    }

    pub fn finish(self) -> Value {
        #[cfg(windows)]
        {
            match self.job {
                Ok(job) => job.finish(),
                Err(reason) => research_not_proven(reason),
            }
        }
        #[cfg(target_os = "linux")]
        {
            self.procfs.finish()
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            research_not_proven("PROCESS_TREE_OBSERVATION_UNSUPPORTED")
        }
    }
}
fn research_not_proven(reason: &'static str) -> Value {
    json!({"schema":"okx.research-process-tree-resources/v1", "status":"NOT_PROVEN", "reason":reason,
        "budget_status":"NOT_PROVEN", "descendants_included":false})
}

#[cfg(target_os = "linux")]
mod linux_proc {
    use super::*;
    use std::{
        collections::{HashMap, HashSet},
        fs,
        sync::mpsc::{self, Sender},
        thread::{self, JoinHandle},
        time::Duration,
    };

    // Snapshot the collector and all current descendants by walking the Linux
    // kernel's per-thread `children` lists. Child processes spawned by a Tokio
    // thread do not necessarily appear in /proc/<tgid>/task/<tgid>/children.
    // This is a sampled, summed RSS witness: shared pages can be counted more
    // than once, and spikes between samples can be missed. No hard limit here.
    fn descendants(root: u32) -> HashSet<u32> {
        let mut seen = HashSet::from([root]);
        let mut pending = vec![root];
        while let Some(pid) = pending.pop() {
            let Ok(tasks) = fs::read_dir(format!("/proc/{pid}/task")) else {
                continue;
            };
            for task in tasks.flatten() {
                let children = task.path().join("children");
                let Ok(contents) = fs::read_to_string(children) else {
                    continue;
                };
                for child in contents
                    .split_whitespace()
                    .filter_map(|s| s.parse::<u32>().ok())
                {
                    if seen.insert(child) {
                        pending.push(child);
                    }
                }
            }
        }
        seen
    }

    fn rss_bytes(pid: u32) -> Option<u64> {
        let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
        let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
        let mut fields = line["VmRSS:".len()..].split_whitespace();
        let kib: u64 = fields.next()?.parse().ok()?;
        (fields.next() == Some("kB") && fields.next().is_none())
            .then(|| kib.checked_mul(1024))
            .flatten()
    }

    #[derive(Clone, Copy, Default)]
    struct IoCounters {
        read_char_bytes: u64,
        read_syscalls: u64,
        storage_read_bytes: u64,
    }
    impl IoCounters {
        fn read(pid: u32) -> Option<Self> {
            let io = fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
            let field = |key: &str| -> Option<u64> {
                io.lines()
                    .find_map(|line| line.strip_prefix(key))?
                    .trim()
                    .parse()
                    .ok()
            };
            Some(Self {
                read_char_bytes: field("rchar:")?,
                read_syscalls: field("syscr:")?,
                storage_read_bytes: field("read_bytes:")?,
            })
        }
        fn max_with(&mut self, other: Self) {
            self.read_char_bytes = self.read_char_bytes.max(other.read_char_bytes);
            self.read_syscalls = self.read_syscalls.max(other.read_syscalls);
            self.storage_read_bytes = self.storage_read_bytes.max(other.storage_read_bytes);
        }
        fn add_since(&mut self, observed: Self, baseline: Self) {
            self.read_char_bytes = self
                .read_char_bytes
                .saturating_add(observed.read_char_bytes.saturating_sub(baseline.read_char_bytes));
            self.read_syscalls = self
                .read_syscalls
                .saturating_add(observed.read_syscalls.saturating_sub(baseline.read_syscalls));
            self.storage_read_bytes = self
                .storage_read_bytes
                .saturating_add(observed.storage_read_bytes.saturating_sub(baseline.storage_read_bytes));
        }
    }

    #[derive(Default)]
    struct Samples {
        count: u64,
        max_active_processes: usize,
        max_active_descendants: usize,
        peak_sampled_sum_rss_bytes: u64,
        peak_sampled_descendant_rss_bytes: u64,
        peak_sampled_collector_rss_bytes: u64,
        descendant_pids_seen: HashSet<u32>,
        missing_rss_observations: u64,
        collector_rss_observed: bool,
        io_by_pid: HashMap<u32, IoCounters>,
        missing_io_observations: u64,
    }
    impl Samples {
        fn observe(&mut self, root: u32) {
            let processes = descendants(root);
            self.count += 1;
            self.max_active_processes = self.max_active_processes.max(processes.len());
            self.max_active_descendants = self.max_active_descendants.max(processes.len() - 1);
            let mut descendant_rss = 0u64;
            let mut collector_rss = 0u64;
            for pid in processes {
                if pid != root {
                    self.descendant_pids_seen.insert(pid);
                }
                match rss_bytes(pid) {
                    Some(bytes) if pid == root => {
                        self.collector_rss_observed = true;
                        collector_rss = bytes;
                    }
                    Some(bytes) => descendant_rss = descendant_rss.saturating_add(bytes),
                    None => self.missing_rss_observations += 1,
                }
                if let Some(io) = IoCounters::read(pid) {
                    self.io_by_pid
                        .entry(pid)
                        .and_modify(|old| old.max_with(io))
                        .or_insert(io);
                } else {
                    self.missing_io_observations += 1;
                }
            }
            self.peak_sampled_collector_rss_bytes =
                self.peak_sampled_collector_rss_bytes.max(collector_rss);
            self.peak_sampled_descendant_rss_bytes =
                self.peak_sampled_descendant_rss_bytes.max(descendant_rss);
            self.peak_sampled_sum_rss_bytes = self
                .peak_sampled_sum_rss_bytes
                .max(collector_rss.saturating_add(descendant_rss));
        }
    }

    pub(super) struct Measurement {
        pid: u32,
        initial_io: Option<IoCounters>,
        stop: Sender<()>,
        sampler: JoinHandle<Samples>,
    }
    impl Measurement {
        pub(super) fn begin() -> Self {
            let pid = std::process::id();
            let initial_io = IoCounters::read(pid);
            let (stop, receiver) = mpsc::channel();
            let sampler = thread::spawn(move || {
                let mut samples = Samples::default();
                loop {
                    samples.observe(pid);
                    if receiver.recv_timeout(Duration::from_millis(20)).is_ok() {
                        break;
                    }
                }
                samples
            });
            Self { pid, initial_io, stop, sampler }
        }
        pub(super) fn finish(self) -> Value {
            let _ = self.stop.send(());
            let Ok(samples) = self.sampler.join() else {
                return research_not_proven("PROCFS_SAMPLER_PANICKED");
            };
            if !samples.collector_rss_observed || samples.count == 0 {
                return research_not_proven("PROCFS_RSS_NOT_OBSERVED");
            }
            let mut io_total = IoCounters::default();
            for (pid, observed) in &samples.io_by_pid {
                let baseline = if *pid == self.pid {
                    self.initial_io.unwrap_or_default()
                } else {
                    IoCounters::default()
                };
                io_total.add_since(*observed, baseline);
            }
            let io_measured = self.initial_io.is_some() && samples.io_by_pid.contains_key(&self.pid);
            json!({
                "schema":"okx.research-process-tree-resources/v1",
                "status":"MEASURED_SAMPLED",
                "budget_status":"NOT_PROVEN",
                "scope":"collector_and_current_descendants",
                "descendants_included":true,
                "collector_pid":self.pid,
                "sample_interval_ms":20,
                "samples":samples.count,
                "max_active_processes":samples.max_active_processes,
                "max_active_descendants":samples.max_active_descendants,
                "distinct_descendant_pids_seen":samples.descendant_pids_seen.len(),
                "peak_sampled_sum_rss_bytes":samples.peak_sampled_sum_rss_bytes,
                "peak_sampled_collector_rss_bytes":samples.peak_sampled_collector_rss_bytes,
                "peak_sampled_descendant_rss_bytes":samples.peak_sampled_descendant_rss_bytes,
                "missing_rss_observations":samples.missing_rss_observations,
                "observed_process_io": if io_measured { Some(json!({
                    "read_char_bytes":io_total.read_char_bytes,
                    "read_syscalls":io_total.read_syscalls,
                    "storage_read_bytes":io_total.storage_read_bytes,
                    "sampled_processes":samples.io_by_pid.len(),
                    "missing_io_observations":samples.missing_io_observations,
                    "source_attribution":"not_attributed",
                    "measurement_semantics":"per-seen-PID maximum Linux procfs cumulative I/O counters; collector baseline subtracted, short-lived children and between-sample I/O can be missed; rchar includes pipes and cached reads, read_bytes is storage-backed"
                })) } else { None },
                "memory_semantics":"20ms-sampled sum of current per-process VmRSS (shared pages may double-count; between-sample peaks may be missed)",
                "process_count_semantics":"active descendants sampled via every task/children file; distinct PIDs not durable process-creation accounting",
                "rss_hard_limit_enforced":false,
                "source":"linux_procfs_task_children_and_status",
            })
        }
    }
}

#[cfg(windows)]
mod windows_job {
    use super::*;
    use std::{
        ffi::c_void,
        mem::size_of,
        os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
        ptr,
    };

    #[repr(C)]
    #[derive(Default)]
    struct BasicAccounting {
        total_user_time: i64,
        total_kernel_time: i64,
        period_user_time: i64,
        period_kernel_time: i64,
        page_faults: u32,
        total_processes: u32,
        active_processes: u32,
        total_terminated_processes: u32,
    }
    #[repr(C)]
    #[derive(Default)]
    struct IoCounters {
        read_ops: u64,
        write_ops: u64,
        other_ops: u64,
        read_bytes: u64,
        write_bytes: u64,
        other_bytes: u64,
    }
    #[repr(C)]
    #[derive(Default)]
    struct Accounting {
        basic: BasicAccounting,
        io: IoCounters,
    }
    #[repr(C)]
    #[derive(Default)]
    struct BasicLimits {
        per_process_time: i64,
        per_job_time: i64,
        flags: u32,
        min_working_set: usize,
        max_working_set: usize,
        active_process_limit: u32,
        affinity: usize,
        priority_class: u32,
        scheduling_class: u32,
    }
    #[repr(C)]
    #[derive(Default)]
    struct ExtendedLimits {
        basic: BasicLimits,
        io: IoCounters,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory: usize,
        peak_job_memory: usize,
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateJobObjectW(attributes: *mut c_void, name: *const u16) -> *mut c_void;
        fn AssignProcessToJobObject(job: *mut c_void, process: *mut c_void) -> i32;
        fn QueryInformationJobObject(
            job: *mut c_void,
            class: i32,
            information: *mut c_void,
            length: u32,
            returned: *mut u32,
        ) -> i32;
        fn GetCurrentProcess() -> *mut c_void;
        fn GetCurrentProcessId() -> u32;
    }
    struct Snapshot {
        accounting: Accounting,
        peak: usize,
    }
    pub(super) struct Measurement {
        job: OwnedHandle,
        initial: Snapshot,
        pid: u32,
    }
    impl Measurement {
        pub(super) fn begin() -> Result<Self, &'static str> {
            // No breakaway flags, limits or kill-on-close are set. The new job
            // contains only this process and children subsequently created by it.
            let raw = unsafe { CreateJobObjectW(ptr::null_mut(), ptr::null()) };
            if raw.is_null() {
                return Err("JOB_CREATE_FAILED");
            }
            // SAFETY: CreateJobObjectW returned an owned, valid kernel handle.
            let job = unsafe { OwnedHandle::from_raw_handle(raw) };
            // SAFETY: handles are valid; this does not assign unrelated processes.
            if unsafe { AssignProcessToJobObject(job.as_raw_handle(), GetCurrentProcess()) } == 0 {
                return Err("JOB_ASSIGN_FAILED_OR_NESTED_JOB_RESTRICTED");
            }
            let initial = snapshot(&job)?;
            if initial.accounting.basic.active_processes != 1
                || initial.accounting.basic.total_processes != 1
            {
                return Err("JOB_INITIAL_MEMBERSHIP_INVALID");
            }
            Ok(Self {
                job,
                initial,
                pid: unsafe { GetCurrentProcessId() },
            })
        }
        pub(super) fn finish(self) -> Value {
            let result = (|| -> Result<Value, &'static str> {
                let end = snapshot(&self.job)?;
                let start = &self.initial.accounting;
                let final_accounting = &end.accounting;
                let delta_i64 = |end: i64, start: i64| -> Result<u64, &'static str> {
                    u64::try_from(end.checked_sub(start).ok_or("JOB_COUNTER_INVALID")?)
                        .map_err(|_| "JOB_COUNTER_INVALID")
                };
                let delta =
                    |end: u64, start: u64| end.checked_sub(start).ok_or("JOB_COUNTER_INVALID");
                let cpu = delta_i64(
                    final_accounting.basic.total_kernel_time,
                    start.basic.total_kernel_time,
                )?
                .checked_add(delta_i64(
                    final_accounting.basic.total_user_time,
                    start.basic.total_user_time,
                )?)
                .ok_or("JOB_COUNTER_INVALID")?;
                let total = final_accounting.basic.total_processes;
                let active = final_accounting.basic.active_processes;
                if total < 1 || active < 1 || active > total || end.peak == 0 {
                    return Err("JOB_METRICS_INCOMPLETE");
                }
                Ok(
                    json!({"schema":"okx.research-process-tree-resources/v1", "status":"MEASURED",
                    "budget_status":"NOT_PROVEN", "scope":"collector_and_descendants", "descendants_included":true,
                    "collector_pid":self.pid, "total_processes":total, "active_processes":active,
                    "descendants_started":total-1, "active_descendants":active-1,
                    "no_longer_active_processes":total-active,
                    "limit_terminated_processes":final_accounting.basic.total_terminated_processes,
                    "process_count_semantics":"TotalProcesses minus ActiveProcesses; not a child reaping proof",
                    "cpu_100ns":cpu, "read_operations":delta(final_accounting.io.read_ops,start.io.read_ops)?,
                    "write_operations":delta(final_accounting.io.write_ops,start.io.write_ops)?,
                    "read_bytes":delta(final_accounting.io.read_bytes,start.io.read_bytes)?,
                    "write_bytes":delta(final_accounting.io.write_bytes,start.io.write_bytes)?,
                    "peak_job_memory_bytes":end.peak,
                    "memory_semantics":"Windows PeakJobMemoryUsed; aggregate peak committed memory, not working set",
                    "io_semantics":"Windows job cumulative IO including terminated descendants and pipes, not disk-only"}),
                )
            })();
            result.unwrap_or_else(research_not_proven)
        }
    }
    fn snapshot(job: &OwnedHandle) -> Result<Snapshot, &'static str> {
        let mut accounting = Accounting::default();
        let mut limits = ExtendedLimits::default();
        // SAFETY: both outputs have Windows-compatible C layouts and exact sizes.
        let accounting_ok = unsafe {
            QueryInformationJobObject(
                job.as_raw_handle(),
                8,
                (&mut accounting as *mut Accounting).cast(),
                size_of::<Accounting>() as u32,
                ptr::null_mut(),
            )
        } != 0;
        let limits_ok = unsafe {
            QueryInformationJobObject(
                job.as_raw_handle(),
                9,
                (&mut limits as *mut ExtendedLimits).cast(),
                size_of::<ExtendedLimits>() as u32,
                ptr::null_mut(),
            )
        } != 0;
        if !accounting_ok || !limits_ok {
            return Err("JOB_QUERY_FAILED");
        }
        Ok(Snapshot {
            accounting,
            peak: limits.peak_job_memory,
        })
    }
}

#[cfg(test)]
mod research_tree_tests {
    use super::*;
    #[test]
    #[cfg(not(any(windows, target_os = "linux")))]
    fn unsupported_platform_never_claims_tree_measurement() {
        let value = ResearchProcessTree::begin().finish();
        assert_eq!(value["status"], "NOT_PROVEN");
        assert_eq!(value["descendants_included"], false);
    }
    #[test]
    #[cfg(target_os = "linux")]
    fn sampler_counts_live_descendants_and_never_claims_a_hard_rss_limit() {
        let observation = ResearchProcessTree::begin();
        let mut child = std::process::Command::new("sleep")
            .arg("0.2")
            .spawn()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(120));
        child.wait().unwrap();
        let value = observation.finish();
        assert_eq!(value["status"], "MEASURED_SAMPLED");
        assert!(value["max_active_descendants"].as_u64().unwrap() >= 1);
        assert!(value["peak_sampled_sum_rss_bytes"].as_u64().unwrap() > 0);
        assert_eq!(value["budget_status"], "NOT_PROVEN");
        assert_eq!(value["rss_hard_limit_enforced"], false);
    }
    #[test]
    #[cfg(windows)]
    #[ignore = "executed by the process-tree parent contract"]
    fn measured_cpu_child() {
        #[repr(C)]
        #[derive(Clone, Copy, Default)]
        struct FileTime {
            low: u32,
            high: u32,
        }
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetCurrentProcess() -> *mut std::ffi::c_void;
            fn GetProcessTimes(
                process: *mut std::ffi::c_void,
                creation: *mut FileTime,
                exit: *mut FileTime,
                kernel: *mut FileTime,
                user: *mut FileTime,
            ) -> i32;
        }
        let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let [mut creation, mut exit, mut kernel, mut user] = [FileTime::default(); 4];
            let ok = unsafe {
                GetProcessTimes(
                    GetCurrentProcess(),
                    &mut creation,
                    &mut exit,
                    &mut kernel,
                    &mut user,
                )
            };
            assert_ne!(ok, 0);
            let ticks = |t: FileTime| ((t.high as u64) << 32) | t.low as u64;
            if ticks(kernel) + ticks(user) >= 200_000 {
                break;
            }
            for value in 0..4096u64 {
                std::hint::black_box(value.wrapping_mul(31));
            }
            assert!(
                std::time::Instant::now() < until,
                "child CPU accounting unavailable"
            );
        }
    }

    #[test]
    #[cfg(windows)]
    fn windows_job_accounts_for_a_terminated_child_without_proving_a_budget() {
        let measurement = ResearchProcessTree::begin();
        // A trivial `cmd /C ver` may finish below the Windows accounting tick.
        // Exercise a Rust child with independently observed nonzero CPU instead.
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", "accounts::native_trade_contracts::native_engine_research::process_tree::research_tree_tests::measured_cpu_child"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("test result: ok. 1 passed;"),
            "CPU helper must actually execute: {output:?}"
        );
        let value = measurement.finish();
        if value["status"] == "NOT_PROVEN" {
            assert_eq!(value["descendants_included"], false);
            return;
        }
        assert_eq!(value["status"], "MEASURED");
        assert_eq!(value["budget_status"], "NOT_PROVEN");
        assert_eq!(value["descendants_included"], true);
        assert_eq!(value["collector_pid"], std::process::id());
        assert!(value["descendants_started"].as_u64().unwrap() >= 1);
        assert!(value["no_longer_active_processes"].as_u64().unwrap() >= 1);
        assert!(value["limit_terminated_processes"].is_u64());
        assert!(value["cpu_100ns"].as_u64().unwrap() > 0);
        assert!(value["read_bytes"].is_u64());
        assert!(value["write_bytes"].is_u64());
        assert!(value["peak_job_memory_bytes"].as_u64().unwrap() > 0);
    }
}
