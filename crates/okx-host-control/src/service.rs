#[cfg(windows)]
mod imp {
    use std::{
        ffi::{OsString},
        fs,
        path::PathBuf,
        sync::OnceLock,
        time::Duration,
    };

    use okx_github::GitHubClient;
    use tokio::sync::watch;
    use windows_service::{
        define_windows_service,
        service::{
            ServiceAccess, ServiceAction, ServiceActionType, ServiceControl,
            ServiceControlAccept, ServiceErrorControl, ServiceExitCode,
            ServiceFailureActions, ServiceFailureResetPeriod, ServiceInfo,
            ServiceStartType, ServiceState, ServiceStatus, ServiceType,
        },
        service_control_handler::{self, ServiceControlHandlerResult},
        service_dispatcher,
        service_manager::{ServiceManager, ServiceManagerAccess},
    };

    use crate::{
        HostControlError, HostControlResult,
        auth::load_machine_github_token,
        executor::HostExecutor,
        runtime::run_with_shutdown,
    };

    pub const SERVICE_NAME: &str = "okx-host-control";
    const SERVICE_DISPLAY_NAME: &str = "OKX Host Control";
    const SERVICE_BINARY: &str = r"C:\okx-control\okx-host-control.exe";
    const SERVICE_ERROR_LOG: &str =
        r"C:\ProgramData\iamaman11\okx\logs\host-control-service-error.log";

    static SERVICE_SHUTDOWN: OnceLock<watch::Sender<bool>> = OnceLock::new();

    define_windows_service!(ffi_service_main, service_main);

    pub fn install_service() -> HostControlResult<()> {
        let manager_access =
            ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE;
        let service_manager =
            ServiceManager::local_computer(None::<&str>, manager_access)
                .map_err(service_error)?;

        let service_info = ServiceInfo {
            name: OsString::from(SERVICE_NAME),
            display_name: OsString::from(SERVICE_DISPLAY_NAME),
            service_type: ServiceType::OWN_PROCESS,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: PathBuf::from(SERVICE_BINARY),
            launch_arguments: vec![OsString::from("service")],
            dependencies: vec![],
            account_name: None,
            account_password: None,
        };

        let access = ServiceAccess::QUERY_STATUS
            | ServiceAccess::CHANGE_CONFIG
            | ServiceAccess::START
            | ServiceAccess::STOP;

        let service = match service_manager.create_service(&service_info, access) {
            Ok(service) => service,
            Err(_) => service_manager
                .open_service(SERVICE_NAME, access)
                .map_err(service_error)?,
        };

        service
            .set_description(
                "Outbound-only typed Windows control plane for iamaman11/okx",
            )
            .map_err(service_error)?;
        service
            .set_delayed_auto_start(false)
            .map_err(service_error)?;

        let failure_actions = ServiceFailureActions {
            reset_period: ServiceFailureResetPeriod::After(Duration::from_secs(24 * 60 * 60)),
            reboot_msg: None,
            command: None,
            actions: Some(vec![
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(5),
                },
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(15),
                },
                ServiceAction {
                    action_type: ServiceActionType::Restart,
                    delay: Duration::from_secs(60),
                },
            ]),
        };
        service
            .update_failure_actions(failure_actions)
            .map_err(service_error)?;
        service
            .set_failure_actions_on_non_crash_failures(true)
            .map_err(service_error)?;

        Ok(())
    }

    pub fn start_service() -> HostControlResult<()> {
        let service_manager = ServiceManager::local_computer(
            None::<&str>,
            ServiceManagerAccess::CONNECT,
        )
        .map_err(service_error)?;
        let service = service_manager
            .open_service(
                SERVICE_NAME,
                ServiceAccess::START | ServiceAccess::QUERY_STATUS,
            )
            .map_err(service_error)?;
        service.start::<&str>(&[]).map_err(service_error)?;
        Ok(())
    }

    pub fn run_service_dispatcher() -> HostControlResult<()> {
        service_dispatcher::start(SERVICE_NAME, ffi_service_main)
            .map_err(service_error)
    }

    fn service_main(_arguments: Vec<OsString>) {
        if let Err(error) = run_service() {
            write_service_error(&error.to_string());
        }
    }

    fn run_service() -> HostControlResult<()> {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let _ = SERVICE_SHUTDOWN.set(shutdown_tx.clone());

        let event_handler = move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    let _ = shutdown_tx.send(true);
                    ServiceControlHandlerResult::NoError
                }
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        };

        let status_handle =
            service_control_handler::register(SERVICE_NAME, event_handler)
                .map_err(service_error)?;

        status_handle
            .set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: ServiceState::StartPending,
                controls_accepted: ServiceControlAccept::empty(),
                exit_code: ServiceExitCode::Win32(0),
                checkpoint: 1,
                wait_hint: Duration::from_secs(15),
                process_id: None,
            })
            .map_err(service_error)?;

        let token = load_machine_github_token()?;
        let github = GitHubClient::new(token, "iamaman11-okx-host-control-service/0.1")?;
        let mut executor = HostExecutor::canonical();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        status_handle
            .set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: ServiceState::Running,
                controls_accepted: ServiceControlAccept::STOP
                    | ServiceControlAccept::SHUTDOWN,
                exit_code: ServiceExitCode::Win32(0),
                checkpoint: 0,
                wait_hint: Duration::default(),
                process_id: None,
            })
            .map_err(service_error)?;

        let result = runtime.block_on(run_with_shutdown(
            &github,
            &mut executor,
            2,
            async move {
                loop {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            },
        ));

        status_handle
            .set_service_status(ServiceStatus {
                service_type: ServiceType::OWN_PROCESS,
                current_state: ServiceState::Stopped,
                controls_accepted: ServiceControlAccept::empty(),
                exit_code: if result.is_ok() {
                    ServiceExitCode::Win32(0)
                } else {
                    ServiceExitCode::Win32(1)
                },
                checkpoint: 0,
                wait_hint: Duration::default(),
                process_id: None,
            })
            .map_err(service_error)?;

        result
    }

    fn write_service_error(message: &str) {
        let path = PathBuf::from(SERVICE_ERROR_LOG);
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::write(path, message.as_bytes());
    }

    fn service_error(error: windows_service::Error) -> HostControlError {
        HostControlError::WindowsService(error.to_string())
    }
}

#[cfg(windows)]
pub use imp::{install_service, run_service_dispatcher, start_service};

#[cfg(not(windows))]
pub fn install_service() -> crate::HostControlResult<()> {
    Err(crate::HostControlError::UnsupportedPlatform)
}

#[cfg(not(windows))]
pub fn start_service() -> crate::HostControlResult<()> {
    Err(crate::HostControlError::UnsupportedPlatform)
}

#[cfg(not(windows))]
pub fn run_service_dispatcher() -> crate::HostControlResult<()> {
    Err(crate::HostControlError::UnsupportedPlatform)
}
