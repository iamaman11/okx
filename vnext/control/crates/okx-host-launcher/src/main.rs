#![cfg_attr(windows, windows_subsystem = "windows")]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _program = args.next();
    match (args.next().as_deref(), args.next()) {
        (Some("run"), None) => {
            let result = okx_host_launcher::run()?;
            #[cfg(not(windows))]
            println!("{}", serde_json::to_string(&result)?);
            #[cfg(windows)]
            let _ = result;
            Ok(())
        }
        _ => Err("usage: okx-host-launcher run".into()),
    }
}
