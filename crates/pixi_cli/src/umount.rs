use std::path::PathBuf;

use clap::Parser;
use pixi_config::{Config, ConfigCli};
use pixi_core::WorkspaceLocator;
use pixi_core::environment::mount_sidecar;

use crate::cli_config::WorkspaceConfig;

/// Unmount a pixi environment virtual filesystem.
///
/// Kills the mount sidecar process and unmounts the environment directory.
#[derive(Parser, Debug)]
pub struct Args {
    #[clap(flatten)]
    workspace_config: WorkspaceConfig,

    #[clap(flatten)]
    config: ConfigCli,

    /// The environment to unmount.
    #[arg(long, short)]
    environment: Option<String>,

    /// Mount point override. Defaults to the environment directory.
    #[arg(long)]
    mount_point: Option<PathBuf>,
}

pub async fn execute(args: Args) -> miette::Result<()> {
    let config = Config::from(args.config);
    let workspace = WorkspaceLocator::for_cli()
        .with_search_start(args.workspace_config.workspace_locator_start())
        .locate()?
        .with_cli_config(config);

    let environment = workspace.environment_from_name_or_env_var(args.environment)?;
    let env_dir = environment.dir();
    let mount_point = args.mount_point.unwrap_or_else(|| env_dir.clone());

    if !mount_sidecar::is_mounted(&mount_point) {
        eprintln!("Not mounted: {}", mount_point.display());
        return Ok(());
    }

    let (_lock_path, pid_path) = mount_sidecar::coordination_paths(&mount_point);

    // Kill the sidecar process if alive
    #[cfg(unix)]
    if mount_sidecar::is_sidecar_alive(&pid_path) {
        if let Ok(pid_str) = std::fs::read_to_string(&pid_path) {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                unsafe {
                    libc::kill(pid, libc::SIGTERM);
                }
                // Give the sidecar a moment to unmount cleanly
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }

    #[cfg(windows)]
    if mount_sidecar::is_sidecar_alive(&pid_path) {
        // Send Ctrl+C to the sidecar's console group for graceful shutdown,
        // then fall through to force_unmount if it doesn't exit in time.
        if let Ok(pid_str) = std::fs::read_to_string(&pid_path) {
            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                // TerminateProcess for immediate stop — ProjFS cleanup
                // happens automatically when the process exits.
                use windows_sys::Win32::Foundation::CloseHandle;
                use windows_sys::Win32::System::Threading::{
                    OpenProcess, TerminateProcess, PROCESS_TERMINATE,
                };
                unsafe {
                    let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
                    if !handle.is_null() {
                        TerminateProcess(handle, 0);
                        CloseHandle(handle);
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }

    // Force unmount if still mounted
    if mount_sidecar::is_mounted(&mount_point) {
        mount_sidecar::force_unmount(&mount_point)?;
    }

    // Clean up coordination files
    let _ = std::fs::remove_file(&pid_path);

    eprintln!("Unmounted {}", mount_point.display());
    Ok(())
}
