use std::path::PathBuf;

use clap::Parser;
use miette::IntoDiagnostic;
use pixi_config::{Config, ConfigCli};
use pixi_core::{
    UpdateLockFileOptions, WorkspaceLocator,
    environment::get_update_lock_file_and_prefix,
    lock_file::{ReinstallPackages, UpdateMode},
};

use crate::cli_config::{LockAndInstallConfig, WorkspaceConfig};

/// Mount a pixi environment as a virtual filesystem.
///
/// In interactive mode (default), the environment is mounted and the command
/// blocks until Ctrl+C is pressed. In managed mode (--managed), the process
/// daemonizes and waits for SIGTERM — this is used internally by `pixi run`
/// and `pixi shell` when `experimental.environment-backend = "mount"`.
#[derive(Parser, Debug)]
pub struct Args {
    #[clap(flatten)]
    workspace_config: WorkspaceConfig,

    #[clap(flatten)]
    lock_and_install_config: LockAndInstallConfig,

    #[clap(flatten)]
    config: ConfigCli,

    /// The environment to mount.
    #[arg(long, short)]
    environment: Option<String>,

    /// Mount point override. Defaults to the environment directory.
    #[arg(long)]
    mount_point: Option<PathBuf>,

    // --- Hidden sidecar flags (used by pixi run/shell internally) ---
    /// Run as a managed sidecar process.
    #[arg(long, hide = true)]
    managed: bool,

    /// Path to the pidfile (used with --managed).
    #[arg(long, hide = true)]
    pidfile: Option<PathBuf>,

    /// File descriptor number for the readiness pipe (used with --managed).
    #[arg(long, hide = true)]
    ready_fd: Option<i32>,
}

pub async fn execute(args: Args) -> miette::Result<()> {
    let config = Config::from(args.config.clone());
    let workspace = WorkspaceLocator::for_cli()
        .with_search_start(args.workspace_config.workspace_locator_start())
        .locate()?
        .with_cli_config(config);

    let environment = workspace.environment_from_name_or_env_var(args.environment)?;

    // Ensure lock file is up-to-date and packages are cached.
    let (_lock_file_data, _prefix) = get_update_lock_file_and_prefix(
        &environment,
        UpdateMode::Revalidate,
        UpdateLockFileOptions {
            lock_file_usage: args.lock_and_install_config.lock_file_usage()?,
            no_install: args.lock_and_install_config.no_install(),
            max_concurrent_solves: workspace.config().max_concurrent_solves(),
        },
        ReinstallPackages::default(),
        &pixi_core::environment::InstallFilter::default(),
    )
    .await?;

    let env_dir = environment.dir();
    let mount_point = args.mount_point.unwrap_or_else(|| env_dir.clone());

    if args.managed {
        execute_managed(&mount_point, args.pidfile.as_deref(), args.ready_fd).await
    } else {
        execute_interactive(&mount_point).await
    }
}

async fn execute_interactive(mount_point: &std::path::Path) -> miette::Result<()> {
    // TODO: Phase 4/5 — call rattler_fs::build_and_mount() here
    eprintln!(
        "Would mount environment at {}. (not yet implemented)",
        mount_point.display()
    );
    eprintln!("Press Ctrl+C to exit.");

    tokio::signal::ctrl_c().await.into_diagnostic()?;
    Ok(())
}

async fn execute_managed(
    mount_point: &std::path::Path,
    _pidfile: Option<&std::path::Path>,
    _ready_fd: Option<i32>,
) -> miette::Result<()> {
    // TODO: Phase 4 — daemonize, mount, write pidfile, signal readiness, wait for SIGTERM
    eprintln!(
        "Would start managed mount sidecar at {}. (not yet implemented)",
        mount_point.display()
    );
    Ok(())
}
