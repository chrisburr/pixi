use std::path::PathBuf;

use clap::Parser;
use miette::IntoDiagnostic;
use pixi_config::{Config, ConfigCli};
use pixi_core::{
    UpdateLockFileOptions, WorkspaceLocator,
    environment::get_update_lock_file_and_prefix,
    lock_file::{ReinstallPackages, UpdateMode},
};
use rattler::package_cache::PackageCache;
use rattler_conda_types::Platform;
use rattler_lock::DEFAULT_ENVIRONMENT_NAME;

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
    let env_dir = environment.dir();
    let mount_point = args.mount_point.unwrap_or_else(|| env_dir.clone());
    let platform = environment.best_platform();

    let env_name = environment.name().as_str();
    let env_name = if env_name.is_empty() {
        DEFAULT_ENVIRONMENT_NAME
    } else {
        env_name
    };

    // Determine overlay based on read-only config
    let overlay_dir = if workspace.config().mount_read_only() {
        None
    } else {
        Some(pixi_core::environment::mount_sidecar::MountGuard::overlay_dir(&env_dir))
    };

    let transport = match workspace.config().mount_backend() {
        pixi_config::MountBackend::Auto => rattler_fs::Transport::Auto,
        pixi_config::MountBackend::Nfs => rattler_fs::Transport::Nfs,
        pixi_config::MountBackend::Fuse => rattler_fs::Transport::Fuse,
    };

    if args.managed {
        // The managed sidecar is spawned by ensure_mount after the parent has
        // already solved and cached packages. Just load the existing lock file
        // and package cache — no solve needed.
        let lock_file = rattler_lock::LockFile::from_path(&workspace.lock_file_path())
            .into_diagnostic()?;
        let package_cache = PackageCache::new(
            pixi_config::get_cache_dir()?
                .join(pixi_consts::consts::CONDA_PACKAGE_CACHE_DIR),
        );

        let env_hash = rattler_fs::compute_env_hash(&lock_file, env_name, platform)
            .map_err(|e| miette::miette!("failed to compute env hash: {e}"))?;

        let grace_period = workspace.config().mount_grace_period();

        execute_managed(
            &lock_file,
            env_name,
            platform,
            &package_cache,
            &mount_point,
            overlay_dir,
            &env_hash,
            transport,
            grace_period,
            args.pidfile.as_deref(),
            args.ready_fd,
        )
        .await
    } else {
        // Interactive mode: ensure lock file is up-to-date and packages are
        // cached (but skip the hardlink install — the mount replaces it).
        let (lock_file_data, _prefix) = get_update_lock_file_and_prefix(
            &environment,
            UpdateMode::Revalidate,
            UpdateLockFileOptions {
                lock_file_usage: args.lock_and_install_config.lock_file_usage()?,
                no_install: true,
                max_concurrent_solves: workspace.config().max_concurrent_solves(),
            },
            ReinstallPackages::default(),
            &pixi_core::environment::InstallFilter::default(),
        )
        .await?;

        let env_hash =
            rattler_fs::compute_env_hash(&lock_file_data.lock_file, env_name, platform)
                .map_err(|e| miette::miette!("failed to compute env hash: {e}"))?;

        execute_interactive(
            &lock_file_data.lock_file,
            env_name,
            platform,
            &lock_file_data.package_cache,
            &mount_point,
            overlay_dir,
            &env_hash,
            transport,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_interactive(
    lock_file: &rattler_lock::LockFile,
    environment_name: &str,
    platform: Platform,
    package_cache: &PackageCache,
    mount_point: &std::path::Path,
    overlay_dir: Option<PathBuf>,
    env_hash: &str,
    transport: rattler_fs::Transport,
) -> miette::Result<()> {
    std::fs::create_dir_all(mount_point).into_diagnostic()?;

    let config = if let Some(overlay_dir) = overlay_dir {
        rattler_fs::MountConfig::new_writable(
            mount_point.to_path_buf(),
            Some(overlay_dir),
            transport,
            env_hash.to_string(),
        )
    } else {
        rattler_fs::MountConfig::new_read_only(
            mount_point.to_path_buf(),
            transport,
            env_hash.to_string(),
        )
    };

    let _handle =
        rattler_fs::build_and_mount(lock_file, environment_name, platform, package_cache, &config)
            .await
            .map_err(|e| miette::miette!("failed to mount: {e}"))?;

    eprintln!(
        "Mounted at {}. Press Ctrl+C to unmount.",
        mount_point.display()
    );

    // Wait for Ctrl+C or SIGTERM
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .into_diagnostic()?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.into_diagnostic()?;

    // _handle drops here, triggering unmount
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_managed(
    lock_file: &rattler_lock::LockFile,
    environment_name: &str,
    platform: Platform,
    package_cache: &PackageCache,
    mount_point: &std::path::Path,
    overlay_dir: Option<PathBuf>,
    env_hash: &str,
    transport: rattler_fs::Transport,
    grace_period: u64,
    pidfile: Option<&std::path::Path>,
    ready_fd: Option<i32>,
) -> miette::Result<()> {
    std::fs::create_dir_all(mount_point).into_diagnostic()?;

    let config = if let Some(overlay_dir) = overlay_dir {
        rattler_fs::MountConfig::new_writable(
            mount_point.to_path_buf(),
            Some(overlay_dir),
            transport,
            env_hash.to_string(),
        )
    } else {
        rattler_fs::MountConfig::new_read_only(
            mount_point.to_path_buf(),
            transport,
            env_hash.to_string(),
        )
    };

    let _handle =
        rattler_fs::build_and_mount(lock_file, environment_name, platform, package_cache, &config)
            .await
            .map_err(|e| miette::miette!("failed to mount: {e}"))?;

    // Write PID file
    if let Some(pidfile) = pidfile {
        std::fs::write(pidfile, format!("{}\n", std::process::id())).into_diagnostic()?;
    }

    // Signal readiness via pipe
    #[cfg(unix)]
    if let Some(fd) = ready_fd {
        use std::os::unix::io::FromRawFd;
        let mut pipe = unsafe { std::fs::File::from_raw_fd(fd) };
        use std::io::Write;
        let _ = pipe.write_all(b"ready\n");
        // pipe is dropped/closed here
    }

    // Poll for client activity using the lock file. When no clients hold a
    // shared lock for `grace_period` seconds, shut down.
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;

        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .into_diagnostic()?;

        let (lock_path, _) =
            pixi_core::environment::mount_sidecar::coordination_paths(mount_point);

        let probe_file = std::fs::OpenOptions::new()
            .read(true)
            .open(&lock_path)
            .into_diagnostic()?;
        let probe_fd = probe_file.as_raw_fd();

        let mut idle_seconds: u64 = 0;
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        // Skip the immediate first tick to avoid racing with the parent
        // acquiring LOCK_SH after receiving the readiness signal.
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let ret = unsafe { libc::flock(probe_fd, libc::LOCK_EX | libc::LOCK_NB) };
                    if ret == 0 {
                        // No clients hold shared locks. Release immediately.
                        unsafe { libc::flock(probe_fd, libc::LOCK_UN); }
                        idle_seconds += 1;
                        if idle_seconds >= grace_period {
                            tracing::info!(
                                "grace period expired ({grace_period}s), shutting down sidecar"
                            );
                            break;
                        }
                    } else {
                        // Clients active, reset timer.
                        idle_seconds = 0;
                    }
                }
                _ = sigterm.recv() => {
                    tracing::debug!("sidecar received SIGTERM, shutting down");
                    break;
                }
                _ = tokio::signal::ctrl_c() => {
                    tracing::debug!("sidecar received Ctrl+C, shutting down");
                    break;
                }
            }
        }
    }

    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.into_diagnostic()?;

    // Clean up pidfile
    if let Some(pidfile) = pidfile {
        let _ = std::fs::remove_file(pidfile);
    }

    // _handle drops here, triggering unmount
    Ok(())
}
