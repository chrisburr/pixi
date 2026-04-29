//! Mount sidecar lifecycle management.
//!
//! Manages the lifecycle of a daemonized mount process for a pixi environment.
//! Uses flock-based reference counting to coordinate between multiple pixi
//! processes sharing the same mounted environment.
//!
//! ## Protocol
//!
//! Per-environment coordination files (in the parent directory of the mount
//! point to avoid routing I/O through the NFS server):
//! - `{name}.rattler-fs.lock` — flock coordination file
//! - `{name}.rattler-fs.pid` — sidecar process PID
//!
//! ### Client side (pixi run/shell):
//! 1. Try `flock(LOCK_EX | LOCK_NB)` on lock file
//!    - Success + sidecar alive → reuse (grace period): downgrade to LOCK_SH
//!    - Success + sidecar dead → start sidecar, downgrade to LOCK_SH
//!    - EWOULDBLOCK → mount exists: acquire LOCK_SH, verify sidecar alive
//! 2. Run user's command
//! 3. On drop: release LOCK_SH (sidecar manages its own lifetime)
//!
//! ### Sidecar side:
//! 1. Mount, write PID, signal readiness
//! 2. Poll for client activity: try LOCK_EX on lock file every second
//!    - Success (no clients) → increment idle counter
//!    - Fail (clients active) → reset idle counter
//! 3. When idle >= grace period → unmount and exit

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::io::{BufRead, BufReader};

use miette::{IntoDiagnostic, miette};

const LOCK_FILENAME: &str = ".rattler-fs.lock";
const PID_FILENAME: &str = ".rattler-fs.pid";
const OVERLAY_DIRNAME: &str = ".rattler-fs-overlay";

/// Derive the coordination file paths for a given mount point.
///
/// Both the lock file and PID file live in the parent directory of the mount
/// point, prefixed with the mount point's basename. This avoids routing I/O
/// through the NFS server that the sidecar hosts.
pub fn coordination_paths(mount_point: &Path) -> (PathBuf, PathBuf) {
    let parent = mount_point.parent().expect("mount_point has no parent");
    let basename = mount_point
        .file_name()
        .expect("mount_point has no file name")
        .to_string_lossy();
    (
        parent.join(format!("{basename}{LOCK_FILENAME}")),
        parent.join(format!("{basename}{PID_FILENAME}")),
    )
}

/// RAII guard that holds a shared flock on the mount coordination file.
///
/// When dropped, releases the shared lock. The sidecar manages its own
/// lifetime via its polling loop and grace period.
pub struct MountGuard {
    #[allow(dead_code)]
    lock_file: File,
    #[allow(dead_code)]
    lock_path: PathBuf,
}

impl MountGuard {
    /// The path to the overlay directory for this environment.
    ///
    /// Lives in the parent directory of `env_dir` (not inside the mount point)
    /// to avoid routing writes through the NFS server.
    pub fn overlay_dir(env_dir: &Path) -> PathBuf {
        let parent = env_dir.parent().expect("env_dir has no parent");
        let name = env_dir
            .file_name()
            .expect("env_dir has no file name")
            .to_string_lossy();
        parent.join(format!("{name}{OVERLAY_DIRNAME}"))
    }
}

// ─── Parent-side prefetch ───────────────────────────────────────────────────

/// Pre-fetch every conda package referenced by `lock_file` for `platform`
/// into `package_cache`, so the mount sidecar's `build_layout` becomes a
/// warm-cache lookup loop and starts within seconds even on cold caches.
///
/// The fetch loop mirrors the one in `pixi_cli::mount::build_layout`, but
/// runs in the parent process so progress (and any I/O delay from CVMFS
/// warming up or large downloads) is visible to the user instead of
/// invisibly tripping the sidecar readiness timeout.
///
/// `client` is converted into a `LazyClient` internally and reused across
/// all fetch tasks. Returns once every package is available in the cache.
pub async fn prefetch_packages_for_mount(
    lock_file: &rattler_lock::LockFile,
    environment_name: &str,
    platform: rattler_conda_types::Platform,
    package_cache: &rattler::package_cache::PackageCache,
    client: impl Into<rattler_networking::LazyClient>,
) -> miette::Result<()> {
    use std::sync::Arc;

    let environment = lock_file
        .environment(environment_name)
        .ok_or_else(|| miette!("environment '{environment_name}' not found in lock file"))?;
    let Some(packages) = environment.packages(platform) else {
        return Ok(());
    };

    let mut conda_packages: Vec<_> = packages.filter_map(|p| p.as_binary_conda()).collect();
    if conda_packages.is_empty() {
        return Ok(());
    }

    // Largest first so long downloads start early.
    conda_packages.sort_by(|a, b| {
        b.package_record
            .size
            .unwrap_or(0)
            .cmp(&a.package_record.size.unwrap_or(0))
    });

    let total = conda_packages.len();
    tracing::info!(
        "prefetching {total} package(s) for mount of environment '{environment_name}'"
    );

    let lazy_client: rattler_networking::LazyClient = client.into();
    let concurrency = Arc::new(tokio::sync::Semaphore::new(16));
    let mut join_set = tokio::task::JoinSet::new();

    for package_data in &conda_packages {
        let cache = package_cache.clone();
        let client = lazy_client.clone();
        let record = package_data.package_record.clone();
        let location = package_data.location.clone();
        let sem = concurrency.clone();

        join_set.spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|e| miette!("concurrency semaphore closed: {e}"))?;
            let url = location
                .as_url()
                .ok_or_else(|| miette!("package has no URL"))?
                .clone();
            cache
                .get_or_fetch_from_url_with_retry(
                    &record,
                    url,
                    client,
                    rattler_networking::retry_policies::default_retry_policy(),
                    None,
                )
                .await
                .map_err(|e| miette!("failed to prefetch package: {e}"))?;
            Ok::<_, miette::Report>(())
        });
    }

    let mut completed = 0usize;
    while let Some(result) = join_set.join_next().await {
        result.map_err(|e| miette!("prefetch task failed: {e}"))??;
        completed += 1;
        tracing::debug!("prefetch progress: {completed}/{total}");
    }

    tracing::info!("prefetch complete ({total} package(s))");
    Ok(())
}

// ─── Unix implementation ────────────────────────────────────────────────────

/// Check if the sidecar process is still alive.
#[cfg(unix)]
pub fn is_sidecar_alive(pid_path: &Path) -> bool {
    let Ok(pid_str) = fs::read_to_string(pid_path) else {
        return false;
    };
    let Ok(pid) = pid_str.trim().parse::<i32>() else {
        return false;
    };
    // kill(pid, 0) checks if process exists without sending a signal
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Check if a path is a mount point.
#[cfg(unix)]
fn is_mountpoint(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;

    // A path is a mount point if its device ID differs from its parent's
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    let Some(parent) = path.parent() else {
        return false;
    };
    let Ok(parent_meta) = fs::metadata(parent) else {
        return false;
    };
    meta.dev() != parent_meta.dev()
}

/// Ensure a mount is running for the given environment directory.
///
/// If no sidecar is running, starts one (by invoking `pixi mount --managed`).
/// If a sidecar is alive in its grace period, reuses it.
/// Returns a guard that keeps the shared flock alive.
#[cfg(unix)]
pub async fn ensure_mount(
    env_dir: &Path,
    workspace_root: &Path,
    environment_name: &str,
) -> miette::Result<MountGuard> {
    fs::create_dir_all(env_dir).into_diagnostic()?;

    let (lock_path, pid_path) = coordination_paths(env_dir);

    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .into_diagnostic()?;

    let fd = lock_file.as_raw_fd();

    // Try exclusive lock (non-blocking)
    let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };

    if ret == 0 {
        // We got exclusive lock — no other clients.
        if is_sidecar_alive(&pid_path) {
            // Sidecar is alive in its grace period. Reuse it.
            tracing::debug!("sidecar alive in grace period, reusing");
        } else {
            // No sidecar running. Clean up stale state and start fresh.
            cleanup_stale_state(env_dir, &pid_path)?;
            start_sidecar(
                env_dir,
                workspace_root,
                environment_name,
                &lock_file,
                &pid_path,
            )
            .await?;
        }

        // Downgrade to shared lock
        let ret = unsafe { libc::flock(fd, libc::LOCK_SH) };
        if ret != 0 {
            return Err(miette!(
                "failed to downgrade to shared lock: {}",
                std::io::Error::last_os_error()
            ));
        }
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            // Mount is starting or running. Acquire shared lock (may block
            // briefly while the sidecar initializes).
            let ret = unsafe { libc::flock(fd, libc::LOCK_SH) };
            if ret != 0 {
                return Err(miette!(
                    "failed to acquire shared lock: {}",
                    std::io::Error::last_os_error()
                ));
            }

            // Verify sidecar is alive
            if !is_sidecar_alive(&pid_path) {
                // Stale sidecar — release lock and retry
                let _ = unsafe { libc::flock(fd, libc::LOCK_UN) };
                drop(lock_file);
                cleanup_stale_state(env_dir, &pid_path)?;
                // Recursive retry (bounded by stale cleanup)
                return Box::pin(ensure_mount(env_dir, workspace_root, environment_name)).await;
            }
        } else {
            return Err(miette!("failed to acquire lock: {err}"));
        }
    }

    Ok(MountGuard {
        lock_file,
        lock_path,
    })
}

/// Start the sidecar mount process.
///
/// Creates a readiness pipe, forks the sidecar via `pixi mount --managed`,
/// and waits for it to signal readiness.
#[cfg(unix)]
async fn start_sidecar(
    env_dir: &Path,
    workspace_root: &Path,
    environment_name: &str,
    _lock_file: &File,
    pid_path: &Path,
) -> miette::Result<()> {
    // Create a pipe for readiness signaling
    let mut pipe_fds = [0i32; 2];
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } != 0 {
        return Err(miette!(
            "failed to create pipe: {}",
            std::io::Error::last_os_error()
        ));
    }
    let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

    // Find the pixi binary
    let pixi_exe = std::env::current_exe().into_diagnostic()?;

    let env_dir_str = env_dir.display().to_string();
    let write_fd_str = write_fd.to_string();
    let pid_path_str = pid_path.display().to_string();

    // Spawn the sidecar as a detached child process.
    let mut cmd = std::process::Command::new(&pixi_exe);
    cmd.args([
        "mount",
        "--managed",
        "-e",
        environment_name,
        "--mount-point",
        &env_dir_str,
        "--pidfile",
        &pid_path_str,
        "--ready-fd",
        &write_fd_str,
    ])
    .current_dir(workspace_root)
    .stdin(std::process::Stdio::null())
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::inherit());

    // Clear the CLOEXEC flag on write_fd so the child inherits it
    unsafe {
        use std::os::unix::process::CommandExt;
        let fd = write_fd;
        cmd.pre_exec(move || {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = cmd.spawn().into_diagnostic()?;

    // Close the write end in the parent
    unsafe {
        libc::close(write_fd);
    }

    // Wait for readiness signal from the sidecar
    let read_file = unsafe { File::from_raw_fd(read_fd) };
    let mut reader = BufReader::new(read_file);
    let mut line = String::new();

    // Use a timeout to avoid blocking forever if the sidecar fails
    let readiness = tokio::task::spawn_blocking(move || reader.read_line(&mut line).map(|_| line));

    match tokio::time::timeout(std::time::Duration::from_secs(30), readiness).await {
        Ok(Ok(Ok(msg))) if msg.starts_with("ready") => {
            tracing::debug!("mount sidecar is ready");
            Ok(())
        }
        Ok(Ok(Ok(msg))) => Err(miette!("sidecar reported error: {}", msg.trim())),
        Ok(Ok(Err(e))) => Err(miette!("failed to read from sidecar pipe: {e}")),
        Ok(Err(e)) => Err(miette!("sidecar readiness task failed: {e}")),
        Err(_) => {
            // Timeout — kill the child if still running
            let _ = child.kill();
            Err(miette!(
                "timed out waiting for mount sidecar to become ready"
            ))
        }
    }
}

// ─── Windows implementation ─────────────────────────────────────────────────

/// Check if the sidecar process is still alive.
#[cfg(windows)]
pub fn is_sidecar_alive(pid_path: &Path) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    let Ok(pid_str) = fs::read_to_string(pid_path) else {
        return false;
    };
    let Ok(pid) = pid_str.trim().parse::<u32>() else {
        return false;
    };

    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return false;
        }
        let mut exit_code: u32 = 0;
        let alive =
            GetExitCodeProcess(handle, &mut exit_code) != 0 && exit_code == STILL_ACTIVE as u32;
        CloseHandle(handle);
        alive
    }
}

/// Check if a path is an active ProjFS virtualization root.
///
/// ProjFS doesn't create mount points. Instead we check if the sidecar PID
/// file exists and the sidecar is alive.
#[cfg(windows)]
fn is_mountpoint(path: &Path) -> bool {
    let (_, pid_path) = coordination_paths(path);
    is_sidecar_alive(&pid_path)
}

/// Ensure a mount is running for the given environment directory (Windows).
///
/// Uses `LockFileEx` for coordination instead of `flock`.
#[cfg(windows)]
pub async fn ensure_mount(
    env_dir: &Path,
    workspace_root: &Path,
    environment_name: &str,
) -> miette::Result<MountGuard> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY, LockFileEx, UnlockFileEx,
    };

    fs::create_dir_all(env_dir).into_diagnostic()?;

    let (lock_path, pid_path) = coordination_paths(env_dir);

    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(false)
        .open(&lock_path)
        .into_diagnostic()?;

    let handle = lock_file.as_raw_handle();
    let mut overlapped: windows_sys::Win32::System::IO::OVERLAPPED = unsafe { std::mem::zeroed() };

    // Try exclusive lock (non-blocking)
    let got_exclusive = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        ) != 0
    };

    if got_exclusive {
        // We got exclusive lock — no other clients.
        if is_sidecar_alive(&pid_path) {
            tracing::debug!("sidecar alive in grace period, reusing");
        } else {
            cleanup_stale_state(env_dir, &pid_path)?;
            start_sidecar(
                env_dir,
                workspace_root,
                environment_name,
                &lock_file,
                &pid_path,
            )
            .await?;
        }

        // Release exclusive lock, then acquire shared lock
        unsafe {
            overlapped = std::mem::zeroed();
            UnlockFileEx(handle, 0, 1, 0, &mut overlapped);
            // Acquire shared (non-exclusive) lock — blocking
            overlapped = std::mem::zeroed();
            if LockFileEx(handle, 0, 0, 1, 0, &mut overlapped) == 0 {
                return Err(miette!(
                    "failed to acquire shared lock: {}",
                    std::io::Error::last_os_error()
                ));
            }
        }
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(ERROR_LOCK_VIOLATION as i32) {
            // Mount is starting or running. Acquire shared lock (blocking).
            unsafe {
                if LockFileEx(handle, 0, 0, 1, 0, &mut overlapped) == 0 {
                    return Err(miette!(
                        "failed to acquire shared lock: {}",
                        std::io::Error::last_os_error()
                    ));
                }
            }

            // Verify sidecar is alive
            if !is_sidecar_alive(&pid_path) {
                unsafe {
                    overlapped = std::mem::zeroed();
                    UnlockFileEx(handle, 0, 1, 0, &mut overlapped);
                }
                drop(lock_file);
                cleanup_stale_state(env_dir, &pid_path)?;
                return Box::pin(ensure_mount(env_dir, workspace_root, environment_name)).await;
            }
        } else {
            return Err(miette!("failed to acquire lock: {err}"));
        }
    }

    Ok(MountGuard {
        lock_file,
        lock_path,
    })
}

/// Start the sidecar mount process (Windows).
///
/// Uses a named event for readiness signaling instead of a pipe/fd.
#[cfg(windows)]
async fn start_sidecar(
    env_dir: &Path,
    workspace_root: &Path,
    environment_name: &str,
    _lock_file: &File,
    pid_path: &Path,
) -> miette::Result<()> {
    // Use a unique named event for readiness signaling.
    // Include a hash of env_dir to prevent collisions when multiple
    // environments are mounted concurrently or PIDs are recycled.
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    env_dir.hash(&mut hasher);
    let event_name = format!(
        "Local\\pixi-mount-ready-{}-{:x}",
        std::process::id(),
        hasher.finish()
    );

    // Create a named event
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    let event_name_wide: Vec<u16> = OsStr::new(&event_name)
        .encode_wide()
        .chain(Some(0))
        .collect();

    let event_handle = unsafe {
        windows_sys::Win32::System::Threading::CreateEventW(
            std::ptr::null(),
            1, // manual reset
            0, // initial state: not signaled
            event_name_wide.as_ptr(),
        )
    };
    if event_handle.is_null() {
        return Err(miette!(
            "failed to create readiness event: {}",
            std::io::Error::last_os_error()
        ));
    }

    let pixi_exe = std::env::current_exe().into_diagnostic()?;
    let env_dir_str = env_dir.display().to_string();
    let pid_path_str = pid_path.display().to_string();

    let mut cmd = std::process::Command::new(&pixi_exe);
    cmd.args([
        "mount",
        "--managed",
        "-e",
        environment_name,
        "--mount-point",
        &env_dir_str,
        "--pidfile",
        &pid_path_str,
        "--ready-event",
        &event_name,
    ])
    .current_dir(workspace_root)
    .stdin(std::process::Stdio::null())
    .stdout(std::process::Stdio::null())
    .stderr(std::process::Stdio::inherit());

    let mut child = cmd.spawn().into_diagnostic()?;

    // Wait for readiness event with timeout.
    // Cast to isize for Send safety — Windows HANDLEs are just kernel object
    // pointers and are safe to use from any thread.
    let event_handle_isize = event_handle as isize;
    let wait_result = tokio::task::spawn_blocking(move || unsafe {
        let h = event_handle_isize as windows_sys::Win32::Foundation::HANDLE;
        let result = windows_sys::Win32::System::Threading::WaitForSingleObject(h, 30_000);
        windows_sys::Win32::Foundation::CloseHandle(h);
        result
    })
    .await
    .into_diagnostic()?;

    use windows_sys::Win32::Foundation::{WAIT_ABANDONED, WAIT_OBJECT_0, WAIT_TIMEOUT};

    match wait_result {
        WAIT_OBJECT_0 => {
            tracing::debug!("mount sidecar is ready");
            Ok(())
        }
        WAIT_TIMEOUT => {
            let _ = child.kill();
            Err(miette!(
                "timed out waiting for mount sidecar to become ready"
            ))
        }
        WAIT_ABANDONED => {
            let _ = child.kill();
            Err(miette!(
                "sidecar readiness event was abandoned (sidecar may have crashed)"
            ))
        }
        other => {
            let _ = child.kill();
            Err(miette!(
                "WaitForSingleObject returned unexpected status: 0x{:08x}",
                other
            ))
        }
    }
}

// ─── Platform-independent code ──────────────────────────────────────────────

/// Clean up stale state from a previous (crashed) sidecar.
fn cleanup_stale_state(env_dir: &Path, pid_path: &Path) -> miette::Result<()> {
    if pid_path.exists() {
        if !is_sidecar_alive(pid_path) {
            tracing::debug!("cleaning up stale pidfile at {}", pid_path.display());
            let _ = fs::remove_file(pid_path);
        }
    }

    // Check if the mount point is still mounted and force unmount
    if is_mountpoint(env_dir) {
        tracing::debug!("force-unmounting stale mount at {}", env_dir.display());
        force_unmount(env_dir)?;
    }

    Ok(())
}

/// Force unmount a mount point.
pub fn force_unmount(mount_point: &Path) -> miette::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let mnt = mount_point.display().to_string();
        let _ = std::process::Command::new("umount")
            .args(["-f", &mnt])
            .status();
    }

    #[cfg(target_os = "linux")]
    {
        let mnt = mount_point.display().to_string();
        let _ = std::process::Command::new("fusermount3")
            .args(["-uz", &mnt])
            .status();
    }

    #[cfg(target_os = "windows")]
    {
        // ProjFS virtualization is stopped when the sidecar process exits.
        // Terminate the sidecar if it's still running, then clean up.
        use windows_sys::Win32::Foundation::CloseHandle;
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_TERMINATE, TerminateProcess,
        };

        let (_, pid_path) = coordination_paths(mount_point);
        if let Ok(pid_str) = fs::read_to_string(&pid_path) {
            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                unsafe {
                    let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
                    if !handle.is_null() {
                        TerminateProcess(handle, 1);
                        CloseHandle(handle);
                    }
                }
            }
        }
        let _ = fs::remove_file(pid_path);
    }

    Ok(())
}

/// Check if a mount is currently active for the given environment.
pub fn is_mounted(env_dir: &Path) -> bool {
    is_mountpoint(env_dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_coordination_paths() {
        let mount_point = PathBuf::from("/tmp/envs/default");
        let (lock_path, pid_path) = coordination_paths(&mount_point);
        assert_eq!(
            lock_path,
            PathBuf::from("/tmp/envs/default.rattler-fs.lock")
        );
        assert_eq!(pid_path, PathBuf::from("/tmp/envs/default.rattler-fs.pid"));
    }

    #[test]
    fn test_is_sidecar_alive_nonexistent_pid() {
        let tmp = TempDir::new().unwrap();
        let pid_path = tmp.path().join("test.pid");
        assert!(!is_sidecar_alive(&pid_path));
    }

    #[test]
    fn test_is_sidecar_alive_invalid_pid() {
        let tmp = TempDir::new().unwrap();
        let pid_path = tmp.path().join("test.pid");
        fs::write(&pid_path, "not_a_number\n").unwrap();
        assert!(!is_sidecar_alive(&pid_path));
    }

    #[test]
    fn test_is_sidecar_alive_dead_pid() {
        let tmp = TempDir::new().unwrap();
        let pid_path = tmp.path().join("test.pid");
        // PID 99999999 almost certainly doesn't exist
        fs::write(&pid_path, "99999999\n").unwrap();
        assert!(!is_sidecar_alive(&pid_path));
    }

    #[test]
    fn test_is_sidecar_alive_current_process() {
        let tmp = TempDir::new().unwrap();
        let pid_path = tmp.path().join("test.pid");
        let pid = std::process::id();
        fs::write(&pid_path, format!("{pid}\n")).unwrap();
        assert!(is_sidecar_alive(&pid_path));
    }

    #[test]
    fn test_cleanup_stale_state_removes_dead_pidfile() {
        let tmp = TempDir::new().unwrap();
        let env_dir = tmp.path().join("env");
        fs::create_dir_all(&env_dir).unwrap();
        let pid_path = env_dir.join(PID_FILENAME);
        fs::write(&pid_path, "99999999\n").unwrap();

        cleanup_stale_state(&env_dir, &pid_path).unwrap();
        assert!(!pid_path.exists());
    }

    #[test]
    fn test_overlay_dir() {
        let env_dir = PathBuf::from("/tmp/test-env");
        assert_eq!(
            MountGuard::overlay_dir(&env_dir),
            PathBuf::from("/tmp/test-env.rattler-fs-overlay")
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_is_mountpoint_regular_dir() {
        let tmp = TempDir::new().unwrap();
        // A regular directory is not a mount point (same device as parent)
        assert!(!is_mountpoint(tmp.path()));
    }
}
