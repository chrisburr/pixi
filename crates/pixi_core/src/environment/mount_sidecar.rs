//! Mount sidecar lifecycle management.
//!
//! Manages the lifecycle of a daemonized mount process for a pixi environment.
//! Uses flock-based reference counting to coordinate between multiple pixi
//! processes sharing the same mounted environment.
//!
//! ## Protocol
//!
//! Per-environment files:
//! - `.rattler-fs.lock` — flock coordination file
//! - `.rattler-fs.pid` — sidecar process PID
//! - `.rattler-fs-overlay/` — writable overlay storage
//!
//! ### Client side (pixi run/shell):
//! 1. Try `flock(LOCK_EX | LOCK_NB)` on `.rattler-fs.lock`
//!    - Success → first user: fork sidecar, wait for readiness, downgrade to LOCK_SH
//!    - EWOULDBLOCK → mount exists: acquire LOCK_SH, verify sidecar alive
//! 2. Run user's command
//! 3. On drop: release LOCK_SH, try LOCK_EX non-blocking
//!    - Success → last user: SIGTERM the sidecar via pidfile
//!    - EWOULDBLOCK → other users still active, do nothing

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};

use miette::{IntoDiagnostic, miette};

const LOCK_FILENAME: &str = ".rattler-fs.lock";
const PID_FILENAME: &str = ".rattler-fs.pid";
const OVERLAY_DIRNAME: &str = ".rattler-fs-overlay";

/// RAII guard that holds a shared flock on the mount coordination file.
///
/// When dropped, releases the shared lock and — if this was the last holder —
/// sends SIGTERM to the sidecar process.
pub struct MountGuard {
    lock_file: File,
    #[allow(dead_code)] // used for debugging; kept for potential future use
    lock_path: PathBuf,
    pid_path: PathBuf,
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

#[cfg(unix)]
impl Drop for MountGuard {
    fn drop(&mut self) {
        // Release the shared lock (happens implicitly when fd closes, but we
        // need to try upgrading first).

        // Try to acquire exclusive lock (non-blocking). If successful, we are
        // the last user and should signal the sidecar to shut down.
        let fd = self.lock_file.as_raw_fd();
        let ret = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
        if ret == 0 {
            // We got exclusive lock — no other clients. Kill the sidecar.
            if let Ok(pid_str) = fs::read_to_string(&self.pid_path) {
                if let Ok(pid) = pid_str.trim().parse::<i32>() {
                    tracing::debug!("last mount client exiting, sending SIGTERM to sidecar pid {pid}");
                    unsafe {
                        libc::kill(pid, libc::SIGTERM);
                    }
                }
            }
            // Clean up the pidfile
            let _ = fs::remove_file(&self.pid_path);
        }
        // Lock is released when lock_file is dropped
    }
}

/// Ensure a mount is running for the given environment directory.
///
/// If no sidecar is running, starts one (by invoking `pixi mount --managed`).
/// Returns a guard that keeps the shared flock alive.
#[cfg(unix)]
pub async fn ensure_mount(
    env_dir: &Path,
    workspace_root: &Path,
    environment_name: &str,
) -> miette::Result<MountGuard> {
    fs::create_dir_all(env_dir).into_diagnostic()?;

    // Coordination files live in the parent directory of the mount point,
    // because the mount point itself will be overlaid by the NFS mount and
    // the sidecar process that hosts the NFS server cannot write through
    // its own mount without deadlocking.
    let parent_dir = env_dir
        .parent()
        .ok_or_else(|| miette!("env_dir has no parent: {}", env_dir.display()))?;
    let env_basename = env_dir
        .file_name()
        .ok_or_else(|| miette!("env_dir has no file name: {}", env_dir.display()))?
        .to_string_lossy();
    let lock_path = parent_dir.join(format!("{env_basename}{LOCK_FILENAME}"));
    let pid_path = parent_dir.join(format!("{env_basename}{PID_FILENAME}"));

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
        // We got exclusive lock — no sidecar running.
        // Check for stale state first.
        cleanup_stale_state(env_dir, &pid_path)?;

        // Start the sidecar
        start_sidecar(env_dir, workspace_root, environment_name, &lock_file, &pid_path).await?;

        // Downgrade to shared lock
        let ret = unsafe { libc::flock(fd, libc::LOCK_SH) };
        if ret != 0 {
            return Err(miette!("failed to downgrade to shared lock: {}", std::io::Error::last_os_error()));
        }
    } else {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            // Mount is starting or running. Acquire shared lock (may block
            // briefly while the sidecar initializes).
            let ret = unsafe { libc::flock(fd, libc::LOCK_SH) };
            if ret != 0 {
                return Err(miette!("failed to acquire shared lock: {}", std::io::Error::last_os_error()));
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
        pid_path,
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
        return Err(miette!("failed to create pipe: {}", std::io::Error::last_os_error()));
    }
    let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

    // Find the pixi binary
    let pixi_exe = std::env::current_exe().into_diagnostic()?;

    let env_dir_str = env_dir.display().to_string();
    let write_fd_str = write_fd.to_string();
    let pid_path_str = pid_path.display().to_string();

    // Spawn the sidecar as a detached child process.
    // We use Command::new rather than fork() for simplicity — the sidecar
    // daemonizes itself internally.
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
    unsafe { libc::close(write_fd); }

    // Wait for readiness signal from the sidecar
    let read_file = unsafe { File::from_raw_fd(read_fd) };
    let mut reader = BufReader::new(read_file);
    let mut line = String::new();

    // Use a timeout to avoid blocking forever if the sidecar fails
    let readiness = tokio::task::spawn_blocking(move || {
        reader.read_line(&mut line).map(|_| line)
    });

    match tokio::time::timeout(std::time::Duration::from_secs(30), readiness).await {
        Ok(Ok(Ok(msg))) if msg.starts_with("ready") => {
            tracing::debug!("mount sidecar is ready");
            Ok(())
        }
        Ok(Ok(Ok(msg))) => {
            Err(miette!("sidecar reported error: {}", msg.trim()))
        }
        Ok(Ok(Err(e))) => {
            Err(miette!("failed to read from sidecar pipe: {e}"))
        }
        Ok(Err(e)) => {
            Err(miette!("sidecar readiness task failed: {e}"))
        }
        Err(_) => {
            // Timeout — kill the child if still running
            let _ = child.kill();
            Err(miette!("timed out waiting for mount sidecar to become ready"))
        }
    }
}

/// Check if the sidecar process is still alive.
#[cfg(unix)]
fn is_sidecar_alive(pid_path: &Path) -> bool {
    let Ok(pid_str) = fs::read_to_string(pid_path) else {
        return false;
    };
    let Ok(pid) = pid_str.trim().parse::<i32>() else {
        return false;
    };
    // kill(pid, 0) checks if process exists without sending a signal
    unsafe { libc::kill(pid, 0) == 0 }
}

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

/// Force unmount a mount point.
pub fn force_unmount(mount_point: &Path) -> miette::Result<()> {
    let mnt = mount_point.display().to_string();

    #[cfg(target_os = "macos")]
    {
        let _ = std::process::Command::new("umount")
            .args(["-f", &mnt])
            .status();
    }

    #[cfg(target_os = "linux")]
    {
        let _ = std::process::Command::new("fusermount3")
            .args(["-uz", &mnt])
            .status();
    }

    Ok(())
}

/// Check if a mount is currently active for the given environment.
pub fn is_mounted(env_dir: &Path) -> bool {
    is_mountpoint(env_dir)
}

#[cfg(unix)]
use std::os::unix::io::FromRawFd;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

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
            PathBuf::from("/tmp/test-env/.rattler-fs-overlay")
        );
    }

    #[test]
    fn test_is_mountpoint_regular_dir() {
        let tmp = TempDir::new().unwrap();
        // A regular directory is not a mount point (same device as parent)
        assert!(!is_mountpoint(tmp.path()));
    }
}
