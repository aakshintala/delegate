pub mod claude;
pub mod cursor;
pub mod pi;
pub mod types;

use crate::types::JobSpec;
use std::io::Read;
use std::os::unix::process::CommandExt;
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use types::{BackendResult, Event, EventFn, Spawned};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Cursor,
    Pi,
    Claude,
}

impl Backend {
    /// None when `name` is not an implemented backend.
    pub fn from_name(name: &str) -> Option<Backend> {
        match name {
            "cursor" => Some(Self::Cursor),
            "pi" => Some(Self::Pi),
            "claude" => Some(Self::Claude),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Cursor => "cursor",
            Self::Pi => "pi",
            Self::Claude => "claude",
        }
    }

    /// Implemented backends, in doctor order.
    pub const ALL: [Backend; 3] = [Self::Cursor, Self::Pi, Self::Claude];

    /// Resolved binary path (env override, then PATH, then ~/.local/bin).
    pub fn bin(self) -> String {
        match self {
            Self::Cursor => cursor::resolve_bin(None),
            Self::Pi => pi::resolve_bin(None),
            Self::Claude => claude::resolve_bin(None),
        }
    }

    /// Full argv (without the binary). Every job runs with writes enabled.
    pub fn argv(self, model: &str, session: Option<&str>, prompt: &str) -> Vec<String> {
        match self {
            Self::Cursor => cursor::argv(model, session, prompt),
            Self::Pi => pi::argv(model, session, prompt),
            Self::Claude => claude::argv(model, session, prompt),
        }
    }

    /// Spawn and drive the child; same contract as today's `Backend::run`.
    pub fn spawn(self, spec: &JobSpec) -> Spawned {
        match self {
            Self::Cursor => cursor::spawn(spec),
            Self::Pi => pi::spawn(spec),
            Self::Claude => claude::spawn(spec),
        }
    }

    pub(crate) fn fill_doctor(
        self,
        report: &mut crate::types::DoctorReport,
        opts: &crate::doctor::RunDoctorOpts<'_>,
    ) {
        match self {
            Self::Cursor => cursor::doctor::fill(report, opts),
            Self::Pi => pi::doctor::fill(report, opts),
            Self::Claude => claude::doctor::fill(report, opts),
        }
    }

    pub(crate) fn doctor_lines(self, report: &crate::types::DoctorReport) -> (String, bool) {
        match self {
            Self::Cursor => cursor::doctor::lines(report),
            Self::Pi => pi::doctor::lines(report),
            Self::Claude => claude::doctor::lines(report),
        }
    }
}

impl types::Runner for Backend {
    fn run(&self, spec: &JobSpec) -> Spawned {
        self.spawn(spec)
    }
}

/// Only a 64 KB tail of stderr is ever reported; pump keeps a bounded window of it.
pub(crate) const STDERR_KEEP: usize = 64 * 1024;

/// A child with its pipes detached, in its own process group (so cancel also stops
/// the shell commands the agent started). The drive closure below owns the wait.
pub(crate) struct Started {
    pub pid: libc::pid_t,
    pub reaped: Arc<AtomicBool>,
    pub child: Child,
    pub stdout: ChildStdout,
    pub stderr: ChildStderr,
}

pub(crate) fn start_child(spec: &JobSpec) -> Result<Started, String> {
    let spawned = Command::new(&spec.bin)
        .args(&spec.argv)
        .current_dir(&spec.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Own process group, so cancel also stops the shell commands the agent started.
        .process_group(0)
        .spawn();
    let mut child = spawned.map_err(|e| e.to_string())?;
    let pid = child.id() as libc::pid_t;
    // Set once the child is reaped, so a late kill can't hit a recycled pid.
    let reaped = Arc::new(AtomicBool::new(false));
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    Ok(Started {
        pid,
        reaped,
        child,
        stdout,
        stderr,
    })
}

/// SIGTERM to the child's process group. Safe on a recycled pid only because the
/// driver sets `reaped` right after `wait`.
pub(crate) fn killer(pid: libc::pid_t, reaped: Arc<AtomicBool>) -> Box<dyn Fn() + Send + Sync> {
    Box::new(move || {
        if !reaped.load(Ordering::SeqCst) {
            unsafe { libc::kill(-pid, libc::SIGTERM) };
        }
    })
}

/// The result when the child never started: the spawn error is the text.
pub(crate) fn spawn_failed(msg: &str) -> Spawned {
    let msg = msg.to_string();
    Spawned {
        kill: Box::new(|| {}),
        session_id: None,
        drive: Box::new(move |_| BackendResult {
            text: msg.clone(),
            is_error: Some(true),
            clean_exit: false,
            stderr: msg.clone(),
            ..Default::default()
        }),
    }
}

pub(crate) struct Pumped {
    pub stderr: String,
    pub saw_stdout: bool,
    pub clean_exit: bool,
}

/// Pump both pipes to EOF (stderr on a scoped thread), then `wait` for the exit
/// status. `handle_line` sees each stdout line without its newline; the backend
/// parses it and emits its own progress events through `on`.
pub(crate) fn pump(
    mut stdout: impl Read,
    mut stderr: impl Read + Send,
    on: EventFn<'_>,
    wait: impl FnOnce() -> bool,
    mut handle_line: impl FnMut(&[u8]),
) -> Pumped {
    std::thread::scope(|s| {
        let err = s.spawn(|| {
            let mut kept: Vec<u8> = Vec::new();
            let mut buf = [0u8; 4096];
            while let Ok(n) = stderr.read(&mut buf) {
                if n == 0 {
                    break;
                }
                kept.extend_from_slice(&buf[..n]);
                if kept.len() > STDERR_KEEP {
                    kept.drain(..kept.len() - STDERR_KEEP);
                }
                on(Event::Stderr);
            }
            String::from_utf8_lossy(&kept).into_owned()
        });

        let mut pending: Vec<u8> = Vec::new();
        let mut buf = [0u8; 8192];
        let mut saw_stdout = false;
        while let Ok(n) = stdout.read(&mut buf) {
            if n == 0 {
                break;
            }
            saw_stdout = true;
            on(Event::Activity);
            pending.extend_from_slice(&buf[..n]);
            while let Some(i) = pending.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = pending.drain(..=i).collect();
                handle_line(&line[..i]);
            }
        }
        // Flush a trailing line that arrived without a terminating newline.
        if !pending.is_empty() {
            handle_line(&pending);
        }
        let stderr = err.join().unwrap_or_default();
        Pumped {
            stderr,
            saw_stdout,
            clean_exit: wait(),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_name_maps_implemented_backends_and_rejects_the_rest() {
        assert_eq!(Backend::from_name("cursor"), Some(Backend::Cursor));
        assert_eq!(Backend::Cursor.name(), "cursor");
        assert_eq!(Backend::from_name("pi"), Some(Backend::Pi));
        assert_eq!(Backend::Pi.name(), "pi");
        assert_eq!(Backend::from_name("claude"), Some(Backend::Claude));
        assert_eq!(Backend::Claude.name(), "claude");
        assert!(Backend::from_name("nope").is_none());
    }
}
