//! Single-job supervisor core: spawn via the backend, idle watchdog, heartbeat records,
//! gate + change-set finalize, and cancel.

use crate::backends::types::{BackendResult, Event, ProgressSnapshotRaw, Runner};
use crate::finalize::{finalize_run, finalize_stall};
use crate::status_record::StatusRecordWriter;
use crate::types::{
    FinalizeCtx, JobSpec, JobStatus, PollResult, ProgressSnapshot, RunOutput, RunStatus,
};
use crate::util::{clamp_wait, random_uuid};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const DEFAULT_WAIT_TIMEOUT: f64 = 120_000.0;
const HEARTBEAT_MS: u64 = 30_000;

pub type FinalizeFn = Arc<dyn Fn(&BackendResult, &FinalizeCtx) -> RunOutput + Send + Sync>;

pub struct JobDeps {
    pub backend: Arc<dyn Runner>,
    pub idle_ms: Option<f64>,
    pub tool_idle_ms: Option<f64>,
    pub finalize: FinalizeFn,
    pub finalize_stall: FinalizeFn,
    pub status_writer: Arc<dyn StatusRecordWriter>,
    pub heartbeat_ms: u64,
}

impl JobDeps {
    pub fn new(backend: Arc<dyn Runner>, idle_ms: Option<f64>, tool_idle_ms: Option<f64>) -> Self {
        Self {
            backend,
            idle_ms,
            tool_idle_ms,
            finalize: Arc::new(finalize_run),
            finalize_stall: Arc::new(finalize_stall),
            status_writer: Arc::new(crate::status_record::file_status_record_writer()),
            heartbeat_ms: HEARTBEAT_MS,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stage {
    Running,
    Finalizing,
    Terminal,
}

struct JobState {
    status: JobStatus,
    stage: Stage,
    spec: Arc<JobSpec>,
    kill: Arc<dyn Fn() + Send + Sync>,
    finalize_abort: Arc<AtomicBool>,
    started: Instant,
    progress: ProgressSnapshotRaw,
    last_event: Instant,
    termination: Option<JobStatus>,
    output: Option<RunOutput>,
}

struct Inner {
    job: Option<(String, JobState)>,
}

pub struct JobHandle {
    st: Mutex<Inner>,
    cv: Condvar,
    deps: JobDeps,
}

fn ms(v: f64) -> Duration {
    Duration::from_millis(v.max(0.0) as u64)
}

fn epoch_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

fn describe_stall_progress(job: &JobState) -> String {
    let verb = if job.termination == Some(JobStatus::Cancelled) {
        "Cancelled"
    } else {
        "Idle watchdog killed this job"
    };
    let p = &job.progress;
    let mut parts = vec![format!(
        "{verb} after {}s.",
        (job.started.elapsed().as_millis() as f64 / 1000.0).round()
    )];
    if let Some(phase) = &p.phase {
        parts.push(format!("Last phase: {phase}."));
    }
    if let Some(tool) = &p.last_tool {
        parts.push(format!("Last tool: {tool}."));
    }
    if p.tokens_so_far > 0.0 {
        parts.push(format!(
            "{} tokens streamed before the kill.",
            p.tokens_so_far
        ));
    }
    if let Some(a) = &p.last_assistant {
        parts.push(format!("Last assistant text: \"{a}\""));
    }
    if !p.files_touched.is_empty() {
        parts.push(format!("Files touched: {}.", p.files_touched.join(", ")));
    }
    parts.join(" ")
}

fn poll_state(st: &Inner, job_id: &str) -> PollResult {
    let Some((id, job)) = &st.job else {
        return PollResult::not_found();
    };
    if id != job_id {
        return PollResult::not_found();
    }
    if job.status == JobStatus::Running {
        let p = &job.progress;
        return PollResult::Running {
            status: "RUNNING",
            last_heartbeat_at: epoch_ms(),
            superseded_by: None,
            progress: ProgressSnapshot {
                last_tool: p.last_tool.clone(),
                tokens_so_far: p.tokens_so_far,
                elapsed_ms: job.started.elapsed().as_millis() as f64,
                last_assistant: p.last_assistant.clone(),
                files_touched_so_far: p.files_touched.clone(),
                phase: p.phase.clone(),
                session_id: p.session_id.clone(),
            },
        };
    }
    PollResult::Terminal {
        status: job.status,
        result: job.output.clone().expect("terminal job has output"),
        superseded_by: None,
    }
}

impl JobHandle {
    pub fn new(deps: JobDeps) -> Arc<Self> {
        Arc::new(Self {
            st: Mutex::new(Inner { job: None }),
            cv: Condvar::new(),
            deps,
        })
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn write_record(&self, st: &Inner, job_id: &str) {
        let record = poll_state(st, job_id);
        let w = &self.deps.status_writer;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| w.write(job_id, &record)));
    }

    pub fn dispatch(self: &Arc<Self>, spec: JobSpec) -> String {
        let spec = Arc::new(spec);
        let job_id = random_uuid();
        let drive;
        {
            let mut st = self.lock();
            let spawned = self.deps.backend.run(&spec);
            drive = spawned.drive;
            let session_id = spawned.session_id;
            let now = Instant::now();
            st.job = Some((
                job_id.clone(),
                JobState {
                    status: JobStatus::Running,
                    stage: Stage::Running,
                    spec: Arc::clone(&spec),
                    kill: Arc::from(spawned.kill),
                    finalize_abort: Arc::new(AtomicBool::new(false)),
                    started: now,
                    progress: ProgressSnapshotRaw {
                        session_id,
                        ..Default::default()
                    },
                    last_event: now,
                    termination: None,
                    output: None,
                },
            ));
            self.write_record(&st, &job_id);
        }

        let (me, id) = (Arc::clone(self), job_id.clone());
        std::thread::spawn(move || {
            let res = drive(&|e| me.on_event(&id, e));
            me.finalize_job(&id, res);
        });
        let (me, id) = (Arc::clone(self), job_id.clone());
        std::thread::spawn(move || me.watchdog(&id));

        job_id
    }

    fn on_event(&self, job_id: &str, e: Event) {
        let mut st = self.lock();
        let Some((id, job)) = st.job.as_mut() else {
            return;
        };
        if id != job_id {
            return;
        }
        job.last_event = Instant::now();
        if let Event::Progress(mut snap) = e {
            let session_changed = snap
                .session_id
                .as_ref()
                .is_some_and(|id| job.progress.session_id.as_ref() != Some(id));
            if snap.session_id.is_none() {
                snap.session_id = job.progress.session_id.clone();
            }
            job.progress = snap;
            self.cv.notify_all();
            if session_changed {
                self.write_record(&st, job_id);
            }
        }
    }

    fn watchdog(&self, job_id: &str) {
        let heartbeat = Duration::from_millis(self.deps.heartbeat_ms);
        let mut next_heartbeat = Instant::now() + heartbeat;
        let mut st = self.lock();
        loop {
            let Some((id, job)) = st.job.as_mut() else {
                return;
            };
            if id != job_id {
                return;
            }
            if job.stage == Stage::Terminal {
                return;
            }
            let now = Instant::now();
            let mut wake_at = next_heartbeat;
            if job.stage == Stage::Running && job.termination.is_none() {
                let spec = &job.spec;
                let window = if job.progress.phase.as_deref() == Some("running_tool") {
                    spec.tool_idle_ms.unwrap_or(self.deps.tool_idle_ms)
                } else {
                    spec.idle_ms.unwrap_or(self.deps.idle_ms)
                };
                if let Some(w) = window {
                    let due = job.last_event + ms(w);
                    if now >= due {
                        job.termination = Some(JobStatus::Stalled);
                        (job.kill)();
                    } else {
                        wake_at = wake_at.min(due);
                    }
                }
            }
            if now >= next_heartbeat {
                self.write_record(&st, job_id);
                next_heartbeat = now + heartbeat;
                wake_at = wake_at.min(next_heartbeat);
            }
            st = self
                .cv
                .wait_timeout(st, wake_at.saturating_duration_since(now))
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    fn finalize_job(&self, job_id: &str, res: BackendResult) {
        let (spec, termination, abort) = {
            let mut st = self.lock();
            let Some((id, job)) = st.job.as_mut() else {
                return;
            };
            if id != job_id {
                return;
            }
            job.stage = Stage::Finalizing;
            (
                Arc::clone(&job.spec),
                job.termination,
                Arc::clone(&job.finalize_abort),
            )
        };
        let ctx = FinalizeCtx {
            cwd: spec.cwd.clone(),
            head_before: spec.head_before.clone(),
            gate: spec.gate.clone(),
            gate_timeout_ms: spec
                .tool_idle_ms
                .unwrap_or(self.deps.tool_idle_ms)
                .map(|ms| ms.max(0.0) as u64),
            model: spec.model.clone(),
            backend: spec.backend.clone(),
            price_map: spec.price_map.clone(),
            job_id: Some(job_id.to_string()),
            run_gate: None,
            signal: Some(abort),
            git_delta: None,
        };
        let f = if termination.is_some() {
            &self.deps.finalize_stall
        } else {
            &self.deps.finalize
        };
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(&res, &ctx)))
            .unwrap_or_else(|_| RunOutput {
                status: RunStatus::Error,
                text: "finalize panicked".into(),
                session_id: None,
                backend: spec.backend.clone(),
                model: spec.model.clone(),
                usage: None,
                cost_usd: None,
                cost_estimated: true,
                duration_ms: None,
                job_id: Some(job_id.to_string()),
                stderr_tail: None,
                gate_result: None,
                change_set: None,
                concerns: None,
                permission_denials: Vec::new(),
            });
        let mut st = self.lock();
        let mut out = out;
        if let Some(term) = termination
            && let Some((_, job)) = st.job.as_ref()
        {
            out.text = describe_stall_progress(job);
            out.status = match term {
                JobStatus::Cancelled => RunStatus::Cancelled,
                JobStatus::Stalled => RunStatus::Stalled,
                _ => out.status,
            };
        }
        self.retire(&mut st, job_id, out);
        self.cv.notify_all();
    }

    fn retire(&self, st: &mut Inner, job_id: &str, out: RunOutput) {
        let Some((id, job)) = st.job.as_mut() else {
            return;
        };
        if id != job_id {
            return;
        }
        job.status = job
            .termination
            .unwrap_or_else(|| JobStatus::from_run(out.status));
        job.stage = Stage::Terminal;
        job.output = Some(out);
        job.kill = Arc::new(|| {});
        self.write_record(st, job_id);
    }

    fn block(
        &self,
        timeout: Duration,
        mut done: impl FnMut(&Inner) -> bool,
    ) -> MutexGuard<'_, Inner> {
        let deadline = Instant::now() + timeout;
        let mut st = self.lock();
        loop {
            if done(&st) {
                return st;
            }
            let now = Instant::now();
            if now >= deadline {
                return st;
            }
            st = self
                .cv
                .wait_timeout(st, deadline - now)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    #[cfg(test)]
    pub(crate) fn poll(&self, job_id: &str) -> PollResult {
        poll_state(&self.lock(), job_id)
    }

    pub fn cancel(&self, job_id: &str) -> PollResult {
        let mut st = self.lock();
        let Some((id, job)) = st.job.as_mut() else {
            return poll_state(&st, job_id);
        };
        if id != job_id || job.stage == Stage::Terminal {
            return poll_state(&st, job_id);
        }
        job.termination = Some(JobStatus::Cancelled);
        job.finalize_abort.store(true, Ordering::SeqCst);
        (job.kill)();
        while st
            .job
            .as_ref()
            .is_some_and(|(i, j)| i == job_id && j.stage != Stage::Terminal)
        {
            st = self.cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
        poll_state(&st, job_id)
    }

    pub fn wait(&self, job_id: &str, timeout_ms: Option<f64>) -> PollResult {
        let timeout = ms(clamp_wait(timeout_ms.unwrap_or(DEFAULT_WAIT_TIMEOUT)));
        let st = self.block(timeout, |st| {
            st.job
                .as_ref()
                .filter(|(id, _)| id == job_id)
                .is_none_or(|(_, j)| j.status != JobStatus::Running)
        });
        poll_state(&st, job_id)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    include!("job/tests.rs");
}
