// Ports of the job-registry tests that still apply to the single-job supervisor.

use super::*;
use crate::backends::types::{BackendResult, Event, ProgressSnapshotRaw, Runner, Spawned};
use crate::output::derive_status;
use crate::status_record::{FileStatusRecordWriter, job_record_path};
use crate::types::{PollResult, ResumeContext};
use std::collections::HashMap;
use std::sync::mpsc;
use std::thread::sleep;
use std::time::{Duration, Instant};

pub(crate) fn spec_of(over: impl FnOnce(&mut JobSpec)) -> JobSpec {
    let mut s = JobSpec {
        bin: "agent".into(),
        argv: vec!["run".into()],
        cwd: "/tmp".into(),
        model: "composer-2.5".into(),
        backend: "cursor".into(),
        path: None,
        head_before: None,
        gate: String::new(),
        idle_ms: None,
        tool_idle_ms: None,
        price_map: HashMap::new(),
        resume_context: ResumeContext {
            model: "composer-2.5".into(),
            gate: String::new(),
        },
    };
    over(&mut s);
    s
}

enum Msg {
    Ev(Event),
    Finish(BackendResult),
}

pub(crate) struct FakeHandle {
    tx: Mutex<mpsc::Sender<Msg>>,
    pub killed: Arc<Mutex<Vec<&'static str>>>,
}

impl FakeHandle {
    pub fn finish(&self, r: BackendResult) {
        let _ = self.tx.lock().unwrap().send(Msg::Finish(r));
    }
    pub fn progress(
        &self,
        tool: &str,
        tokens: f64,
        assistant: Option<&str>,
        files: &[&str],
        phase: Option<&str>,
    ) {
        let snap = ProgressSnapshotRaw {
            last_tool: Some(tool.into()),
            tokens_so_far: tokens,
            last_assistant: assistant.map(Into::into),
            files_touched: files.iter().map(|f| f.to_string()).collect(),
            phase: phase.map(Into::into),
            session_id: None,
        };
        let _ = self.tx.lock().unwrap().send(Msg::Ev(Event::Progress(snap)));
    }
    pub fn session_id(&self, id: &str) {
        let snap = ProgressSnapshotRaw {
            session_id: Some(id.into()),
            ..Default::default()
        };
        let _ = self.tx.lock().unwrap().send(Msg::Ev(Event::Progress(snap)));
    }
    pub fn activity(&self) {
        let _ = self.tx.lock().unwrap().send(Msg::Ev(Event::Activity));
    }
    pub fn killed(&self) -> Vec<&'static str> {
        self.killed.lock().unwrap().clone()
    }
}

#[derive(Default)]
pub(crate) struct FakeBackend {
    handles: Mutex<Vec<Arc<FakeHandle>>>,
    pub auto: Mutex<Option<BackendResult>>,
}

impl FakeBackend {
    pub fn handle(&self, i: usize) -> Arc<FakeHandle> {
        for _ in 0..500 {
            if let Some(h) = self.handles.lock().unwrap().get(i) {
                return Arc::clone(h);
            }
            sleep(Duration::from_millis(2));
        }
        panic!("no handle {i}");
    }
}

impl Runner for FakeBackend {
    fn run(&self, spec: &JobSpec) -> Spawned {
        let session_id = spec
            .argv
            .windows(2)
            .find(|w| w[0] == "--session-id")
            .map(|w| w[1].clone());
        let (tx, rx) = mpsc::channel();
        if let Some(r) = &*self.auto.lock().unwrap() {
            tx.send(Msg::Finish(r.clone())).unwrap();
        }
        let killed = Arc::new(Mutex::new(Vec::new()));
        let h = Arc::new(FakeHandle {
            tx: Mutex::new(tx.clone()),
            killed: Arc::clone(&killed),
        });
        self.handles.lock().unwrap().push(h);
        let tx = Mutex::new(tx);
        Spawned {
            session_id,
            kill: Box::new(move || {
                killed.lock().unwrap().push("SIGTERM");
                let _ = tx
                    .lock()
                    .unwrap()
                    .send(Msg::Finish(BackendResult::default()));
            }),
            drive: Box::new(move |on| {
                loop {
                    match rx.recv() {
                        Ok(Msg::Ev(e)) => on(e),
                        Ok(Msg::Finish(r)) => return r,
                        Err(_) => return BackendResult::default(),
                    }
                }
            }),
        }
    }
}

pub(crate) fn fake_finalize(res: &BackendResult, ctx: &FinalizeCtx) -> RunOutput {
    RunOutput {
        status: derive_status(&res.text, res.is_error, res.clean_exit),
        text: res.text.clone(),
        session_id: res.session_id.clone(),
        backend: ctx.backend.clone(),
        model: ctx.model.clone(),
        usage: res.usage.clone(),
        cost_usd: res.cost_usd,
        cost_estimated: res.cost_usd.is_none(),
        duration_ms: res.duration_ms,
        job_id: ctx.job_id.clone(),
        stderr_tail: None,
        gate_result: None,
        change_set: None,
        concerns: None,
        permission_denials: res.permission_denials.clone(),
    }
}

pub(crate) fn done_ok() -> BackendResult {
    BackendResult {
        text: "ok\nSTATUS: DONE".into(),
        is_error: Some(false),
        clean_exit: true,
        stderr: String::new(),
        ..Default::default()
    }
}

#[derive(Default)]
struct Spy(Mutex<Vec<(String, serde_json::Value)>>);

impl StatusRecordWriter for Spy {
    fn write(&self, job_id: &str, record: &PollResult) {
        self.0
            .lock()
            .unwrap()
            .push((job_id.into(), serde_json::to_value(record).unwrap()));
    }
}

impl Spy {
    fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
    fn nth(&self, i: usize) -> (String, serde_json::Value) {
        self.0.lock().unwrap()[i].clone()
    }
    fn last(&self) -> (String, serde_json::Value) {
        self.0.lock().unwrap().last().unwrap().clone()
    }
}

struct Setup {
    reg: Arc<JobHandle>,
    fake: Arc<FakeBackend>,
    spy: Arc<Spy>,
}

fn setup_with(f: impl FnOnce(&mut JobDeps)) -> Setup {
    let fake = Arc::new(FakeBackend::default());
    let spy = Arc::new(Spy::default());
    let mut deps = JobDeps::new(fake.clone(), None, None);
    deps.finalize = Arc::new(fake_finalize);
    deps.finalize_stall = Arc::new(fake_finalize);
    deps.status_writer = spy.clone();
    f(&mut deps);
    Setup {
        reg: JobHandle::new(deps),
        fake,
        spy,
    }
}

fn setup() -> Setup {
    setup_with(|_| {})
}

fn settle(reg: &JobHandle, id: &str) -> String {
    for _ in 0..500 {
        let p = reg.poll(id);
        if p.status_label() != "RUNNING" {
            return p.status_label().to_string();
        }
        sleep(Duration::from_millis(2));
    }
    "RUNNING".into()
}

fn terminal_text(reg: &JobHandle, id: &str) -> String {
    match reg.poll(id) {
        PollResult::Terminal { result, .. } => result.text,
        other => panic!("expected terminal poll, got {other:?}"),
    }
}

fn status_of(v: &serde_json::Value) -> &str {
    v["status"].as_str().unwrap()
}

#[test]
fn heartbeat_refreshes_running_record_and_stops_at_retirement() {
    let s = setup_with(|d| d.heartbeat_ms = 100);
    let id = s.reg.dispatch(spec_of(|_| {}));
    assert_eq!(s.spy.len(), 1);

    sleep(Duration::from_millis(150));
    assert_eq!(s.spy.len(), 2);
    let (jid, rec) = s.spy.nth(1);
    assert_eq!(jid, id);
    assert_eq!(status_of(&rec), "RUNNING");
    assert!(rec["lastHeartbeatAt"].is_u64());

    sleep(Duration::from_millis(100));
    assert_eq!(s.spy.len(), 3);

    s.fake.handle(0).finish(done_ok());
    assert_eq!(settle(&s.reg, &id), "DONE");
    let after = s.spy.len();
    assert_eq!(status_of(&s.spy.last().1), "DONE");
    sleep(Duration::from_millis(300));
    assert_eq!(s.spy.len(), after);
}

#[test]
fn dispatch_persists_start_and_terminal_records() {
    let s = setup();
    let id = s.reg.dispatch(spec_of(|_| {}));
    assert_eq!(s.spy.len(), 1);
    s.fake.handle(0).finish(done_ok());
    assert_eq!(settle(&s.reg, &id), "DONE");
    assert_eq!(s.spy.len(), 2);
    assert_eq!(status_of(&s.spy.nth(1).1), "DONE");
}

#[test]
fn dispatch_seeds_running_progress_with_the_launched_session_id() {
    let s = setup();
    let id = s.reg.dispatch(spec_of(|spec| {
        spec.argv = vec!["--session-id".into(), "launch-sid".into()];
    }));
    let (recorded_id, record) = s.spy.nth(0);
    assert_eq!(recorded_id, id);
    assert_eq!(record["status"], "RUNNING");
    assert_eq!(record["progress"]["sessionId"], "launch-sid");
}

#[test]
fn session_id_progress_is_written_without_waiting_for_the_heartbeat() {
    let s = setup();
    let id = s.reg.dispatch(spec_of(|_| {}));
    s.fake.handle(0).session_id("s-init");
    for _ in 0..500 {
        if s.spy.len() > 1 {
            break;
        }
        sleep(Duration::from_millis(2));
    }
    assert_eq!(s.spy.len(), 2);
    let record = s.spy.last().1;
    assert_eq!(record["status"], "RUNNING");
    assert_eq!(record["progress"]["sessionId"], "s-init");
    assert_eq!(serde_json::to_value(s.reg.poll(&id)).unwrap()["progress"]["sessionId"], "s-init");
    s.fake.handle(0).session_id("s-init");
    sleep(Duration::from_millis(30));
    assert_eq!(s.spy.len(), 2);
}

#[test]
fn idle_watchdog_writes_a_stalled_terminal_record() {
    let s = setup_with(|d| d.idle_ms = Some(50.0));
    let id = s.reg.dispatch(spec_of(|_| {}));
    assert_eq!(s.spy.len(), 1);
    assert_eq!(settle(&s.reg, &id), "STALLED");
    assert_eq!(s.fake.handle(0).killed(), ["SIGTERM"]);
    assert_eq!(s.spy.len(), 2);
    assert_eq!(status_of(&s.spy.nth(1).1), "STALLED");
}

#[test]
fn cancel_persists_a_cancelled_terminal_record() {
    let s = setup();
    let id = s.reg.dispatch(spec_of(|_| {}));
    assert_eq!(s.spy.len(), 1);
    s.reg.cancel(&id);
    assert_eq!(s.spy.len(), 2);
    assert_eq!(status_of(&s.spy.nth(1).1), "CANCELLED");
    assert_eq!(s.fake.handle(0).killed(), ["SIGTERM"]);
}

#[test]
fn a_panicking_status_writer_does_not_affect_completion() {
    struct Bad(Mutex<u32>);
    impl StatusRecordWriter for Bad {
        fn write(&self, _: &str, _: &PollResult) {
            let mut n = self.0.lock().unwrap();
            *n += 1;
            if *n == 2 {
                drop(n);
                panic!("boom");
            }
        }
    }
    let bad = Arc::new(Bad(Mutex::new(0)));
    let s = setup_with(|d| d.status_writer = bad.clone());
    let id = s.reg.dispatch(spec_of(|_| {}));
    s.fake.handle(0).finish(done_ok());
    assert_eq!(settle(&s.reg, &id), "DONE");
    assert_eq!(*bad.0.lock().unwrap(), 2);
}

#[test]
fn progress_events_do_not_trigger_status_writes() {
    let s = setup();
    let id = s.reg.dispatch(spec_of(|_| {}));
    s.fake.handle(0).progress(
        "shell",
        42.0,
        Some("running the test suite now"),
        &["src/foo.rs"],
        Some("running_tool"),
    );
    sleep(Duration::from_millis(30));
    assert_eq!(s.spy.len(), 1);
    s.fake.handle(0).finish(done_ok());
    assert_eq!(settle(&s.reg, &id), "DONE");
    assert_eq!(s.spy.len(), 2);
}

#[test]
fn file_status_record_is_overwritten_from_running_to_terminal() {
    let s = setup_with(|d| d.status_writer = Arc::new(FileStatusRecordWriter));
    let id = s.reg.dispatch(spec_of(|_| {}));
    let path = job_record_path(&id);
    let read = || -> serde_json::Value {
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap()
    };
    assert_eq!(read()["status"], "RUNNING");
    s.fake.handle(0).finish(done_ok());
    assert_eq!(settle(&s.reg, &id), "DONE");
    let terminal = read();
    assert_eq!(terminal["status"], "DONE");
    let polled = serde_json::to_value(s.reg.poll(&id)).unwrap();
    assert_eq!(terminal["result"], polled["result"]);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn dispatch_returns_immediately() {
    let s = setup();
    let t = Instant::now();
    s.reg.dispatch(spec_of(|_| {}));
    assert!(t.elapsed() < Duration::from_millis(1000));
}

#[test]
fn cancel_sigterms_the_child_and_marks_cancelled() {
    let s = setup();
    let id = s.reg.dispatch(spec_of(|_| {}));
    assert_eq!(s.reg.cancel(&id), "CANCELLED");
    assert_eq!(s.fake.handle(0).killed(), ["SIGTERM"]);
}

#[test]
fn idle_watchdog_sigterms_a_silent_job() {
    let s = setup_with(|d| d.idle_ms = Some(50.0));
    let id = s.reg.dispatch(spec_of(|_| {}));
    assert_eq!(settle(&s.reg, &id), "STALLED");
    assert_eq!(s.fake.handle(0).killed(), ["SIGTERM"]);
}

#[test]
fn a_stalled_jobs_text_summarizes_last_known_progress() {
    let s = setup_with(|d| d.idle_ms = Some(100.0));
    let id = s.reg.dispatch(spec_of(|_| {}));
    s.fake.handle(0).progress(
        "shell",
        42.0,
        Some("running the test suite now"),
        &["src/foo.rs"],
        Some("thinking"),
    );
    assert_eq!(settle(&s.reg, &id), "STALLED");
    let text = terminal_text(&s.reg, &id);
    for needle in [
        "shell",
        "42 tokens",
        "src/foo.rs",
        "running the test suite now",
    ] {
        assert!(text.contains(needle), "{needle:?} missing from {:?}", text);
    }
}

#[test]
fn a_progress_event_rearms_the_idle_watchdog() {
    let s = setup_with(|d| d.idle_ms = Some(300.0));
    let id = s.reg.dispatch(spec_of(|_| {}));
    sleep(Duration::from_millis(200));
    s.fake.handle(0).progress("shell", 1.0, None, &[], None);
    sleep(Duration::from_millis(200));
    assert!(s.fake.handle(0).killed().is_empty());
    assert_eq!(s.reg.poll(&id), "RUNNING");
}

#[test]
fn a_per_call_idle_override_beats_the_server_default() {
    let s = setup_with(|d| d.idle_ms = Some(50.0));
    let id = s.reg.dispatch(spec_of(|s| s.idle_ms = Some(Some(400.0))));
    sleep(Duration::from_millis(150));
    assert!(s.fake.handle(0).killed().is_empty());
    assert_eq!(settle(&s.reg, &id), "STALLED");
    assert_eq!(s.fake.handle(0).killed(), ["SIGTERM"]);
}

#[test]
fn a_per_call_idle_null_disables_the_watchdog() {
    let s = setup_with(|d| d.idle_ms = Some(30.0));
    let id = s.reg.dispatch(spec_of(|s| s.idle_ms = Some(None)));
    sleep(Duration::from_millis(200));
    assert!(s.fake.handle(0).killed().is_empty());
    assert_eq!(s.reg.poll(&id), "RUNNING");
}

#[test]
fn a_tool_in_flight_uses_the_tool_idle_window() {
    let s = setup_with(|d| {
        d.idle_ms = Some(50.0);
        d.tool_idle_ms = Some(400.0);
    });
    let id = s.reg.dispatch(spec_of(|_| {}));
    s.fake
        .handle(0)
        .progress("shell", 1.0, None, &[], Some("running_tool"));
    sleep(Duration::from_millis(150));
    assert!(s.fake.handle(0).killed().is_empty());
    assert_eq!(s.reg.poll(&id), "RUNNING");
    assert_eq!(settle(&s.reg, &id), "STALLED");
    assert_eq!(s.fake.handle(0).killed(), ["SIGTERM"]);
}

#[test]
fn leaving_the_tool_phase_reverts_to_the_short_window() {
    let s = setup_with(|d| {
        d.idle_ms = Some(80.0);
        d.tool_idle_ms = Some(10_000.0);
    });
    let id = s.reg.dispatch(spec_of(|_| {}));
    s.fake
        .handle(0)
        .progress("shell", 1.0, None, &[], Some("running_tool"));
    sleep(Duration::from_millis(150));
    assert_eq!(s.reg.poll(&id), "RUNNING");
    s.fake.handle(0).progress(
        "shell",
        2.0,
        Some("done with that"),
        &[],
        Some("responding"),
    );
    assert_eq!(settle(&s.reg, &id), "STALLED");
    assert_eq!(s.fake.handle(0).killed(), ["SIGTERM"]);
}

#[test]
fn a_per_call_tool_idle_override_applies_while_a_tool_is_in_flight() {
    let s = setup_with(|d| {
        d.idle_ms = Some(50.0);
        d.tool_idle_ms = Some(80.0);
    });
    let id = s.reg.dispatch(spec_of(|s| s.tool_idle_ms = Some(Some(10_000.0))));
    s.fake
        .handle(0)
        .progress("shell", 1.0, None, &[], Some("running_tool"));
    sleep(Duration::from_millis(200));
    assert!(s.fake.handle(0).killed().is_empty());
    assert_eq!(s.reg.poll(&id), "RUNNING");
}

#[test]
fn raw_activity_rearms_the_watchdog() {
    let s = setup_with(|d| d.idle_ms = Some(300.0));
    let id = s.reg.dispatch(spec_of(|_| {}));
    sleep(Duration::from_millis(200));
    s.fake.handle(0).activity();
    sleep(Duration::from_millis(200));
    assert!(s.fake.handle(0).killed().is_empty());
    assert_eq!(s.reg.poll(&id), "RUNNING");
}

#[test]
fn wait_returns_when_the_job_completes() {
    let s = setup();
    let id = s.reg.dispatch(spec_of(|_| {}));
    let h = s.fake.handle(0);
    std::thread::spawn(move || {
        sleep(Duration::from_millis(30));
        h.finish(done_ok());
    });
    assert_eq!(s.reg.wait(&id, Some(10_000.0)), "DONE");
}
