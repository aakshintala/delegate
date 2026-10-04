//! Drives the real `delegate` binary as a process against a fake cursor-agent.

use serde_json::Value;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const FAKE_AGENT: &str = r#"#!/bin/sh
printf '%s\n' "$@" > "$(dirname "$0")/argv.txt"
rel="$(dirname "$0")/go"
# Stays RUNNING behind a grandchild: `sleep` inherits the agent's process group, so
# killing only the agent would leave it behind.
case "$*" in
  *CANCELME*)
    sleep 300 &
    echo $! > "$(dirname "$0")/grandchild.pid"
    echo $$ > "$(dirname "$0")/agent.pid"
    printf '%s\n' '{"type":"system","subtype":"init","session_id":"s-init"}'
    i=0
    while [ ! -e "$rel" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i+1)); done
    printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"late\nSTATUS: DONE","session_id":"s-9"}'
    exit 0
    ;;
esac
# Silent jobs emit nothing, so the model-idle window is what kills them.
case "$*" in
  *SILENT*)
    i=0
    while [ ! -e "$rel" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i+1)); done
    exit 0
    ;;
esac
case "$*" in
  *SLOW*) printf '%s\n' '{"type":"system","subtype":"init","session_id":"s-init"}' ;;
esac
printf '%s\n' '{"type":"tool_call","subtype":"started","tool_call":{"shellToolCall":{"args":{"command":"ls"}}}}'
case "$*" in
  *SLOW*)
    i=0
    while [ ! -e "$rel" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i+1)); done
    ;;
esac
case "$*" in
  *COMMIT*)
    printf 'x\n' >> calc.py
    git add calc.py && git commit -q -m "Fix add"
    ;;
esac
case "$*" in
  *DIRTY*)
    printf 'x\n' >> calc.py
    git add calc.py && git commit -q -m "Fix add"
    printf 'leftover\n' >> extra.py
    ;;
esac
case "$*" in
  *ASKME*) printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"Which db?\nSTATUS: NEEDS_CONTEXT","session_id":"s-2"}' ;;
  *) printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"391\nSTATUS: DONE","session_id":"s-1"}' ;;
esac
"#;

struct Env {
    dir: PathBuf,
}

impl Env {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("cdm-cli-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let agent = dir.join("agent.sh");
        std::fs::write(&agent, FAKE_AGENT).unwrap();
        std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();
        Env { dir }
    }

    /// Waits for the CANCELME fake to write its pids; on timeout, shows the job record.
    fn until_agent_spawned(&self, id: &str) {
        let t = Instant::now();
        while !(self.dir.join("agent.pid").exists() && self.dir.join("grandchild.pid").exists()) {
            assert!(
                t.elapsed() < Duration::from_secs(10),
                "timed out: agent spawn; record: {}",
                self.record(id)
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Lets SLOW jobs finish; until then they stay RUNNING (10s cap).
    fn release(&self) {
        std::fs::write(self.dir.join("go"), "").unwrap();
    }

    fn argv(&self) -> Vec<String> {
        let s = std::fs::read_to_string(self.dir.join("argv.txt")).unwrap();
        s.lines().map(String::from).collect()
    }

    fn delegate(&self, args: &[&str], stdin: Option<&str>) -> Output {
        self.delegate_env(args, stdin, &[])
    }

    fn delegate_env(&self, args: &[&str], stdin: Option<&str>, env: &[(&str, &str)]) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_delegate"));
        cmd.args(args)
            .current_dir(&self.dir)
            .env("TMPDIR", &self.dir)
            .env("CURSOR_AGENT_BIN", self.dir.join("agent.sh"))
            .env("DELEGATE_HEARTBEAT_MS", "100");
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut si = child.stdin.take().unwrap();
        if let Some(s) = stdin {
            let _ = si.write_all(s.as_bytes());
        }
        drop(si);
        child.wait_with_output().unwrap()
    }

    fn run(&self, prompt: &str) -> String {
        self.ok(&["run", "--model", "composer-2.5"], prompt, &[])
    }

    fn ok(&self, args: &[&str], prompt: &str, env: &[(&str, &str)]) -> String {
        self.ok_output(&self.delegate_env(args, Some(prompt), env))
    }

    fn ok_output(&self, out: &Output) -> String {
        assert!(
            out.status.success(),
            "exit {:?}\nstderr: {}\nstdout: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
        String::from_utf8(out.stdout.clone())
            .unwrap()
            .trim()
            .to_string()
    }

    fn record(&self, id: &str) -> Value {
        let p: PathBuf = self.dir.join("delegate-jobs").join(format!("{id}.json"));
        serde_json::from_str(&std::fs::read_to_string(p).unwrap()).unwrap()
    }

    fn wait_terminal(&self, id: &str) -> Value {
        let out = self.delegate(&["watch", id, "--timeout", "10"], None);
        assert_eq!(out.status.code(), Some(0));
        serde_json::from_slice(&out.stdout).unwrap()
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn alive(pid: i64) -> bool {
    unsafe { libc::kill(pid as i32, 0) == 0 }
}

fn until(what: &str, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < Duration::from_secs(10), "timed out: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn run_returns_id_while_job_runs_then_it_finishes() {
    let e = Env::new("run");
    let id = e.run("SLOW do it");
    assert_eq!(id.len(), 36);
    let r = e.record(&id);
    assert_eq!(r["status"], "RUNNING");
    assert_eq!(r["resume"]["model"], "composer-2.5");
    assert!(r["resume"].get("capability").is_none(), "{r}");
    // The SLOW fake prints its init line at once; the record picks up the id as soon as it arrives.
    until("session id in record", || {
        e.record(&id)["resume"]["sessionId"] == "s-init"
    });
    let pid = r["supervisorPid"].as_i64().unwrap();
    assert!(alive(pid), "supervisor must outlive `run`");
    e.release();

    let done = e.wait_terminal(&id);
    let argv = e.argv();
    assert!(
        argv.windows(2).any(|w| w == ["--sandbox", "disabled"]),
        "{argv:?}"
    );
    assert!(argv.contains(&"--force".to_string()), "{argv:?}");
    assert_eq!(done["status"], "DONE");
    assert_eq!(done["result"]["text"], "391\nSTATUS: DONE");
    assert_eq!(done["result"]["jobId"], id.as_str());
    assert_eq!(done["resume"]["sessionId"], "s-1");
    until("supervisor exit", || !alive(pid));
}

#[test]
fn running_record_heartbeat_advances() {
    let e = Env::new("hb");
    let id = e.run("SLOW");
    let a = e.record(&id)["lastHeartbeatAt"].as_f64().unwrap();
    until("heartbeat", || {
        let r = e.record(&id);
        r["status"] != "RUNNING" || r["lastHeartbeatAt"].as_f64().unwrap() > a
    });
    assert_ne!(
        e.record(&id)["status"],
        "DONE",
        "heartbeat must advance while RUNNING"
    );
    e.release();
    e.wait_terminal(&id);
}

#[test]
fn needs_context_is_parsed() {
    let e = Env::new("nc");
    let id = e.run("ASKME");
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "NEEDS_CONTEXT");
    assert_eq!(done["resume"]["sessionId"], "s-2");
}

#[test]
fn prompt_file_and_write_argv() {
    let e = Env::new("rw");
    std::fs::write(e.dir.join("p.txt"), "from file").unwrap();
    let out = e.delegate(
        &["run", "--model", "composer-2.5", "--prompt-file", "p.txt"],
        None,
    );
    assert!(out.status.success());
    let id = String::from_utf8(out.stdout).unwrap().trim().to_string();
    e.wait_terminal(&id);
    let argv = e.argv();
    assert!(
        argv.windows(2).any(|w| w == ["--sandbox", "disabled"]),
        "{argv:?}"
    );
    assert!(argv.contains(&"--force".to_string()), "{argv:?}");
}

#[test]
fn watch_prints_all_in_order_and_times_out_with_exit_1() {
    let e = Env::new("watch");
    let slow = e.run("SLOW");
    let fast = e.run("quick");
    let out = e.delegate(&["watch", &slow, &fast, "--timeout", "1"], None);
    assert_eq!(out.status.code(), Some(1));
    let lines: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["status"], "RUNNING");
    let stderr = String::from_utf8(out.stderr).unwrap();
    let errors: Vec<&str> = stderr.lines().collect();
    let expected_ids: Vec<&String> = [(&slow, &lines[0]), (&fast, &lines[1])]
        .into_iter()
        .filter(|(_, record)| record["status"] == "RUNNING")
        .map(|(id, _)| id)
        .collect();
    assert_eq!(errors.len(), expected_ids.len(), "{stderr:?}");
    for (error, id) in errors.iter().zip(expected_ids) {
        assert!(
            error.starts_with(&format!(
                "delegate watch: timed out after 1s; {id} still RUNNING"
            )),
            "{error:?}"
        );
        assert!(
            error.contains("see its progress field; run watch again"),
            "{error:?}"
        );
    }

    e.release();
    let out = e.delegate(&["watch", &fast, &slow, "--timeout", "10"], None);
    assert_eq!(out.status.code(), Some(0));
    let lines: Vec<Value> = String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines[0]["result"]["jobId"], fast.as_str());
    assert_eq!(lines[1]["result"]["jobId"], slow.as_str());
}

#[test]
fn watch_unknown_job_exits_2() {
    let e = Env::new("unknown");
    let out = e.delegate(&["watch", "nope"], None);
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown job nope"));
}

#[test]
fn bad_run_input_exits_2() {
    let e = Env::new("bad");
    let out = e.delegate(&["run", "--model", "nope-9"], Some("hi"));
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("nope-9") && err.contains("composer-2.5"),
        "{err}"
    );

    let out = e.delegate(&["run", "--model", "composer-2.5"], Some("  \n"));
    assert_eq!(out.status.code(), Some(2));
    std::fs::write(e.dir.join("empty.txt"), "").unwrap();
    let out = e.delegate(
        &[
            "run",
            "--model",
            "composer-2.5",
            "--prompt-file",
            "empty.txt",
        ],
        None,
    );
    assert_eq!(out.status.code(), Some(2));
    assert!(!Path::new(&e.dir.join("delegate-jobs")).exists());
    for bad in ["0", "-5"] {
        let out = e.delegate(
            &["run", "--model", "composer-2.5", "--tool-idle-ms", bad],
            Some("hi"),
        );
        assert_eq!(out.status.code(), Some(2), "{bad}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains(&format!("invalid --tool-idle-ms {bad}")),
            "{err}"
        );
    }
}

fn init_repo(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let hooks = dir.with_file_name("githooks");
    std::fs::create_dir_all(&hooks).unwrap();
    let g = |args: &[&str]| {
        let out = Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    g(&["init", "-q"]);
    g(&["config", "user.email", "t@t.com"]);
    g(&["config", "user.name", "t"]);
    g(&["config", "commit.gpgsign", "false"]);
    g(&["config", "core.hooksPath", hooks.to_str().unwrap()]);
    std::fs::write(dir.join("calc.py"), "def add(a, b):\n    return a - b\n").unwrap();
    g(&["add", "calc.py"]);
    g(&["commit", "-q", "-m", "init"]);
}

fn git_head(dir: &Path) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

#[test]
fn gate_pass_keeps_done() {
    let e = Env::new("gate-ok");
    let id = e.ok(
        &["run", "--model", "composer-2.5", "--gate", "true"],
        "ship it",
        &[],
    );
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "DONE");
    assert_eq!(done["result"]["gateResult"]["passed"], true);
    assert_eq!(done["result"]["gateResult"]["exitCode"], 0);
    assert_eq!(done["result"]["gateResult"]["command"], "true");
    assert_eq!(done["resume"]["gate"], "true");
    assert!(done["resume"]["toolIdleMs"].is_null());
}

#[test]
fn gate_fail_downgrades_and_records_output() {
    let e = Env::new("gate-bad");
    let id = e.ok(
        &[
            "run",
            "--model",
            "composer-2.5",
            "--gate",
            "echo AssertionError; exit 1",
            "--tool-idle-ms",
            "2500",
        ],
        "ship it",
        &[],
    );
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "DONE_WITH_CONCERNS");
    assert_eq!(done["result"]["gateResult"]["passed"], false);
    assert_eq!(done["result"]["gateResult"]["exitCode"], 1);
    assert_eq!(
        done["result"]["gateResult"]["command"],
        "echo AssertionError; exit 1"
    );
    assert!(
        done["result"]["gateResult"]["outputTail"]
            .as_str()
            .unwrap()
            .contains("AssertionError"),
        "{done}"
    );
    assert_eq!(done["resume"]["gate"], "echo AssertionError; exit 1");
    assert_eq!(done["resume"]["toolIdleMs"], serde_json::json!(2500));
}

#[test]
fn gate_killed_after_tool_idle_window() {
    let e = Env::new("gate-timeout");
    let id = e.ok(
        &[
            "run",
            "--model",
            "composer-2.5",
            "--tool-idle-ms",
            "400",
            "--gate",
            "sleep 30",
        ],
        "ship it",
        &[],
    );
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "DONE_WITH_CONCERNS");
    assert_eq!(done["result"]["gateResult"]["passed"], false);
    assert!(
        done["result"]["gateResult"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("timeout"),
        "{done}"
    );
}

#[test]
fn change_set_lists_the_commit() {
    let e = Env::new("cset");
    let repo = e.dir.join("repo");
    init_repo(&repo);
    let before = git_head(&repo);
    let id = e.ok(
        &[
            "run",
            "--model",
            "composer-2.5",
            "--cwd",
            repo.to_str().unwrap(),
        ],
        "COMMIT the fix",
        &[],
    );
    let done = e.wait_terminal(&id);
    let head = git_head(&repo);
    assert_ne!(head, before);
    assert_eq!(done["status"], "DONE");
    let commits = done["result"]["changeSet"]["newCommits"]
        .as_array()
        .unwrap();
    assert!(
        commits.iter().any(|c| c.as_str() == Some(head.as_str())),
        "{commits:?} head {head}"
    );
    let files = done["result"]["changeSet"]["filesChanged"]
        .as_array()
        .unwrap();
    assert!(files.iter().any(|f| f.as_str() == Some("calc.py")));
    assert_eq!(done["result"]["changeSet"]["dirtyAfter"], false);
    assert!(done["result"]["concerns"].is_null());
}

#[test]
fn dirty_tree_after_commit_is_a_concern() {
    let e = Env::new("dirty");
    let repo = e.dir.join("repo");
    init_repo(&repo);
    let id = e.ok(
        &[
            "run",
            "--model",
            "composer-2.5",
            "--cwd",
            repo.to_str().unwrap(),
        ],
        "DIRTY the tree",
        &[],
    );
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "DONE_WITH_CONCERNS");
    assert_eq!(done["result"]["changeSet"]["dirtyAfter"], true);
    let files = done["result"]["changeSet"]["uncommittedFiles"]
        .as_array()
        .unwrap();
    assert!(
        files.iter().any(|f| f.as_str() == Some("extra.py")),
        "{files:?}"
    );
    let concerns = done["result"]["concerns"].as_array().unwrap();
    assert!(
        concerns
            .iter()
            .any(|c| c.as_str().unwrap().contains("still dirty")),
        "{concerns:?}"
    );
}

#[test]
fn tool_idle_stalls_a_quiet_tool_call() {
    let e = Env::new("tool-idle");
    let id = e.ok(
        &["run", "--model", "composer-2.5", "--tool-idle-ms", "300"],
        "SLOW",
        &[],
    );
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "STALLED");
    let text = done["result"]["text"].as_str().unwrap();
    assert!(text.contains("Idle watchdog killed this job"), "{text}");
}

#[test]
fn silent_agent_stalls_and_record_says_why() {
    let e = Env::new("stall");
    let id = e.ok(
        &["run", "--model", "composer-2.5"],
        "SILENT",
        &[("DELEGATE_IDLE_MS", "200")],
    );
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "STALLED");
    let text = done["result"]["text"].as_str().unwrap();
    assert!(text.contains("Idle watchdog killed this job"), "{text}");
}

#[test]
fn two_runs_in_the_same_cwd_both_reach_terminal() {
    // No serialisation: a second run in the same directory starts while the first is RUNNING.
    let e = Env::new("parallel");
    let a = e.run("SLOW first");
    let b = e.run("SLOW second");
    assert_eq!(e.record(&a)["status"], "RUNNING");
    assert_eq!(e.record(&b)["status"], "RUNNING");
    e.release();
    assert_eq!(e.wait_terminal(&a)["status"], "DONE");
    assert_eq!(e.wait_terminal(&b)["status"], "DONE");
}

#[test]
fn capability_flag_is_an_unknown_flag_error() {
    let e = Env::new("capflag");
    let out = e.delegate(
        &[
            "run",
            "--model",
            "composer-2.5",
            "--capability",
            "read-only",
        ],
        Some("hi"),
    );
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("unknown flag --capability"), "{err}");
    assert!(out.stdout.is_empty());

    let out = e.delegate(
        &["resume", "whatever", "--capability", "read-only"],
        Some("hi"),
    );
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("unknown flag --capability"), "{err}");
    assert!(out.stdout.is_empty());
}

#[test]
fn watch_reports_dead_supervisor() {
    let e = Env::new("dead-sup");
    let id = e.run("SLOW");
    until("cursor init session id", || {
        e.record(&id)["resume"]["sessionId"] == "s-init"
    });
    let pid = e.record(&id)["supervisorPid"].as_i64().unwrap();
    unsafe { libc::kill(pid as i32, libc::SIGKILL) };
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "ERROR");
    assert_eq!(done["result"]["text"], "supervisor died");
    assert_eq!(done["result"]["sessionId"], "s-init");
    assert_eq!(e.record(&id)["status"], "ERROR");
    assert_eq!(e.record(&id)["result"]["text"], "supervisor died");
    e.release();
}

impl Env {
    fn resume(&self, id: &str, extra: &[&str], prompt: Option<&str>) -> Output {
        let mut args: Vec<&str> = vec!["resume", id];
        args.extend(extra);
        self.delegate(&args, prompt)
    }

    fn resume_ok(&self, id: &str, extra: &[&str], prompt: &str) -> String {
        let out = self.resume(id, extra, Some(prompt));
        assert!(
            out.status.success(),
            "exit {:?}\nstderr: {}\nstdout: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }
}

#[test]
fn resume_continues_session_and_links_chain() {
    let e = Env::new("resume");
    let a = e.run("first brief");
    let first = e.wait_terminal(&a);
    assert_eq!(first["status"], "DONE");
    assert_eq!(first["resume"]["sessionId"], "s-1");
    let pid = first["supervisorPid"].as_i64().unwrap();
    // The original supervisor is gone: everything resume needs is in the record.
    until("supervisor exit", || !alive(pid));

    let b = e.resume_ok(&a, &[], "follow up");
    assert_eq!(b.len(), 36);
    let done = e.wait_terminal(&b);
    assert_eq!(done["status"], "DONE");
    assert_eq!(done["result"]["jobId"], b.as_str());
    assert_eq!(done["resume"]["sessionId"], "s-1");
    // No heartbeat/progress on a terminal record.
    assert!(done.get("lastHeartbeatAt").is_none(), "{done}");
    assert!(done.get("progress").is_none(), "{done}");
    // The fake agent saw the resume flag with the original model and cwd.
    let argv = e.argv();
    assert!(
        argv.windows(2).any(|w| w == ["--resume", "s-1"]),
        "{argv:?}"
    );
    assert!(argv.contains(&"composer-2.5".to_string()), "{argv:?}");
    assert!(
        argv.windows(2).any(|w| w == ["--sandbox", "disabled"]),
        "{argv:?}"
    );
    assert_eq!(done["resume"]["cwd"], first["resume"]["cwd"]);
    assert!(done["resume"].get("capability").is_none(), "{done}");
    // Chain links: B points back, A points forward, nothing else on A changed.
    assert_eq!(done["resumedFrom"], a.as_str());
    let again = e.record(&a);
    assert_eq!(again["supersededBy"], b.as_str());
    assert_eq!(again["result"], first["result"]);
    assert_eq!(again["resume"], first["resume"]);
}

#[test]
fn resume_overrides_replace_stored_values() {
    let e = Env::new("resume-ov");
    let a = e.run("first");
    e.wait_terminal(&a);

    let b = e.resume_ok(&a, &["--model", "grok-4.7-high"], "again");
    let done = e.wait_terminal(&b);
    assert_eq!(done["resume"]["model"], "grok-4.7-high");
    assert!(
        e.argv().contains(&"grok-4.7-high".to_string()),
        "{:?}",
        e.argv()
    );

    std::fs::write(e.dir.join("follow.txt"), "gated follow-up").unwrap();
    let out = e.resume(&a, &["--gate", "true", "--prompt-file", "follow.txt"], None);
    let d = e.ok_output(&out);
    let done = e.wait_terminal(&d);
    assert_eq!(done["resume"]["gate"], "true");
    assert_eq!(done["result"]["gateResult"]["command"], "true");
    assert_eq!(done["result"]["gateResult"]["passed"], true);
}

#[test]
fn resume_cross_backend_and_unknown_models_exit_2() {
    let e = Env::new("resume-xb");
    let a = e.run("first");
    e.wait_terminal(&a);
    for m in ["openai-codex/gpt-6-luna:xhigh", "claude-sonnet-5-5"] {
        let out = e.resume(&a, &["--model", m], Some("again"));
        assert_eq!(out.status.code(), Some(2), "{m}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("backend"), "{err}");
        assert!(out.stdout.is_empty());
    }
    let out = e.resume(&a, &["--model", "nope-9"], Some("again"));
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("nope-9") && err.contains("composer-2.5"),
        "{err}"
    );
}

#[test]
fn resume_running_job_exits_2() {
    let e = Env::new("resume-run");
    let slow = e.run("SLOW");
    assert_eq!(e.record(&slow)["status"], "RUNNING");
    let out = e.resume(&slow, &[], Some("follow"));
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("RUNNING"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    e.release();
    e.wait_terminal(&slow);
}

#[test]
fn resume_unknown_no_session_and_empty_prompt_exit_2() {
    let e = Env::new("resume-err");
    let out = e.resume("nope", &[], Some("hi"));
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("unknown job nope"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let a = e.run("first");
    e.wait_terminal(&a);
    let out = e.resume(&a, &[], Some("  \n"));
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("prompt is empty"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // A record with no session id is not resumable.
    let slow = e.run("SILENT");
    let cancelled = e.delegate(&["cancel", &slow], None);
    assert_eq!(cancelled.status.code(), Some(0));
    assert_eq!(e.record(&slow)["status"], "CANCELLED");
    let out = e.resume(&slow, &[], Some("follow"));
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("session"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn read_pid(dir: &std::path::Path, name: &str) -> i64 {
    std::fs::read_to_string(dir.join(name))
        .unwrap()
        .trim()
        .parse()
        .unwrap()
}

#[test]
fn cancel_kills_agent_and_grandchild() {
    let e = Env::new("cancel");
    let id = e.run("CANCELME");
    e.until_agent_spawned(&id);
    until("cursor init session id", || {
        e.record(&id)["resume"]["sessionId"] == "s-init"
    });
    assert_eq!(e.record(&id)["status"], "RUNNING");
    assert_eq!(e.record(&id)["resume"]["sessionId"], "s-init");
    assert_eq!(e.record(&id)["resume"]["backend"], "cursor");
    let agent = read_pid(&e.dir, "agent.pid");
    let grand = read_pid(&e.dir, "grandchild.pid");
    assert!(alive(agent) && alive(grand), "agent {agent} grand {grand}");

    let out = e.delegate(&["cancel", &id], None);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let final_rec: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(final_rec["status"], "CANCELLED");
    assert_eq!(final_rec["result"]["status"], "CANCELLED");
    assert_eq!(final_rec["result"]["jobId"], id.as_str());
    assert_eq!(final_rec["result"]["sessionId"], "s-init");
    assert_eq!(final_rec["resume"]["sessionId"], "s-init");
    until("agent death", || !alive(agent) && !alive(grand));
    assert_eq!(e.record(&id)["status"], "CANCELLED");
    let resumed = e.resume(&id, &[], Some("continue"));
    assert_eq!(
        resumed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    let resumed_id = String::from_utf8(resumed.stdout)
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(e.wait_terminal(&resumed_id)["status"], "DONE");
    let argv = e.argv();
    assert!(
        argv.windows(2).any(|w| w == ["--resume", "s-init"]),
        "{argv:?}"
    );
}

#[test]
fn cancel_sigkill_fallback_after_stopped_supervisor() {
    let e = Env::new("cancel-kill");
    let id = e.run("CANCELME");
    e.until_agent_spawned(&id);
    until("cursor init session id", || {
        e.record(&id)["resume"]["sessionId"] == "s-init"
    });
    assert_eq!(e.record(&id)["status"], "RUNNING");
    let sup = e.record(&id)["supervisorPid"].as_i64().unwrap() as i32;
    let agent = read_pid(&e.dir, "agent.pid");
    let grand = read_pid(&e.dir, "grandchild.pid");
    // Freeze the supervisor so SIGTERM can never be answered: `cancel` must escalate
    // through the 5s wait, SIGKILL both groups and write CANCELLED itself.
    unsafe { libc::kill(sup, libc::SIGSTOP) };
    let start = Instant::now();
    let out = e.delegate(&["cancel", &id], None);
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let final_rec: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(final_rec["status"], "CANCELLED");
    assert_eq!(final_rec["result"]["status"], "CANCELLED");
    assert_eq!(final_rec["result"]["sessionId"], "s-init");
    assert_eq!(final_rec["resume"]["sessionId"], "s-init");
    assert_eq!(final_rec["result"]["backend"], "cursor");
    // The full 5s SIGTERM wait elapsed: the fast supervisor path could not have fired.
    assert!(start.elapsed() >= Duration::from_secs(5));
    until("agent death", || !alive(agent) && !alive(grand));
    assert_eq!(e.record(&id)["status"], "CANCELLED");
}

#[test]
fn cancel_on_terminal_job_prints_record_and_exits_0() {
    let e = Env::new("cancel-noop");
    let id = e.run("quick");
    let done = e.wait_terminal(&id);
    let out = e.delegate(&["cancel", &id], None);
    assert_eq!(out.status.code(), Some(0));
    let printed: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(printed, done);
}

#[test]
fn cancel_unknown_job_exits_2() {
    let e = Env::new("cancel-unknown");
    let out = e.delegate(&["cancel", "nope"], None);
    assert_eq!(out.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("unknown job nope"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
