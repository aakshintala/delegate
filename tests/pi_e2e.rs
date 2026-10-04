//! Drives the real `delegate` binary as a process against a fake pi
//! that replays `tests/fixtures/contract/pi/plain-answer.stdout`.

use serde_json::Value;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

const MODEL: &str = "opencode-go/muse-spark-1.3-contributor";

const FAKE_PI: &str = r#"#!/bin/sh
# Replays a recorded `pi -p --mode json` stream. Every invocation appends its
# argv, so run + resume assertions can read both.
{
  printf '%s\n' "$@"
  printf '%s\n' '---'
} >> "$(dirname "$0")/argv.txt"
rel="$(dirname "$0")/go"
# SLOW jobs wait for the release file, so the test can assert RUNNING first.
case "$*" in
  *SLOW*)
    i=0
    while [ ! -e "$rel" ] && [ "$i" -lt 400 ]; do sleep 0.05; i=$((i+1)); done
    ;;
esac
# Replay the fixture as this session, like the real pi echoes the id back.
sid=""
prev=""
for a in "$@"; do
  if [ "$prev" = "--session-id" ]; then sid="$a"; fi
  prev="$a"
done
sed "s/afcd8926-430b-4d9a-a552-d7c6d1b900ba/$sid/g" "$DELEGATE_TEST_PI_FIXTURE"
"#;

struct Env {
    dir: PathBuf,
}

impl Env {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("cdm-pi-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let agent = dir.join("pi.sh");
        std::fs::write(&agent, FAKE_PI).unwrap();
        std::fs::set_permissions(&agent, std::fs::Permissions::from_mode(0o755)).unwrap();
        Env { dir }
    }

    fn fixture(&self) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/contract/pi/plain-answer.stdout")
    }

    /// Lets SLOW jobs finish; until then they stay RUNNING (10s cap).
    fn release(&self) {
        std::fs::write(self.dir.join("go"), "").unwrap();
    }

    /// Every recorded argv line across all invocations.
    fn argv(&self) -> Vec<String> {
        std::fs::read_to_string(self.dir.join("argv.txt"))
            .unwrap()
            .lines()
            .map(String::from)
            .collect()
    }

    fn delegate(&self, args: &[&str], stdin: Option<&str>) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_delegate"));
        cmd.args(args)
            .current_dir(&self.dir)
            .env("TMPDIR", &self.dir)
            .env("PI_BIN", self.dir.join("pi.sh"))
            .env("DELEGATE_TEST_PI_FIXTURE", self.fixture())
            .env("DELEGATE_HEARTBEAT_MS", "100")
            // Isolate from the developer machine's real host profile.
            .env(
                "DELEGATE_HOST_PROFILE",
                self.dir.join("nonexistent-profile.json"),
            );
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

    fn run_write(&self, prompt: &str) -> String {
        let out = self.delegate(&["run", "--model", MODEL], Some(prompt));
        assert!(
            out.status.success(),
            "exit {:?}\nstderr: {}\nstdout: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_string()
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

fn until(what: &str, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(t.elapsed() < Duration::from_secs(10), "timed out: {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn pi_run_finishes_with_pi_reported_cost() {
    let e = Env::new("run");
    let id = e.run_write("What is 17 * 23?");
    assert_eq!(id.len(), 36);
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "DONE");
    assert_eq!(done["result"]["text"], "391\nSTATUS: DONE");
    assert_eq!(done["result"]["backend"], "pi");
    assert_eq!(done["result"]["model"], MODEL);
    // Cost is pi's own reported sum, not a table estimate.
    let cost = done["result"]["costUsd"].as_f64().unwrap();
    assert!((cost - 0.0005365).abs() < 1e-12, "{cost}");
    assert_eq!(done["result"]["costEstimated"], false);

    // The session id was chosen before launch: argv's fresh uuid is what the
    // record carries.
    let argv = e.argv();
    let sid = argv
        .windows(2)
        .filter(|w| w[0] == "--session-id")
        .map(|w| w[1].as_str())
        .collect::<Vec<_>>();
    assert_eq!(sid.len(), 1, "{argv:?}");
    assert_eq!(sid[0].len(), 36, "{argv:?}");
    assert!(
        sid[0]
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
        "{argv:?}"
    );
    assert_eq!(done["resume"]["sessionId"], sid[0]);
    assert!(argv.contains(&"-p".to_string()), "{argv:?}");
    assert!(argv.windows(2).any(|w| w == ["--mode", "json"]), "{argv:?}");
    assert!(argv.windows(2).any(|w| w == ["--model", MODEL]), "{argv:?}");
    assert!(
        argv.iter().any(|a| a.contains("What is 17 * 23?")),
        "{argv:?}"
    );
}

#[test]
fn pi_run_stays_running_until_released() {
    let e = Env::new("slow");
    let id = e.run_write("SLOW do it");
    until("RUNNING", || e.record(&id)["status"] == "RUNNING");
    until("argv", || e.dir.join("argv.txt").exists());
    let argv = e.argv();
    let sid = argv.windows(2).find(|w| w[0] == "--session-id").unwrap()[1].clone();
    assert_eq!(e.record(&id)["resume"]["sessionId"], sid.as_str());
    e.release();
    let done = e.wait_terminal(&id);
    assert_eq!(done["status"], "DONE");
}

#[test]
fn pi_resume_reuses_the_session_id() {
    let e = Env::new("resume");
    let a = e.run_write("first brief");
    let first = e.wait_terminal(&a);
    assert_eq!(first["status"], "DONE");
    let first_sid = first["resume"]["sessionId"].as_str().unwrap().to_string();

    let out = e.delegate(&["resume", &a], Some("follow up"));
    assert!(out.status.success());
    let b = String::from_utf8(out.stdout).unwrap().trim().to_string();
    let done = e.wait_terminal(&b);
    assert_eq!(done["status"], "DONE");
    assert_eq!(done["resume"]["sessionId"], first_sid.as_str());
    // Both invocations passed the same --session-id.
    let argv = e.argv();
    let ids: Vec<&String> = argv
        .windows(2)
        .filter(|w| w[0] == "--session-id")
        .map(|w| &w[1])
        .collect();
    assert_eq!(ids.len(), 2, "{argv:?}");
    assert_eq!(ids[0], ids[1]);
    assert_eq!(ids[1].as_str(), first_sid.as_str());
}

#[test]
fn pi_cancel_kills_the_agent() {
    let e = Env::new("cancel");
    let id = e.run_write("SLOW do it");
    until("RUNNING", || e.record(&id)["status"] == "RUNNING");
    let out = e.delegate(&["cancel", &id], None);
    assert_eq!(out.status.code(), Some(0));
    let final_rec: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(final_rec["status"], "CANCELLED");
    assert_eq!(e.record(&id)["status"], "CANCELLED");
}
