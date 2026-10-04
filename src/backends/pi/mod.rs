//! The pi backend: binary, argv, spawn, and stream-json parsing.
//!
//! `pi -p --mode json` prints one JSON object per line. The `session` header
//! carries the id, `agent_end.messages` holds the final turn, and usage/cost
//! accumulate over assistant `message_end` events. pi exits 0 even on errors,
//! so success comes from the last assistant message's `stopReason`, never the
//! exit code.

pub(crate) mod doctor;

use super::types::{BackendResult, Event, ProgressSnapshotRaw, Spawned};
use crate::types::{JobSpec, Usage};
use std::sync::atomic::Ordering;

const NO_RESULT: &str = "no result line";

pub(crate) fn resolve_bin(r#override: Option<&str>) -> String {
    if let Some(o) = r#override.filter(|s| !s.is_empty()) {
        return o.to_string();
    }
    if let Ok(env) = std::env::var("PI_BIN")
        && !env.is_empty()
    {
        return env;
    }
    if let Ok(out) = std::process::Command::new("which").arg("pi").output()
        && out.status.success()
    {
        let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !found.is_empty() {
            return found;
        }
    }
    crate::util::homedir()
        .join(".local/bin/pi")
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn argv(model: &str, session: Option<&str>, prompt: &str) -> Vec<String> {
    // The session id is chosen before launch: resume passes the stored id, a fresh
    // run mints one (pi creates the session if absent, continues it if present).
    let session = session
        .map(str::to_string)
        .unwrap_or_else(crate::util::random_uuid);
    let args = vec![
        "-p".into(),
        "--mode".into(),
        "json".into(),
        "--model".into(),
        model.to_string(),
        "--session-id".into(),
        session,
        "--".into(),
        prompt.to_string(),
    ];
    args
}

/// The `--session-id` value in an argv built by [`argv`].
fn session_from_argv(argv: &[String]) -> Option<String> {
    argv.windows(2)
        .find(|w| w[0] == "--session-id")
        .map(|w| w[1].clone())
}

pub(crate) fn spawn(spec: &JobSpec) -> Spawned {
    let started = match super::start_child(spec) {
        Ok(s) => s,
        Err(msg) => return super::spawn_failed(&msg),
    };
    // The stream usually repeats this id in its `session` header; when it
    // doesn't (a crash before the first line), the result still carries it.
    let launched_session = session_from_argv(&spec.argv);
    let pid = started.pid;
    let reaped = started.reaped;
    let reaped_k = std::sync::Arc::clone(&reaped);
    let stdout = started.stdout;
    let stderr = started.stderr;
    let mut child = started.child;
    let spawned_session = launched_session.clone();
    Spawned {
        kill: super::killer(pid, reaped_k),
        session_id: spawned_session,
        drive: Box::new(move |on| {
            let mut state = PiState::default();
            let pumped = super::pump(
                stdout,
                stderr,
                on,
                move || {
                    let clean = child.wait().map(|s| s.success()).unwrap_or(false);
                    reaped.store(true, Ordering::SeqCst);
                    clean
                },
                |line| {
                    if handle_line(line, &mut state) {
                        on(Event::Progress(ProgressSnapshotRaw {
                            last_tool: state.last_tool.clone(),
                            tokens_so_far: 0.0,
                            last_assistant: state.last_assistant.clone(),
                            files_touched: Vec::new(),
                            phase: state.phase.clone(),
                            session_id: state
                                .session_id
                                .clone()
                                .or_else(|| launched_session.clone()),
                        }));
                    }
                },
            );
            finish(
                state,
                pumped.clean_exit,
                &pumped.stderr,
                launched_session.as_deref(),
            )
        }),
    }
}

/// Pure parse of a finished `pi -p --mode json` stdout. The spawn driver uses
/// [`finish`] too.
pub fn parse_stdout(stdout: &str, clean_exit: bool, stderr: &str) -> BackendResult {
    let mut state = PiState::default();
    for line in stdout.split_inclusive('\n') {
        handle_line(line.as_bytes(), &mut state);
    }
    finish(state, clean_exit, stderr, None)
}

#[derive(Debug, Default)]
struct PiState {
    session_id: Option<String>,
    agent_end: Option<serde_json::Value>,
    input: f64,
    output: f64,
    cache_read: f64,
    cache_write: f64,
    cost: f64,
    saw_assistant_end: bool,
    last_tool: Option<String>,
    last_assistant: Option<String>,
    phase: Option<String>,
}

/// A finite f64 field, or 0.0 when it is missing or malformed. Each field
/// decodes on its own so one bad number doesn't void the rest of the usage.
fn num(obj: Option<&serde_json::Value>, key: &str) -> f64 {
    obj.and_then(|u| u.get(key))
        .and_then(|n| n.as_f64())
        .filter(|n| n.is_finite())
        .unwrap_or(0.0)
}

/// Fold one stdout line into the state. Unparseable lines are ignored.
/// Returns whether a progress event should fire.
fn handle_line(line: &[u8], state: &mut PiState) -> bool {
    let trimmed = String::from_utf8_lossy(line).trim().to_string();
    if trimmed.is_empty() {
        return false;
    }
    let ev: serde_json::Value = match serde_json::from_str(&trimmed) {
        Ok(v) => v,
        Err(_) => return false,
    };
    if ev.get("type").and_then(|t| t.as_str()).is_none() {
        return false;
    }
    match ev.get("type").and_then(|t| t.as_str()) {
        Some("session") => {
            if let Some(id) = ev.get("id").and_then(|i| i.as_str()) {
                let changed = state.session_id.as_deref() != Some(id);
                state.session_id = Some(id.to_string());
                changed
            } else {
                false
            }
        }
        Some("tool_execution_start") => {
            if let Some(tool) = ev.get("toolName").and_then(|t| t.as_str()) {
                state.last_tool = Some(tool.to_string());
            }
            state.phase = Some("running_tool".into());
            true
        }
        Some("message_update") => {
            let end = ev
                .get("assistantMessageEvent")
                .filter(|e| e.get("type").and_then(|t| t.as_str()) == Some("text_end"))
                .and_then(|e| e.get("content"))
                .and_then(|c| c.as_str());
            if let Some(text) = end {
                let truncated: String = text.chars().take(200).collect();
                state.last_assistant = Some(truncated);
                state.phase = Some("responding".into());
                return true;
            }
            false
        }
        Some("message_end") => {
            let message = match ev.get("message") {
                Some(m) if m.get("role").and_then(|r| r.as_str()) == Some("assistant") => m,
                _ => return false,
            };
            let usage = message.get("usage");
            state.input += num(usage, "input");
            state.output += num(usage, "output");
            state.cache_read += num(usage, "cacheRead");
            state.cache_write += num(usage, "cacheWrite");
            state.cost += num(usage.and_then(|u| u.get("cost")), "total");
            state.saw_assistant_end = true;
            true
        }
        Some("agent_end") => {
            state.agent_end = Some(ev);
            false
        }
        _ => false,
    }
}

/// Concatenated `text` parts of one assistant message.
fn assistant_text(message: &serde_json::Value) -> String {
    message
        .get("content")
        .and_then(|c| c.as_array())
        .map(|parts| {
            parts
                .iter()
                .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
                .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

/// Turn the terminal `agent_end` (or the lack of one) into the normalized result.
/// The exit code never decides success: pi exits 0 on errors. CANCELLED is the
/// supervisor's status, never ours. `launched_session` is the `--session-id`
/// the run was launched with, used when the stream never sent a header.
fn finish(
    state: PiState,
    clean_exit: bool,
    stderr: &str,
    launched_session: Option<&str>,
) -> BackendResult {
    let session_id = state
        .session_id
        .or_else(|| launched_session.map(str::to_string));
    let Some(end) = state.agent_end else {
        return BackendResult {
            text: NO_RESULT.to_string(),
            session_id,
            is_error: Some(true),
            clean_exit,
            stderr: stderr.to_string(),
            ..Default::default()
        };
    };
    let assistant = end
        .get("messages")
        .and_then(|m| m.as_array())
        .and_then(|msgs| {
            msgs.iter()
                .rev()
                .find(|m| m.get("role").and_then(|r| r.as_str()) == Some("assistant"))
        });
    let (text, is_error) = match assistant {
        Some(m) if m.get("stopReason").and_then(|s| s.as_str()) == Some("error") => (
            m.get("errorMessage")
                .and_then(|e| e.as_str())
                .unwrap_or_default()
                .to_string(),
            Some(true),
        ),
        Some(m) => (assistant_text(m), Some(false)),
        // No assistant turn in the transcript at all: an error with no text.
        None => (NO_RESULT.to_string(), Some(true)),
    };
    BackendResult {
        text,
        session_id,
        usage: state.saw_assistant_end.then_some(Usage {
            input_tokens: state.input,
            output_tokens: state.output,
            cache_read_tokens: state.cache_read,
            cache_write_tokens: state.cache_write,
        }),
        cost_usd: state.saw_assistant_end.then_some(state.cost),
        is_error,
        duration_ms: None,
        clean_exit,
        stderr: stderr.to_string(),
        permission_denials: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_run_mints_a_session_id_and_resume_reuses_it() {
        let fresh = argv("openai-codex/gpt-6-luna", None, "hi");
        assert_eq!(
            &fresh[..6],
            [
                "-p",
                "--mode",
                "json",
                "--model",
                "openai-codex/gpt-6-luna",
                "--session-id",
            ]
        );
        let minted = &fresh[6];
        assert_eq!(minted.len(), 36, "{fresh:?}");
        assert!(
            minted
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "{fresh:?}"
        );
        assert_eq!(&fresh[7..], ["--", "hi"]);
        // A second fresh run mints a different id.
        let other = argv("openai-codex/gpt-6-luna", None, "hi");
        assert_ne!(fresh[6], other[6]);

        let resume = argv(
            "openai-codex/gpt-6-luna",
            Some("afcd8926-430b-4d9a-a552-d7c6d1b900ba"),
            "go",
        );
        assert_eq!(resume[6], "afcd8926-430b-4d9a-a552-d7c6d1b900ba");
        assert_eq!(&resume[7..], ["--", "go"]);
    }

    #[test]
    fn explicit_override_wins() {
        assert_eq!(resolve_bin(Some("/custom/pi")), "/custom/pi");
    }

    #[test]
    fn env_used_when_no_override() {
        let prev = std::env::var("PI_BIN").ok();
        unsafe { std::env::set_var("PI_BIN", "/env/pi") };
        assert_eq!(resolve_bin(None), "/env/pi");
        match prev {
            Some(v) => unsafe { std::env::set_var("PI_BIN", v) },
            None => unsafe { std::env::remove_var("PI_BIN") },
        }
    }

    #[test]
    fn session_header_emits_the_stream_session_id_as_progress() {
        let mut state = PiState::default();
        assert!(handle_line(
            br#"{"type":"session","id":"stream-sid"}"#,
            &mut state
        ));
        assert_eq!(state.session_id.as_deref(), Some("stream-sid"));
    }

    #[test]
    fn tool_start_and_text_end_emit_progress() {
        let mut state = PiState::default();
        assert!(handle_line(
            br#"{"type":"tool_execution_start","toolCallId":"c1","toolName":"bash","args":{"command":"ls"}}"#,
            &mut state,
        ));
        assert_eq!(state.last_tool.as_deref(), Some("bash"));
        assert_eq!(state.phase.as_deref(), Some("running_tool"));
        assert!(handle_line(
            br#"{"type":"message_update","assistantMessageEvent":{"type":"text_end","contentIndex":0,"content":"hello"}}"#,
            &mut state,
        ));
        assert_eq!(state.last_assistant.as_deref(), Some("hello"));
        assert_eq!(state.phase.as_deref(), Some("responding"));
        assert!(!handle_line(b"not json", &mut state));
        assert!(!handle_line(b"", &mut state));
        // Deltas and non-assistant message_ends carry no progress of their own.
        assert!(!handle_line(
            br#"{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"hi"}}"#,
            &mut state,
        ));
        assert!(!handle_line(
            br#"{"type":"message_end","message":{"role":"user","content":[]}}"#,
            &mut state,
        ));
    }

    #[test]
    fn missing_header_falls_back_to_the_launch_session_id() {
        let dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/contract/pi");
        let stdout = std::fs::read_to_string(dir.join("plain-answer.stdout")).unwrap();
        let stripped: String = stdout
            .lines()
            .filter(|l| !l.contains("\"type\":\"session\""))
            .collect::<Vec<_>>()
            .join("\n");
        assert_ne!(stripped.len(), stdout.len());
        let mut state = PiState::default();
        for line in stripped.split_inclusive('\n') {
            handle_line(line.as_bytes(), &mut state);
        }
        assert!(state.session_id.is_none());
        let res = finish(
            state,
            true,
            "",
            Some("11111111-2222-4333-8444-555555555555"),
        );
        assert_eq!(
            res.session_id.as_deref(),
            Some("11111111-2222-4333-8444-555555555555")
        );
        assert_eq!(res.text, "391\nSTATUS: DONE");
        assert_eq!(res.is_error, Some(false));
    }

    #[test]
    fn agent_end_without_an_assistant_message_is_an_error() {
        let stdout = concat!(
            "{\"type\":\"session\",\"id\":\"sid-1\"}\n",
            "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":",
            "[{\"type\":\"text\",\"text\":\"hi\"}],\"usage\":{\"input\":1,\"output\":2,",
            "\"cacheRead\":0,\"cacheWrite\":0,\"cost\":{\"total\":0.5}}}}\n",
            "{\"type\":\"agent_end\",\"messages\":[{\"role\":\"user\",\"content\":[]}],",
            "\"willRetry\":false}\n",
        );
        let res = parse_stdout(stdout, true, "");
        assert_eq!(res.text, NO_RESULT);
        assert_eq!(res.is_error, Some(true));
        assert_eq!(res.session_id.as_deref(), Some("sid-1"));
    }

    #[test]
    fn spawn_failure_is_an_error_result() {
        let spec = crate::job::tests::spec_of(|s| s.bin = "/nonexistent/pi".into());
        let spawned = spawn(&spec);
        (spawned.kill)();
        let res = (spawned.drive)(&|_| {});
        assert!(!res.clean_exit);
        assert_eq!(res.is_error, Some(true));
        assert!(!res.text.is_empty());
    }

    #[test]
    fn real_child_runs_and_sigterm_kills_it() {
        let spec = crate::job::tests::spec_of(|s| {
            s.bin = "/bin/sh".into();
            s.argv = vec!["-c".into(), "exec sleep 30".into()];
        });
        let spawned = spawn(&spec);
        let kill = spawned.kill;
        let t = std::thread::spawn(move || (spawned.drive)(&|_| {}));
        std::thread::sleep(std::time::Duration::from_millis(100));
        kill();
        let res = t.join().unwrap();
        assert!(!res.clean_exit);
    }

    /// Exit status is not stored in the fixtures. Only `cancelled` stops
    /// mid-stream (SIGTERM exits 143); every other fixture ran to `agent_end`.
    fn clean_exit_for(stem: &str) -> bool {
        !matches!(stem, "cancelled")
    }

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-12
    }

    #[test]
    fn fixtures_parse_to_a_normalized_result() {
        let dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/contract/pi");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("stdout"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "no pi stdout fixtures");

        for path in files {
            let stem = path.file_stem().unwrap().to_str().unwrap().to_string();
            let stdout = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let stderr_path = path.with_extension("stderr");
            let stderr = std::fs::read_to_string(&stderr_path).unwrap_or_default();
            let clean = clean_exit_for(&stem);
            let res = parse_stdout(&stdout, clean, &stderr);

            assert_eq!(res.clean_exit, clean, "{stem}");
            assert_eq!(res.stderr, stderr, "{stem}");
            assert_eq!(res.duration_ms, None, "{stem}");
            // The exit code never decides success: error-bad-model exits 0.
            if stem == "cancelled" {
                assert_eq!(res.is_error, Some(true), "{stem}");
                assert_eq!(res.text, NO_RESULT, "{stem}");
                assert_eq!(
                    res.session_id.as_deref(),
                    Some("aab8b00e-3999-48d2-b8ee-3ebf95ff5350"),
                    "{stem}"
                );
                assert!(res.usage.is_none(), "{stem}");
                assert_eq!(res.cost_usd, None, "{stem}");
                continue;
            }
            assert!(res.session_id.is_some() && res.usage.is_some(), "{stem}");
            assert!(res.cost_usd.is_some(), "{stem}");

            match stem.as_str() {
                "plain-answer" => assert_eq!(
                    res,
                    BackendResult {
                        text: "391\nSTATUS: DONE".into(),
                        session_id: Some("afcd8926-430b-4d9a-a552-d7c6d1b900ba".into()),
                        usage: Some(Usage {
                            input_tokens: 5320.0,
                            output_tokens: 9.0,
                            cache_read_tokens: 0.0,
                            cache_write_tokens: 0.0,
                        }),
                        cost_usd: Some(0.0005365),
                        is_error: Some(false),
                        duration_ms: None,
                        clean_exit: true,
                        stderr,
                        permission_denials: Vec::new(),
                    },
                    "{stem}"
                ),
                "tool-calls-fix" => {
                    assert_eq!(
                        res.text,
                        "Fixed `add` in `calc.py` to add its arguments. \
                         `python3 test_calc.py` passes.\n\nSTATUS: DONE",
                        "{stem}"
                    );
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("e04e84ae-41c0-4ed8-b9e5-de410d2f3bdf"),
                        "{stem}"
                    );
                    let usage = res.usage.as_ref().expect("usage");
                    assert_eq!(usage.input_tokens, 8940.0, "{stem}");
                    assert_eq!(usage.output_tokens, 214.0, "{stem}");
                    assert_eq!(usage.cache_read_tokens, 18432.0, "{stem}");
                    assert_eq!(usage.cache_write_tokens, 0.0, "{stem}");
                    assert!(approx(res.cost_usd.unwrap(), 0.00118532), "{stem}");
                    assert_eq!(res.is_error, Some(false), "{stem}");
                }
                "resume-answer" => {
                    assert_eq!(res.text, "392\nSTATUS: DONE", "{stem}");
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("afcd8926-430b-4d9a-a552-d7c6d1b900ba"),
                        "{stem}"
                    );
                    let usage = res.usage.as_ref().expect("usage");
                    assert_eq!(usage.input_tokens, 814.0, "{stem}");
                    assert_eq!(usage.output_tokens, 33.0, "{stem}");
                    assert_eq!(usage.cache_read_tokens, 4608.0, "{stem}");
                    assert_eq!(usage.cache_write_tokens, 0.0, "{stem}");
                    assert!(approx(res.cost_usd.unwrap(), 0.00014398), "{stem}");
                    assert_eq!(res.is_error, Some(false), "{stem}");
                }
                "error-bad-model" => {
                    assert_eq!(
                        res.text,
                        "Codex error: The 'no-such-model' model is not supported \
                         when using Codex with a ChatGPT account.",
                        "{stem}"
                    );
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("a86b4f04-f9fe-4fe2-88f2-9c07c39dda68"),
                        "{stem}"
                    );
                    assert_eq!(res.is_error, Some(true), "{stem}");
                    assert_eq!(res.cost_usd, Some(0.0), "{stem}");
                }
                "needs-context" => {
                    assert_eq!(
                        res.text,
                        "What would you like the function in `calc.py` to be called?\
                         \n\nSTATUS: NEEDS_CONTEXT",
                        "{stem}"
                    );
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("b7a8e40b-c58f-4468-b202-0ea1ff35dbac"),
                        "{stem}"
                    );
                    let usage = res.usage.as_ref().expect("usage");
                    assert_eq!(usage.input_tokens, 7912.0, "{stem}");
                    assert_eq!(usage.output_tokens, 174.0, "{stem}");
                    assert_eq!(usage.cache_read_tokens, 13824.0, "{stem}");
                    assert_eq!(usage.cache_write_tokens, 0.0, "{stem}");
                    assert!(approx(res.cost_usd.unwrap(), 0.00101644), "{stem}");
                    assert_eq!(res.is_error, Some(false), "{stem}");
                }
                other => panic!("unexpected pi fixture: {other}"),
            }
        }
    }

    #[test]
    fn recorded_fixtures_parse_without_panicking() {
        let dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/recorded/pi");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("stdout"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "no recorded pi fixtures");

        for path in files {
            let stdout = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let stderr = std::fs::read_to_string(path.with_extension("stderr")).unwrap_or_default();
            // Archive only: a recorded run must parse, under either exit
            // code. No assertions on the result itself.
            let _ = parse_stdout(&stdout, true, &stderr);
            let _ = parse_stdout(&stdout, false, &stderr);
        }
    }

    #[test]
    fn every_pi_fixture_has_progress() {
        // tool_execution_start sets last_tool; assistant text sets last_assistant.
        let dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/contract/pi");
        for stem in [
            "plain-answer",
            "tool-calls-fix",
            "needs-context",
            "resume-answer",
        ] {
            let stdout = std::fs::read_to_string(dir.join(format!("{stem}.stdout"))).unwrap();
            let mut state = PiState::default();
            for line in stdout.split_inclusive('\n') {
                handle_line(line.as_bytes(), &mut state);
            }
            assert!(
                state.last_assistant.is_some(),
                "{stem} progress saw no assistant text"
            );
            if stem == "tool-calls-fix" || stem == "needs-context" {
                assert!(
                    state.last_tool.is_some(),
                    "{stem} progress saw no tool call"
                );
            }
        }
    }
}
