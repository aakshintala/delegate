//! The cursor-agent backend: binary, argv, spawn, and stream-json parsing.

pub(crate) mod doctor;

use super::types::{BackendResult, Event, EventFn, ProgressSnapshotRaw, Spawned};
use crate::stream::{RawCursorJson, StreamState, init_stream_state, parse_line};
use crate::types::JobSpec;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::Ordering;

const NO_RESULT: &str = "no result line";

pub(crate) fn resolve_bin(r#override: Option<&str>) -> String {
    if let Some(o) = r#override.filter(|s| !s.is_empty()) {
        return o.to_string();
    }
    if let Ok(env) = std::env::var("CURSOR_AGENT_BIN")
        && !env.is_empty()
    {
        return env;
    }
    if let Ok(out) = Command::new("which").arg("cursor-agent").output()
        && out.status.success()
    {
        let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !found.is_empty() {
            return found;
        }
    }
    crate::util::homedir()
        .join(".local/bin/cursor-agent")
        .to_string_lossy()
        .into_owned()
}

/// Argv for a run. Every job runs with writes enabled: `--sandbox disabled --force`.
pub(crate) fn argv(model: &str, session: Option<&str>, prompt: &str) -> Vec<String> {
    let mut args = vec![
        "--print".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--trust".into(),
        "--approve-mcps".into(),
        "--model".into(),
        model.to_string(),
        "--sandbox".into(),
        "disabled".into(),
        "--force".into(),
    ];
    if let Some(s) = session {
        args.push("--resume".into());
        args.push(s.to_string());
    }
    args.push("--".into());
    args.push(prompt.to_string());
    args
}

pub(crate) fn spawn(spec: &JobSpec) -> Spawned {
    let launched_session = spec
        .argv
        .windows(2)
        .find(|w| w[0] == "--resume")
        .map(|w| w[1].clone());
    spawn_with_session(spec, launched_session)
}

/// `launched_session` is an id chosen before launch. The stream's own id wins;
/// this fills in when the child dies before a result line (cancel).
pub(crate) fn spawn_with_session(spec: &JobSpec, launched_session: Option<String>) -> Spawned {
    let started = match super::start_child(spec) {
        Ok(s) => s,
        Err(msg) => return super::spawn_failed(&msg),
    };
    let pid = started.pid;
    let reaped = started.reaped;
    let reaped_k = Arc::clone(&reaped);
    let stdout = started.stdout;
    let stderr = started.stderr;
    let mut child = started.child;
    let spawned_session = launched_session.clone();
    Spawned {
        kill: super::killer(pid, reaped_k),
        session_id: spawned_session,
        drive: Box::new(move |on| {
            let mut state = init_stream_state();
            let mut result: Option<RawCursorJson> = None;
            let mut messages: Vec<String> = Vec::new();
            let pumped = super::pump(
                stdout,
                stderr,
                on,
                move || {
                    let clean = child.wait().map(|s| s.success()).unwrap_or(false);
                    reaped.store(true, Ordering::SeqCst);
                    clean
                },
                |line| handle_line(line, &mut state, &mut result, &mut messages, on),
            );
            finish(
                result,
                &messages,
                pumped.clean_exit,
                &pumped.stderr,
                !pumped.saw_stdout,
                state.session_id.as_deref(),
                launched_session.as_deref(),
            )
        }),
    }
}

/// Pure parse of a finished cursor-agent stdout. The spawn driver uses [`finish`] too.
pub fn parse_stdout(stdout: &str, clean_exit: bool, stderr: &str) -> BackendResult {
    let mut state = init_stream_state();
    let mut raw = None;
    let mut messages: Vec<String> = Vec::new();
    for line in stdout.split_inclusive('\n') {
        handle_line(
            line.as_bytes(),
            &mut state,
            &mut raw,
            &mut messages,
            &|_: Event| {},
        );
    }
    finish(
        raw,
        &messages,
        clean_exit,
        stderr,
        stdout.is_empty(),
        state.session_id.as_deref(),
        None,
    )
}

fn handle_line(
    line: &[u8],
    state: &mut StreamState,
    raw: &mut Option<RawCursorJson>,
    messages: &mut Vec<String>,
    on: EventFn<'_>,
) {
    let parsed = parse_line(&String::from_utf8_lossy(line), state);
    if parsed.result.is_some() {
        *raw = parsed.result;
    }
    if let Some(text) = parsed.assistant_text {
        messages.push(text);
    }
    if parsed.changed {
        on(Event::Progress(ProgressSnapshotRaw {
            last_tool: state.last_tool.clone(),
            tokens_so_far: state.tokens_so_far,
            last_assistant: state.last_assistant.clone(),
            files_touched: state.files_touched.clone(),
            phase: state.phase.clone(),
            session_id: state.session_id.clone(),
        }));
    }
}

fn join_assistant_messages(messages: &[String]) -> String {
    let mut text = String::new();
    for msg in messages {
        if msg.is_empty() {
            continue;
        }
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(msg);
    }
    text
}

fn text_from_result_and_messages(result: &str, messages: &[String]) -> String {
    if messages.concat() == result {
        join_assistant_messages(messages)
    } else {
        result.to_string()
    }
}

/// Turn the last result line (or the lack of one) into the normalized result.
fn finish(
    raw: Option<RawCursorJson>,
    messages: &[String],
    clean_exit: bool,
    stderr: &str,
    stdout_empty: bool,
    stream_session: Option<&str>,
    launched_session: Option<&str>,
) -> BackendResult {
    let session = || stream_session.map(str::to_string);
    let launched = || launched_session.map(str::to_string);
    if let Some(raw) = raw {
        let result = raw.result.unwrap_or_default();
        return BackendResult {
            text: text_from_result_and_messages(&result, messages),
            session_id: raw.session_id.or_else(session).or_else(launched),
            usage: raw.usage,
            cost_usd: raw.cost_usd,
            is_error: raw.is_error,
            duration_ms: raw.duration_ms,
            clean_exit,
            stderr: stderr.to_string(),
            permission_denials: raw.permission_denials,
        };
    }
    // A non-clean exit with no stdout is the bad-model case: the text is the stderr we kept.
    // Any other missing result line is an error. CANCELLED is the supervisor's status, never ours.
    // ponytail: the exit code is not carried (clean_exit is a bool); carry Option<i32> if an error text ever needs it.
    let text = if !clean_exit && stdout_empty {
        stderr.to_string()
    } else {
        NO_RESULT.to_string()
    };
    BackendResult {
        text,
        session_id: session().or_else(launched),
        is_error: Some(true),
        clean_exit,
        stderr: stderr.to_string(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Usage;
    use std::sync::Mutex;

    fn drive(stdout: &[u8], stderr: &[u8], on: EventFn<'_>, clean: bool) -> BackendResult {
        let mut state = init_stream_state();
        let mut result: Option<RawCursorJson> = None;
        let mut messages: Vec<String> = Vec::new();
        let pumped = crate::backends::pump(
            stdout,
            stderr,
            on,
            move || clean,
            |line| {
                handle_line(line, &mut state, &mut result, &mut messages, on);
            },
        );
        finish(
            result,
            &messages,
            pumped.clean_exit,
            &pumped.stderr,
            !pumped.saw_stdout,
            state.session_id.as_deref(),
            None,
        )
    }

    #[test]
    fn write_argv_disables_the_sandbox() {
        let write = argv("composer-2.5", Some("sid"), "go");
        let pair = |a, b| write.windows(2).any(|w| w[0] == a && w[1] == b);
        assert!(pair("--sandbox", "disabled"));
        assert!(pair("--resume", "sid"));
        assert!(!pair("--sandbox", "enabled"));
        assert!(write.contains(&"--force".to_string()));
        assert_eq!(
            write.iter().map(String::as_str).collect::<Vec<_>>(),
            [
                "--print",
                "--output-format",
                "stream-json",
                "--trust",
                "--approve-mcps",
                "--model",
                "composer-2.5",
                "--sandbox",
                "disabled",
                "--force",
                "--resume",
                "sid",
                "--",
                "go",
            ]
        );
    }

    #[test]
    fn explicit_override_wins() {
        assert_eq!(
            resolve_bin(Some("/custom/cursor-agent")),
            "/custom/cursor-agent"
        );
    }

    #[test]
    fn env_used_when_no_override() {
        let prev = std::env::var("CURSOR_AGENT_BIN").ok();
        unsafe { std::env::set_var("CURSOR_AGENT_BIN", "/env/cursor-agent") };
        assert_eq!(resolve_bin(None), "/env/cursor-agent");
        match prev {
            Some(v) => unsafe { std::env::set_var("CURSOR_AGENT_BIN", v) },
            None => unsafe { std::env::remove_var("CURSOR_AGENT_BIN") },
        }
    }

    #[test]
    fn parses_ndjson_emits_progress_resolves_result() {
        // A tool call line (with newline), then the terminal result WITHOUT a trailing newline.
        let stdout = concat!(
            r#"{"type":"tool_call","subtype":"started","tool_call":{"shellToolCall":{}}}"#,
            "\n",
            r#"{"type":"result","subtype":"success","is_error":false,"result":"all good","session_id":"sid","usage":{"outputTokens":7}}"#,
        );
        let progress = Mutex::new(Vec::new());
        let on = |e: Event| {
            if let Event::Progress(p) = e {
                progress.lock().unwrap().push(p);
            }
        };
        let res = drive(stdout.as_bytes(), &b""[..], &on, true);
        assert!(res.clean_exit);
        assert_eq!(res.text, "all good");
        assert_eq!(res.session_id.as_deref(), Some("sid"));
        let progress = progress.into_inner().unwrap();
        assert!(!progress.is_empty());
        assert_eq!(progress[0].last_tool.as_deref(), Some("shell"));
    }

    #[test]
    fn non_zero_exit_is_unclean_and_stderr_is_captured() {
        let res = drive(&b""[..], &b"trouble"[..], &|_| {}, false);
        assert!(!res.clean_exit);
        assert_eq!(res.stderr, "trouble");
        assert_eq!(res.is_error, Some(true));
        assert_eq!(res.text, "trouble");
    }

    #[test]
    fn stderr_keeps_only_a_bounded_tail() {
        let big = vec![b'x'; crate::backends::STDERR_KEEP * 2 + 5];
        let res = drive(&b""[..], &big[..], &|_| {}, true);
        assert_eq!(res.stderr.len(), crate::backends::STDERR_KEEP);
    }

    #[test]
    fn spawn_failure_is_an_error_result() {
        let spec = crate::job::tests::spec_of(|s| s.bin = "/nonexistent/cursor-agent".into());
        let spawned = spawn(&spec);
        (spawned.kill)();
        let res = (spawned.drive)(&|_| {});
        assert!(!res.clean_exit);
        assert_eq!(res.is_error, Some(true));
        assert_eq!(res.text, res.stderr);
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

    #[test]
    fn unparseable_lines_are_skipped_and_a_clean_exit_without_a_result_is_an_error() {
        let skipped = parse_stdout("not json\n", true, "");
        assert!(skipped.clean_exit);
        assert_eq!(skipped.is_error, Some(true));
        assert_eq!(skipped.text, NO_RESULT);
        assert!(!skipped.text.contains("CANCELLED"));

        let res = parse_stdout(
            "not json\n{\"type\":\"result\",\"is_error\":false,\"result\":\"ok\",\"session_id\":\"s\"}\n",
            true,
            "",
        );
        assert_eq!(res.text, "ok");
        assert_eq!(res.session_id.as_deref(), Some("s"));
        assert_eq!(res.is_error, Some(false));
    }

    #[test]
    fn unclean_empty_stdout_uses_stderr_as_text() {
        let res = parse_stdout("", false, "Cannot use this model");
        assert!(!res.clean_exit);
        assert_eq!(res.is_error, Some(true));
        assert_eq!(res.text, "Cannot use this model");
        assert!(res.session_id.is_none());
        assert!(res.usage.is_none());
    }

    /// Exit status is not stored in the fixtures. Everything else is a clean exit.
    fn clean_exit_for(stem: &str) -> bool {
        !matches!(stem, "error-bad-model" | "cancelled" | "cancel-mid-tool")
    }

    #[test]
    fn result_session_id_is_authoritative_over_the_stream_id() {
        let stdout = concat!(
            r#"{"type":"system","subtype":"init","session_id":"stream-sid"}"#,
            "\n",
            r#"{"type":"result","is_error":false,"result":"ok","session_id":"result-sid"}"#,
        );
        let res = parse_stdout(stdout, true, "");
        assert_eq!(res.session_id.as_deref(), Some("result-sid"));
    }

    #[test]
    fn cancelled_mid_tool_keeps_its_stream_session_id() {
        let stdout = include_str!("../../../tests/fixtures/contract/cursor/cancel-mid-tool.stdout");
        let res = parse_stdout(stdout, false, "");
        assert_eq!(
            res.session_id.as_deref(),
            Some("2a2056ae-a4c8-48ba-aaf1-e493a08074a5")
        );
        assert_eq!(res.is_error, Some(true));
        assert!(!res.clean_exit);
    }

    #[test]
    fn fixtures_parse_to_a_normalized_result() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/contract/cursor");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("stdout"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "no cursor stdout fixtures");

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
            assert_eq!(res.cost_usd, None, "{stem}");
            if !clean && stdout.is_empty() {
                assert_eq!(res.is_error, Some(true), "{stem}");
                assert_eq!(res.text, stderr, "{stem}");
                assert!(res.session_id.is_none() && res.usage.is_none(), "{stem}");
            } else if !clean {
                assert_eq!(res.is_error, Some(true), "{stem}");
                assert_eq!(res.text, NO_RESULT, "{stem}");
                if stem == "cancel-mid-tool" || stem == "cancelled" {
                    let expected = if stem == "cancel-mid-tool" {
                        "2a2056ae-a4c8-48ba-aaf1-e493a08074a5"
                    } else {
                        "62115336-178d-43d1-a45b-b83b5f597c2e"
                    };
                    assert_eq!(res.session_id.as_deref(), Some(expected), "{stem}");
                    assert!(res.usage.is_none(), "{stem}");
                } else {
                    assert!(res.session_id.is_none() && res.usage.is_none(), "{stem}");
                }
            } else {
                assert_eq!(res.is_error, Some(false), "{stem}");
                assert!(res.session_id.is_some() && res.usage.is_some(), "{stem}");
                assert!(!res.text.is_empty(), "{stem}");
            }

            match stem.as_str() {
                "plain-answer" => assert_eq!(
                    res,
                    BackendResult {
                        text: "391\n\nSTATUS: DONE".into(),
                        session_id: Some("54b96753-cec4-4820-ade9-aa49ebbb463a".into()),
                        usage: Some(Usage {
                            input_tokens: 9451.0,
                            output_tokens: 72.0,
                            cache_read_tokens: 3872.0,
                            cache_write_tokens: 0.0,
                        }),
                        cost_usd: None,
                        is_error: Some(false),
                        duration_ms: Some(3279.0),
                        clean_exit: true,
                        stderr: String::new(),
                        permission_denials: Vec::new(),
                    }
                ),
                "tool-calls-fix" => {
                    let mut state = init_stream_state();
                    for line in stdout.lines() {
                        parse_line(line, &mut state);
                    }
                    assert!(
                        state.last_tool.is_some(),
                        "tool-calls-fix progress saw no tool call"
                    );
                }
                "cancel-mid-tool-resume" => {
                    assert_eq!(res.text, "BANANA\n\nSTATUS: DONE", "{stem}");
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("2a2056ae-a4c8-48ba-aaf1-e493a08074a5"),
                        "{stem}"
                    );
                }
                "resume-answer" => {
                    assert_eq!(res.text, "392\n\nSTATUS: DONE", "{stem}");
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("54b96753-cec4-4820-ade9-aa49ebbb463a"),
                        "{stem}"
                    );
                }
                "needs-context" => {
                    assert!(res.text.contains("STATUS: NEEDS_CONTEXT"), "{stem}");
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("64cdc2ca-3627-49c7-a6de-d1ae305121f6"),
                        "{stem}"
                    );
                }
                "cancel-mid-tool" => {
                    assert_eq!(res.text, NO_RESULT, "{stem}");
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("2a2056ae-a4c8-48ba-aaf1-e493a08074a5"),
                        "{stem}"
                    );
                }
                "cancelled" => {
                    assert_eq!(res.text, NO_RESULT, "{stem}");
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("62115336-178d-43d1-a45b-b83b5f597c2e"),
                        "{stem}"
                    );
                }
                // Empty stdout: the text is the stderr we kept.
                "error-bad-model" => assert_eq!(res.text, stderr, "{stem}"),
                "glued-messages" => {
                    let want = "I'll read `calc.py` first.\n`calc.py` defines a single `add` function that returns the sum of its two arguments.\nVERDICT: APPROVE\n\n`calc.py` contains one function, `add`, which returns the sum of its two arguments. The workspace listing shows only that file.\n\nSTATUS: DONE";
                    assert_eq!(res.text, want, "{stem}");
                }
                other => panic!("unexpected cursor fixture: {other}"),
            }
        }
    }

    #[test]
    fn mismatched_messages_and_result_keep_result_verbatim() {
        let stdout = concat!(
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":"msg-a"}]}}"#,
            "\n",
            r#"{"type":"result","is_error":false,"result":"not-the-messages"}"#,
        );
        let res = parse_stdout(stdout, true, "");
        assert_eq!(res.text, "not-the-messages");
    }

    #[test]
    fn result_without_assistant_events_keeps_result_verbatim() {
        let res = parse_stdout(
            r#"{"type":"result","is_error":false,"result":"verbatim"}"#,
            true,
            "",
        );
        assert_eq!(res.text, "verbatim");
    }

    #[test]
    fn composer_recorded_fixture_text_equals_result_field() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/recorded/cursor/ponytail-33.stdout");
        let stdout = std::fs::read_to_string(&path).unwrap();
        let stderr = std::fs::read_to_string(path.with_extension("stderr")).unwrap_or_default();
        let expected = stdout
            .lines()
            .rev()
            .find_map(|l| parse_line(l, &mut init_stream_state()).result)
            .and_then(|raw| raw.result)
            .unwrap();
        let res = parse_stdout(&stdout, true, &stderr);
        assert_eq!(res.text, expected);
    }

    #[test]
    fn recorded_fixtures_parse_without_panicking() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/recorded/cursor");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("stdout"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "no recorded cursor fixtures");

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
}
