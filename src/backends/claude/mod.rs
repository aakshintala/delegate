//! Claude Code backend: binary, argv, and spawn. Parsing is the shared stream parser.

pub(crate) mod doctor;

use super::types::Spawned;
use crate::types::JobSpec;
use std::process::Command;

pub(crate) fn resolve_bin(r#override: Option<&str>) -> String {
    if let Some(o) = r#override.filter(|s| !s.is_empty()) {
        return o.to_string();
    }
    if let Ok(env) = std::env::var("CLAUDE_BIN")
        && !env.is_empty()
    {
        return env;
    }
    if let Ok(out) = Command::new("which").arg("claude").output()
        && out.status.success()
    {
        let found = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if !found.is_empty() {
            return found;
        }
    }
    crate::util::homedir()
        .join(".local/bin/claude")
        .to_string_lossy()
        .into_owned()
}

/// Argv for a run. Every job runs with writes enabled: `--permission-mode auto`.
pub(crate) fn argv(model: &str, session: Option<&str>, prompt: &str) -> Vec<String> {
    let mut args = vec![
        "-p".into(),
        "--output-format".into(),
        "stream-json".into(),
        "--verbose".into(),
        "--model".into(),
        model.to_string(),
        "--permission-mode".into(),
        "auto".into(),
    ];
    if let Some(id) = session {
        args.push("--resume".into());
        args.push(id.to_string());
    } else {
        args.push("--session-id".into());
        args.push(crate::util::random_uuid());
    }
    args.push("--".into());
    args.push(prompt.to_string());
    args
}

/// Same child driver as cursor. The id minted in [`argv`] is kept when the
/// stream never sends one, so a cancelled run can resume.
pub(crate) fn spawn(spec: &JobSpec) -> Spawned {
    super::cursor::spawn_with_session(spec, session_from_argv(&spec.argv))
}

fn session_from_argv(argv: &[String]) -> Option<String> {
    argv.windows(2)
        .find(|w| w[0] == "--session-id" || w[0] == "--resume")
        .map(|w| w[1].clone())
}

#[cfg(test)]
pub(crate) static CLAUDE_BIN_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::cursor::parse_stdout;
    use crate::backends::types::BackendResult;
    use crate::finalize::{default_finalize_ctx, finalize_run};
    use crate::stream::{init_stream_state, parse_line};
    use crate::types::{RunStatus, Usage};

    #[test]
    fn argv_always_uses_auto_mode() {
        let fresh = argv("claude-sonnet-5-5", None, "hi");
        assert_eq!(
            fresh[..8].iter().map(String::as_str).collect::<Vec<_>>(),
            [
                "-p",
                "--output-format",
                "stream-json",
                "--verbose",
                "--model",
                "claude-sonnet-5-5",
                "--permission-mode",
                "auto",
            ]
        );
        assert_eq!(fresh[8], "--session-id");
        assert!(
            fresh[9].len() == 36 && fresh[9].chars().all(|c| c.is_ascii_hexdigit() || c == '-'),
            "{}",
            fresh[9]
        );
        assert_eq!(&fresh[10..], &["--".to_string(), "hi".to_string()]);
        assert!(!fresh.iter().any(|a| a.contains("disallowedTools")));

        let write = argv("claude-sonnet-5-5", Some("sid"), "go");
        let pair = |a, b| write.windows(2).any(|w| w[0] == a && w[1] == b);
        assert!(pair("--permission-mode", "auto"));
        assert!(pair("--resume", "sid"));
        assert!(!write.iter().any(|a| a == "--session-id"));
        assert!(!write.iter().any(|a| a.contains("disallowedTools")));
    }

    #[test]
    fn explicit_override_wins() {
        assert_eq!(resolve_bin(Some("/custom/claude")), "/custom/claude");
    }

    #[test]
    fn env_used_when_no_override() {
        let _guard = CLAUDE_BIN_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var("CLAUDE_BIN").ok();
        unsafe { std::env::set_var("CLAUDE_BIN", "/env/claude") };
        assert_eq!(resolve_bin(None), "/env/claude");
        match prev {
            Some(v) => unsafe { std::env::set_var("CLAUDE_BIN", v) },
            None => unsafe { std::env::remove_var("CLAUDE_BIN") },
        }
    }

    /// Exit status is not stored in the fixtures. A killed run and a bad model are unclean.
    fn clean_exit_for(stem: &str) -> bool {
        !matches!(stem, "error-bad-model" | "cancelled")
    }

    #[test]
    fn fixtures_parse_to_a_normalized_result() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/contract/claude");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("stdout"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "no claude stdout fixtures");

        for path in files {
            let stem = path.file_stem().unwrap().to_str().unwrap().to_string();
            let stdout = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let stderr = std::fs::read_to_string(path.with_extension("stderr")).unwrap_or_default();
            let clean = clean_exit_for(&stem);
            let res = parse_stdout(&stdout, clean, &stderr);

            assert_eq!(res.clean_exit, clean, "{stem}");
            assert_eq!(res.stderr, stderr, "{stem}");
            assert!(res.permission_denials.is_empty(), "{stem}");

            if stem == "cancelled" {
                assert_eq!(res.is_error, Some(true), "{stem}");
                assert_eq!(res.text, "no result line", "{stem}");
                assert_eq!(
                    res.session_id.as_deref(),
                    Some("96fc5446-dc3e-4ffa-b3bf-fad8fad2bbe0"),
                    "{stem}"
                );
                assert!(res.usage.is_none(), "{stem}");
                continue;
            }

            assert!(res.session_id.is_some(), "{stem}");
            assert!(res.usage.is_some(), "{stem}");
            assert!(res.cost_usd.is_some(), "{stem}");
            assert!(!res.text.is_empty(), "{stem}");
            match stem.as_str() {
                "auto-refusal" | "auto-refusal-exfil" => {
                    // #6 recorded no refusal: the model declined in prose, permission_denials stayed empty.
                    assert_eq!(res.is_error, Some(false), "{stem}");
                    assert!(res.permission_denials.is_empty(), "{stem}");
                    assert!(!res.text.is_empty(), "{stem}");
                }
                "error-bad-model" => {
                    // subtype is "success"; is_error is what counts.
                    assert_eq!(res.is_error, Some(true), "{stem}");
                    assert_eq!(
                        res.text,
                        "There's an issue with the selected model (no-such-model). It may not exist or you may not have access to it. Run --model to pick a different model."
                    );
                }
                "plan-edit-request" | "needs-context" => {
                    assert_eq!(res.is_error, Some(false), "{stem}");
                    assert!(res.text.contains("STATUS: NEEDS_CONTEXT"), "{stem}");
                }
                "resume-answer" => {
                    assert_eq!(res.is_error, Some(false), "{stem}");
                    assert_eq!(
                        res.session_id.as_deref(),
                        Some("25dddfc3-4271-4019-9de0-6795bd6483fa")
                    );
                }
                "auto-fix" => {
                    assert_eq!(res.is_error, Some(false), "{stem}");
                    let mut state = init_stream_state();
                    for line in stdout.lines() {
                        parse_line(line, &mut state);
                    }
                    assert!(state.last_tool.is_some(), "auto-fix saw no tool call");
                    assert!(
                        state.files_touched.iter().any(|p| p.ends_with("calc.py")),
                        "{:?}",
                        state.files_touched
                    );
                }
                "plan-review" => {
                    assert_eq!(res.is_error, Some(false), "{stem}");
                    assert!(!res.text.is_empty(), "{stem}");
                }
                "plan-plain-answer" => {
                    assert_eq!(
                        res,
                        BackendResult {
                            text: "391\n\nSTATUS: DONE".into(),
                            session_id: Some("25dddfc3-4271-4019-9de0-6795bd6483fa".into()),
                            usage: Some(Usage {
                                input_tokens: 2.0,
                                output_tokens: 12.0,
                                cache_read_tokens: 10261.0,
                                cache_write_tokens: 16891.0,
                            }),
                            cost_usd: Some(0.0697402),
                            is_error: Some(false),
                            duration_ms: Some(2245.0),
                            clean_exit: true,
                            stderr: String::new(),
                            permission_denials: Vec::new(),
                        }
                    );
                    let mut ctx = default_finalize_ctx("/repo", "claude-sonnet-5-5", "claude");
                    ctx.git_delta = Some(Box::new(|_, _| None));
                    let out = finalize_run(&res, &ctx);
                    assert_eq!(out.cost_usd, Some(0.0697402));
                    assert!(!out.cost_estimated);
                    assert_eq!(out.status, RunStatus::Done);
                }
                other => panic!("unexpected claude fixture: {other}"),
            }
        }
    }

    #[test]
    fn recorded_fixtures_parse_without_panicking() {
        let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/recorded/claude");
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("stdout"))
            .collect();
        files.sort();
        assert!(!files.is_empty(), "no recorded claude fixtures");

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
    fn permission_denials_downgrade_unless_already_error() {
        // synthetic: no refusal was recorded in #6
        let line = concat!(
            r#"{"type":"result","subtype":"success","is_error":false,"result":"blocked\nSTATUS: DONE","session_id":"s","permission_denials":[{"tool_name":"Bash"},{"tool_name":"Edit"}]}"#,
            "\n",
        );
        let res = parse_stdout(line, true, "");
        assert_eq!(res.permission_denials.len(), 2);
        let mut ctx = default_finalize_ctx("/repo", "claude-sonnet-5-5", "claude");
        ctx.git_delta = Some(Box::new(|_, _| None));
        let out = finalize_run(&res, &ctx);
        assert_eq!(out.status, RunStatus::DoneWithConcerns);
        let concerns = out.concerns.unwrap();
        assert!(
            concerns
                .iter()
                .any(|c| c.contains("Bash") && c.contains("Edit")),
            "{concerns:?}"
        );
        assert_eq!(out.permission_denials, res.permission_denials);

        let err = concat!(
            r#"{"type":"result","subtype":"success","is_error":true,"result":"boom","permission_denials":[{"tool_name":"Bash"}]}"#,
            "\n",
        );
        let res = parse_stdout(err, true, "");
        let out = finalize_run(&res, &ctx);
        assert_eq!(out.status, RunStatus::Error);
        assert!(out.concerns.is_none());
        assert_eq!(out.permission_denials.len(), 1);
    }
}
