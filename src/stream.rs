use crate::types::Usage;
use serde::Deserialize;
use serde_json::Value;

/// Each field decodes on its own: one malformed field reads as absent instead of voiding the rest.
/// Shared by the cursor and Claude parsers. `is_error` is authoritative; `subtype` is ignored.
#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RawCursorJson {
    #[serde(default, deserialize_with = "crate::util::lenient")]
    pub is_error: Option<bool>,
    #[serde(default, deserialize_with = "crate::util::lenient")]
    pub duration_ms: Option<f64>,
    #[serde(default, deserialize_with = "crate::util::lenient")]
    pub result: Option<String>,
    #[serde(default, deserialize_with = "crate::util::lenient")]
    pub session_id: Option<String>,
    #[serde(default, deserialize_with = "crate::util::lenient")]
    pub usage: Option<Usage>,
    /// Claude's `total_cost_usd`. Absent on cursor, so cursor cost stays estimated.
    #[serde(
        default,
        rename = "total_cost_usd",
        deserialize_with = "crate::util::lenient"
    )]
    pub cost_usd: Option<f64>,
    #[serde(default)]
    pub permission_denials: Vec<Value>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamState {
    pub last_tool: Option<String>,
    pub tokens_so_far: f64,
    pub last_assistant: Option<String>,
    pub files_touched: Vec<String>,
    pub phase: Option<String>,
    pub session_id: Option<String>,
}

pub fn init_stream_state() -> StreamState {
    StreamState::default()
}

#[derive(Debug, Clone, Default)]
pub struct ParsedLine {
    pub(crate) result: Option<RawCursorJson>,
    pub changed: bool,
    pub(crate) assistant_text: Option<String>,
}

const PATH_ARG_KEYS: [&str; 5] = ["path", "filePath", "file_path", "target", "destination"];

fn path_from_args(args: Option<&Value>) -> Option<String> {
    let obj = args?.as_object()?;
    for key in PATH_ARG_KEYS {
        if let Some(v) = obj
            .get(key)
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(v.to_string());
        }
    }
    None
}

fn extract_tool(tc: &Value) -> (Option<String>, Option<String>) {
    if let Some(mcp) = tc.get("mcpToolCall") {
        let tool = mcp
            .get("toolName")
            .and_then(|x| x.as_str())
            .unwrap_or("mcp")
            .to_string();
        return (Some(tool), None);
    }
    if let Some(obj) = tc.as_object() {
        for (key, val) in obj {
            if let Some(name) = key.strip_suffix("ToolCall") {
                let args = val.get("args");
                return (Some(name.to_string()), path_from_args(args));
            }
        }
    }
    (None, None)
}

fn output_tokens(ev: &Value) -> Option<f64> {
    for ptr in [
        "/usage/outputTokens",
        "/usage/output_tokens",
        "/message/usage/outputTokens",
        "/message/usage/output_tokens",
    ] {
        if let Some(n) = ev
            .pointer(ptr)
            .and_then(|x| x.as_f64())
            .filter(|n| n.is_finite())
        {
            return Some(n);
        }
    }
    None
}

/// Claude `tool_use` content: tool name, and a path from `input.file_path` or `input.path`.
fn note_tool_use(item: &Value, state: &mut StreamState) -> bool {
    if item.get("type").and_then(|t| t.as_str()) != Some("tool_use") {
        return false;
    }
    state.phase = Some("running_tool".into());
    if let Some(name) = item
        .get("name")
        .and_then(|n| n.as_str())
        .filter(|s| !s.is_empty())
    {
        state.last_tool = Some(name.to_string());
    }
    if let Some(p) = path_from_args(item.get("input"))
        && !state.files_touched.contains(&p)
    {
        state.files_touched.push(p);
    }
    true
}

fn extract_assistant_text(message: Option<&Value>) -> Option<String> {
    let content = message?.get("content")?.as_array()?;
    let parts: Vec<&str> = content
        .iter()
        .filter(|b| b.get("type").and_then(|t| t.as_str()) == Some("text"))
        .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
        .collect();
    if parts.is_empty() {
        return None;
    }
    Some(parts.join(""))
}

pub fn parse_line(line: &str, state: &mut StreamState) -> ParsedLine {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return ParsedLine {
            changed: false,
            result: None,
            assistant_text: None,
        };
    }
    let ev: Value = match serde_json::from_str(trimmed) {
        Ok(v) => v,
        Err(_) => {
            return ParsedLine {
                changed: false,
                result: None,
                assistant_text: None,
            };
        }
    };
    let mut changed = false;
    if state.session_id.is_none()
        && let Some(session_id) = ev.get("session_id").and_then(Value::as_str)
    {
        state.session_id = Some(session_id.to_string());
        changed = true;
    }

    let ty = match ev.get("type").and_then(|t| t.as_str()) {
        Some(t) => t,
        None => {
            return ParsedLine {
                changed,
                result: None,
                assistant_text: None,
            };
        }
    };

    // Claude system/* (hook_started, hook_response, init, commands_changed, …) and
    // rate_limit_event carry no progress. Skip them before the usage peek.
    if ty == "system" || ty == "rate_limit_event" {
        return ParsedLine {
            changed,
            result: None,
            assistant_text: None,
        };
    }

    let assistant_text = if ty == "assistant" {
        extract_assistant_text(ev.get("message"))
    } else {
        None
    };
    if let Some(n) = output_tokens(&ev) {
        state.tokens_so_far = state.tokens_so_far.max(n);
        changed = true;
    }

    match ty {
        "tool_call" => {
            if ev.get("subtype").and_then(|s| s.as_str()) == Some("started")
                && ev.get("tool_call").is_some()
            {
                state.phase = Some("running_tool".into());
                changed = true;
                let (tool, path) = extract_tool(ev.get("tool_call").unwrap());
                if let Some(t) = tool {
                    state.last_tool = Some(t);
                }
                if let Some(p) = path
                    && !state.files_touched.contains(&p)
                {
                    state.files_touched.push(p);
                }
            }
        }
        "assistant" => {
            if let Some(text) = assistant_text.as_ref() {
                let truncated: String = text.chars().take(200).collect();
                state.last_assistant = Some(truncated);
                state.phase = Some("responding".into());
                changed = true;
            }
            // Claude puts tool calls inside the assistant message, not a tool_call event.
            if let Some(items) = ev.pointer("/message/content").and_then(|c| c.as_array()) {
                for item in items {
                    if note_tool_use(item, state) {
                        changed = true;
                    }
                }
            }
        }
        "thinking" => {
            if ev.get("subtype").and_then(|s| s.as_str()) == Some("delta")
                && ev.get("text").and_then(|t| t.as_str()).is_some()
            {
                state.phase = Some("thinking".into());
                changed = true;
            }
        }
        "result" => {
            let raw: RawCursorJson = serde_json::from_value(ev).unwrap_or_default();
            return ParsedLine {
                result: Some(raw),
                changed: true,
                assistant_text: None,
            };
        }
        _ => {}
    }

    ParsedLine {
        result: None,
        changed,
        assistant_text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn records_the_first_session_id_even_on_a_system_event() {
        let fixture = include_str!("../tests/fixtures/contract/cursor/cancel-mid-tool.stdout");
        let mut state = init_stream_state();
        let mut lines = fixture.lines();
        let first = lines.next().unwrap();
        assert!(parse_line(first, &mut state).changed);
        assert_eq!(
            state.session_id.as_deref(),
            Some("2a2056ae-a4c8-48ba-aaf1-e493a08074a5")
        );
        for line in lines {
            parse_line(line, &mut state);
        }
        assert_eq!(
            state.session_id.as_deref(),
            Some("2a2056ae-a4c8-48ba-aaf1-e493a08074a5")
        );
    }

    #[test]
    fn ignores_blank_and_non_json() {
        let mut s = init_stream_state();
        assert!(!parse_line("", &mut s).changed);
        assert!(!parse_line("   ", &mut s).changed);
        assert!(!parse_line("not json", &mut s).changed);
    }

    #[test]
    fn ignores_events_with_no_type() {
        let mut s = init_stream_state();
        assert!(!parse_line(&json!({"foo":1}).to_string(), &mut s).changed);
    }

    #[test]
    fn shell_tool_call_sets_last_tool() {
        let mut s = init_stream_state();
        parse_line(
            &json!({
                "type":"tool_call","subtype":"started",
                "tool_call":{"shellToolCall":{}}
            })
            .to_string(),
            &mut s,
        );
        assert_eq!(s.last_tool.as_deref(), Some("shell"));
        assert_eq!(s.phase.as_deref(), Some("running_tool"));
    }

    #[test]
    fn edit_tool_call_records_path() {
        let mut s = init_stream_state();
        parse_line(
            &json!({
                "type":"tool_call","subtype":"started",
                "tool_call":{"editToolCall":{"args":{"path":"src/a.ts"}}}
            })
            .to_string(),
            &mut s,
        );
        assert_eq!(s.last_tool.as_deref(), Some("edit"));
        assert_eq!(s.files_touched, ["src/a.ts"]);
    }

    #[test]
    fn write_delete_record_path_shaped_args() {
        let mut s = init_stream_state();
        parse_line(
            &json!({
                "type":"tool_call","subtype":"started",
                "tool_call":{"writeToolCall":{"args":{"filePath":"src/new.ts"}}}
            })
            .to_string(),
            &mut s,
        );
        assert_eq!(s.last_tool.as_deref(), Some("write"));
        assert_eq!(s.files_touched, ["src/new.ts"]);
        parse_line(
            &json!({
                "type":"tool_call","subtype":"started",
                "tool_call":{"deleteToolCall":{"args":{"path":"old.ts"}}}
            })
            .to_string(),
            &mut s,
        );
        assert_eq!(s.files_touched, ["src/new.ts", "old.ts"]);
    }

    #[test]
    fn mcp_tool_call_uses_tool_name() {
        let mut s = init_stream_state();
        parse_line(
            &json!({
                "type":"tool_call","subtype":"started",
                "tool_call":{"mcpToolCall":{"toolName":"search"}}
            })
            .to_string(),
            &mut s,
        );
        assert_eq!(s.last_tool.as_deref(), Some("search"));
        let mut s2 = init_stream_state();
        parse_line(
            &json!({
                "type":"tool_call","subtype":"started",
                "tool_call":{"mcpToolCall":{}}
            })
            .to_string(),
            &mut s2,
        );
        assert_eq!(s2.last_tool.as_deref(), Some("mcp"));
    }

    #[test]
    fn non_started_does_not_update() {
        let mut s = init_stream_state();
        parse_line(
            &json!({
                "type":"tool_call","subtype":"completed",
                "tool_call":{"shellToolCall":{}}
            })
            .to_string(),
            &mut s,
        );
        assert!(s.last_tool.is_none());
    }

    #[test]
    fn assistant_text_truncated_to_200() {
        let mut s = init_stream_state();
        let long = "x".repeat(300);
        let parsed = parse_line(
            &json!({
                "type":"assistant",
                "message":{"content":[{"type":"text","text": long}]}
            })
            .to_string(),
            &mut s,
        );
        assert_eq!(s.last_assistant.as_ref().map(|t| t.len()), Some(200));
        assert_eq!(s.phase.as_deref(), Some("responding"));
        assert_eq!(parsed.assistant_text.as_deref(), Some(long.as_str()));

        let mut s2 = init_stream_state();
        let parsed2 = parse_line(
            &json!({"type":"tool_call","subtype":"started","tool_call":{"shellToolCall":{}}})
                .to_string(),
            &mut s2,
        );
        assert!(parsed2.assistant_text.is_none());
    }

    #[test]
    fn thinking_delta_sets_phase() {
        let mut s = init_stream_state();
        parse_line(
            &json!({"type":"thinking","subtype":"delta","text":"hmm"}).to_string(),
            &mut s,
        );
        assert_eq!(s.phase.as_deref(), Some("thinking"));
    }

    #[test]
    fn usage_output_tokens_updates() {
        let mut s = init_stream_state();
        parse_line(
            &json!({"type":"assistant","usage":{"outputTokens":42}}).to_string(),
            &mut s,
        );
        assert_eq!(s.tokens_so_far, 42.0);
    }

    #[test]
    fn tokens_so_far_monotonic() {
        let mut s = init_stream_state();
        parse_line(
            &json!({"type":"assistant","usage":{"outputTokens":100}}).to_string(),
            &mut s,
        );
        parse_line(
            &json!({"type":"assistant","usage":{"outputTokens":40}}).to_string(),
            &mut s,
        );
        assert_eq!(s.tokens_so_far, 100.0);
    }

    #[test]
    fn claude_tool_use_sets_name_and_path() {
        let mut s = init_stream_state();
        parse_line(
            &json!({
                "type": "assistant",
                "message": {"content": [
                    {"type": "tool_use", "name": "Edit", "input": {"file_path": "calc.py"}}
                ]}
            })
            .to_string(),
            &mut s,
        );
        assert_eq!(s.last_tool.as_deref(), Some("Edit"));
        assert_eq!(s.files_touched, ["calc.py"]);
        assert_eq!(s.phase.as_deref(), Some("running_tool"));
    }

    #[test]
    fn skips_system_and_rate_limit_events() {
        let mut s = init_stream_state();
        for line in [
            json!({"type": "system", "subtype": "hook_started"}),
            json!({"type": "system", "subtype": "hook_response"}),
            json!({"type": "system", "subtype": "init"}),
            json!({"type": "system", "subtype": "commands_changed"}),
            json!({"type": "rate_limit_event"}),
        ] {
            assert!(!parse_line(&line.to_string(), &mut s).changed);
        }
        assert!(s.last_tool.is_none());
        assert_eq!(s.tokens_so_far, 0.0);
    }

    #[test]
    fn result_event_returns_raw() {
        let mut s = init_stream_state();
        let r = parse_line(
            &json!({
                "type":"result","subtype":"success","is_error":false,
                "result":"final","session_id":"sid","duration_ms":99,
                "usage":{"outputTokens":10}
            })
            .to_string(),
            &mut s,
        );
        assert!(r.result.is_some());
        assert_eq!(r.result.as_ref().unwrap().result.as_deref(), Some("final"));
        assert_eq!(
            r.result.as_ref().unwrap().session_id.as_deref(),
            Some("sid")
        );
        assert_eq!(s.tokens_so_far, 10.0);
    }

    #[test]
    fn malformed_usage_keeps_the_rest_of_the_result() {
        let mut s = init_stream_state();
        let r = parse_line(
            &json!({
                "type":"result","is_error":false,"result":"final","session_id":"sid",
                "usage":{"outputTokens":"7"}
            })
            .to_string(),
            &mut s,
        );
        let raw = r.result.unwrap();
        assert_eq!(raw.result.as_deref(), Some("final"));
        assert_eq!(raw.session_id.as_deref(), Some("sid"));
        assert_eq!(raw.is_error, Some(false));
        assert!(raw.usage.is_none());
    }
}
