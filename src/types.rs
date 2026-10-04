use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    #[serde(serialize_with = "crate::util::js_num")]
    #[serde(default, alias = "input_tokens")]
    pub input_tokens: f64,
    #[serde(serialize_with = "crate::util::js_num")]
    #[serde(default, alias = "output_tokens")]
    pub output_tokens: f64,
    /// Claude reports this as `cache_read_input_tokens`.
    #[serde(serialize_with = "crate::util::js_num")]
    #[serde(default, alias = "cache_read_input_tokens")]
    pub cache_read_tokens: f64,
    /// Claude reports this as `cache_creation_input_tokens`.
    #[serde(serialize_with = "crate::util::js_num")]
    #[serde(default, alias = "cache_creation_input_tokens")]
    pub cache_write_tokens: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RunStatus {
    #[serde(rename = "DONE")]
    Done,
    #[serde(rename = "DONE_WITH_CONCERNS")]
    DoneWithConcerns,
    #[serde(rename = "BLOCKED")]
    Blocked,
    #[serde(rename = "NEEDS_CONTEXT")]
    NeedsContext,
    #[serde(rename = "ERROR")]
    Error,
    /// The registry killed the run: `cancel` or the idle watchdog. Only ever set by the
    /// registry itself, never parsed from an agent STATUS line.
    #[serde(rename = "CANCELLED")]
    Cancelled,
    #[serde(rename = "STALLED")]
    Stalled,
}

pub const RUN_STATUSES: [&str; 5] = [
    "DONE",
    "DONE_WITH_CONCERNS",
    "BLOCKED",
    "NEEDS_CONTEXT",
    "ERROR",
];

impl RunStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Done => "DONE",
            Self::DoneWithConcerns => "DONE_WITH_CONCERNS",
            Self::Blocked => "BLOCKED",
            Self::NeedsContext => "NEEDS_CONTEXT",
            Self::Error => "ERROR",
            Self::Cancelled => "CANCELLED",
            Self::Stalled => "STALLED",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "DONE" => Some(Self::Done),
            "DONE_WITH_CONCERNS" => Some(Self::DoneWithConcerns),
            "BLOCKED" => Some(Self::Blocked),
            "NEEDS_CONTEXT" => Some(Self::NeedsContext),
            "ERROR" => Some(Self::Error),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeSet {
    pub head_before: Option<String>,
    pub head_after: Option<String>,
    pub new_commits: Vec<String>,
    pub files_changed: Vec<String>,
    pub diffstat: String,
    pub uncommitted_files: Vec<String>,
    pub dirty_after: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GateResult {
    pub command: String,
    pub exit_code: i32,
    pub passed: bool,
    pub output_tail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunOutput {
    pub status: RunStatus,
    pub text: String,
    pub session_id: Option<String>,
    pub backend: String,
    pub model: String,
    pub usage: Option<Usage>,
    #[serde(serialize_with = "crate::util::js_num_opt")]
    pub cost_usd: Option<f64>,
    pub cost_estimated: bool,
    #[serde(serialize_with = "crate::util::js_num_opt")]
    pub duration_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr_tail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gate_result: Option<GateResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_set: Option<ChangeSet>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub concerns: Option<Vec<String>>,
    /// Claude `permission_denials` objects, kept whole. Empty stays off the record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub permission_denials: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Price {
    pub input: f64,
    pub output: f64,
    #[serde(rename = "cacheRead")]
    pub cache_read: f64,
    #[serde(rename = "cacheWrite")]
    pub cache_write: f64,
}

pub type PriceMap = std::collections::HashMap<String, Price>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelEntry {
    pub label: String,
    pub backend: String,
    pub price: Price,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tiers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedModel {
    pub model: String,
    pub backend: String,
    pub price: Price,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct HostProfile {
    pub default: Option<String>,
    pub models: Option<std::collections::HashMap<String, ModelEntry>>,
    pub gate: Option<String>,
    pub idle_ms: Option<Option<f64>>,
    pub tool_idle_ms: Option<Option<f64>>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub default: String,
    pub models: std::collections::HashMap<String, ModelEntry>,
    pub price_map: PriceMap,
    pub profile: HostProfile,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DoctorPluginInfo {
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DoctorAgentInfo {
    pub found: bool,
    pub path: Option<String>,
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DoctorAccountInfo {
    pub logged_in: bool,
    pub email: Option<String>,
    pub subscription: Option<String>,
    pub current_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DoctorModelMenuInfo {
    pub configured_ids: Vec<String>,
    pub account_ids: Option<Vec<String>>,
    pub missing_from_account: Vec<String>,
    pub prices_checkable: bool,
    pub note: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DoctorBackendSection {
    pub backend: String,
    pub found: bool,
    pub path: Option<String>,
    pub version: Option<String>,
    pub version_error: Option<String>,
    pub model_failures: Vec<String>,
}

/// Claude doctor probes. Absent when the cursor test seam skipped them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DoctorClaudeInfo {
    pub found: bool,
    pub path: Option<String>,
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_error: Option<String>,
    pub logged_in: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub login_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DoctorReport {
    pub ok: bool,
    pub plugin: DoctorPluginInfo,
    pub agent: DoctorAgentInfo,
    pub account: DoctorAccountInfo,
    pub model_menu: DoctorModelMenuInfo,
    #[serde(default)]
    pub sections: Vec<DoctorBackendSection>,
    pub warnings: Vec<String>,
    pub failures: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude: Option<DoctorClaudeInfo>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResumeContext {
    pub model: String,
    pub gate: String,
}

#[derive(Debug, Clone)]
pub struct JobSpec {
    pub bin: String,
    pub argv: Vec<String>,
    pub cwd: String,
    pub model: String,
    pub backend: String,
    pub path: Option<String>,
    pub head_before: Option<String>,
    pub gate: String,
    pub idle_ms: Option<Option<f64>>,
    pub tool_idle_ms: Option<Option<f64>>,
    pub price_map: PriceMap,
    pub resume_context: ResumeContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    #[serde(rename = "RUNNING")]
    Running,
    #[serde(rename = "DONE")]
    Done,
    #[serde(rename = "DONE_WITH_CONCERNS")]
    DoneWithConcerns,
    #[serde(rename = "BLOCKED")]
    Blocked,
    #[serde(rename = "NEEDS_CONTEXT")]
    NeedsContext,
    #[serde(rename = "ERROR")]
    Error,
    #[serde(rename = "CANCELLED")]
    Cancelled,
    #[serde(rename = "STALLED")]
    Stalled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Done => "DONE",
            Self::DoneWithConcerns => "DONE_WITH_CONCERNS",
            Self::Blocked => "BLOCKED",
            Self::NeedsContext => "NEEDS_CONTEXT",
            Self::Error => "ERROR",
            Self::Cancelled => "CANCELLED",
            Self::Stalled => "STALLED",
        }
    }

    pub fn from_run(s: RunStatus) -> Self {
        match s {
            RunStatus::Done => Self::Done,
            RunStatus::DoneWithConcerns => Self::DoneWithConcerns,
            RunStatus::Blocked => Self::Blocked,
            RunStatus::NeedsContext => Self::NeedsContext,
            RunStatus::Error => Self::Error,
            RunStatus::Cancelled => Self::Cancelled,
            RunStatus::Stalled => Self::Stalled,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressSnapshot {
    pub last_tool: Option<String>,
    #[serde(serialize_with = "crate::util::js_num")]
    pub tokens_so_far: f64,
    #[serde(serialize_with = "crate::util::js_num")]
    pub elapsed_ms: f64,
    pub last_assistant: Option<String>,
    pub files_touched_so_far: Vec<String>,
    pub phase: Option<String>,
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum PollResult {
    Running {
        status: &'static str,
        #[serde(rename = "lastHeartbeatAt", serialize_with = "crate::util::js_num")]
        last_heartbeat_at: f64,
        #[serde(rename = "supersededBy", skip_serializing_if = "Option::is_none")]
        superseded_by: Option<String>,
        progress: ProgressSnapshot,
    },
    Terminal {
        status: JobStatus,
        result: RunOutput,
        #[serde(rename = "supersededBy", skip_serializing_if = "Option::is_none")]
        superseded_by: Option<String>,
    },
    NotFound {
        status: &'static str,
    },
}

impl PollResult {
    pub fn not_found() -> Self {
        Self::NotFound {
            status: "NOT_FOUND",
        }
    }

    pub fn status_label(&self) -> &str {
        match self {
            Self::Running { .. } => "RUNNING",
            Self::Terminal { status, .. } => status.as_str(),
            Self::NotFound { .. } => "NOT_FOUND",
        }
    }
}

impl PartialEq<&str> for PollResult {
    fn eq(&self, other: &&str) -> bool {
        self.status_label() == *other
    }
}

pub struct FinalizeCtx {
    pub cwd: String,
    pub head_before: Option<String>,
    pub gate: String,
    /// Kill the gate after this many milliseconds. `None` keeps the gate's own default.
    pub gate_timeout_ms: Option<u64>,
    pub model: String,
    pub backend: String,
    pub price_map: PriceMap,
    pub job_id: Option<String>,
    pub run_gate:
        Option<Box<dyn Fn(&str, &str, Option<&crate::util::Abort>) -> GateResult + Send + Sync>>,
    pub signal: Option<crate::util::Abort>,
    pub git_delta: Option<Box<dyn Fn(&str, Option<&str>) -> Option<ChangeSet> + Send + Sync>>,
}
