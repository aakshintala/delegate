use crate::types::PollResult;
use crate::util::{json_compact, random_uuid};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub trait StatusRecordWriter: Send + Sync {
    fn write(&self, job_id: &str, record: &PollResult);
}

/// Status JSON for one job under `$TMPDIR/delegate-jobs/`.
pub fn job_record_path(job_id: &str) -> PathBuf {
    std::env::temp_dir()
        .join("delegate-jobs")
        .join(format!("{job_id}.json"))
}

pub struct FileStatusRecordWriter;

impl StatusRecordWriter for FileStatusRecordWriter {
    fn write(&self, job_id: &str, record: &PollResult) {
        let _ = write_atomic(&job_record_path(job_id), &json_compact(record));
    }
}

/// Write to a unique tmp sibling, then rename over `path`, so readers never see a partial file.
pub fn write_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    fs::create_dir_all(dir)?;
    let tmp = dir.join(format!(".{}.tmp", random_uuid()));
    fs::write(&tmp, contents)
        .and_then(|_| fs::rename(&tmp, path))
        .inspect_err(|_| {
            let _ = fs::remove_file(&tmp);
        })
}

pub fn file_status_record_writer() -> FileStatusRecordWriter {
    FileStatusRecordWriter
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CliResume {
    pub model: String,
    pub backend: Option<String>,
    pub cwd: String,
    pub session_id: Option<String>,
    pub gate: String,
    #[serde(serialize_with = "crate::util::js_num_opt")]
    pub tool_idle_ms: Option<f64>,
}

/// `PollResult` plus what a watcher needs to find and resume the job, plus the resume
/// chain links: `resumedFrom` (this job continues that one) and `supersededBy` (written
/// into the old record once the new job is spawned).
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CliRecord {
    #[serde(flatten)]
    pub poll: PollResult,
    pub supervisor_pid: u32,
    pub resume: CliResume,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumed_from: Option<String>,
}

/// Writes the CLI record for the one job this supervisor runs. A failed write exits the
/// supervisor: the file is the only truth, so a job that cannot report is not worth running.
/// ponytail: the agent child is orphaned on that exit; `watch` reports the dead supervisor.
pub struct CliRecordWriter {
    pub job_id: String,
    pub model: String,
    pub backend: Option<String>,
    pub cwd: String,
    pub gate: String,
    pub tool_idle_ms: Option<f64>,
    pub resumed_from: Option<String>,
    pub last_session_id: Mutex<Option<String>>,
}

impl StatusRecordWriter for CliRecordWriter {
    fn write(&self, _registry_id: &str, record: &PollResult) {
        let mut poll = record.clone();
        if let PollResult::Terminal { result, .. } = &mut poll {
            result.job_id = Some(self.job_id.clone());
        }
        let current_session_id = match &poll {
            PollResult::Running { progress, .. } => progress.session_id.clone(),
            PollResult::Terminal { result, .. } => result.session_id.clone(),
            PollResult::NotFound { .. } => None,
        };
        let session_id = {
            let mut last = self
                .last_session_id
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if current_session_id.is_some() {
                *last = current_session_id;
            }
            last.clone()
        };
        let rec = CliRecord {
            poll,
            supervisor_pid: std::process::id(),
            resume: CliResume {
                model: self.model.clone(),
                backend: self.backend.clone(),
                cwd: self.cwd.clone(),
                session_id,
                gate: self.gate.clone(),
                tool_idle_ms: self.tool_idle_ms,
            },
            resumed_from: self.resumed_from.clone(),
        };
        let path = job_record_path(&self.job_id);
        let res = write_atomic(&path, &json_compact(&rec));
        if let Err(e) = res {
            eprintln!("cannot write status record {}: {e}", path.display());
            std::process::exit(1);
        }
    }
}

/// A RUNNING record whose supervisor is gone. Keeps resume so a later command can still
/// see what the job was; the result text is the whole reason.
pub fn write_supervisor_died(job_id: &str, prior: &serde_json::Value) -> std::io::Result<()> {
    let mut rec = prior.clone();
    let Some(obj) = rec.as_object_mut() else {
        return Err(std::io::Error::other("status record is not an object"));
    };
    obj.insert("status".into(), "ERROR".into());
    obj.remove("lastHeartbeatAt");
    obj.remove("progress");
    let resume = &prior["resume"];
    obj.insert(
        "result".into(),
        serde_json::json!({
            "status": "ERROR",
            "text": "supervisor died",
            "sessionId": resume["sessionId"].clone(),
            "backend": resume["backend"].clone(),
            "model": resume["model"].as_str().unwrap_or(""),
            "usage": null,
            "costUsd": null,
            "costEstimated": false,
            "durationMs": null,
            "jobId": job_id,
        }),
    );
    write_atomic(&job_record_path(job_id), &json_compact(&rec))
}

/// A RUNNING record whose supervisor is gone or had to be SIGKILLed: nobody is left to
/// finalize, so `cancel` writes the CANCELLED terminal record itself.
pub fn write_cancelled(job_id: &str, prior: &serde_json::Value) -> std::io::Result<()> {
    let mut rec = prior.clone();
    let Some(obj) = rec.as_object_mut() else {
        return Err(std::io::Error::other("status record is not an object"));
    };
    let resume = &prior["resume"];
    let session_id = resume["sessionId"].clone();
    let backend = resume["backend"].clone();
    let model = resume
        .get("model")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    obj.insert("status".into(), "CANCELLED".into());
    obj.remove("lastHeartbeatAt");
    obj.remove("progress");
    obj.insert(
        "result".into(),
        serde_json::json!({
            "status": "CANCELLED",
            "text": "Cancelled by user.",
            "sessionId": session_id,
            "backend": backend,
            "model": model,
            "usage": null,
            "costUsd": null,
            "costEstimated": true,
            "durationMs": null,
            "jobId": job_id,
        }),
    );
    write_atomic(&job_record_path(job_id), &json_compact(&rec))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn written_record(id: &str) -> serde_json::Value {
        serde_json::from_str(&fs::read_to_string(job_record_path(id)).unwrap()).unwrap()
    }

    #[test]
    fn fallback_writers_preserve_the_session_and_configured_backend() {
        let prior = json!({
            "status": "RUNNING",
            "resume": {"model": "model", "backend": "pi", "sessionId": "s-init"}
        });
        let cancelled_id = random_uuid();
        write_cancelled(&cancelled_id, &prior).unwrap();
        let cancelled = written_record(&cancelled_id);
        assert_eq!(cancelled["resume"]["sessionId"], "s-init");
        assert_eq!(cancelled["result"]["sessionId"], "s-init");
        assert_eq!(cancelled["result"]["backend"], "pi");
        let _ = fs::remove_file(job_record_path(&cancelled_id));

        let dead_id = random_uuid();
        write_supervisor_died(&dead_id, &prior).unwrap();
        let dead = written_record(&dead_id);
        assert_eq!(dead["resume"]["sessionId"], "s-init");
        assert_eq!(dead["result"]["sessionId"], "s-init");
        assert_eq!(dead["result"]["backend"], "pi");
        let _ = fs::remove_file(job_record_path(&dead_id));
    }

    #[test]
    fn supervisor_fallback_does_not_guess_missing_backend_or_session() {
        let id = random_uuid();
        let prior = json!({
            "status": "RUNNING",
            "resume": {"model": "old-model", "sessionId": null}
        });
        write_supervisor_died(&id, &prior).unwrap();
        let dead = written_record(&id);
        assert!(dead["result"]["backend"].is_null());
        assert!(dead["result"]["sessionId"].is_null());
        let _ = fs::remove_file(job_record_path(&id));
    }
}
