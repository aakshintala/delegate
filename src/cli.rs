//! The `delegate` CLI: `run` starts a detached supervisor, `watch` reads job records.
//! The record file under `$TMPDIR/delegate-jobs/` is the only link between them.

use crate::backends::Backend;
use crate::config::build_deps;
use crate::git::capture_head;
use crate::job::{JobDeps, JobHandle};
use crate::models::resolve_model;
use crate::prompt::status_block;
use crate::status_record::{
    CliRecordWriter, job_record_path, write_atomic, write_cancelled, write_supervisor_died,
};
use crate::types::{Config, JobSpec, ResumeContext};
use crate::util::{json_compact, random_uuid, resolve_path};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::{Duration, Instant};

const USAGE: &str =
    "usage: delegate run --model M [--cwd D] [--gate CMD] [--tool-idle-ms N] [--prompt-file F]
       delegate resume <jobId> [--model M] [--gate CMD] [--prompt-file F | stdin]
       delegate cancel <jobId>
       delegate watch <jobId>... [--timeout S]  (exit 1: timed out; jobs still RUNNING)
       delegate models
       delegate doctor";

/// Bad input: reason on stderr, exit 2.
struct Usage(String);

fn usage<T>(msg: impl Into<String>) -> Result<T, Usage> {
    Err(Usage(msg.into()))
}

pub fn main() -> i32 {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let res = match args.first().map(String::as_str) {
        Some("run") => run(&args[1..]),
        Some("resume") => resume(&args[1..]),
        Some("cancel") => cancel(&args[1..]),
        Some("watch") => watch(&args[1..]),
        Some("models") => crate::cli_info::models().map_err(Usage),
        Some("doctor") => crate::cli_info::doctor().map_err(Usage),
        Some("__supervise") => return supervise(&args[1..]),
        _ => usage(USAGE),
    };
    match res {
        Ok(code) => code,
        Err(Usage(m)) => {
            eprintln!("{m}");
            2
        }
    }
}

/// Splits `--flag value` pairs from positionals.
fn parse(args: &[String], flags: &[&str]) -> Result<(Vec<(String, String)>, Vec<String>), Usage> {
    let (mut kv, mut pos) = (vec![], vec![]);
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a.starts_with("--") {
            if !flags.contains(&a.as_str()) {
                return usage(format!("unknown flag {a}\n{USAGE}"));
            }
            let Some(v) = it.next() else {
                return usage(format!("{a} needs a value"));
            };
            kv.push((a.clone(), v.clone()));
        } else {
            pos.push(a.clone());
        }
    }
    Ok((kv, pos))
}

fn flag<'a>(kv: &'a [(String, String)], name: &str) -> Option<&'a str> {
    kv.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
}

/// `run`'s unknown-id message, reused wherever a model flag is resolved.
fn unknown_model(config: &Config, model: &str) -> String {
    let mut ids: Vec<_> = config.models.keys().cloned().collect();
    ids.sort();
    format!("unknown model {model}; valid models: {}", ids.join(", "))
}

/// The shared half of `run` and `resume`: stash the prompt, spawn the detached
/// supervisor and wait for its first record. Returns the new job id. `Usage` means exit
/// 2; `Done` means the reason is already on stderr, return the code.
enum LaunchErr {
    Usage(String),
    Done(i32),
}

struct LaunchParams<'a> {
    prompt: &'a str,
    model: &'a str,
    cwd: &'a str,
    gate: &'a str,
    tool_idle_ms: Option<f64>,
    session: Option<&'a str>,
    resumed_from: Option<&'a str>,
}

fn launch(p: LaunchParams<'_>) -> Result<String, LaunchErr> {
    let id = random_uuid();
    let record = job_record_path(&id);
    let dir = record.parent().expect("record has a parent");
    if let Err(e) = std::fs::create_dir_all(dir) {
        return Err(LaunchErr::Usage(format!("{}: {e}", dir.display())));
    }
    let prompt_file = dir.join(format!("{id}.prompt"));
    let written = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&prompt_file)
        .and_then(|mut f| f.write_all(p.prompt.as_bytes()));
    if let Err(e) = written {
        let _ = std::fs::remove_file(&prompt_file);
        return Err(LaunchErr::Usage(format!("{}: {e}", prompt_file.display())));
    }

    let mut supervise_args = vec![
        "__supervise".to_string(),
        id.clone(),
        p.model.to_string(),
        p.cwd.to_string(),
    ];
    if !p.gate.is_empty() {
        supervise_args.push("--gate".into());
        supervise_args.push(p.gate.to_string());
    }
    if let Some(ms) = p.tool_idle_ms {
        supervise_args.push("--tool-idle-ms".into());
        // Shortest round-trip so the supervisor parses the same number back.
        supervise_args.push(ms.to_string());
    }
    if let Some(s) = p.session {
        supervise_args.push("--session".into());
        supervise_args.push(s.to_string());
    }
    if let Some(r) = p.resumed_from {
        supervise_args.push("--resumed-from".into());
        supervise_args.push(r.to_string());
    }
    // Own session and process group, stdio closed: the supervisor outlives this process.
    let exe = std::env::current_exe().map_err(|e| LaunchErr::Usage(e.to_string()))?;
    let mut child = unsafe {
        Command::new(exe)
            .args(&supervise_args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .pre_exec(|| {
                libc::setsid();
                Ok(())
            })
            .spawn()
    }
    .map_err(|e| {
        let _ = std::fs::remove_file(&prompt_file);
        LaunchErr::Usage(format!("cannot start supervisor: {e}"))
    })?;
    // The supervisor writes the first RUNNING record before the agent starts; wait for it.
    let t = Instant::now();
    while !record.exists() {
        if child.try_wait().ok().flatten().is_some() || t.elapsed() > Duration::from_secs(10) {
            let _ = std::fs::remove_file(&prompt_file);
            eprintln!("supervisor failed to write {}", record.display());
            return Err(LaunchErr::Done(1));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(id)
}

fn run(args: &[String]) -> Result<i32, Usage> {
    let (kv, _) = parse(
        args,
        &[
            "--model",
            "--cwd",
            "--gate",
            "--tool-idle-ms",
            "--prompt-file",
        ],
    )?;
    let Some(model) = flag(&kv, "--model") else {
        return usage("--model is required");
    };
    let gate = flag(&kv, "--gate").unwrap_or("").to_string();
    let tool_idle_ms = match flag(&kv, "--tool-idle-ms") {
        None => None,
        Some(s) => match s.parse::<f64>() {
            Ok(n) if n.is_finite() && n > 0.0 => Some(n),
            _ => return usage(format!("invalid --tool-idle-ms {s}")),
        },
    };
    let deps = match build_deps() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{e}");
            return Ok(1);
        }
    };
    if let Err(e) = resolve_model(Some(model), &deps.config) {
        // An unknown id lists the valid ids; any other failure (e.g. a model on
        // a backend that is not implemented yet) prints the actual error.
        if e.downcast_ref::<crate::models::ModelNotAllowedError>()
            .is_some()
        {
            return usage(unknown_model(&deps.config, model));
        }
        return usage(format!("{e}"));
    }
    let mut prompt = String::new();
    match flag(&kv, "--prompt-file") {
        Some(f) => prompt = std::fs::read_to_string(f).map_err(|e| Usage(format!("{f}: {e}")))?,
        None => {
            let _ = std::io::stdin().read_to_string(&mut prompt);
        }
    }
    if prompt.trim().is_empty() {
        return usage("prompt is empty");
    }
    let cwd = match flag(&kv, "--cwd") {
        Some(d) => resolve_path(d),
        None => std::env::current_dir()
            .map(|p| p.to_string_lossy().into_owned())
            .map_err(|e| Usage(e.to_string()))?,
    };
    if !std::path::Path::new(&cwd).is_dir() {
        return usage(format!("cwd {cwd} is not a directory"));
    }

    match launch(LaunchParams {
        prompt: &prompt,
        model,
        cwd: &cwd,
        gate: &gate,
        tool_idle_ms,
        session: None,
        resumed_from: None,
    }) {
        Ok(id) => {
            println!("{id}");
            Ok(0)
        }
        Err(LaunchErr::Usage(m)) => Err(Usage(m)),
        Err(LaunchErr::Done(c)) => Ok(c),
    }
}

fn resume(args: &[String]) -> Result<i32, Usage> {
    let (kv, pos) = parse(args, &["--model", "--gate", "--prompt-file"])?;
    let [old_id] = pos.as_slice() else {
        return usage(USAGE);
    };
    let Some(raw) = read_record(old_id) else {
        return usage(format!("unknown job {old_id}"));
    };
    let old: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| Usage(format!("cannot read job {old_id}: {e}")))?;
    if old["status"] == "RUNNING" {
        return usage(format!("cannot resume {old_id}: job is RUNNING"));
    }
    let stored_model = old["resume"]["model"].as_str().unwrap_or("").to_string();
    if stored_model.is_empty() {
        return usage(format!("cannot resume {old_id}: record has no model"));
    }
    let Some(session) = old["resume"]["sessionId"].as_str() else {
        return usage(format!("cannot resume {old_id}: record has no session id"));
    };
    let session = session.to_string();
    let stored_cwd = old["resume"]["cwd"].as_str().unwrap_or("").to_string();
    let stored_gate = old["resume"]["gate"].as_str().unwrap_or("").to_string();
    let tool_idle_ms = old["resume"]["toolIdleMs"].as_f64();

    let deps = match build_deps() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{e}");
            return Ok(1);
        }
    };
    // Overrides replace stored values. A model on another backend would continue the
    // wrong session, so it is rejected before anything spawns.
    let model = flag(&kv, "--model").unwrap_or(&stored_model).to_string();
    if flag(&kv, "--model").is_some() {
        let stored_backend = deps
            .config
            .models
            .get(&stored_model)
            .map(|e| e.backend.as_str());
        let new_backend = deps.config.models.get(&model).map(|e| e.backend.as_str());
        match (stored_backend, new_backend) {
            (Some(a), Some(b)) if a != b => {
                return usage(format!(
                    "cannot resume {old_id}: model {model} uses backend \"{b}\", \
                     but {stored_model} ran on backend \"{a}\""
                ));
            }
            (_, None) => return usage(unknown_model(&deps.config, &model)),
            _ => {}
        }
        // Unknown ids are rejected above; what remains is a same-backend id the
        // resolver may still refuse (e.g. a backend that is not implemented yet).
        if let Err(e) = resolve_model(Some(&model), &deps.config) {
            return usage(format!("{e}"));
        }
    }
    let gate = flag(&kv, "--gate").unwrap_or(&stored_gate).to_string();

    let mut prompt = String::new();
    match flag(&kv, "--prompt-file") {
        Some(f) => prompt = std::fs::read_to_string(f).map_err(|e| Usage(format!("{f}: {e}")))?,
        None => {
            let _ = std::io::stdin().read_to_string(&mut prompt);
        }
    }
    if prompt.trim().is_empty() {
        return usage("prompt is empty");
    }
    if !std::path::Path::new(&stored_cwd).is_dir() {
        return usage(format!("cwd {stored_cwd} is not a directory"));
    }

    let new_id = match launch(LaunchParams {
        prompt: &prompt,
        model: &model,
        cwd: &stored_cwd,
        gate: &gate,
        tool_idle_ms,
        session: Some(&session),
        resumed_from: Some(old_id),
    }) {
        Ok(id) => id,
        Err(LaunchErr::Usage(m)) => return Err(Usage(m)),
        Err(LaunchErr::Done(c)) => return Ok(c),
    };
    // B is spawned and its record exists; link A to it, preserving everything else.
    // Re-read so a concurrent `watch` settle is not clobbered. B is already running,
    // so a failed link is a warning: the caller still needs B's id.
    let link = read_record(old_id)
        .ok_or_else(|| format!("record for {old_id} is gone"))
        .and_then(|raw| {
            serde_json::from_str::<serde_json::Value>(&raw)
                .map_err(|e| format!("cannot read job {old_id}: {e}"))
        });
    match link {
        Ok(mut old) => {
            if let Some(obj) = old.as_object_mut() {
                obj.insert("supersededBy".into(), new_id.clone().into());
            }
            if let Err(e) = write_atomic(&job_record_path(old_id), &json_compact(&old)) {
                eprintln!("warning: cannot link {old_id} to {new_id}: {e}");
            }
        }
        Err(reason) => eprintln!("warning: {reason}"),
    }
    println!("{new_id}");
    Ok(0)
}

fn cancel(args: &[String]) -> Result<i32, Usage> {
    let (_, pos) = parse(args, &[])?;
    let [id] = pos.as_slice() else {
        return usage(USAGE);
    };
    let Some(raw) = read_record(id) else {
        return usage(format!("unknown job {id}"));
    };
    let rec: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| Usage(format!("cannot read job {id}: {e}")))?;
    if rec["status"] != "RUNNING" {
        println!("{rec}");
        return Ok(0);
    }
    let Some(pid) = rec["supervisorPid"].as_i64() else {
        return usage(format!("cannot cancel {id}: record has no supervisor pid"));
    };
    if pid <= 0 || pid > i32::MAX as i64 {
        return usage(format!("cannot cancel {id}: bad supervisor pid {pid}"));
    }
    let pid = pid as i32;
    // A recycled pid must never be signalled: confirm it is still this job's supervisor.
    // If it is gone or something else, there is nothing to signal; report CANCELLED.
    if !supervisor_cmd_matches(pid, id) {
        return cancel_write(id, &rec);
    }
    // The supervisor is a session leader, so its pgid is its pid: one signal reaches it.
    // It translates that into `registry.cancel`, which SIGTERMs the agent's own group.
    unsafe {
        libc::kill(-pid, libc::SIGTERM);
    }
    if let Some(final_rec) = wait_for_terminal_record(id, Duration::from_secs(5)) {
        println!("{final_rec}");
        return Ok(0);
    }
    // The supervisor is hung or already dead: take down its group and the agent's group
    // (a direct child still parented to the supervisor) with SIGKILL, then report CANCELLED.
    for child in child_pids(pid) {
        unsafe {
            libc::kill(-child, libc::SIGKILL);
        }
    }
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
    if let Some(final_rec) = wait_for_terminal_record(id, Duration::from_secs(2)) {
        println!("{final_rec}");
        return Ok(0);
    }
    let Some(raw) = read_record(id).and_then(|s| serde_json::from_str(&s).ok()) else {
        return usage(format!("unknown job {id}"));
    };
    cancel_write(id, &raw)
}

/// `ps -o command= -p <pid>` contains `__supervise <id>` only while this job's
/// supervisor is that pid. A reused pid shows another command (or nothing).
fn supervisor_cmd_matches(pid: i32, id: &str) -> bool {
    let out = std::process::Command::new("ps")
        .args(["-o", "command=", "-p", &pid.to_string()])
        .output();
    let Ok(out) = out else { return false };
    if !out.status.success() {
        return false;
    }
    let want = format!("__supervise {id}");
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .any(|l| l.contains(&want))
}

/// Nobody is left to finalize, so write the CANCELLED terminal record and print it.
fn cancel_write(id: &str, prior: &serde_json::Value) -> Result<i32, Usage> {
    if let Err(e) = write_cancelled(id, prior) {
        eprintln!(
            "cannot write status record {}: {e}",
            job_record_path(id).display()
        );
        return Ok(1);
    }
    println!("{}", read_record(id).unwrap_or_else(|| "{}".to_string()));
    Ok(0)
}

/// The record when it is no longer RUNNING; `None` while it still is (or is unreadable).
fn terminal_record(id: &str) -> Option<serde_json::Value> {
    let raw = read_record(id)?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    if v["status"] == "RUNNING" {
        None
    } else {
        Some(v)
    }
}

/// Poll the record until it turns terminal, or `timeout` passes. Returns the terminal
/// record, or `None` if it is still RUNNING when the time is up.
fn wait_for_terminal_record(id: &str, timeout: Duration) -> Option<serde_json::Value> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(rec) = terminal_record(id) {
            return Some(rec);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    terminal_record(id)
}

/// Direct children of `ppid`, via `ps`. Used only to find the agent after its supervisor
/// stopped answering SIGTERM; best-effort, so an empty list just means SIGKILL the
/// supervisor's group and write the record ourselves.
fn child_pids(ppid: i32) -> Vec<i32> {
    let out = std::process::Command::new("ps")
        .args(["-ax", "-o", "pid=", "-o", "ppid="])
        .output();
    let Ok(out) = out else { return vec![] };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|l| {
            let mut it = l.split_whitespace();
            let pid: i32 = it.next()?.parse().ok()?;
            let pp: i32 = it.next()?.parse().ok()?;
            (pp == ppid).then_some(pid)
        })
        .collect()
}

/// The detached half of `run`: drives one job, keeping its record fresh until it ends.
fn supervise(args: &[String]) -> i32 {
    let fail = |m: String| {
        eprintln!("{m}");
        1
    };
    let (kv, pos) = match parse(
        args,
        &["--gate", "--tool-idle-ms", "--session", "--resumed-from"],
    ) {
        Ok(v) => v,
        Err(Usage(m)) => return fail(m),
    };
    let [id, model, cwd] = pos.as_slice() else {
        return 2;
    };
    let gate = flag(&kv, "--gate").unwrap_or("").to_string();
    let tool_idle_ms = match flag(&kv, "--tool-idle-ms") {
        None => None,
        Some(s) => match s.parse::<f64>() {
            Ok(n) if n.is_finite() && n > 0.0 => Some(n),
            _ => return 2,
        },
    };
    let deps = match build_deps() {
        Ok(d) => d,
        Err(e) => return fail(e.to_string()),
    };
    let config = deps.config;
    let prompt_file = job_record_path(id).with_extension("prompt");
    let Ok(prompt) = std::fs::read_to_string(&prompt_file) else {
        return fail(format!("cannot read {}", prompt_file.display()));
    };
    let _ = std::fs::remove_file(&prompt_file);
    let prompt = format!("{prompt}\n\n---\n\n{}", status_block()).replace('\0', "");

    let resolved = match resolve_model(Some(model), &config) {
        Ok(r) => r,
        Err(e) => return fail(e.to_string()),
    };
    let Some(backend) = Backend::from_name(&resolved.backend) else {
        return fail(format!(
            "model \"{model}\" uses backend \"{}\", which is not implemented yet",
            resolved.backend
        ));
    };
    let session = flag(&kv, "--session").map(str::to_string);
    let resumed_from = flag(&kv, "--resumed-from").map(str::to_string);
    let argv = backend.argv(model, session.as_deref(), &prompt);
    let spec = JobSpec {
        bin: backend.bin(),
        argv,
        cwd: cwd.clone(),
        model: model.clone(),
        backend: backend.name().into(),
        path: Some(resolve_path(cwd)),
        head_before: capture_head(cwd, None),
        gate: gate.clone(),
        idle_ms: None,
        tool_idle_ms: tool_idle_ms.map(Some),
        price_map: config.price_map.clone(),
        resume_context: ResumeContext {
            model: model.clone(),
            gate: gate.clone(),
        },
    };

    let idle = |v: Option<Option<f64>>, d| match v {
        None => Some(d),
        Some(inner) => inner,
    };
    let mut rd = JobDeps::new(
        Arc::new(backend),
        idle(config.profile.idle_ms, 300_000.0),
        idle(config.profile.tool_idle_ms, 1_800_000.0),
    );
    if let Some(ms) = std::env::var("DELEGATE_HEARTBEAT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        rd.heartbeat_ms = ms;
    }
    // `DELEGATE_IDLE_MS`: test-only override of the model-idle window.
    if let Some(ms) = std::env::var("DELEGATE_IDLE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
    {
        rd.idle_ms = Some(ms);
    }
    rd.status_writer = Arc::new(CliRecordWriter {
        job_id: id.clone(),
        model: model.clone(),
        backend: Some(resolved.backend.clone()),
        cwd: cwd.clone(),
        last_session_id: std::sync::Mutex::new(None),
        gate: gate.clone(),
        tool_idle_ms,
        resumed_from: resumed_from.clone(),
    });
    let registry = JobHandle::new(rd);
    let cancel_fd = install_cancel_handler();
    let job = registry.dispatch(spec);
    if let Some(fd) = cancel_fd {
        spawn_cancel_waiter(Arc::clone(&registry), job.clone(), fd);
    }
    while registry.wait(&job, None) == "RUNNING" {}
    0
}

/// Write end of the self-pipe; the SIGTERM handler drops one byte in it.
static CANCEL_PIPE_W: AtomicI32 = AtomicI32::new(-1);

extern "C" fn on_cancel_sigterm(_: libc::c_int) {
    // Async-signal-safe: just wake the cancel thread.
    let b = 0u8;
    unsafe {
        libc::write(
            CANCEL_PIPE_W.load(Ordering::Relaxed),
            (&b as *const u8).cast(),
            1,
        )
    };
}

/// `cancel` SIGTERMs the supervisor's process group. Translate that into
/// `registry.cancel`, which SIGTERMs the agent's own group and writes the CANCELLED
/// terminal record. A handler plus self-pipe (not a blocked mask plus sigwait) carries
/// the signal: a blocked mask would be inherited by the agent child and make it
/// unkillable by SIGTERM, and the pipe fds are CLOEXEC so the agent never holds them.
/// Returns the read end; the caller spawns the waiter once the job id exists.
fn install_cancel_handler() -> Option<i32> {
    let mut fds = [0 as libc::c_int; 2];
    unsafe {
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            return None;
        }
        for fd in fds {
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags >= 0 {
                libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC);
            }
        }
    }
    CANCEL_PIPE_W.store(fds[1], Ordering::Relaxed);
    let handler = on_cancel_sigterm as extern "C" fn(libc::c_int) as libc::sighandler_t;
    unsafe {
        libc::signal(libc::SIGTERM, handler);
    }
    Some(fds[0])
}

fn spawn_cancel_waiter(registry: Arc<JobHandle>, job: String, fd: i32) {
    std::thread::spawn(move || {
        let mut b = 0u8;
        // One SIGTERM is all `cancel` ever sends before escalating to SIGKILL.
        unsafe { libc::read(fd, (&mut b as *mut u8).cast(), 1) };
        unsafe { libc::close(fd) };
        registry.cancel(&job);
    });
}

fn read_record(id: &str) -> Option<String> {
    std::fs::read_to_string(job_record_path(id)).ok()
}

fn watch(args: &[String]) -> Result<i32, Usage> {
    let (kv, ids) = parse(args, &["--timeout"])?;
    if ids.is_empty() {
        return usage(USAGE);
    }
    let timeout_arg = flag(&kv, "--timeout");
    let timeout = match timeout_arg {
        None => None,
        Some(s) => match s.parse::<f64>() {
            Ok(t) if t >= 0.0 => Some(Duration::from_secs_f64(t)),
            _ => return usage(format!("invalid --timeout {s}")),
        },
    };
    // An id is a file stem; a separator would escape the jobs directory.
    if let Some(id) = ids
        .iter()
        .find(|i| i.contains('/') || read_record(i).is_none())
    {
        return usage(format!("unknown job {id}"));
    }
    let start = Instant::now();
    loop {
        for id in &ids {
            settle_dead_supervisor(id);
        }
        // A read can land on no file only if the record was deleted under us; treat as running.
        let recs: Vec<Option<serde_json::Value>> = ids
            .iter()
            .map(|i| read_record(i).and_then(|s| serde_json::from_str(&s).ok()))
            .collect();
        let all_done = recs
            .iter()
            .all(|r| r.as_ref().is_some_and(|v| v["status"] != "RUNNING"));
        let timed_out = timeout.is_some_and(|t| start.elapsed() >= t);
        if all_done || timed_out {
            for r in recs.iter().flatten() {
                println!("{r}");
            }
            if timed_out {
                let elapsed = format!("{}s", timeout_arg.expect("timeout elapsed without timeout"));
                for (id, record) in ids.iter().zip(&recs) {
                    let Some(record) = record.as_ref().filter(|r| r["status"] == "RUNNING") else {
                        continue;
                    };
                    let progress = &record["progress"];
                    let mut details = Vec::new();
                    if let Some(phase) = progress["phase"].as_str() {
                        details.push(format!("phase {phase}"));
                    }
                    if let Some(tool) = progress["lastTool"].as_str() {
                        details.push(format!("last tool {tool}"));
                    }
                    let details = if details.is_empty() {
                        String::new()
                    } else {
                        format!(" ({})", details.join(", "))
                    };
                    eprintln!(
                        "delegate watch: timed out after {elapsed}; {id} still RUNNING{details}; see its progress field; run watch again"
                    );
                }
            }
            return Ok(if all_done { 0 } else { 1 });
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `kill(pid, 0)` fails with `ESRCH` only when the process is gone. `EPERM` means it exists.
fn process_missing(pid: i32) -> bool {
    let rc = unsafe { libc::kill(pid, 0) };
    rc != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}

/// A RUNNING record whose supervisor has died is terminal: rewrite it once and let the
/// caller print the ERROR record. Re-read after the liveness check so a supervisor that
/// finished and exited in between is not overwritten.
///
/// ponytail: pid-only liveness. A reused pid hides a dead supervisor; add a heartbeat-age check if that shows up.
fn settle_dead_supervisor(id: &str) {
    let Some(v) = read_record(id).and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
    else {
        return;
    };
    if v["status"] != "RUNNING" {
        return;
    }
    let Some(pid) = v["supervisorPid"].as_i64() else {
        return;
    };
    if pid <= 0 || pid > i32::MAX as i64 || !process_missing(pid as i32) {
        return;
    }
    let Some(v) = read_record(id).and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
    else {
        return;
    };
    if v["status"] != "RUNNING" {
        return;
    }
    if let Err(e) = write_supervisor_died(id, &v) {
        eprintln!(
            "cannot write status record {}: {e}",
            job_record_path(id).display()
        );
    }
}
