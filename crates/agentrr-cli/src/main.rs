//! `agentrr` CLI — deterministic record & replay for AI agents.
//!
//! M1 wires `ls`, `steps`, `show` against the store. Record/replay/etc. land in
//! later milestones.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use agentrr_core::RunId;
use agentrr_store::Store;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use comfy_table::{ContentArrangement, Table};

#[derive(Parser)]
#[command(
    name = "agentrr",
    version,
    about = "Deterministic record & replay for AI agents",
    long_about = None
)]
struct Cli {
    /// Store directory (default: ~/.agentrr).
    #[arg(long, env = "AGENTRR_STORE", global = true)]
    store: Option<PathBuf>,
    /// Emit machine-readable JSON where supported.
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List recorded runs.
    Ls,
    /// List the steps (events) in a run.
    Steps {
        /// Run id or path.
        #[arg(long)]
        run: String,
    },
    /// Show a run summary, or the event at `--step`.
    Show {
        /// Run id or path.
        #[arg(long)]
        run: String,
        /// Show a specific step instead of the run summary.
        #[arg(long)]
        step: Option<u64>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<()> {
    let store_root = resolve_store(cli.store);
    let store = Store::open(&store_root)
        .with_context(|| format!("opening store at {}", store_root.display()))?;
    match cli.command {
        Command::Ls => cmd_ls(&store, cli.json),
        Command::Steps { run } => cmd_steps(&store, &run, cli.json),
        Command::Show { run, step } => cmd_show(&store, &run, step, cli.json),
    }
}

fn cmd_ls(store: &Store, json: bool) -> Result<()> {
    let runs = store.list_runs()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&runs)?);
        return Ok(());
    }
    if runs.is_empty() {
        println!("(no runs in {})", store.root().display());
        return Ok(());
    }
    let mut t = Table::new();
    t.set_content_arrangement(ContentArrangement::Dynamic);
    t.set_header(vec!["RUN", "CREATED", "PROVIDER", "NAME", "EVENTS"]);
    for m in runs {
        t.add_row(vec![
            short_id(&m.id),
            m.created_at.clone(),
            m.provider.clone().unwrap_or_else(|| "-".into()),
            m.name.clone().unwrap_or_else(|| "-".into()),
            m.event_count.to_string(),
        ]);
    }
    println!("{t}");
    Ok(())
}

fn cmd_steps(store: &Store, run: &str, json: bool) -> Result<()> {
    let id = store.resolve(run)?;
    let reader = store.open_run(&id)?;
    let events = reader.events()?;
    if json {
        println!("{}", serde_json::to_string_pretty(&events)?);
        return Ok(());
    }
    let mut t = Table::new();
    t.set_content_arrangement(ContentArrangement::Dynamic);
    t.set_header(vec!["STEP", "KIND", "MATCH_KEY", "STREAM", "MONO_NS"]);
    for ev in &events {
        t.add_row(vec![
            ev.step.get().to_string(),
            ev.kind.as_str().to_string(),
            ev.match_key
                .as_ref()
                .map(|k| short(k))
                .unwrap_or_else(|| "-".into()),
            if ev.is_stream { "Y" } else { "" }.to_string(),
            ev.ts_mono_ns.to_string(),
        ]);
    }
    println!("{t}");
    Ok(())
}

fn cmd_show(store: &Store, run: &str, step: Option<u64>, json: bool) -> Result<()> {
    let id = store.resolve(run)?;
    let reader = store.open_run(&id)?;
    if let Some(step) = step {
        let ev = reader
            .event_at(step)?
            .ok_or_else(|| anyhow::anyhow!("step {step} not found in run {id}"))?;
        if json {
            println!("{}", serde_json::to_string_pretty(&ev)?);
        } else {
            print_event(&ev);
        }
        return Ok(());
    }
    let m = reader.manifest();
    if json {
        println!("{}", serde_json::to_string_pretty(m)?);
        return Ok(());
    }
    println!("run       {}", short_id(&m.id));
    println!("created   {}", m.created_at);
    println!("provider  {}", m.provider.as_deref().unwrap_or("-"));
    println!("name      {}", m.name.as_deref().unwrap_or("-"));
    if let Some(p) = m.parent {
        println!(
            "parent    {} (fork at step {})",
            short_id(&p),
            m.fork_at.map(|s| s.get()).unwrap_or(0)
        );
    }
    println!("events    {}", m.event_count);
    println!("schema    v{}", m.schema_version);
    println!("dir       {}", reader.dir().display());
    Ok(())
}

fn print_event(ev: &agentrr_core::Event) {
    println!("step      {}", ev.step.get());
    println!("kind      {}", ev.kind.as_str());
    println!("mono_ns   {}", ev.ts_mono_ns);
    if let Some(k) = &ev.match_key {
        println!("match_key {k}");
    }
    if let Some(b) = &ev.request_blob {
        println!("request   blob:{b}");
    }
    if let Some(b) = &ev.response_blob {
        println!("response  blob:{b}");
    }
    if ev.is_stream {
        println!("stream    yes");
    }
    if !ev.meta.is_null() {
        println!(
            "meta      {}",
            serde_json::to_string(&ev.meta).unwrap_or_else(|_| "<bad json>".into())
        );
    }
}

fn resolve_store(flag: Option<PathBuf>) -> PathBuf {
    if let Some(p) = flag {
        return p;
    }
    home_dir().join(".agentrr")
}

fn home_dir() -> PathBuf {
    if let Ok(h) = std::env::var("HOME") {
        return PathBuf::from(h);
    }
    if let Ok(h) = std::env::var("USERPROFILE") {
        return PathBuf::from(h);
    }
    PathBuf::from(".")
}

fn short_id(id: &RunId) -> String {
    // Show the full UUIDv7 — it's already compact (36 chars) and is the dir name.
    id.to_string()
}

fn short(s: &str) -> String {
    const N: usize = 12;
    if s.len() <= N {
        s.to_string()
    } else {
        format!("{}…", &s[..N])
    }
}
