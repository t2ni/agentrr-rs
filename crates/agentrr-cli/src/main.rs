//! `agentrr` CLI — deterministic record & replay for AI agents.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::process::ExitCode;

use agentrr_core::{Event, RunId, RunManifest};
use agentrr_match::Provider;
use agentrr_proxy::serve_record;
use agentrr_store::Store;
use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use comfy_table::{ContentArrangement, Table};
use tokio::net::TcpListener;
use url::Url;

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
        #[arg(long)]
        run: String,
    },
    /// Show a run summary, or the event at `--step`.
    Show {
        #[arg(long)]
        run: String,
        #[arg(long)]
        step: Option<u64>,
    },
    /// Start the recording proxy.
    Record {
        /// Upstream origin to forward to (e.g. https://api.openai.com). If omitted,
        /// inferred from --provider.
        #[arg(long)]
        upstream: Option<String>,
        /// Local port to listen on.
        #[arg(long, default_value_t = 8080)]
        port: u16,
        /// Human label for the run.
        #[arg(long)]
        name: Option<String>,
        /// Provider wire format.
        #[arg(long, value_enum, default_value_t = ProviderArg::Auto)]
        provider: ProviderArg,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum ProviderArg {
    Openai,
    Anthropic,
    Auto,
}

impl ProviderArg {
    fn label(self) -> &'static str {
        match self {
            ProviderArg::Openai => "openai",
            ProviderArg::Anthropic => "anthropic",
            ProviderArg::Auto => "auto",
        }
    }
    fn as_match(self) -> Option<Provider> {
        match self {
            ProviderArg::Openai => Some(Provider::OpenAi),
            ProviderArg::Anthropic => Some(Provider::Anthropic),
            ProviderArg::Auto => None,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let store_root = resolve_store(cli.store);
    let store = Store::open(&store_root)
        .with_context(|| format!("opening store at {}", store_root.display()))?;
    match cli.command {
        Command::Ls => cmd_ls(&store, cli.json),
        Command::Steps { run } => cmd_steps(&store, &run, cli.json),
        Command::Show { run, step } => cmd_show(&store, &run, step, cli.json),
        Command::Record {
            upstream,
            port,
            name,
            provider,
        } => cmd_record(&store, upstream, port, name, provider).await,
    }
}

async fn cmd_record(
    store: &Store,
    upstream: Option<String>,
    port: u16,
    name: Option<String>,
    provider: ProviderArg,
) -> Result<()> {
    let upstream_url = resolve_upstream(upstream, provider)?;

    let mut manifest = RunManifest::new()?;
    manifest.name = name.clone();
    manifest.provider = Some(provider.label().to_string());
    let writer = store.create_run(manifest)?;
    let run_id = writer.id();
    let run_dir = store.run_dir(&run_id);

    println!("export OPENAI_BASE_URL=http://127.0.0.1:{port}/v1");
    println!("export ANTHROPIC_BASE_URL=http://127.0.0.1:{port}");
    eprintln!("# run_id: {run_id}");
    eprintln!("# recording -> {}", run_dir.display());
    eprintln!("# upstream: {upstream_url}");
    eprintln!("# (Ctrl-C to stop and finalize the run)");

    let listener = TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("binding 127.0.0.1:{port}"))?;

    let manifest = serve_record(upstream_url, provider.as_match(), listener, writer, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;

    eprintln!(
        "# stopped. saved {} events to {}",
        manifest.event_count,
        run_dir.display()
    );
    Ok(())
}

fn resolve_upstream(upstream: Option<String>, provider: ProviderArg) -> Result<Url> {
    let raw = match upstream {
        Some(u) => u,
        None => match provider {
            ProviderArg::Anthropic => "https://api.anthropic.com".to_string(),
            _ => "https://api.openai.com".to_string(),
        },
    };
    Url::parse(&raw).with_context(|| format!("parsing upstream URL {raw:?}"))
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

fn print_event(ev: &Event) {
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
