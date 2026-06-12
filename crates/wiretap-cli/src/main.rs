use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::EnvFilter;
use std::sync::Arc;
use wiretap_core::{
    config::EXAMPLE_TOML, pipeline::Start, Config, Cursor, FilterSpec, GrpcLayoutProvider,
    GrpcSource, LayoutResolver, Pipeline, SinkConfig,
};

#[derive(Parser)]
#[command(name = "wiretap", version, about = "Lightweight Sui gRPC event indexer")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Write a wiretap.toml template to the current directory.
    Init {
        #[arg(long, default_value = "wiretap.toml")]
        out: PathBuf,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    /// Run the indexer from wiretap.toml, or from inline flags.
    Watch {
        #[arg(long, default_value = "wiretap.toml")]
        config: PathBuf,
        /// Override [source.endpoint]; if set, config is not required.
        #[arg(long)]
        endpoint: Option<String>,
        /// Filter by event type; may be repeated. Supports `pkg::module::*`.
        #[arg(long = "event")]
        events: Vec<String>,
        /// Sink shorthand, e.g. `sqlite://events.db`, `stdout`, `webhook://https://...`.
        #[arg(long)]
        sink: Option<String>,
    },
    /// Replay a historical range via LedgerService.GetCheckpoint.
    Backfill {
        #[arg(long, default_value = "wiretap.toml")]
        config: PathBuf,
        #[arg(long)]
        from: u64,
        #[arg(long)]
        to: u64,
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long)]
        sink: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        // Keep stdout reserved for the stdout sink's NDJSON output.
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Init { out, force } => cmd_init(out, force),
        Cmd::Watch {
            config,
            endpoint,
            events,
            sink,
        } => cmd_watch(config, endpoint, events, sink).await,
        Cmd::Backfill {
            config,
            from,
            to,
            endpoint,
            sink,
        } => cmd_backfill(config, from, to, endpoint, sink).await,
    }
}

fn cmd_init(out: PathBuf, force: bool) -> Result<()> {
    if out.exists() && !force {
        anyhow::bail!("{} already exists (pass --force to overwrite)", out.display());
    }
    std::fs::write(&out, EXAMPLE_TOML).context("writing template")?;
    println!("wrote {}", out.display());
    Ok(())
}

/// Build a config-equivalent from CLI flags so `wiretap watch --endpoint ... --event ...
/// --sink stdout` works with zero TOML.
fn config_from_flags(
    endpoint: String,
    events: Vec<String>,
    sink: String,
) -> Result<Config> {
    let sink_cfg = parse_sink_shorthand(&sink)?;
    Ok(Config {
        source: wiretap_core::SourceConfig {
            endpoint,
            start_checkpoint: "latest".into(),
        },
        watch: vec![wiretap_core::WatchSpec {
            events,
            ..Default::default()
        }],
        sink: sink_cfg,
        cursor: Default::default(),
    })
}

fn parse_sink_shorthand(s: &str) -> Result<SinkConfig> {
    let mut params = toml::Table::new();
    let kind = if let Some(rest) = s.strip_prefix("sqlite://") {
        params.insert("path".into(), toml::Value::String(rest.into()));
        "sqlite"
    } else if let Some(rest) = s.strip_prefix("webhook://") {
        params.insert("url".into(), toml::Value::String(rest.into()));
        "webhook"
    } else if let Some(rest) = s.strip_prefix("postgres://") {
        params.insert(
            "url".into(),
            toml::Value::String(format!("postgres://{rest}")),
        );
        "postgres"
    } else if s == "stdout" {
        "stdout"
    } else {
        anyhow::bail!("unrecognized --sink `{s}` (use sqlite://, webhook://, postgres://, or stdout)");
    };
    Ok(SinkConfig {
        kind: kind.into(),
        params,
    })
}

fn load_config(
    config: PathBuf,
    endpoint: Option<String>,
    events: Vec<String>,
    sink: Option<String>,
) -> Result<Config> {
    match (endpoint, sink) {
        (Some(ep), Some(sk)) => config_from_flags(ep, events, sk),
        _ => Config::load(&config)
            .with_context(|| format!("loading {}", config.display())),
    }
}

async fn cmd_watch(
    config: PathBuf,
    endpoint: Option<String>,
    events: Vec<String>,
    sink: Option<String>,
) -> Result<()> {
    let cfg = load_config(config, endpoint, events.clone(), sink)?;

    let source = GrpcSource::connect(&cfg.source.endpoint).await?;
    let resolver = Arc::new(LayoutResolver::new(
        Arc::new(GrpcLayoutProvider::new(source.channel())),
        2048,
    ));
    let cursor = Cursor::open_sqlite(&cfg.cursor.path)?;
    let mut filter = cfg.combined_filter();
    // CLI --event flags merge into config filters.
    filter.events.extend(events);
    let handler = wiretap_sinks::build_from_config(&cfg.sink)?;

    let start = match cfg.source.start_checkpoint.as_str() {
        "latest" => Start::Latest,
        s => Start::At(s.parse().context("start_checkpoint must be \"latest\" or a u64")?),
    };

    info!(
        endpoint = %cfg.source.endpoint,
        cursor = %cursor.path().display(),
        sink = %cfg.sink.kind,
        "wiretap: starting watch"
    );
    Pipeline::new(source, cursor, filter)
        .start_at(start)
        .with_resolver(resolver)
        .run(BoxedHandler(handler))
        .await
}

async fn cmd_backfill(
    config: PathBuf,
    from: u64,
    to: u64,
    endpoint: Option<String>,
    sink: Option<String>,
) -> Result<()> {
    let cfg = load_config(config, endpoint, vec![], sink)?;
    let source = GrpcSource::connect(&cfg.source.endpoint).await?;
    let resolver = Arc::new(LayoutResolver::new(
        Arc::new(GrpcLayoutProvider::new(source.channel())),
        2048,
    ));
    let filter: FilterSpec = cfg.combined_filter();
    let compiled = filter.compile();
    let handler = wiretap_sinks::build_from_config(&cfg.sink)?;

    use wiretap_core::source::Source;
    use wiretap_core::decode_event;

    info!(from, to, "wiretap: backfilling range");
    // Stream in pages to bound memory.
    let mut cursor = from;
    while cursor <= to {
        let end = (cursor + 99).min(to);
        let batches = source.fetch_range(cursor, end).await?;
        for batch in batches {
            let cp = &batch.checkpoint;
            let seq = batch.seq();
            for tx in &cp.transactions {
                let events = match tx.events.as_ref() {
                    Some(e) => &e.events,
                    None => continue,
                };
                for (i, ev) in events.iter().enumerate() {
                    if compiled.matches(tx, ev) {
                        let e = decode_event(cp, tx, ev, i as u64, Some(&resolver)).await;
                        handler.on_event(e).await?;
                    }
                }
            }
            handler.on_checkpoint(seq).await?;
        }
        cursor = end + 1;
    }
    Ok(())
}

/// Adapter so a `Box<dyn Handler>` can satisfy `Handler` for Pipeline::run.
struct BoxedHandler(Box<dyn wiretap_core::Handler>);

#[async_trait::async_trait]
impl wiretap_core::Handler for BoxedHandler {
    async fn on_event(&self, e: wiretap_core::Event) -> anyhow::Result<()> {
        self.0.on_event(e).await
    }
    async fn on_checkpoint(&self, seq: u64) -> anyhow::Result<()> {
        self.0.on_checkpoint(seq).await
    }
}
