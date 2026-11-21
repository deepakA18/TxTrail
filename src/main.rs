use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use txtrail::{config::Config, report, store};
#[derive(Parser)]
#[command(
    name = "txtrail",
    version,
    about = "Evidence-first Solana transaction debugger"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Clone, ValueEnum)]
enum Format {
    Markdown,
    Json,
    Csv,
}
#[derive(Subcommand)]
enum Command {
    Send {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        keypair: PathBuf,
        #[arg(long, value_enum, default_value = "transfer")]
        workload: txtrail::transaction::Workload,
    },
    Reconcile {
        #[arg(long)]
        run: PathBuf,
        #[arg(long)]
        config: PathBuf,
    },
    Doctor {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        offline: bool,
    },
    Replay {
        #[arg(long)]
        input: PathBuf,
        #[arg(long, value_enum, default_value = "markdown")]
        format: Format,
    },
    Inspect {
        #[arg(long)]
        run: PathBuf,
        #[arg(long)]
        signature: String,
    },
    Report {
        #[arg(long)]
        run: PathBuf,
        #[arg(long, value_enum, default_value = "markdown")]
        format: Format,
    },
    Export {
        #[arg(long)]
        run: PathBuf,
        #[arg(long, value_enum, default_value = "csv")]
        format: Format,
    },
}
fn output(path: PathBuf, format: Format, signature: Option<&str>) -> Result<(), String> {
    let input = store::read(&path)?;
    let report = report::build(&input)?;
    let text = match format {
        Format::Markdown => report::markdown(&report, signature)?,
        Format::Json => serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?,
        Format::Csv => report::csv(&report),
    };
    use std::io::Write;
    match writeln!(std::io::stdout().lock(), "{text}") {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}
#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("txtrail: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), String> {
    match Cli::parse().command {
        Command::Send {
            config,
            keypair,
            workload,
        } => {
            let config = Config::load_config(&config)?;
            let path = txtrail::runner::send(&config, &keypair, workload).await?;
            output(path, Format::Markdown, None)
        }
        Command::Reconcile { run, config } => {
            let config = Config::load_config(&config)?;
            let path = txtrail::runner::reconcile(&config, &run).await?;
            output(path, Format::Markdown, None)
        }

        Command::Doctor { config, offline } => {
            let config = Config::load_config(&config)?;
            if offline {
                println!("Configuration valid; network checks skipped.");
            } else {
                let rpc = txtrail::rpc::Rpc::new(&config)?;
                let identity = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    rpc.verify_network(&config),
                )
                .await
                .map_err(|_| "Network identity check budget exceeded")?
                .map_err(|e| e.to_string())?;
                println!("Network identity verified: {identity}");
                for method in ["getVersion", "getHealth", "getBlockHeight"] {
                    let params = if method == "getBlockHeight" {
                        serde_json::json!([{"commitment":"finalized"}])
                    } else {
                        serde_json::json!([])
                    };
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        rpc.call(method, params),
                    )
                    .await
                    .map_err(|_| format!("{method} budget exceeded"))?
                    {
                        Ok(v) => println!("{method}: {v}"),
                        Err(e) if method == "getHealth" => println!("getHealth unavailable: {e}"),
                        Err(e) => return Err(e.to_string()),
                    }
                }
            }
            Ok(())
        }
        Command::Replay { input, format } => output(input, format, None),
        Command::Report { run, format } | Command::Export { run, format } => {
            output(run, format, None)
        }
        Command::Inspect { run, signature } => output(run, Format::Markdown, Some(&signature)),
    }
}
