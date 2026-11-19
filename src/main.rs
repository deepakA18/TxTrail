mod config;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "txtrail", version, about = "Transaction lifecycle debugger")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Doctor {
        #[arg(long)]
        config: PathBuf,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Doctor { config } => match config::Config::load_config(&config) {
            Ok(_) => {
                println!("Configuration valid; network checks not implemented.");
            }
            Err(err) => {
                eprintln!("{err}");
                std::process::exit(1);
            }
        },
    }
}
