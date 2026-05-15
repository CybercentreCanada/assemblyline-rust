#![allow(clippy::collapsible_if)]

mod yugabyte;
mod tables;
use clap::{Parser, Subcommand};

/// Commands for managing relational index tables in assemblyline
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Create tables and indices
    Init {
        #[arg(short, long, default_value_t=false)]
        wipe: bool,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.command {
        Commands::Init { wipe } => {
            tables::init_database_tables(yugabyte::Yugabyte::development().await.unwrap(), wipe).await.unwrap();
        },
    }


}