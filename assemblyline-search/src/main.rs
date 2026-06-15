#![allow(clippy::collapsible_if)]

mod yugabyte;
mod tidb;
mod tables;
mod lucene;
mod search;
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
            // let client = yugabyte::Yugabyte::development(false).await.unwrap();
            let client = tidb::TiDb::development(false).await.unwrap();
            tables::init_database_tables(&tables::Database::Ti(client), wipe).await.unwrap();
        },
    }

}