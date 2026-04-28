mod index;
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
    Init {},

    /// Import data from the assemblyline elastic collections into the
    Import {}
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args.command {
        Commands::Init { } => {
            index::main().await;
        },
        Commands::Import {  } => {
            todo!("read to_be_deleted before ingest");
        },
    }


}