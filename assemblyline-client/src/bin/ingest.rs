use std::path::{Path, PathBuf};
use std::sync::Arc;

use assemblyline_client::TLSSettings;
use assemblyline_models::Sha256;
use clap::{Args, Parser};
use expanduser::expanduser;
use rand::{thread_rng, Rng};
use serde::de;
use serde_json::json;


#[derive(Debug, Parser)]
#[command(name="ingest")]
#[command(bin_name="ingest")]
struct Cli {
    /// Secure outgoing connections (default true)
    #[arg(long)]
    secure_connection: Option<bool>,

    #[command(flatten)]
    target: TargetFile,

    /// Address of the assemblyline server
    #[arg(long)]
    host: Option<String>,

    /// Username
    #[arg(long)]
    username: Option<String>,

    /// API Key
    #[arg(long)]
    apikey: Option<String>,

    /// Number of retry attempts to make
    #[arg(long)]
    retries: Option<u32>,

    /// Number of retry attempts to make
    #[arg(long)]
    timeout: Option<f64>,

    /// 
    #[arg(long)]
    repeats: Option<u32>,

    /// 
    #[arg(long)]
    threads: Option<u32>,

    /// 
    #[arg(long, default_value_t=false)]
    never_drop: bool,
    
    /// Path to the config file
    #[arg(short, long)]
    config: Option<String>,
}


#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct TargetFile {
    /// Upload a single file
    #[arg(short, long)]
    file: Option<PathBuf>,

    /// Upload every file in a directory
    #[arg(short, long)]
    directory: Option<PathBuf>,

    /// Upload by sha256
    #[arg(short, long)]
    sha: Option<String>,
}

struct Config {
    host: String,
    username: String,
    apikey: String,
    verify: bool
}

#[tokio::main]
async fn main() {
    // ----------------------------------------------------
    // Parse arguments
    let args = Arc::new(Cli::parse());

    // ----------------------------------------------------
    // Open a config file if we can find one
    let default_config_path = expanduser("~/.al/submit.cfg").unwrap_or(PathBuf::from("./submit.cfg"));
    let config = if let Some(config) = &args.config {
        let config = expanduser(config).unwrap();
        load_config(&config, &args)
    } else if matches!(tokio::fs::try_exists(&default_config_path).await, Ok(true)) {
        load_config(&default_config_path, &args)
    } else {
        Config {
            host: args.host.clone().expect("A server url must be configured"),
            username: args.username.clone().expect("A username must be configured"),
            apikey: args.apikey.clone().expect("An apikey must be configured"),
            verify: args.secure_connection.unwrap_or(true),
        }
    };
    
    // ----------------------------------------------------
    // Connect to the AL server
    let connection = assemblyline_client::Connection::connect(
        config.host, 
        assemblyline_client::Authentication::ApiKey { username: config.username, key: config.apikey }, 
        args.retries, 
        if config.verify {
            TLSSettings::Native
        } else {
            TLSSettings::UnsafeNoVerify
        },
        Default::default(),
        Some(args.timeout.unwrap_or(60.0))
    ).await.expect("Connection failed");

    let client = Arc::new(assemblyline_client::Client::from_connection(Arc::new(connection)).await.expect("Connection failed"));

    // ----------------------------------------------------
    // Ingest the targeted file(s)
    let parallelism = args.threads.unwrap_or(1).max(1) as usize;
    let mut pool = tokio::task::JoinSet::new();

    let handle = |result| {
        match result {
            Ok(Ok(id)) => println!("{id}"),
            Ok(Err(inner_error)) => println!("Error: {inner_error}"),
            Err(join_error) => panic!("{join_error}"), 
        }
    };

    let (enqueue, mut dequeue) = tokio::sync::mpsc::channel(100);
    tokio::spawn({
        let args = args.clone();

        async move {
            for _ in 0..args.repeats.unwrap_or(1) {
        
                if let Some(path) = &args.target.file {
                    enqueue.send(Target::Path(path.to_path_buf())).await.unwrap();
                } else if let Some(hash) = &args.target.sha {
                    enqueue.send(Target::Hash(hash.parse().expect("sha256 was not parsable"))).await.unwrap();
                } else if let Some(path) = &args.target.directory {
                    let mut directories = vec![path.clone()];
                    while let Some(directory) = directories.pop() {
                        let mut listing = tokio::fs::read_dir(directory).await.unwrap();
                        while let Some(item) = listing.next_entry().await.unwrap() {
                            let file_type = item.file_type().await.unwrap();
                            if file_type.is_dir() {
                                directories.push(item.path());
                            } else if file_type.is_file() {
                                enqueue.send(Target::Path(item.path())).await.unwrap();
                            }
                        }
                    }
                } else {
                    panic!("A target file must be specified");
                };
            }    
        }
    });

    while let Some(target) = dequeue.recv().await {
        pool.spawn(ingest_file(client.clone(), target, args.clone()));
        if pool.len() < parallelism { continue }

        while pool.len() >= parallelism {
            match pool.join_next().await {
                Some(value) => handle(value),
                None => break
            }
        }
        while let Some(task) = pool.try_join_next() {
            handle(task);
        }
    }

    while let Some(result) = pool.join_next().await {
        handle(result)
    }
}

enum Target {
    Path(PathBuf),
    Hash(Sha256),
}

async fn ingest_file(client: Arc<assemblyline_client::Client>, target: Target, args: Arc<Cli>) -> Result<String, String> {
    let mut builder = client.ingest.single();

    if args.never_drop {
        builder = builder.parameter("never_drop".to_string(), serde_json::Value::Bool(true));
    }

    builder = builder.parameter("service_spec".to_string(), json!({
        "ServiceName": {
            "field": thread_rng().gen::<u64>().to_string()
        }
    }));

    let result = match target {
        Target::Path(path) => builder.path(&path).await,
        Target::Hash(sha256) => builder.sha256(sha256).await,
    };
        
    result.map(|result| result.ingest_id).map_err(|err|err.to_string())
}

fn load_config(path: &Path, args: &Cli) -> Config {
    let mut config = configparser::ini::Ini::new();
    config.load(path).expect("Could not parse configuration file");

    let host = args.host.clone()
        .or_else(||{
            if let Some(url) = config.get("server", "url") {
                return Some(url)
            }
            let host = match config.get("server", "host") {
                Some(host) => host,
                None => return None
            };

            let transport = config.get("server", "transport").unwrap_or("https".to_owned());
            let port = config.get("server", "port").unwrap_or("443".to_owned());
            Some(format!("{transport}://{host}:{port}"))
        })
        .expect("A server must be configured");

    let username = args.username.clone()
        .or(config.get("auth", "user"))
        .expect("A username must be configured");

    let apikey = args.apikey.clone()
        .or(config.get("auth", "apikey"))
        .expect("An apikey must be configured");

    let verify = args.secure_connection
        .or(config.get("auth", "insecure").map(|value| !["true", "yes"].contains(&value.to_lowercase().as_str())))
        .unwrap_or(true);

    Config {
        host,
        username,
        apikey,
        verify,
    }
}