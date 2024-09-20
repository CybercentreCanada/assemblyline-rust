//!
//! Utilities for logging.
//!
use std::{sync::Arc, time::Instant};

use anyhow::Result;
use assemblyline_models::config::{Config, SyslogTransport};
use assemblyline_models::config::LogLevel;
use flexi_logger::{DeferredNow, LoggerHandle};
use log::Record;
use log::{debug, error};
use poem::{Endpoint, Middleware, Request};
use serde::Serialize;

/// Middleware for poem http server to add query logging
///
/// Successful queries are logged at the debug level. Errors at the error level.
/// All logs include the time elapsed processing the request.
pub struct LoggerMiddleware;

impl<E: Endpoint> Middleware<E> for LoggerMiddleware {
    type Output = LoggerMiddlewareImpl<E>;

    fn transform(&self, ep: E) -> Self::Output {
        LoggerMiddlewareImpl { ep }
    }
}

/// Endpoint wrapper that implements the details of `LoggerMiddleware`
pub struct LoggerMiddlewareImpl<E> {
    /// Inner endpoint wrapped by this object
    ep: E,
}


#[poem::async_trait]
impl<E: Endpoint> Endpoint for LoggerMiddlewareImpl<E> {
    type Output = E::Output;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        let start = Instant::now();
        let uri = req.uri().clone();
        debug!("starting request for {uri}");
        match self.ep.call(req).await {
            Ok(resp) => {
                debug!("request for {uri} handled ({} ms)", start.elapsed().as_millis());
                Ok(resp)
            },
            Err(err) => {
                error!("error handling {uri} ({} ms) {err}", start.elapsed().as_millis());
                Err(err)
            },
        }
    }
}


static INIT_LOGGING: std::sync::Mutex<bool> = std::sync::Mutex::new(false);
static HOSTNAME: std::sync::OnceLock<String> = std::sync::OnceLock::new();
static LOCAL_IP: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub fn configure_logging(config: &Arc<Config>) -> Result<LoggerHandle> {
    use flexi_logger::*;
    use flexi_logger::writers::{FileLogWriter, Syslog, SyslogWriter};
    
    // make sure we only init logging once
    {
        let mut init = INIT_LOGGING.lock().unwrap();
        if *init {
            return Err(anyhow::anyhow!("Logger double initalized"))
        }
        *init = true;

        HOSTNAME.get_or_init(|| gethostname::gethostname().to_string_lossy().into_owned());
        let local_ip = local_ip_address::local_ip()?.to_string();
        LOCAL_IP.get_or_init(|| local_ip);
    }

    // map to the log model's log level
    let log_level  = match config.logging.log_level {
        LogLevel::Debug => log::Level::Debug,
        LogLevel::Info => log::Level::Info,
        LogLevel::Warning => log::Level::Warn,
        LogLevel::Error => log::Level::Error,
        // nothing above error in this framework so we flatten it all to error
        LogLevel::Critical => log::Level::Error, 
        // if logging is disabled initilize the backend with the filter set to ignore all
        LogLevel::Disabled => {
            let logger = Logger::with(LogSpecification::off());
            return Ok(logger.start()?)
        },
    };

    // setup our log handler
    //  log level is WARN for everything by default
    //  if the package name includes assemblyline set the level to the given value
    let log_spec = format!("warn, assemblyline={log_level}");
    let spec = LogSpecification::env_or_parse(log_spec)?;

    let formatter = if config.logging.log_as_json {
        json_format
    } else {
        basic_format
    };

    // build our log handler
    let mut builder = Logger::with(spec)
        .format(formatter);

    if config.logging.log_to_file {
        let log_directory = config.logging.log_directory.clone();
        if !log_directory.exists() {
            println!("Warning: log directory does not exist. Will try to create {}", log_directory.to_string_lossy());
            std::fs::create_dir_all(&log_directory)?;
        }

        if log_level <= log::Level::Debug {
            builder = builder.add_writer(
                "DebugFile", 
                Box::new(FileLogWriter::builder(FileSpec::default().directory(&log_directory).suffix(".dbg"))
                .append()
                .max_level(log::LevelFilter::Debug)
                .rotate(Criterion::Size(10485760), Naming::Numbers, Cleanup::KeepLogFiles(5))
                .format(formatter)
                .try_build()?)
            );
        }

        if log_level <= log::Level::Info {
            builder = builder.add_writer(
                "LogFile", 
                Box::new(FileLogWriter::builder(FileSpec::default().directory(&log_directory).suffix(".log"))
                .append()
                .max_level(log::LevelFilter::Info)
                .rotate(Criterion::Size(10485760), Naming::Numbers, Cleanup::KeepLogFiles(5))
                .format(formatter)
                .try_build()?)
            );
        }

        if log_level <= log::Level::Error {
            builder = builder.add_writer(
                "ErrorFile", 
                Box::new(FileLogWriter::builder(FileSpec::default().directory(&log_directory).suffix(".err"))
                .append()
                .max_level(log::LevelFilter::Error)
                .rotate(Criterion::Size(10485760), Naming::Numbers, Cleanup::KeepLogFiles(5))
                .format(formatter)
                .try_build()?)
            );
        }
    }

    // log to stdout
    if config.logging.log_to_console {
        builder = builder.log_to_stdout();
    }

    if config.logging.log_to_syslog {
        let connection = match config.logging.syslog_transport {
            SyslogTransport::Udp => Syslog::try_udp(("0.0.0.0", 0), (&config.logging.syslog_host, config.logging.syslog_port))?,
            SyslogTransport::Tcp => Syslog::try_tcp( (config.logging.syslog_host.as_str(), config.logging.syslog_port))?,
        };
        builder = builder.add_writer("Syslog", SyslogWriter::try_new(
            writers::SyslogFacility::SystemDaemons,
            None,
            log_level.to_level_filter(),
            "assemblyline".to_owned(),
            connection
        )?);
    }

    Ok(builder.start()?)
}

fn basic_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    let time = now.format_rfc3339();
    let level = record.level();
    let name = record.module_path().unwrap_or("<unknown>");
    let message = record.args();
    let process = std::process::id();
    let hostname = HOSTNAME.get().map_or("<unknown>", |row|row.as_str());

    // AL_LOG_FORMAT = f'%(asctime)-16s %(levelname)8s {hostname} %(process)d %(name)40s | %(message)s'
    write!(w, "{time:-16} {level:8} {hostname} {process} {name:40} | {message}")
}

// AL_JSON_FORMAT = f'{{' \
//     f'"@timestamp": "%(asctime)s", ' \
//     f'"event": {{ "module": "assemblyline", "dataset": "%(name)s" }}, ' \
//     f'"host": {{ "ip": "{ip}", "hostname": "{hostname}" }}, ' \
//     f'"log": {{ "level": "%(levelname)s", "logger": "%(name)s" }}, ' \
//     f'"process": {{ "pid": "%(process)d" }}, ' \
//     f'"message": %(message)s}}'

#[derive(Serialize)]
struct LogLineEvent<'b> {
    module: &'static str,
    dataset: &'b str,
}

#[derive(Serialize)]
struct LogLineHost<'b> {
    ip: &'b str, 
    hostname: &'b str,
}

#[derive(Serialize)]
struct LogLineLevel<'b> {
    level: &'static str, 
    logger: &'b str,
}

#[derive(Serialize)]
struct LogLineProcess<'b> {
    pid: &'b str,
}

#[derive(Serialize)]
struct LogLine<'a, 'b> {
    #[serde(rename="@timestamp")]
    timestamp: String, 
    event: LogLineEvent<'b>, 
    host: LogLineHost<'b>, 
    log: LogLineLevel<'b>, 
    process: LogLineProcess<'b>, 
    message: &'a std::fmt::Arguments<'a>,
}

fn json_format(
    w: &mut dyn std::io::Write,
    now: &mut DeferredNow,
    record: &Record,
) -> Result<(), std::io::Error> {
    let module = record.module_path().unwrap_or("<unknown>");
    let hostname = HOSTNAME.get().map_or("<unknown>", |row|row.as_str());
    let ip = LOCAL_IP.get().map_or("<unknown>", |row|row.as_str());
    let process = std::process::id().to_string();

    let line = LogLine {
        timestamp: now.format_rfc3339(),
        event: LogLineEvent { module: "assemblyline", dataset: module },
        host: LogLineHost { ip, hostname },
        log: LogLineLevel { level: record.level().as_str(), logger: module },
        process: LogLineProcess { pid: &process },
        message: record.args(),
    };

    serde_json::to_writer(w, &line)?;
    Ok(())
}