use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
#[derive(Debug, Clone, Copy, Parser)]
#[command(
    name = "valkyrie",
    about = "High-performance TCP demo with dispatcher/reuseport modes"
)]
pub struct StartupArguments {
    #[arg(long = "mode", value_enum, default_value_t = Mode::ReusePort, help = "Runtime mode: reuseport or dispatcher")]
    pub mode: Mode,

    #[arg(
        long = "address",
        default_value = "0.0.0.0:8080",
        help = "Socket address to bind, e.g. 0.0.0.0:8080"
    )]
    pub address: SocketAddr,

    #[arg(long = "shards", default_value_t = StartupArguments::default_shards(), help = "Number of shards; can also be set via SHARDS env var")]
    pub shards: usize,
}

impl StartupArguments {
    /// Returns the default shards count, preferring SHARDS env var, then CPU count, then 4.
    fn default_shards() -> usize {
        std::env::var("SHARDS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
            .unwrap_or(4)
    }

    /// Parse command line arguments using clap.
    ///
    /// Usage:
    ///     --mode=dispatcher|reuseport --address=0.0.0.0:8080 --shards=4
    pub fn parse_args() -> Self {
        Self::parse()
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
    #[value(name = "reuseport")]
    ReusePort,
    #[value(name = "dispatcher")]
    Dispatcher,
}
