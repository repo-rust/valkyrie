use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
#[derive(Debug, Clone, Copy, Parser)]
#[command(name = "valkyrie", about = "High-performance Key-Value storage")]
pub struct StartupArguments {
    // #[arg(long = "mode", value_enum, default_value_t = Mode::ReusePort, help = "Runtime mode: reuseport or dispatcher")]
    // pub mode: Mode,
    #[arg(
        long = "address",
        default_value = "127.0.0.1:6379",
        help = "Socket address to bind, default is 127.0.0.1:6379"
    )]
    pub address: SocketAddr,

    #[arg(
        long = "tcp-handlers",
        default_value_t = usize::MAX,
        help = "Number of TCP handler threads"
    )]
    pub tcp_handlers: usize,

    #[arg(
        long = "shards",
        default_value_t = usize::MAX,
        help = "Number of storage shards"
    )]
    pub shards: usize,
}

impl StartupArguments {
    /// Parse command line arguments using clap.
    ///
    /// Usage:
    ///     --mode=dispatcher|reuseport --address=0.0.0.0:8080 --tcp-handlers=4 --shards=4
    pub fn parse_args() -> Self {
        let mut args = Self::parse();

        // Limit shards to the minimum of the user-provided value and half of the available CPUs (at least 1)
        let available = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let half = std::cmp::max(1, available / 2);

        args.shards = std::cmp::min(args.shards, half);
        args.tcp_handlers = std::cmp::min(args.tcp_handlers, half);

        args
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Mode {
    #[value(name = "reuseport")]
    ReusePort,
    #[value(name = "dispatcher")]
    Dispatcher,
}
