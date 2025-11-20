use std::net::SocketAddr;
#[derive(Debug, Clone, Copy)]
pub struct StartupArguments {
    pub mode: Mode,
    pub socket_address: SocketAddr,
    pub shards_count: usize,
}

impl StartupArguments {
    ///
    /// Command lines arguments:
    /// 0 - program name
    /// 1 - runtime mode, can be 'dispatcher' or 'reuseport'. Default to 'reuseport'
    /// 2 - host and port to bind. Default to '0.0.0.0:8080'  
    ///
    /// Environment variables:
    /// 'SHARDS' - number of shards to be created. Default to number of CPUs or 4 if CPUs information can't be obtained.
    ///
    pub fn parse_args() -> Self {
        let mut args = std::env::args().skip(1);

        // parse Mode
        let mode = match args.next().as_deref() {
            Some("dispatcher") => Mode::Dispatcher,
            _ => Mode::ReusePort,
        };

        // parse SocketAddr
        let socket_address: SocketAddr = args
            .next()
            .as_deref()
            .unwrap_or("0.0.0.0:8080")
            .parse()
            .expect("invalid bind address");

        // parse shards count
        let shards_env = std::env::var("SHARDS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok());
        let shards_count = shards_env
            .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
            .unwrap_or(4);

        Self {
            mode,
            socket_address,
            shards_count,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Mode {
    ReusePort,
    Dispatcher,
}
