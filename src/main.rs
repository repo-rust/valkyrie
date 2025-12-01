mod startup_arguments;

use std::sync::Arc;

use crate::{startup_arguments::StartupArguments, storage::engine::StorageEngine};

mod command;
mod network;
mod protocol;
mod storage;
mod utils;

fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("valkyrie=debug")),
        )
        .with_thread_names(true)
        .with_target(false)
        .init();

    for i in 0..10 {
        tracing::debug!("i = {i}");
    }

    let arguments = StartupArguments::parse_args();

    tracing::info!("StartupArguments: {arguments}");

    let storage_affinity_cores = 0..arguments.shards;
    let storage = Arc::new(StorageEngine::new(arguments.shards, storage_affinity_cores));

    #[cfg(target_os = "linux")]
    {
        use crate::network::reuse::start_reuseport_tcp_handlers;
        start_reuseport_tcp_handlers(&arguments, storage)?;
    }

    #[cfg(any(target_os = "windows", target_os = "macos"))]
    {
        use crate::network::dispatcher::start_dispatcher_tcp_handlers;
        start_dispatcher_tcp_handlers(&arguments, storage)?;
    }

    Ok(())
}
