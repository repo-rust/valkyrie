use std::thread::JoinHandle;
use std::{io, thread};

mod startup_arguments;

use crate::startup_arguments::StartupArguments;
use crate::utils::thread_utils::pin_current_thread_to_cpu;

mod command;
mod network;
mod protocol;
mod utils;

// struct Request {
//     response: oneshot::Sender<String>,
//     data: String,
// }

fn main() -> io::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("valkyrie=debug")),
        )
        .with_thread_names(true)
        .with_target(false)
        .init();

    // let (queue_sender, queue_receiver) = crossbeam_channel::unbounded::<Request>();

    // thread::spawn(move || {
    //     for single_request in queue_receiver.iter() {
    //         println!("received message: {}", single_request.data);

    //         single_request
    //             .response
    //             .send(format!("response: {}", single_request.data))
    //             .expect("Failed to send response for oneshort channel")

    //         // println!("{single_request}");
    //     }
    // });

    // let rt = tokio::runtime::Builder::new_current_thread()
    //     .enable_io()
    //     .enable_time()
    //     .build()
    //     .expect("build runtime");

    // rt.block_on(async move {
    //     for i in 0..5 {
    //         let (sender, receiver) = oneshot::channel();

    //         let req = Request {
    //             response: sender,
    //             data: format!("Hello-{i}").to_string(),
    //         };

    //         queue_sender
    //             .send(req)
    //             .expect("Failed to send message to channel");

    //         let resp = receiver.await.expect("Await on oneshot receiver failed");

    //         println!("response {}", resp);
    //     }
    // });

    // Ok(())

    let arguments = StartupArguments::parse_args();

    tracing::info!("Program arguments: {arguments:?}");

    let storage_affinity_cores = 0..arguments.shards;
    let _ = start_storage_shard_threads(arguments.shards, storage_affinity_cores);

    #[cfg(target_os = "linux")]
    {
        use crate::network::reuse::start_reuseport_tcp_handlers;
        start_reuseport_tcp_handlers(&arguments)
    }

    #[cfg(target_os = "windows")]
    {
        use crate::network::dispatcher::start_dispatcher_tcp_handlers;
        start_dispatcher_tcp_handlers(&arguments)
    }
}

fn start_storage_shard_threads(
    shards: usize,
    core_affinity_range: std::ops::Range<usize>,
) -> Vec<JoinHandle<()>> {
    let mut shard_handlers = Vec::with_capacity(shards);

    for shard_id in 0..shards {
        let core_affinity_range_copy = core_affinity_range.clone();

        let shard_handler = thread::Builder::new()
            .name(format!("storage-shard-{shard_id}"))
            .spawn(move || {
                pin_current_thread_to_cpu("storage-shard", shard_id, core_affinity_range_copy);

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("Failed to create tokio runtime");

                rt.block_on(async move {
                    tracing::info!("Started");
                    loop {
                        //TODO: storage shard logic here
                        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    }
                });
            })
            .expect("Can't spawn storage-shard thread");

        shard_handlers.push(shard_handler);
    }

    shard_handlers
}
