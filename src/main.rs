use std::thread::JoinHandle;
use std::{io, thread};

mod startup_arguments;

use crate::startup_arguments::StartupArguments;

mod network;
mod protocol;

// struct Request {
//     response: oneshot::Sender<String>,
//     data: String,
// }

fn main() -> io::Result<()> {
    // println!("Main done...");

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

    println!("[main] Program arguments: {arguments:?}");

    let affinity_cores1 = 0..arguments.shards;
    let shard_handlers = start_storage_shard_threads(arguments.shards, affinity_cores1);

    let affinity_cores2 = arguments.shards..arguments.shards + arguments.tcp_handlers;
    let tcp_handlers = start_tcp_handler_threads(arguments.tcp_handlers, affinity_cores2);

    for tcp_handler in tcp_handlers {
        tcp_handler
            .join()
            .expect("Failed to join TCP handler thread");
    }

    for shard_handler in shard_handlers {
        shard_handler
            .join()
            .expect("Failed to join shard handler thread");
    }

    Ok(())

    // match program_arguments.mode {
    //     Mode::ReusePort => run_reuseport(program_arguments.address, program_arguments.shards),
    //     Mode::Dispatcher => run_dispatcher(program_arguments.address, program_arguments.shards),
    // }
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
                    println!(
                        "[{}] Started",
                        current_thread_name_or_default("storage-shard-???")
                    );

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

fn start_tcp_handler_threads(
    tcp_handlers_count: usize,
    core_affinity_range: std::ops::Range<usize>,
) -> Vec<JoinHandle<()>> {
    let mut tcp_handlers = Vec::with_capacity(tcp_handlers_count);

    for handler_id in 0..tcp_handlers_count {
        let core_affinity_range_copy = core_affinity_range.clone();

        let tcp_handler = thread::Builder::new()
            .name(format!("tcp-handler-{handler_id}"))
            .spawn(move || {
                pin_current_thread_to_cpu("tcp-handler", handler_id, core_affinity_range_copy);

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                    .expect("Failed to create tokio runtime");

                rt.block_on(async move {
                    println!(
                        "[{}] Started",
                        current_thread_name_or_default("tcp-handler-???")
                    );

                    loop {
                        //TODO: TCP handler logic here
                        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    }
                });
            })
            .expect("spawn shard thread");

        tcp_handlers.push(tcp_handler);
    }

    tcp_handlers
}

fn current_thread_name_or_default(default_name: &str) -> String {
    std::thread::current()
        .name()
        .unwrap_or(default_name)
        .to_string()
}

/// Pin current thread to a CPU core for stronger isolation and better performance (Linux only).
#[cfg(target_os = "linux")]
fn pin_current_thread_to_cpu(label: &str, id: usize, core_affinity_range: std::ops::Range<usize>) {
    let core = core_affinity_range.start + (id % core_affinity_range.len());

    let _ = affinity::set_thread_affinity([core]);
    println!("[{label}-{id}] Pinned to CPU {core}");
}

#[cfg(target_os = "windows")]
fn pin_current_thread_to_cpu(
    _label: &str,
    _id: usize,
    _core_affinity_range: std::ops::Range<usize>,
) {
    // No-op for Windows platforms
}
