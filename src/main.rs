use std::io;

mod startup_arguments;
use crate::startup_arguments::{Mode, StartupArguments};

mod network;
use crate::network::dispatcher::run_dispatcher;
use crate::network::reuse::run_reuseport;

fn main() -> io::Result<()> {
    let program_arguments = StartupArguments::parse_args();

    println!("[main] Program arguments: {program_arguments:?}");

    match program_arguments.mode {
        Mode::ReusePort => run_reuseport(
            program_arguments.socket_address,
            program_arguments.shards_count,
        ),
        Mode::Dispatcher => run_dispatcher(
            program_arguments.socket_address,
            program_arguments.shards_count,
        ),
    }
}
