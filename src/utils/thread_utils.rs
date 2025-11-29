#![allow(dead_code)]

use std::thread::JoinHandle;

pub fn current_thread_name_or_default(default_name: &str) -> String {
    std::thread::current()
        .name()
        .unwrap_or(default_name)
        .to_string()
}

pub fn wait_for_all(handlers: Vec<JoinHandle<()>>) {
    for single_handler in handlers {
        single_handler.join().expect("Failed to join handler");
    }
}

/// Pin current thread to a CPU core for stronger isolation and better performance (Linux only).
#[cfg(target_os = "linux")]
pub fn pin_current_thread_to_cpu(id: usize, core_affinity_range: std::ops::Range<usize>) {
    let core = core_affinity_range.start + (id % core_affinity_range.len());

    let _ = affinity::set_thread_affinity([core]);
    tracing::info!("Pinned to CPU {core}");
}

#[cfg(target_os = "windows")]
pub fn pin_current_thread_to_cpu(_id: usize, _core_affinity_range: std::ops::Range<usize>) {
    // No-op for Windows platforms
}
