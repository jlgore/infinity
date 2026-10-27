//! Runtime-agnostic timers.
//!
//! Native builds use tokio's timer (so `tokio::time::pause` keeps working in
//! tests). On wasm32 there is no tokio reactor, so sleeps go through the host's
//! `setTimeout` instead.

use core::time::Duration;

/// Waits for `duration` on the host's timer.
#[cfg(not(target_arch = "wasm32"))]
pub async fn sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
}

/// Waits for `duration` on the host's timer.
#[cfg(target_arch = "wasm32")]
pub async fn sleep(duration: Duration) {
    // JS timer futures are `!Send`, but a wasm32 isolate is single-threaded,
    // so the wrapper's same-thread check always holds.
    send_wrapper::SendWrapper::new(gloo_timers::future::sleep(duration)).await;
}
