use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

static IMPORT_SEMAPHORE: Lazy<Arc<Semaphore>> = Lazy::new(|| {
    let limit = std::env::var("IMPORT_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);
    Arc::new(Semaphore::new(limit))
});

pub async fn acquire_import_permit() -> OwnedSemaphorePermit {
    match IMPORT_SEMAPHORE.clone().acquire_owned().await {
        Ok(permit) => permit,
        Err(_) => {
            // Fallback to a new semaphore if the main one is closed
            let fallback = Arc::new(Semaphore::new(1));
            fallback.acquire_owned().await.expect("Failed to acquire import permit: semaphore closed")
        }
    }
}
