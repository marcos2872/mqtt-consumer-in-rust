use crate::application::buffering::ReadingBuffer;
use crate::infrastructure::database::repository::SensorRepository;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use chrono::Utc;

pub struct BatchProcessor {
    buffer: ReadingBuffer,
    repository: SensorRepository,
    batch_size: usize,
    interval: Duration,
}

impl BatchProcessor {
    pub fn new(
        buffer: ReadingBuffer,
        repository: SensorRepository,
        batch_size: usize,
        interval: Duration,
    ) -> Self {
        Self {
            buffer,
            repository,
            batch_size,
            interval,
        }
    }

    pub async fn run(self) {
        loop {
            sleep(self.interval).await;

            let readings = self.buffer.pop_batch(self.batch_size);
            if !readings.is_empty() {
                println!(
                    "{} Processing batch of {} readings",
                    Utc::now(),
                    readings.len()
                );
                // TODO: Add retry logic or error handling
                if let Err(e) = self.repository.insert_readings_batch(&readings).await {
                    eprintln!("Failed to insert batch: {}", e);
                    // In a real app, we might want to put them back in buffer or DLQ
                }
            }
        }
    }
}
