use crate::domain::aggregation::Aggregation;
use crate::domain::anomaly::Anomaly;
use crate::infrastructure::database::repository::AggregationRepository;
use crate::infrastructure::redis::publisher::RedisPublisher;
use chrono::{DateTime, Utc};
use std::error::Error;

pub struct Aggregator {
    repository: AggregationRepository,
    publisher: RedisPublisher,
}

impl Aggregator {
    pub fn new(repository: AggregationRepository, publisher: RedisPublisher) -> Self {
        Self {
            repository,
            publisher,
        }
    }

    pub async fn process_aggregation(
        &self,
        machine_id: String,
        sensor_id: String,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // 1. Fetch data
        let values = self.repository
            .get_readings_in_range(&machine_id, window_start, window_end)
            .await?;

        if values.is_empty() {
            return Ok(());
        }

        // 2. Calculate aggregations
        let count = values.len() as i64;
        let sum: f64 = values.iter().sum();
        let avg_value = sum / count as f64;
        let min_value = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_value = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        let aggregation = Aggregation::new(
            machine_id.clone(),
            sensor_id.clone(),
            window_start,
            window_end,
            avg_value,
            min_value,
            max_value,
            count,
        );

        // 3. Save to DB
        self.repository.save_aggregation(&aggregation).await?;

        // 4. Publish to Redis
        self.publisher.publish_aggregation(&aggregation).await?;

        // 5. Check for Anomalies (Simple Rule: Z-Score)
        // Note: For real Z-score we need historical mean/std_dev. 
        // For this implementation, I'll use a simplified check or hardcoded threshold 
        // as getting history is expensive without a separate cache/query.
        // Let's assume a static threshold for demonstration if no history is available.
        // OR better: Assume simple limits based on sensor type (mocked logic here).
        
        let expected_min = 20.0;
        let expected_max = 120.0; // Example for temperature

        if avg_value < expected_min || avg_value > expected_max {
             let anomaly = Anomaly::new(
                machine_id.clone(),
                sensor_id.clone(),
                avg_value,
                expected_min,
                expected_max,
                0.0, // placeholder z-score
                format!("Value {} out of range [{}-{}]", avg_value, expected_min, expected_max),
            );
            self.publisher.publish_anomaly(&anomaly).await?;
            println!("Anomaly detected for {}/{}: {}", machine_id, sensor_id, anomaly.description);
        }

        Ok(())
    }
}
