use crate::infrastructure::database::postgres::DbPool;
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::error::Error;

// Local struct removed in favor of domain::SensorReading

#[derive(Clone)]
pub struct SensorRepository {
    pool: DbPool,
}

impl SensorRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn insert_readings_batch(
        &self,
        readings: &[crate::domain::sensor_reading::SensorReading],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if readings.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO sensor_readings (time, machine_id, sensor_id, value) "
        );

        query_builder.push_values(readings, |mut b, reading| {
            b.push_bind(reading.timestamp)
                .push_bind(&reading.machine_id)
                .push_bind(&reading.sensor_id)
                .push_bind(reading.value);
        });

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }
}
