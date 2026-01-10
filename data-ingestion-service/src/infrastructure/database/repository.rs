use crate::infrastructure::database::postgres::DbPool;
use chrono::{DateTime, Utc};
use sqlx::Row;
use std::error::Error;

#[derive(Debug, Clone)]
pub struct SensorReading {
    pub time: DateTime<Utc>,
    pub machine_id: String,
    pub sensor_id: String,
    pub sensor_type: String,
    pub value: f64,
    pub unit: Option<String>,
    pub status: Option<String>,
}

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
        readings: &[SensorReading],
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if readings.is_empty() {
            return Ok(());
        }

        let mut query_builder = sqlx::QueryBuilder::new(
            "INSERT INTO sensor_readings (time, machine_id, sensor_id, sensor_type, value, unit, status) "
        );

        query_builder.push_values(readings, |mut b, reading| {
            b.push_bind(reading.time)
                .push_bind(&reading.machine_id)
                .push_bind(&reading.sensor_id)
                .push_bind(&reading.sensor_type)
                .push_bind(reading.value)
                .push_bind(&reading.unit)
                .push_bind(&reading.status);
        });

        let query = query_builder.build();
        query.execute(&self.pool).await?;

        Ok(())
    }
}
