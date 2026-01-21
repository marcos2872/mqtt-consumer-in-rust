use crate::domain::aggregation::Aggregation;
use crate::infrastructure::database::postgres::DbPool;
use chrono::{DateTime, Utc};
use sqlx::{Error, FromRow};

#[derive(Clone)]
pub struct AggregationRepository {
    pool: DbPool,
}

#[derive(FromRow)]
struct ReadingRow {
    value: f64,
}

impl AggregationRepository {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    pub async fn get_readings_in_range(
        &self,
        machine_id: &str,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<f64>, Error> {
        let rows: Vec<ReadingRow> = sqlx::query_as(
            "SELECT value FROM sensor_readings 
             WHERE machine_id = $1 AND time >= $2 AND time < $3"
        )
        .bind(machine_id)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.value).collect())
    }

    pub async fn save_aggregation(&self, aggregation: &Aggregation) -> Result<(), Error> {
        // Assuming 'aggregations' table exists, if not we might need migration. 
        // Based on architecture doc, it should exist or be created.
        // We might want to check if migration exists in data-ingestion-service or create one.
        // For now, I'll write the query assuming it follows the structure.

        sqlx::query(
            "INSERT INTO aggregations (machine_id, sensor_id, window_start, window_end, avg_value, min_value, max_value, count_value, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        )
        .bind(&aggregation.machine_id)
        .bind(&aggregation.sensor_id)
        .bind(aggregation.window_start)
        .bind(aggregation.window_end)
        .bind(aggregation.avg_value)
        .bind(aggregation.min_value)
        .bind(aggregation.max_value)
        .bind(aggregation.count)
        .bind(aggregation.timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
