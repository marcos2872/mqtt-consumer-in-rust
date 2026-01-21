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

    pub async fn get_active_sensors(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<(String, String)>, Error> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT machine_id, sensor_id FROM sensor_readings WHERE time >= $1"
        )
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn save_aggregation(&self, aggregation: &Aggregation) -> Result<(), Error> {
        let query = match aggregation.sensor_id.as_str() {
            "temperature" | "sensor1" => 
                "INSERT INTO aggregations (time, machine_id, temp_avg, temp_min, temp_max, status) VALUES ($1, $2, $3, $4, $5, 'OK')",
            "pressure" | "sensor2" => 
                "INSERT INTO aggregations (time, machine_id, pressure_avg, status) VALUES ($1, $2, $3, 'OK')",
            "vibration" | "sensor3" => 
                "INSERT INTO aggregations (time, machine_id, vibration_p99, status) VALUES ($1, $2, $3, 'OK')",
            _ => {
                println!("Skipping aggregation save for unknown sensor_id: {}", aggregation.sensor_id);
                return Ok(());
            }
        };

        let mut q = sqlx::query(query)
            .bind(aggregation.window_end) // Using window_end as the time anchor
            .bind(&aggregation.machine_id);
            
        // Bind the variable arguments
        if aggregation.sensor_id == "temperature" || aggregation.sensor_id == "sensor1" {
             q = q.bind(aggregation.avg_value)
                  .bind(aggregation.min_value)
                  .bind(aggregation.max_value);
        } else if aggregation.sensor_id == "pressure" || aggregation.sensor_id == "sensor2" {
             q = q.bind(aggregation.avg_value);
        } else if aggregation.sensor_id == "vibration" || aggregation.sensor_id == "sensor3" {
             q = q.bind(aggregation.p99_value.unwrap_or(aggregation.max_value));
        }

        q.execute(&self.pool).await?;

        Ok(())
    }
}
