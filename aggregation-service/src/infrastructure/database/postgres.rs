use sqlx::postgres::{PgPoolOptions, PgPool};
use std::time::Duration;

pub type DbPool = PgPool;

pub async fn create_pool(connection_string: &str) -> Result<DbPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(connection_string)
        .await
}
