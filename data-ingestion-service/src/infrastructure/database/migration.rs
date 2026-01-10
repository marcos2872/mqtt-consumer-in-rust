use sqlx::migrate::Migrator;
use sqlx::Pool;
use sqlx::Postgres;
use std::path::Path;

pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub async fn run_migrations(pool: &Pool<Postgres>) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
