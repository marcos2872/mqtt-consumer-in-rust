use crate::domain::aggregation::Aggregation;
use crate::domain::anomaly::Anomaly;
use redis::{AsyncCommands,Client, RedisError};

#[derive(Clone)]
pub struct RedisPublisher {
    client: Client,
}

impl RedisPublisher {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn publish_aggregation(&self, aggregation: &Aggregation) -> Result<(), RedisError> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let json = serde_json::to_string(aggregation).unwrap_or_default();
        
        let _: String = con.xadd(
            "aggregations",
            "*",
            &[("data", json.as_str()), ("type", "aggregation")]
        ).await?;
        
        Ok(())
    }

    pub async fn publish_anomaly(&self, anomaly: &Anomaly) -> Result<(), RedisError> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let json = serde_json::to_string(anomaly).unwrap_or_default();

        let _: String = con.xadd(
            "anomalies",
            "*",
            &[("data", json.as_str()), ("type", "anomaly")]
        ).await?;

        Ok(())
    }
}
