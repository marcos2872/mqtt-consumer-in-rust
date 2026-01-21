use redis::Client;

pub async fn create_client(connection_string: &str) -> Result<Client, redis::RedisError> {
    Client::open(connection_string)
}
