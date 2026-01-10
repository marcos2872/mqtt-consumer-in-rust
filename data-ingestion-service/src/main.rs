mod infrastructure;

use infrastructure::mqtt::client;

#[tokio::main]
async fn main() {
    client::client().await;
}
