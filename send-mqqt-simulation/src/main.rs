use chrono::{DateTime, Utc};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::{Duration, Instant};
use tokio::{task, time};

#[derive(serde::Serialize)]
struct SensorReading {
    timestamp: DateTime<Utc>,
    machine_id: String,
    sensor_id: String,
    // sensor_type: String,
    value: f64,
    // unit: Option<String>,
    // status: Option<String>,
}

#[tokio::main]
async fn main() {
    let mut mqttoptions = MqttOptions::new("rumqtt-async", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    // Buffer muito maior para suportar 100k+ msgs/s
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100000);
    client
        .subscribe("machines/machine1/data", QoS::AtMostOnce)
        .await
        .unwrap();

    // Task para processar o eventloop em paralelo (essencial para alta performance)
    task::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Eventloop error: {:?}", e);
                    break;
                }
            }
        }
    });

    task::spawn(async move {
        let mut i = 0;
        let total_messages = 1_000_000;
        let start = Instant::now();
        let mut last_log = Instant::now();
        let mut messages_since_log = 0;

        // Pre-serializar o timestamp base para ganhar performance
        let base_timestamp = Utc::now();

        for _ in 0..total_messages {
            // Payload simplificado para máxima velocidade
            let readings = vec![
                SensorReading {
                    timestamp: base_timestamp,
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor1".to_string(),
                    value: 25.5 + (i % 10) as f64 * 0.5,
                },
                SensorReading {
                    timestamp: base_timestamp,
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor2".to_string(),
                    value: 60.0 + (i % 20) as f64 * 0.2,
                },
                SensorReading {
                    timestamp: base_timestamp,
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor3".to_string(),
                    value: 113.25 + (i % 5) as f64 * 0.1,
                },
                SensorReading {
                    timestamp: base_timestamp,
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor4".to_string(),
                    value: 12.0 + (i % 5) as f64 * 0.1,
                },
                SensorReading {
                    timestamp: base_timestamp,
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor5".to_string(),
                    value: 2.5 + (i % 10) as f64 * 0.05,
                },
            ];
            let json = serde_json::to_string(&readings).unwrap();
            if let Err(e) = client
                .publish("machines/machine1/data", QoS::AtMostOnce, false, json)
                .await
            {
                eprintln!("Failed to publish: {:?}", e);
                break;
            }

            i += 1;
            messages_since_log += 1;

            // Log a cada segundo
            if last_log.elapsed() >= Duration::from_secs(1) {
                let rate = messages_since_log as f64 / last_log.elapsed().as_secs_f64();
                println!(
                    "Mensagens enviadas: {} de {} (Taxa: {:.0} msgs/s)",
                    i, total_messages, rate
                );
                last_log = Instant::now();
                messages_since_log = 0;
            }
        }

        let duration = start.elapsed();
        let actual_rate = total_messages as f64 / duration.as_secs_f64();
        println!("\n=== Resumo ===");
        println!("Total de mensagens: {}", total_messages);
        println!("Tempo total: {:?}", duration);
        println!("Taxa média: {:.0} msgs/s", actual_rate);
        std::process::exit(0);
    })
    .await
    .unwrap();
}
