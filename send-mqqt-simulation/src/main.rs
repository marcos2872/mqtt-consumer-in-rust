use chrono::{DateTime, Utc};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;
use tokio::{task, time};

#[derive(serde::Serialize)]
struct SensorReading {
    time: DateTime<Utc>,
    machine_id: String,
    sensor_id: String,
    sensor_type: String,
    value: f64,
    unit: Option<String>,
    status: Option<String>,
}

#[tokio::main]
async fn main() {
    let mut mqttoptions = MqttOptions::new("rumqtt-async", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client
        .subscribe("iot/sensors", QoS::AtMostOnce)
        .await
        .unwrap();

    task::spawn(async move {
        let mut i = 0;
        loop {
            let readings = vec![
                SensorReading {
                    time: Utc::now(),
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor1".to_string(),
                    sensor_type: "temperature".to_string(),
                    value: 25.5 + (i % 10) as f64 * 0.5,
                    unit: Some("C".to_string()),
                    status: Some("ok".to_string()),
                },
                SensorReading {
                    time: Utc::now(),
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor2".to_string(),
                    sensor_type: "humidity".to_string(),
                    value: 60.0 + (i % 20) as f64 * 0.2,
                    unit: Some("%".to_string()),
                    status: Some("ok".to_string()),
                },
                SensorReading {
                    time: Utc::now(),
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor3".to_string(),
                    sensor_type: "pressure".to_string(),
                    value: 1013.25 + (i % 5) as f64 * 0.1,
                    unit: Some("hPa".to_string()),
                    status: Some("ok".to_string()),
                },
                SensorReading {
                    time: Utc::now(),
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor4".to_string(),
                    sensor_type: "voltage".to_string(),
                    value: 12.0 + (i % 5) as f64 * 0.1,
                    unit: Some("V".to_string()),
                    status: Some("ok".to_string()),
                },
                SensorReading {
                    time: Utc::now(),
                    machine_id: "machine1".to_string(),
                    sensor_id: "sensor5".to_string(),
                    sensor_type: "current".to_string(),
                    value: 2.5 + (i % 10) as f64 * 0.05,
                    unit: Some("A".to_string()),
                    status: Some("ok".to_string()),
                },
            ];
            let json = serde_json::to_string(&readings).unwrap();
            if let Err(e) = client
                .publish("iot/sensors", QoS::AtLeastOnce, false, json)
                .await
            {
                eprintln!("Failed to publish: {:?}", e);
                break;
            }
            time::sleep(Duration::from_millis(3)).await;
            i += 1;
        }
    });

    loop {
        let _ = eventloop.poll().await;
    }
}
