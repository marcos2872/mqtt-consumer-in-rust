use chrono::{DateTime, Utc};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::{Duration, Instant};
use tokio::{task, time};

#[derive(serde::Serialize)]
struct SensorReading {
    timestamp: DateTime<Utc>,
    machine_id: &'static str,
    sensor_id: &'static str,
    value: f64,
}

#[tokio::main]
async fn main() {
    // ========================================
    // CONFIGURAÇÃO DE FREQUÊNCIA
    // ========================================
    // None = Máxima velocidade (sem limite)
    // Some(100_000) = 100k mensagens por segundo
    // Some(10_000) = 10k mensagens por segundo
    // Some(1_000) = 1k mensagens por segundo
    let target_rate: Option<u64> = Some(10_000);
    let total_messages = 1_000_000;

    let mut mqttoptions = MqttOptions::new("rumqtt-async", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    // Buffer muito maior para suportar 100k+ msgs/s
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 500_000);
    client
        .subscribe("machines/machine1/data", QoS::ExactlyOnce)
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
        let mut sent_count: u64 = 0;
        let mut failed_count: u64 = 0;
        let start = Instant::now();
        let mut last_log = Instant::now();
        let mut messages_since_log = 0;
        let mut total_publish_time = Duration::ZERO;

        // Pre-serializar o timestamp base para ganhar performance
        let base_timestamp = Utc::now();

        if let Some(rate) = target_rate {
            println!("🎯 Taxa configurada: {} msgs/s", rate);
            println!("   Método: Controle de taxa baseado em tempo decorrido");
        } else {
            println!("🚀 Modo: Máxima velocidade (sem limite)");
        }

        for _ in 0..total_messages {
            // Payload único e pequeno para máxima velocidade
            let reading = vec![
                SensorReading {
                    timestamp: base_timestamp,
                    machine_id: "machine1",
                    sensor_id: "sensor1",
                    value: 25.5 + (i % 100) as f64 * 0.1,
                },
                // SensorReading {
                //     timestamp: base_timestamp,
                //     machine_id: "machine1",
                //     sensor_id: "sensor2",
                //     value: 25.5 + (i % 100) as f64 * 0.1,
                // },
                // SensorReading {
                //     timestamp: base_timestamp,
                //     machine_id: "machine1",
                //     sensor_id: "sensor3",
                //     value: 25.5 + (i % 100) as f64 * 0.1,
                // },
            ];

            let json = serde_json::to_string(&reading).unwrap();

            let publish_start = Instant::now();
            match client
                .publish("machines/machine1/data", QoS::ExactlyOnce, false, json)
                .await
            {
                Ok(_) => {
                    sent_count += 1;
                }
                Err(e) => {
                    failed_count += 1;
                    // Diagnóstico detalhado de erros
                    match e {
                        rumqttc::ClientError::Request(_) => {
                            eprintln!("❌ Buffer cheio - mensagem descartada (Request error)");
                        }
                        _ => {
                            eprintln!("❌ Erro de publish: {:?}", e);
                        }
                    }
                    // Pequena pausa antes de tentar novamente
                    time::sleep(Duration::from_millis(1)).await;
                }
            }
            total_publish_time += publish_start.elapsed();

            i += 1;
            messages_since_log += 1;

            // Controle de taxa baseado em tempo decorrido (mais preciso que sleep por mensagem)
            if let Some(target) = target_rate {
                let elapsed = start.elapsed();
                let expected_time = Duration::from_secs_f64(i as f64 / target as f64);

                if expected_time > elapsed {
                    let sleep_time = expected_time - elapsed;
                    // Só dorme se o tempo for significativo (> 100µs)
                    if sleep_time > Duration::from_micros(100) {
                        time::sleep(sleep_time).await;
                    }
                }
            }

            // Log a cada segundo
            if last_log.elapsed() >= Duration::from_secs(1) {
                let rate = messages_since_log as f64 / last_log.elapsed().as_secs_f64();
                let avg_publish_time = total_publish_time / messages_since_log as u32;
                println!(
                    "Mensagens enviadas: {} de {} (Taxa: {:.0} msgs/s, Tempo médio publish: {:?})",
                    i, total_messages, rate, avg_publish_time
                );
                last_log = Instant::now();
                messages_since_log = 0;
                total_publish_time = Duration::ZERO;
            }
        }

        let duration = start.elapsed();
        let actual_rate = total_messages as f64 / duration.as_secs_f64();
        println!("\n=== Resumo ===");
        println!("Total de mensagens: {}", total_messages);
        println!("Mensagens enviadas: {}", sent_count);
        println!("Mensagens falhadas: {}", failed_count);
        println!(
            "Mensagens recebidas no broker: {} (estimativa)",
            sent_count.saturating_sub(failed_count)
        );
        println!("Tempo total: {:?}", duration);
        println!("Taxa média: {:.0} msgs/s", actual_rate);
        if let Some(target) = target_rate {
            let efficiency = (actual_rate / target as f64) * 100.0;
            println!("Taxa alvo: {} msgs/s ({:.1}% da meta)", target, efficiency);
        }

        println!("\n💡 Diagnóstico:");
        println!("- QoS::ExactlyOnce garante entrega (mais lento que AtMostOnce)");
        println!("- Se mensagens falhadas > 0: problema de conexão ou buffer");
        println!("- 'Buffer cheio' = broker não consegue processar rápido o suficiente");
        println!("- Tempo médio de publish: indica velocidade do eventloop");
        println!("- Se taxa < meta: ajuste do controle de taxa ou limitação do broker");
        println!("- Para taxas > 50k msgs/s, use None (velocidade máxima)");
        println!("- Solução: reduzir target_rate ou aumentar buffer do broker");
        std::process::exit(0);
    })
    .await
    .unwrap();
}
