# me fale qual a fincao de capa .rs

A leitura é “cada .rs”, né? Então vamos por serviço/pasta e explicar a função de **cada arquivo .rs** em alto nível.

***

## Padrão geral da arquitetura

Antes de ir arquivo por arquivo, o padrão é:

- **domain/** → modelo de domínio e regras de negócio (não depende de Axum, Redis, DB etc.)
- **application/** → casos de uso, orquestração, “service layer”
- **infrastructure/** → integração com mundo externo (HTTP, DB, Redis, MQTT…)
- **config.rs** → carregamento de configuração
- **lib.rs** → ponto central de export das partes do crate
- **main.rs** → entrypoint binário (`fn main`)

Vou seguir esse padrão em cada serviço.

***

## data-ingestion-service

### src/domain/

- **sensor_reading.rs**
Define a entidade principal de leitura de sensor (`SensorReading`): machine_id, sensor_id, tipo, valor, unidade, timestamp, status, métodos de validação básica.
- **machine.rs**
Modelo de uma máquina e seus sensores (ex.: id, nome, localização, lista de sensores). Serve para operações de nível de máquina, não de leitura individual.
- **error.rs**
Tipos de erro do domínio (ex.: `DomainError`, `ValidationError`), sem depender de HTTP/DB. Usado para propagar falhas de validação/regra de negócio.
- **validation_rules.rs**
Regras de validação de leitura (faixas de temperatura, pressão, umidade, etc.). Centraliza “o que é um dado válido” separado do parsing/infra.


### src/application/

- **mqtt_handler.rs**
Orquestra a conexão com o broker MQTT: subscribe aos tópicos, recebe mensagens, envia para o parser/validador e coloca no buffer.
- **message_parser.rs**
Converte payload bruto (JSON, binário) em `SensorReading`. Faz `serde`/`bincode`, lida com erros de parsing.
- **buffering.rs**
Implementa o buffer circular (ex. `VecDeque` + lock) para segurar até N leituras antes de persistir em batch.
- **batch_processor.rs**
Pega os itens do buffer em lotes (10k, 100ms, etc.) e chama o repositório para fazer `COPY`/inserts no banco.
- **health_check.rs**
Lógica para checar saúde do serviço (estado do buffer, conexão DB/MQTT) e responder no endpoint `/health`.


### src/infrastructure/mqtt/

- **client.rs**
Wrapper em torno do `rumqttc` async client. Configura opções, reconexão, QoS, etc.
- **topics.rs**
Centraliza nomes/padrões de tópicos MQTT (`machines/+/sensors/+/data`, etc.) para não ficar string mágica solta.
- **connection.rs**
Gerencia ciclo de vida da conexão MQTT (connect, reconnect, event loop).


### src/infrastructure/database/

- **postgres.rs**
Cria e configura `PgPool`/`Pool` do SQLx (connection string, pool size, timeout).
- **repository.rs**
Funções de acesso ao banco para leitura/escrita de leituras de sensor (incluindo batch insert).
- **migration.rs**
Roda/organiza migrations do schema inicial (pode chamar `sqlx::migrate!` ou scripts SQL).


### src/infrastructure/http/

- **routes.rs**
Define as rotas Axum (`/health`, `/metrics`, `/stats/...`) e monta o `Router`.
- **handlers.rs**
Implementa os handlers HTTP (funções que recebem `State`, `Json`, etc. e devolvem resposta).
- **middleware.rs**
Middlewares HTTP (auth, logging, CORS, tracing, rate limit se tiver).


### src/infrastructure/metrics/

- **prometheus.rs**
Registra e expõe métricas Prometheus (counters, gauges, histograms) e integra com `/metrics`.
- **gauges.rs**
Métricas do tipo gauge específicas (buffer usage, mensagens por segundo, lag de DB…).
- **collectors.rs**
Código que coleta valores atuais (ex.: tamanho do buffer) e atualiza métricas.


### src raiz

- **config.rs**
Struct `Config` + leitura de env/arquivo (`DATABASE_URL`, `MQTT_BROKER_URL`, etc.).
- **lib.rs**
Reexporta módulos principais, separa binário de biblioteca. Útil para testes e reuso.
- **main.rs**
Entry point: carrega config, cria pool DB, conecta MQTT, monta Axum, inicia tasks (buffer/batch) e `tokio::main`.

***

## aggregation-service

### src/domain/

- **aggregation.rs**
Struct de agregação (por exemplo, por máquina/janela): médias, min/max, p95, p99, contagem, status.
- **metric.rs**
Abstração de métrica individual (pode ser usada para compor várias métricas num `Aggregation`).
- **window.rs**
Representação de uma janela de tempo (tamanho, timestamps, dados dentro da janela) e operações sobre ela.
- **anomaly.rs**
Tipos e structs para anomalias: valor, expected_range, z-score, severidade, tipo da anomalia.
- **error.rs**
Tipos de erro específicos do domínio de agregação (ex.: falta de histórico suficiente).


### src/application/

- **aggregator.rs**
Orquestra o processo de agregação: lê dados, aplica janelas, calcula métricas, chama detector de anomalia e publisher.
- **stream_processor.rs**
Usa Tokio Streams para processar fluxos contínuos de leituras vindas do DB ou de outro canal.
- **sliding_window.rs**
Implementa de fato o algoritmo de janela deslizante (array circular, add/pop em O(1), etc.).
- **anomaly_detector.rs**
Implementa Z-score, IQR, thresholds, etc. sobre as janelas/histórico.
- **publisher.rs**
Envia o resultado da agregação para dois destinos: TimescaleDB (persistência) e Redis Streams (real-time).
- **scheduler.rs**
Agenda execuções periódicas (ex.: a cada 60s roda agregação de 1h, etc.) usando `tokio::time::interval`.


### src/infrastructure/database/

- **postgres.rs**
Configuração de pool e funções helper de conexão pro SQLx.
- **repository.rs**
Queries específicas: ler leituras de um período, salvar agregações, buscar últimas agregações.
- **cache.rs**
Cache de agregações (ex.: Moka) para evitar reconsultas repetidas para mesma janela/período.


### src/infrastructure/redis/

- **client.rs**
Configura conexão assíncrona com Redis.
- **streams.rs**
Funções para `XADD`, `XREAD`, criação de consumer groups para o stream de agregações.
- **cache.rs**
Wrapper de cache usando Redis/Moka para estado auxiliar de agregações, se necessário.


### src/infrastructure/http/

- **routes.rs**
Define rotas HTTP para expor agregações, anomalias, trends, etc.
- **handlers.rs**
Implementação dos handlers dessas rotas (montam queries ao repository, formatam resposta).


### src/infrastructure/metrics/

- **prometheus.rs**
Registro de métricas Prometheus de agregação (tempo de processamento, total de agregações, anomalias, lag).


### src raiz

- **config.rs**
Config do serviço de agregação (intervalos, janelas, thresholds, conexões).
- **main.rs**
Bootstrap: carrega config, inicializa DB/Redis, registra metrics, sobe HTTP, inicia scheduler.

***

## alert-service

### src/domain/

- **alert_rule.rs**
Modelo de regra de alerta: condição (ex. JSON ou DSL), severidade, cooldown, canais de notificação.
- **alert_event.rs**
Representa um alerta disparado: id, máquina, regra, mensagem, severity, timestamps (fired, ack, resolved).
- **severity.rs**
Enum de severidade (`Info`, `Warning`, `Critical`), possivelmente com helpers.
- **notification.rs**
Modelo da notificação gerada (mensagem, destino, canal, payload extenso).
- **error.rs**
Erros de domínio do sistema de alertas.


### src/application/

- **alert_engine.rs**
Orquestrador geral: recebe agregações/events, passa pelo rule engine, deduplicator, notifier.
- **rule_engine.rs**
Avalia as regras (`alert_rule.rs`) contra os dados de agregação/anomalia.
- **threshold_checker.rs**
Lógica de thresholds simples (temp > X, vibração > Y etc.), usada pelo rule_engine.
- **anomaly_checker.rs**
Lê anomalias do Aggregation Service/DB e verifica regras relacionadas a elas.
- **deduplicator.rs**
Evita disparar o mesmo alerta repetidas vezes em curto intervalo; usa cache (Moka/Redis) para lembrar últimos alertas.
- **notifier.rs**
Fala com a camada de infraestrutura de notificações (Slack, e‑mail, SMS, webhooks) para efetivamente enviar alertas.
- **consumers/stream_consumer.rs**
Consumer das Redis Streams de agregações em tempo real (modo reativo/tempo real).
- **consumers/db_poller.rs**
Faz polling periódico no banco (modo batch) para regras menos urgentes (daily/periodic checks).
- **scheduler.rs**
Agenda tasks recorrentes (pollers, limpezas de cache, reprocessamentos).


### src/infrastructure/database/

- **postgres.rs**
Pool e helpers de conexão ao banco (TimescaleDB/PostgreSQL).
- **repository.rs**
CRUD de alertas, regras, histórico; queries para buscar agregações necessárias para avaliação de regras.


### src/infrastructure/redis/

- **streams.rs**
Funções para ler/escrever streams de eventos (aggregations, alerts, dead letters, etc.).


### src/infrastructure/cache/

- **dedup.rs**
Implementa cache de deduplicação de alertas (por chave de alerta/regra + máquina) usando Moka ou Redis.


### src/infrastructure/notifications/

- **slack.rs**
Cliente para enviar mensagens para Slack (webhook, canal, formatação).
- **email.rs**
Cliente SMTP (ou serviço tipo SES) para enviar e-mails de alerta.
- **sms.rs**
Wrapper para provedor de SMS (Twilio, AWS SNS, etc.).
- **webhook.rs**
Envio de notificações genéricas via HTTP POST para sistemas externos.


### src/infrastructure/http/

- **routes.rs**
Rotas para gerenciar/consultar alertas e regras (`/alerts`, `/rules`, `/notifications/...`).


### src/infrastructure/metrics/

- **prometheus.rs**
Métricas de alertas: quantos disparados, latência alerta→envio, tamanho do dedup cache, etc.


### src raiz

- **main.rs**
Sobe tudo: consumers de stream, pollers de DB, HTTP, métricas, etc.

***

## shared (módulo compartilhado)

Mesmo não aparecendo no snippet, em geral:

- **src/models/sensor_reading.rs**
Versão compartilhada da entidade `SensorReading` usada entre serviços.
- **src/models/aggregation.rs**
Modelo compartilhado de agregações (para não duplicar struct em cada crate).
- **src/models/alert.rs**
Modelo compartilhado de alertas (útil entre alert-service, API, dashboards).
- **src/errors.rs**
Tipos de erro comuns (ex.: `AppError`) usados por vários serviços.
- **src/config.rs**
Helper genérico de config (carregar `.env`, mapear para struct, etc.).
- **src/constants.rs**
Constantes compartilhadas (nomes de tópicos MQTT, nomes de streams Redis, etc.).
- **lib.rs**
Exporta todos esses modelos/helpers para os outros crates do workspace.

***
