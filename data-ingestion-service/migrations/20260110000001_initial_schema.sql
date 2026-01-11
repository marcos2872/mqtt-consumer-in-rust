-- Hipertable para sensor readings (1B+ rows)
CREATE TABLE IF NOT EXISTS sensor_readings (
    time TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    sensor_id TEXT NOT NULL,
    -- sensor_type TEXT NOT NULL,
    value FLOAT8 NOT NULL
    -- unit TEXT,
    -- status TEXT
);

SELECT create_hypertable('sensor_readings', 'time', if_not_exists => TRUE);

-- Compressão automática
ALTER TABLE sensor_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'machine_id,sensor_id'
);

SELECT add_compression_policy('sensor_readings', INTERVAL '1 week', if_not_exists => TRUE);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_machine_sensor_time
    ON sensor_readings (machine_id, sensor_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_machine_time
    ON sensor_readings (machine_id, time DESC);

-- Retention (1 ano)
SELECT add_retention_policy('sensor_readings', INTERVAL '1 year', if_not_exists => TRUE);

-- Aggregations table
CREATE TABLE IF NOT EXISTS aggregations (
    time TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    temp_avg FLOAT8,
    temp_max FLOAT8,
    temp_min FLOAT8,
    pressure_avg FLOAT8,
    vibration_p99 FLOAT8,
    anomalies INTEGER,
    status TEXT
);

SELECT create_hypertable('aggregations', 'time', if_not_exists => TRUE);

-- Alert Events table
CREATE TABLE IF NOT EXISTS alert_events (
    id UUID PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    machine_id TEXT NOT NULL,
    rule_id UUID NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_by TEXT,
    resolved_at TIMESTAMPTZ
);

-- Alert Rules table
CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    condition TEXT NOT NULL,  -- JSON serializado
    severity TEXT NOT NULL,
    notification_channels TEXT[] NOT NULL,  -- {slack,email,sms}
    cooldown_minutes INTEGER DEFAULT 5,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Machines table
CREATE TABLE IF NOT EXISTS machines (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    status TEXT DEFAULT 'active',
    sensors JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
