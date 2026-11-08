CREATE TABLE IF NOT EXISTS meta.etl_watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_extracted_timestamp TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00',
    last_batch_id VARCHAR(64),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO meta.etl_watermark (table_name, last_extracted_timestamp) VALUES
('customers', '1970-01-01 00:00:00'),
('hubs', '1970-01-01 00:00:00'),
('vehicles', '1970-01-01 00:00:00'),
('couriers', '1970-01-01 00:00:00'),
('service_types', '1970-01-01 00:00:00'),
('payment_methods', '1970-01-01 00:00:00'),
('status_reasons', '1970-01-01 00:00:00'),
('shipments', '1970-01-01 00:00:00'),
('shipment_tracking_events', '1970-01-01 00:00:00'),
('cod_settlements', '1970-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS meta.etl_batch_log (
    batch_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    run_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_end TIMESTAMP,
    rows_extracted INT DEFAULT 0,
    rows_loaded INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    status VARCHAR(30) NOT NULL, -- RUNNING / SUCCESS / FAILED
    error_message TEXT,
    PRIMARY KEY (batch_id, table_name)
);

CREATE TABLE IF NOT EXISTS meta.data_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    record_id VARCHAR(100) NOT NULL,
    rule_name VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- Blocker / Warning
    error_message TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE
);