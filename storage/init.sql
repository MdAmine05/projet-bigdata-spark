CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    ip VARCHAR(50),
    alert_type VARCHAR(100),
    details TEXT,
    severity VARCHAR(20)
);
