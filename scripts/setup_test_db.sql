-- Setup table for Kinetic CDC testing
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255),
    data TEXT,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set replica identity to full so update/delete events contain old tuples
ALTER TABLE users REPLICA IDENTITY FULL;

-- Ensure publication is created for replication
CREATE PUBLICATION kinetic_pub FOR ALL TABLES;

-- Insert sample records
INSERT INTO users (name, data, active) VALUES
('Alice', 'Initial load record 1', true),
('Bob', 'Initial load record 2', true),
('Charlie', 'Initial load record 3', false);
