-- DB schema for Postgres

-- queue status enum
CREATE TYPE queue_status AS ENUM ('claimed', 'grabbed');
CREATE TABLE IF NOT EXISTS queue (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    target_url TEXT NOT NULL,
    payload TEXT NOT NULL,
    status queue_status NOT NULL DEFAULT 'claimed'
);
CREATE INDEX queue_status_idx ON queue (status);