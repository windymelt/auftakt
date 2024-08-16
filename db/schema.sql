-- DB schema for Postgres

-- queue status enum
CREATE TYPE queue_status AS ENUM ('waiting', 'claimed', 'grabbed', 'finished');
CREATE TABLE IF NOT EXISTS queue (
    id BIGSERIAL PRIMARY KEY,
    dag_id BIGINT,
    node_id BIGINT,
    grabber_id INT,
    prerequisite_node_ids BIGINT[],
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    target_url TEXT NOT NULL,
    payload TEXT NOT NULL,
    status queue_status NOT NULL DEFAULT 'claimed',
    run_after TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX queue_status_idx ON queue (status);
