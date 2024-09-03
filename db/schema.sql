-- DB schema for Postgres

-- queue status enum
CREATE TYPE queue_status AS ENUM ('waiting', 'claimed', 'grabbed', 'finished', 'failed');
CREATE TABLE IF NOT EXISTS queue (
    id BIGSERIAL PRIMARY KEY,
    dag_id BIGINT,
    node_id BIGINT,
    grabber_id INT,
    prerequisite_node_ids BIGINT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    target_url TEXT NOT NULL,
    payload TEXT NOT NULL,
    status queue_status NOT NULL DEFAULT 'claimed',
    run_after TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dag_id_node_id_unique UNIQUE (dag_id, node_id),
    CONSTRAINT node_cannot_depend_itself CHECK (prerequisite_node_ids = '{}' OR node_id != ANY (prerequisite_node_ids))
);
CREATE INDEX queue_status_idx ON queue (status);

CREATE TABLE IF NOT EXISTS target (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    retry_strategy JSONB, -- NULL means no retry
    throttle_strategy JSONB, -- NULL means no throttle
    max_concurrency INT, -- NULL means no limit (every node can run concurrently)
    -- buffered job can be lost if the worker dies (will be retried). endpoint should be idempotent
    buffer_size INT, -- NULL means no job buffer (blocking)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
