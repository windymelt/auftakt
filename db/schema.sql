-- DB schema for Postgres

-- queue status enum
CREATE TYPE queue_status AS ENUM ('waiting', 'claimed', 'grabbed', 'finished');
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
    CONSTRAINT node_cannot_depend_itself CHECK (node_id != ANY (prerequisite_node_ids))
);
CREATE INDEX queue_status_idx ON queue (status);
