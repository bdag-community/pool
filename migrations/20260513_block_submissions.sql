CREATE TABLE IF NOT EXISTS block_submissions (
    id SERIAL PRIMARY KEY,
    candidate_hash TEXT NOT NULL,
    node_block_hash TEXT,
    height BIGINT,
    backend TEXT,
    template_seq BIGINT,
    accepted BOOLEAN NOT NULL DEFAULT FALSE,
    outcome TEXT NOT NULL,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS block_submissions_created_at_idx
    ON block_submissions (created_at);

CREATE INDEX IF NOT EXISTS block_submissions_outcome_created_idx
    ON block_submissions (outcome, created_at);
