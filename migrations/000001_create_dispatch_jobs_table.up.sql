CREATE TABLE dispatch_jobs (
    id BIGSERIAL PRIMARY KEY,
    channel VARCHAR(20) NOT NULL,
    recipient TEXT NOT NULL,
    message TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 3,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);