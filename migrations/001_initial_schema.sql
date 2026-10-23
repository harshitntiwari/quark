CREATE TABLE jobs (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue           TEXT        NOT NULL DEFAULT 'default',
    kind            TEXT        NOT NULL,
    args            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    unique_key      TEXT,
    priority        INT         NOT NULL DEFAULT 0,
    state           TEXT        NOT NULL DEFAULT 'available',
    attempts        INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 3,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    timeout_ms      BIGINT      NOT NULL DEFAULT 60000,
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    errors          JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX idx_claimable_jobs ON jobs (queue, priority, run_at, id) WHERE state = 'available';
CREATE UNIQUE INDEX idx_jobs_unique_key ON jobs (unique_key) WHERE unique_key IS NOT NULL AND state IN ('available', 'running');