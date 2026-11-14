CREATE TABLE accounts (
    id          UUID PRIMARY KEY,
    owner_name  VARCHAR(120) NOT NULL,
    currency    CHAR(3)      NOT NULL,
    balance     BIGINT       NOT NULL DEFAULT 0,
    type        VARCHAR(16)  NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT accounts_type_chk CHECK (type IN ('CUSTOMER', 'SETTLEMENT')),
    CONSTRAINT accounts_non_negative_chk CHECK (type = 'SETTLEMENT' OR balance >= 0)
);

CREATE TABLE transfers (
    id                 UUID PRIMARY KEY,
    idempotency_key    VARCHAR(64)  NOT NULL,
    request_hash       CHAR(64)     NOT NULL,
    source_account_id  UUID         NOT NULL REFERENCES accounts (id),
    target_account_id  UUID         NOT NULL REFERENCES accounts (id),
    amount             BIGINT       NOT NULL CHECK (amount > 0),
    currency           CHAR(3)      NOT NULL,
    status             VARCHAR(16)  NOT NULL,
    failure_reason     VARCHAR(64),
    reference          VARCHAR(140),
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT transfers_idempotency_key_uq UNIQUE (idempotency_key),
    CONSTRAINT transfers_status_chk CHECK (status IN ('COMPLETED', 'REJECTED')),
    CONSTRAINT transfers_distinct_accounts_chk CHECK (source_account_id <> target_account_id)
);

CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    transfer_id    UUID        NOT NULL REFERENCES transfers (id),
    account_id     UUID        NOT NULL REFERENCES accounts (id),
    direction      VARCHAR(6)  NOT NULL,
    amount         BIGINT      NOT NULL CHECK (amount > 0),
    balance_after  BIGINT      NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ledger_entries_direction_chk CHECK (direction IN ('DEBIT', 'CREDIT'))
);

CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id, id DESC);
CREATE INDEX ledger_entries_transfer_idx ON ledger_entries (transfer_id);

CREATE TABLE outbox_events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_type  VARCHAR(32)  NOT NULL,
    aggregate_id    UUID         NOT NULL,
    event_type      VARCHAR(64)  NOT NULL,
    payload         JSONB        NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX outbox_events_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;

INSERT INTO accounts (id, owner_name, currency, balance, type)
VALUES ('00000000-0000-0000-0000-000000000001', 'Settlement IDR', 'IDR', 0, 'SETTLEMENT'),
       ('00000000-0000-0000-0000-000000000002', 'Settlement USD', 'USD', 0, 'SETTLEMENT');
