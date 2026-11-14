# payment-ledger-service

A double-entry ledger and money-transfer service built with Java 17 and Spring Boot.
It focuses on the problems a payment backend actually has to get right:
**money is never created or lost, retries never charge twice, concurrent
requests never overdraw an account, and every state change is reliably
published as an event.**

![CI](https://github.com/marvinds1/payment-ledger-service/actions/workflows/ci.yml/badge.svg)

## Highlights

| Concern | How it is handled |
|---|---|
| Correct balances | Double-entry bookkeeping. Every transfer writes one `DEBIT` and one `CREDIT` entry. The sum of all balances is always zero, and each balance equals the sum of its entries (both checked in tests). |
| Safe retries | `Idempotency-Key` header with a unique constraint. A retry with the same key returns the original result (`Idempotent-Replayed: true`). Reusing a key with a different payload is rejected. |
| Concurrency | Pessimistic row locks (`SELECT … FOR UPDATE`) taken in a deterministic order (by account id), so opposing transfers (A→B and B→A) cannot deadlock. |
| No overdraft | Balance is checked under the lock, backed by a database `CHECK` constraint. |
| Reliable events | Transactional outbox: the event row is written in the same DB transaction as the transfer and relayed to Kafka by a poller using `FOR UPDATE SKIP LOCKED` (safe with multiple instances, at-least-once delivery). |
| Auditability | Rejected transfers (e.g. insufficient funds) are persisted too, so a retry gets the same answer and there is a full trail. |
| Schema | Versioned migrations with Flyway. Money is stored as `BIGINT` minor units, never floating point. |

## Architecture

```mermaid
flowchart LR
    client([Client]) -->|REST + Idempotency-Key| api[Spring Boot API]
    api --> svc[TransferService<br/>idempotency]
    svc --> exec[TransferExecutor<br/>@Transactional]
    exec -->|lock accounts, write transfer,<br/>entries, outbox| db[(PostgreSQL)]
    relay[OutboxRelay<br/>scheduled] -->|SKIP LOCKED batch| db
    relay -->|ledger.transfer-events| kafka[(Kafka)]
```

Flow of `POST /api/v1/transfers`:

1. Look up the `Idempotency-Key`. If it exists, compare the request hash and replay the stored result.
2. Otherwise, in one transaction: lock both accounts in id order, validate currency, check funds,
   insert the transfer, update balances, insert two ledger entries, and append an outbox event.
3. If two requests with the same key race, the unique constraint lets exactly one win; the loser
   reads the winner's result and replays it.
4. The outbox relay publishes pending events to Kafka (keyed by transfer id, so events for one
   transfer stay ordered within a partition) and marks them as published.

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/accounts` | Open an account (`ownerName`, `currency`) |
| `GET` | `/api/v1/accounts/{id}` | Account with current balance |
| `GET` | `/api/v1/accounts/{id}/entries?beforeId=&limit=` | Ledger entries, keyset pagination |
| `POST` | `/api/v1/accounts/{id}/deposits` | Fund an account from the settlement account (requires `Idempotency-Key`) |
| `POST` | `/api/v1/transfers` | Transfer between customer accounts (requires `Idempotency-Key`) |
| `GET` | `/api/v1/transfers/{id}` | Transfer status |

Errors use RFC 7807 `application/problem+json` with a machine-readable `code`
(`INSUFFICIENT_FUNDS`, `CURRENCY_MISMATCH`, `IDEMPOTENCY_KEY_REUSED`, …).
OpenAPI UI is available at `/swagger-ui.html`.

```bash
curl -s -X POST localhost:8080/api/v1/transfers \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 3f1c9a52-7d1e-4a43-9c55-0b1f6f2c8a10' \
  -d '{"sourceAccountId":"<alice>","targetAccountId":"<bob>","amount":250000,"currency":"IDR"}'
```

| Outcome | HTTP status |
|---|---|
| Completed | `201 Created` |
| Replayed (same key, same payload) | `200 OK` + `Idempotent-Replayed: true` |
| Rejected (insufficient funds) | `422` with the persisted transfer (`status: REJECTED`) |
| Same key, different payload | `422` problem with `code: IDEMPOTENCY_KEY_REUSED` |

## Running locally

Requirements: Java 17, Maven, Docker.

```bash
docker compose up -d postgres kafka     # infrastructure only
mvn spring-boot:run                     # app on :8080

# or everything in containers
docker compose --profile app up --build
```

## Tests

```bash
mvn verify
```

Integration tests run against real PostgreSQL and Kafka via Testcontainers and cover:

- happy-path transfer with balanced ledger entries
- idempotent replay, and key reuse with a different payload
- insufficient funds recorded as `REJECTED` without touching balances
- 30 concurrent transfers draining one account → exactly 10 succeed, balance never negative
- 40 opposing concurrent transfers (A→B / B→A) → no deadlock, money conserved
- 10 concurrent requests with the same key → exactly one transfer
- global invariants: sum of balances is zero, every balance matches its entries
- outbox event delivered to the Kafka topic

## Load test

A [k6](https://k6.io) script lives in [`load-test/transfers.js`](load-test/transfers.js):

```bash
docker run --rm -i --network host -e BASE_URL=http://localhost:8080 grafana/k6 run - < load-test/transfers.js
```

## Design notes

- **Why pessimistic locking instead of optimistic versioning?** Transfers on a hot account would
  otherwise fail and retry constantly. Short row locks in a fixed order give predictable latency and
  no deadlocks.
- **Why a settlement account?** Deposits must also be double-entry. Money enters the system by
  debiting a settlement account (allowed to go negative), so the global sum stays zero.
- **Why persist rejected transfers?** Idempotency must cover failures too; otherwise a retry after a
  top-up could succeed where the original request failed, which surprises the client.
- **Why an outbox instead of publishing to Kafka inside the request?** Writing to the database and
  Kafka are two systems with no shared transaction. The outbox makes the event part of the same
  commit, so an event exists exactly when the transfer does.

## Roadmap

- Redis-backed rate limiting per client
- Multi-currency transfers with FX quotes
- Consumer example (notification / fraud scoring service) reading `ledger.transfer-events`
- Prometheus metrics and Grafana dashboard

## Tech stack

Java 17 · Spring Boot 3.4 · Spring JDBC (`JdbcClient`) · PostgreSQL 16 · Flyway · Apache Kafka ·
Testcontainers · springdoc-openapi · Docker Compose · GitHub Actions · k6
