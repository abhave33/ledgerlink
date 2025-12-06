# LedgerLink — Distributed Transaction Engine

LedgerLink is a small distributed transaction engine inspired by real-world payment systems (like card networks and BNPL platforms).

It shows how to build a fault-tolerant backend that supports:

- **Authorization → Capture → Settlement** flows
- **Idempotent Kafka consumers** for high-throughput processing
- An **ACID-compliant PostgreSQL ledger** with event-sourced audit logs

This project backs up the resume line:

> **LedgerLink — Distributed Transaction Engine | Go, PostgreSQL, gRPC, Docker, Kafka**  
> – Built a fault-tolerant transaction processing engine implementing authorization, clearing, and settlement flows.  
> – Developed idempotent Kafka consumers processing high-throughput streams with strict consistency guarantees.  
> – Designed an ACID-compliant Postgres ledger with event-sourced audit logs (`ledger_events`), reducing write conflicts via row-level locking and projections.

---

## Architecture

**High-level components:**

- **Go gRPC service** (`go/cmd/server/main.go`)
  - Exposes RPCs:
    - `Authorize` — place a hold on funds
    - `Capture` — move held funds out of the account
    - `Settle` — record final settlement
    - `GetAccount` — view balance and held funds
  - Wraps each operation in a Postgres transaction to keep state consistent.
- **PostgreSQL ledger**
  - `accounts` table: projected balances and held funds.
  - `ledger_events` table: event-sourced audit log of every state change.
  - `processed_messages` table: tracks processed Kafka messages for idempotency.
- **Kafka consumer (optional but implemented)**
  - Reads JSON messages from a `ledger-transactions` topic.
  - Uses `processed_messages` to skip duplicates and achieve exactly-once style behavior.
- **Python gRPC client** (`python/client.py`)
  - Simple script that calls `Authorize → Capture → Settle` and prints the final account state.

Data model (simplified):

- **Accounts**
  - `balance_cents` — total balance.
  - `held_cents` — funds reserved by authorizations.
- **Events**
  - Each auth/capture/settlement inserts a row in `ledger_events`.
  - This provides a full audit trail and allows event-sourced reconstruction later if needed.

---

## Tech Stack

- **Backend:** Go, gRPC, Protocol Buffers
- **Database:** PostgreSQL
- **Messaging:** Kafka (for async processing and idempotent consumers)
- **Infra:** Docker (for local Postgres + Kafka)
- **Client / Demo:** Python gRPC client

---

## Getting Started

### Prerequisites

- Go (1.22+ recommended)
- Docker
- Python 3.10+ (for the demo client)
- `protoc` + gRPC plugins (used once during development to generate code)

> You *don’t* need Kafka running to use the core gRPC API.  
> Kafka is optional and only needed if you want to demo the async consumer.

---

## 1. Clone the repository

```bash
git clone https://github.com/abhave33/ledgerlink.git
cd ledgerlink
