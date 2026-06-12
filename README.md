# wiretap

A lightweight Rust event indexer for [Sui](https://sui.io), built on the new
gRPC streaming API. Replaces deprecated JSON-RPC polling
(`suix_queryTransactionBlocks`, `suix_queryEvents`) — both deactivated July 2026
— with a checkpoint subscription that streams ordered, gapless data and
backfills automatically on reconnect.

Designed for solo devs who want filtered chain data without standing up
`sui-indexer-alt-framework`, Postgres, or Docker.

```bash
wiretap watch \
  --endpoint https://fullnode.testnet.sui.io:443 \
  --event 0xPACKAGE::pool::SwapEvent \
  --sink sqlite://events.db
```

That's the entire setup. Cursor, gap-fill, filtering, and persistence all
handled.

## Features

- **gRPC streaming** — `SubscribeCheckpoints` from Sui's v2 fullnode API.
  Ordered, gapless delivery; reconnect with automatic gap backfill via
  `LedgerService.GetCheckpoint`.
- **Cursor persistence** — sqlite (default) or file. At-least-once delivery,
  monotonic commit; restart resumes from the last fully processed checkpoint.
- **Compiled filters** — exact and wildcard event types
  (`pkg::module::*`), package id, sender, affected address. Large address
  watchlists go through a bloom filter pre-check.
- **Decodes events locally** — same JSON output on any fullnode, any
  provider. Sui's gRPC `SubscribeCheckpoints` doesn't render the `json`
  field server-side; wiretap fetches the Move datatype layout via
  `MovePackageService.GetDatatype`, LRU-caches it, and decodes the BCS
  payload against the layout. Nested structs, generics, well-known wrappers
  (`0x1::string::String`, `0x2::object::ID`) all handled.
- **Sinks** — sqlite (zero setup), Postgres (feature flag), webhook (batched
  POST with exponential backoff), stdout NDJSON. Custom sinks via the
  `Handler` trait.
- **No system protoc required** — the build uses a vendored `protoc`
  binary (`protoc-bin-vendored`) at compile time.
- **Library-first** — embed `wiretap-core` with your own handler in under
  20 lines (see below).

## Install

Prebuilt binaries (Linux x86_64/aarch64, macOS x86_64/aarch64, Windows
x86_64) ship on tagged releases:

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/Iwetan77/wiretap/releases/latest/download/wiretap-sui-installer.sh | sh
```

From crates.io:

```bash
cargo install wiretap-sui
```

From source:

```bash
git clone https://github.com/Iwetan77/wiretap
cd wiretap && cargo install --path crates/wiretap-cli
wiretap --help
```

## Quick start

```bash
wiretap init                              # writes wiretap.toml template
$EDITOR wiretap.toml                      # set endpoint, events, sink
wiretap watch                             # run
wiretap backfill --from 1000 --to 2000    # replay historical range
```

## Embedding (under 20 lines)

```rust
use async_trait::async_trait;
use wiretap_core::{Cursor, Event, FilterSpec, GrpcSource, Handler, Pipeline};

struct Print;

#[async_trait]
impl Handler for Print {
    async fn on_event(&self, e: Event) -> anyhow::Result<()> {
        println!("cp {} {} from {}", e.checkpoint, e.event_type, e.sender);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let source = GrpcSource::connect("https://fullnode.testnet.sui.io:443").await?;
    let cursor = Cursor::open_sqlite("wiretap.db")?;
    let filter = FilterSpec::default().with_event("0xPACKAGE::pool::SwapEvent");
    Pipeline::new(source, cursor, filter).run(Print).await
}
```

## Migrating from `suix_queryTransactionBlocks` / `suix_queryEvents`

Deactivation: **July 2026.** wiretap is a drop-in for the most common
filter shapes. Translate your existing JSON-RPC queries to `wiretap.toml`:

| JSON-RPC filter (deprecated) | wiretap.toml equivalent |
| --- | --- |
| `{ "MoveEventType": "0xPKG::pool::SwapEvent" }` | `events = ["0xPKG::pool::SwapEvent"]` |
| `{ "MoveEventModule": { "package": "0xPKG", "module": "pool" } }` | `events = ["0xPKG::pool::*"]` |
| `{ "Package": "0xPKG" }` | `packages = ["0xPKG"]` |
| `{ "Sender": "0xADDR" }` | `senders = ["0xADDR"]` |
| `{ "FromAddress": "0xADDR" }` | `senders = ["0xADDR"]` |
| `{ "ToAddress": "0xADDR" }` | `affected = ["0xADDR"]` |
| `{ "InputObject": "0xOBJ" }` | `affected = ["0xOBJ"]` *(addresses touched by the transaction; v2 surfaces these via balance changes + sender)* |
| `{ "ChangedObject": "0xOBJ" }` | `affected = ["0xOBJ"]` |
| `{ "All": [F1, F2] }` (AND) | combine keys inside a single `[[watch]]` block |
| `{ "Any": [F1, F2] }` (OR) | use multiple `[[watch]]` blocks |
| `query_transaction_blocks` pagination | not needed — wiretap streams; for history use `wiretap backfill --from N --to M` |
| polling on a `cursor` reply field | not needed — cursor persisted automatically |
| `suix_queryEvents` event-type filter | identical: `events = [...]` |
| `EventType` wildcard inside a module | `events = ["0xPKG::module::*"]` |

### Behavioral differences worth knowing

- **No polling.** wiretap holds a long-lived gRPC stream; you no longer
  manage a `cursor` reply field or backoff loop.
- **Ordered + gapless.** Checkpoints arrive in strict order. On reconnect,
  the gap between your last cursor and the new stream head is fetched via
  `LedgerService.GetCheckpoint` before normal streaming resumes.
- **At-least-once.** The cursor commits *after* the handler returns. Make
  your handler idempotent (the default sqlite sink uses
  `INSERT OR IGNORE` on `(tx_digest, event_seq)`; Postgres uses
  `ON CONFLICT DO NOTHING`).
- **Page size doesn't exist.** Filter at config time, not at query time.

## Config (`wiretap.toml`)

```toml
[source]
endpoint = "https://fullnode.testnet.sui.io:443"
start_checkpoint = "latest"  # or a number

[[watch]]
events = ["0xPACKAGE::pool::SwapEvent", "0xPACKAGE::pool::*"]
# packages = ["0xPACKAGE"]
# senders  = ["0xYOURADDR"]
# affected = ["0xYOURADDR"]

[sink]
type = "sqlite"
path = "events.db"

[cursor]
path = "wiretap.db"
```

Filter dimensions AND together within a `[[watch]]` block; multiple blocks
OR together.

### Sinks

| `type` | params | notes |
| --- | --- | --- |
| `sqlite` | `path` | default, zero setup |
| `postgres` | `url` | requires `--features postgres` |
| `webhook` | `url`, `batch_size` (default 50) | POST `{events:[...]}`, exp. backoff |
| `stdout` | — | NDJSON, pipe into `jq` |

## Architecture

Three crates:

- **`wiretap-core`** — `Source` trait, `GrpcSource`, `Cursor`,
  `FilterEngine`, `LayoutResolver`, `Pipeline`, `Handler` trait.
- **`wiretap-sinks`** — built-in sinks; opt-in via Cargo features.
- **`wiretap-sui`** — the CLI crate. Installs as the `wiretap` binary
  (`wiretap init | watch | backfill`).

Hot path:

```
Sui fullnode gRPC
        │  SubscribeCheckpoints
        ▼
   GrpcSource ── on disconnect ──► LedgerService.GetCheckpoint (gap fill)
        │
        ▼ (bounded channel, backpressure-safe)
   FilterEngine  (compiled matchers + bloom for large lists)
        │
        ▼
    Decoder  (server json → local BCS decode via LayoutResolver → raw BCS preserved)
        │
        ▼
   Handler / Sink
        │
        ▼
   Cursor.commit(seq)   ← only after handler returns
```

## Protos

The `crates/wiretap-core/proto/` tree contains the upstream Sui v2 protos
(plus their `google.protobuf` and `google.rpc` deps) vendored from
[`MystenLabs/sui-rust-sdk`](https://github.com/MystenLabs/sui-rust-sdk).
The exact rev — together with the `MystenLabs/sui` commit it was pinned
through — is recorded in [`crates/wiretap-core/proto/REVISION`](crates/wiretap-core/proto/REVISION).

To re-vendor (e.g. when bumping to a newer rev):

```bash
./scripts/vendor-protos.sh         # pins from sui's main by default
SUI_COMMIT=<sha> ./scripts/vendor-protos.sh
```

## Development

```bash
cargo build              # build all crates
cargo test               # unit + integration (uses a mock source, no network)
cargo clippy --workspace -- -D warnings
cargo run -p wiretap-sui -- init
cargo run -p wiretap-sui -- watch --endpoint ... --event ... --sink stdout
```

## License

Apache-2.0. See [LICENSE](LICENSE). Copyright 2026 Kaizen Labs.
