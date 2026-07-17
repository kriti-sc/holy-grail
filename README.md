# holy-grail

OLTP as a row cache over Iceberg: WAL + memtable on top, Parquet/Iceberg as the durable bottom level of the LSM.

`Iceberg + WAL suffix` is the complete source of truth. Everything else — memtable, WAL prefix, local Parquet cache — is a disposable acceleration layer that can be thrown away and rebuilt.

See [spec.md](spec.md) for what this is and why, and [PLAN.md](PLAN.md) for the build order and current state.

## Running

```sh
docker compose up -d        # MinIO on :9000 (console :9001), Iceberg REST on :8181
cargo test                  # unit tests, hermetic
cargo test -- --ignored     # integration tests, need the containers up
```

## Configuration

Environment variables, all optional — the defaults match `docker-compose.yml`.

| Variable | Default | Meaning |
|---|---|---|
| `HG_S3_ENDPOINT` | `http://localhost:9000` | MinIO S3 API |
| `HG_S3_BUCKET` | `warehouse` | |
| `HG_CATALOG_URI` | `http://localhost:8181` | Iceberg REST catalog |
| `HG_NAMESPACE` / `HG_TABLE` | `holy_grail` / `kv` | |
| `HG_WAL_DIR` | `./data/wal` | |
| `HG_MEMTABLE_MAX_BYTES` | 64 MiB | Flush threshold |
| `HG_MAX_FROZEN` | 2 | Frozen memtables before writes stall |
| `HG_CACHE_BYTES` | 256 MiB | Local Parquet cache |
| `HG_LATENCY_GET_MS` / `_PUT_MS` / `_LIST_MS` / `_DELETE_MS` / `_JITTER_MS` | 0 | Injected object-store latency |

Latency injection is **off by default**, so correctness tests run at full speed and benchmarks have to ask for it. MinIO answers in about a millisecond; every number this prototype produces depends on that not being the case, so the client shapes it into something S3-like (`src/store/latency.rs`).
