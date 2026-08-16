# holy-grail

OLTP as a row cache over **DuckLake**: WAL + memtable on top, Parquet on object storage as the durable bottom level of the LSM, with the table's metadata (snapshots, file list, stats, blooms) living as rows in Postgres.

`DuckLake + WAL suffix` is the complete source of truth. Everything else — memtable, WAL prefix, local Parquet cache — is a disposable acceleration layer that can be thrown away and rebuilt.

The engine is a **read-only** client of the DuckLake catalog. Flushes are published by shelling out to a forked DuckDB binary (the "write the table" library, the analog of what the iceberg crate used to do) — it writes the Parquet data file to object storage and the catalog rows to Postgres in one transaction. holy-grail keeps the orchestration: WAL, freeze, watermark, truncate, recovery. See [spec.md](spec.md) and [PLAN.md](PLAN.md).

## Running

```sh
docker compose up -d        # MinIO on :9000 (console :9001); Postgres catalog
cargo test                  # unit tests, hermetic
cargo test -- --ignored     # integration tests, need the containers + forked duckdb up
```

The integration path needs a forked DuckDB binary built with `httpfs` + `postgres` and the DuckLake catalog-bloom modification (`HG_DUCKDB_BIN`), and the DuckLake table bootstrapped in Postgres (`catalog::bootstrap`).

## Configuration

Environment variables, all optional — the defaults match the local stack.

| Variable | Default | Meaning |
|---|---|---|
| `HG_S3_ENDPOINT` | `http://localhost:9000` | MinIO S3 API |
| `HG_S3_BUCKET` | `warehouse` | |
| `HG_PG_CONN` | `host=127.0.0.1 dbname=holy_grail` | DuckLake catalog (Postgres) |
| `HG_SCHEMA` / `HG_TABLE` | `main` / `kv` | |
| `HG_DATA_PATH` | `s3://warehouse/hg/` | DuckLake DATA_PATH |
| `HG_DUCKDB_BIN` | *(path to forked duckdb)* | Publishes flushes |
| `HG_WAL_DIR` | `./data/wal` | |
| `HG_MEMTABLE_MAX_BYTES` | 64 MiB | Flush threshold |
| `HG_MAX_FROZEN` | 2 | Frozen memtables before writes stall |
| `HG_CACHE_BYTES` | 256 MiB | Local Parquet cache |
| `HG_LATENCY` | `none` | `none` \| `s3` \| `express` |

Latency injection is **off by default**, so correctness tests run at full speed and benchmarks have to ask for it. MinIO answers in about a millisecond; every number this prototype produces depends on that not being the case, so the client shapes it into something S3-like (`src/store/latency.rs`). Because DuckLake keeps **no metadata on object storage** — only data Parquet — the whole read path's object-store traffic goes through that shim, closing the opendal measurement hole the Iceberg version had.
