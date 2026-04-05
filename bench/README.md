# Parquet Compression Benchmarks

Supporting benchmarks for the blog post's "what does the 10-year cold tier
actually cost" section.

All scripts generate synthetic data matching TigerBeetle's exact 128-byte
`Transfer` schema:

- 16 fields (ID, debit/credit accounts, amount, timestamp, ledger, code, flags…)
- `Uint128` represented as `uint64 _hi/_lo` pairs
- Realistic distributions: ULID-ish time-ordered IDs, zipf-skewed account IDs
  from a 400M pool, log-normal amounts, monotonic timestamps at 12K TPS

## Running

```shell
# per-row compression at 4 scales (8,190 / 81,900 / 819K / 8.19M — TB batch multiples)
uv run bench/parquet_scales.py

# full codec matrix at 819K rows (100 MB raw)
uv run bench/parquet_codecs.py

# row group size effect (batching unit inside Parquet)
uv run bench/parquet_rowgroup.py
```

## Headline result

| strategy                          | bytes/row | ratio |
|-----------------------------------|-----------|-------|
| zstd(3) plain                     | 28.03     | 4.57× |
| **zstd(3) + dict** (recommended)  | **27.27** | 4.69× |
| zstd(3) + dict + BYTE_STREAM_SPLIT| 25.70     | 4.98× |
| brotli(11) + dict + BSS (max)     | 24.96     | 5.13× |

- **zstd(3) does ~97% of the compression** by itself
- **Dictionary** adds ~2% on low-cardinality columns
  (`ledger`, `flags`, `code`, `timeout`, zero-filled halves)
- **DELTA_BINARY_PACKED** contributes **<1%** after zstd — noise
- **BYTE_STREAM_SPLIT** on random 64-bit columns (amount, IDs) adds ~5%
- Ratio **saturates at 81K rows** (10× a TigerBeetle batch)

## Scale projection

At 1B transfers/day × 3,650 days (10-year regulatory retention):

| compression           | cold-tier size | S3 standard cost/month |
|-----------------------|----------------|------------------------|
| none                  | 467.2 TB       | $10,745                |
| snappy                | 152.6 TB       | $3,510                 |
| zstd(3) + dict        | **99.6 TB**    | **$2,290**             |
| zstd(3) + dict + BSS  | 93.8 TB        | $2,158                 |

## Dependencies

Scripts use inline uv metadata — `uv run` installs pyarrow + numpy
automatically on first run.
