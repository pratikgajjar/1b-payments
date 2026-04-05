#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyarrow>=15.0.0", "numpy"]
# ///
"""
Does row group size (Parquet's batching unit) affect compression?
Test 800K rows ≈ 100 MB raw with row_group_size from 1K → 800K.
"""
import numpy as np, pyarrow as pa, pyarrow.parquet as pq, os, time, sys

N = 800_000  # ~100 MB raw (800K × 128B = 102.4 MB)
N_ACCOUNTS = 400_000_000
rng = np.random.default_rng(42)

print(f"generating {N:,} rows ({N*128/1e6:.1f} MB raw)...")
z = np.zeros(N, dtype=np.uint64)
z32 = np.zeros(N, dtype=np.uint32)
table = pa.table({
    "id_hi": (np.arange(N, dtype=np.uint64) // 100) + 1_735_689_600,
    "id_lo": rng.integers(0, 2**63, N, dtype=np.uint64),
    "debit_hi": z, "debit_lo": (rng.zipf(1.5, N) % N_ACCOUNTS).astype(np.uint64) + 1,
    "credit_hi": z, "credit_lo": (rng.zipf(1.5, N) % N_ACCOUNTS).astype(np.uint64) + 1,
    "amount_hi": z, "amount_lo": np.clip(rng.lognormal(6, 2, N), 1, 1e12).astype(np.uint64),
    "pending_id_hi": z,
    "pending_id_lo": np.where(rng.random(N) < 0.05,
                              rng.integers(1, 2**32, N, dtype=np.uint64), z),
    "ud128_hi": z,
    "ud128_lo": np.where(rng.random(N) < 0.5,
                         rng.integers(0, 2**63, N, dtype=np.uint64), z),
    "ud64": rng.integers(0, 2**48, N, dtype=np.uint64),
    "ud32": rng.integers(0, 30, N, dtype=np.uint32),
    "timeout": z32, "ledger": np.full(N, 1, dtype=np.uint32),
    "code": rng.integers(1, 20, N, dtype=np.uint16),
    "flags": rng.choice([0, 1, 2, 4, 8], size=N).astype(np.uint16),
    "timestamp": (1_735_689_600_000_000_000 +
                  np.cumsum(rng.exponential(83_333, N))).astype(np.uint64),
})

DELTA = {"id_hi": "DELTA_BINARY_PACKED", "timestamp": "DELTA_BINARY_PACKED"}
DICT = ["amount_hi", "pending_id_hi", "pending_id_lo", "ud128_hi", "ud128_lo",
        "ud32", "timeout", "ledger", "code", "flags", "debit_hi", "credit_hi"]

raw = N * 128
print(f"\n{'row_group':>12} {'n_groups':>10} {'size':>10} {'B/row':>8} {'ratio':>8} {'write':>8} {'read':>8}")
print("-" * 72)

sizes = [1_000, 10_000, 100_000, 800_000]
for rg_size in sizes:
    path = f"/tmp/rg_{rg_size}.parquet"
    t0 = time.time()
    pq.write_table(table, path, compression="zstd", compression_level=3,
                   use_dictionary=DICT, column_encoding=DELTA,
                   data_page_version="2.0", write_statistics=False,
                   row_group_size=rg_size)
    write_t = time.time() - t0
    sz = os.path.getsize(path)
    n_groups = (N + rg_size - 1) // rg_size
    t0 = time.time()
    _ = pq.read_table(path)
    read_t = time.time() - t0
    print(f"{rg_size:>12,} {n_groups:>10,} {sz/1e6:>7.1f} MB {sz/N:>7.2f} {raw/sz:>7.2f}× {write_t:>7.2f}s {read_t:>7.2f}s")

print(f"\n(raw: {raw/1e6:.1f} MB, codec: zstd(3) + delta+dict)")
