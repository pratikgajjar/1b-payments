#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyarrow>=15.0.0", "numpy"]
# ///
"""
Test zstd(3) at TigerBeetle batch-size multiples:
  8,190      = 1 batch        ~1 MB
  81,900     = 10 batches    ~10 MB
  819,000    = 100 batches  ~100 MB
  8,190,000  = 1K batches      ~1 GB

Ablation: does DELTA actually help on top of DICT when using zstd(3)?
"""
import numpy as np, pyarrow as pa, pyarrow.parquet as pq, os, time

N_ACCOUNTS = 400_000_000

def gen(N, seed=42):
    rng = np.random.default_rng(seed)
    z = np.zeros(N, dtype=np.uint64)
    z32 = np.zeros(N, dtype=np.uint32)
    return pa.table({
        "id_hi": (np.arange(N, dtype=np.uint64) // 100) + 1_735_689_600,
        "id_lo": rng.integers(0, 2**63, N, dtype=np.uint64),
        "debit_hi": z,
        "debit_lo": (rng.zipf(1.5, N) % N_ACCOUNTS).astype(np.uint64) + 1,
        "credit_hi": z,
        "credit_lo": (rng.zipf(1.5, N) % N_ACCOUNTS).astype(np.uint64) + 1,
        "amount_hi": z,
        "amount_lo": np.clip(rng.lognormal(6, 2, N), 1, 1e12).astype(np.uint64),
        "pending_id_hi": z,
        "pending_id_lo": np.where(rng.random(N) < 0.05,
                                  rng.integers(1, 2**32, N, dtype=np.uint64), z),
        "ud128_hi": z,
        "ud128_lo": np.where(rng.random(N) < 0.5,
                             rng.integers(0, 2**63, N, dtype=np.uint64), z),
        "ud64": rng.integers(0, 2**48, N, dtype=np.uint64),
        "ud32": rng.integers(0, 30, N, dtype=np.uint32),
        "timeout": z32,
        "ledger": np.full(N, 1, dtype=np.uint32),
        "code": rng.integers(1, 20, N, dtype=np.uint16),
        "flags": rng.choice([0, 1, 2, 4, 8], size=N).astype(np.uint16),
        "timestamp": (1_735_689_600_000_000_000 +
                      np.cumsum(rng.exponential(83_333, N))).astype(np.uint64),
    })

DICT_COLS = ["amount_hi", "pending_id_hi", "pending_id_lo", "ud128_hi", "ud128_lo",
             "ud32", "timeout", "ledger", "code", "flags", "debit_hi", "credit_hi"]
DELTA_COLS = {"id_hi": "DELTA_BINARY_PACKED", "timestamp": "DELTA_BINARY_PACKED"}

def write_parquet(table, path, dict_on, delta_on):
    kw = dict(compression="zstd", compression_level=3,
              data_page_version="2.0", write_statistics=False)
    if dict_on and delta_on:
        kw["use_dictionary"] = DICT_COLS
        kw["column_encoding"] = DELTA_COLS
    elif dict_on:
        kw["use_dictionary"] = DICT_COLS
    elif delta_on:
        kw["use_dictionary"] = False
        kw["column_encoding"] = DELTA_COLS
    else:
        kw["use_dictionary"] = False
    pq.write_table(table, path, **kw)
    return os.path.getsize(path)

scales = [
    ("1× batch",    8_190),
    ("10× batch",   81_900),
    ("100× batch",  819_000),
    ("1K× batch",   8_190_000),
]

print(f"all tests: zstd(3) codec, 16-field TB Transfer schema\n")
print(f"{'rows':>12} {'raw_MB':>8}  {'plain':>10} {'dict':>10} {'delta':>10} {'dict+delta':>12}")
print("-" * 78)

for label, N in scales:
    t0 = time.time()
    table = gen(N)
    gen_t = time.time() - t0
    raw_mb = N * 128 / 1e6

    results = {}
    for name, dict_on, delta_on in [("plain", False, False),
                                     ("dict", True, False),
                                     ("delta", False, True),
                                     ("dict+delta", True, True)]:
        p = f"/tmp/s_{N}_{name}.parquet"
        sz = write_parquet(table, p, dict_on, delta_on)
        results[name] = sz / N  # bytes per row

    print(f"{N:>12,} {raw_mb:>7.1f}M  "
          f"{results['plain']:>8.2f} B  {results['dict']:>8.2f} B  "
          f"{results['delta']:>8.2f} B  {results['dict+delta']:>10.2f} B")

print(f"\nColumns: dict on low-card fields (ledger/code/flags/timeout/zero-fields),")
print(f"         delta on monotonic (id_hi, timestamp)")
