#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["pyarrow>=15.0.0", "numpy"]
# ///
"""
Full codec + encoding matrix on 819K rows (100× TB batch = 100 MB raw).

Parquet 2.0 encodings explored:
  PLAIN                    - baseline fixed-width
  RLE_DICTIONARY           - dict with RLE-packed indices (use_dictionary=True)
  DELTA_BINARY_PACKED      - differences, packed with bit-width
  BYTE_STREAM_SPLIT        - byte-interleave for column-store compression synergy

Codecs tested at best-case (dict + delta):
  NONE, SNAPPY, LZ4, ZSTD{1,3,9,22}, GZIP{6,9}, BROTLI{4,8,11}
"""
import numpy as np, pyarrow as pa, pyarrow.parquet as pq, os, time

N = 819_000  # 100× TB batch
N_ACCOUNTS = 400_000_000
rng = np.random.default_rng(42)
z = np.zeros(N, dtype=np.uint64)
z32 = np.zeros(N, dtype=np.uint32)

print(f"generating {N:,} rows ({N*128/1e6:.1f} MB raw)...")
table = pa.table({
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

# Also test BYTE_STREAM_SPLIT on random u64 columns (interleaves bytes,
# often helps zstd find patterns across rows in columnar layouts)
BSS_COLS = {"id_lo": "BYTE_STREAM_SPLIT", "ud64": "BYTE_STREAM_SPLIT",
            "amount_lo": "BYTE_STREAM_SPLIT", "debit_lo": "BYTE_STREAM_SPLIT",
            "credit_lo": "BYTE_STREAM_SPLIT"}

def bench(codec, level, scheme):
    path = f"/tmp/c_{codec}_{level}_{scheme}.parquet"
    kw = dict(compression=codec, data_page_version="2.0", write_statistics=False)
    if level is not None:
        kw["compression_level"] = level

    if scheme == "dict+delta":
        kw["use_dictionary"] = DICT_COLS
        kw["column_encoding"] = DELTA_COLS
    elif scheme == "dict+delta+bss":
        kw["use_dictionary"] = DICT_COLS
        kw["column_encoding"] = {**DELTA_COLS, **BSS_COLS}
    elif scheme == "bss_only":
        kw["use_dictionary"] = DICT_COLS
        kw["column_encoding"] = BSS_COLS
    elif scheme == "plain":
        kw["use_dictionary"] = False

    t0 = time.time()
    try:
        pq.write_table(table, path, **kw)
        dt = time.time() - t0
        sz = os.path.getsize(path)
        # decode time
        t0 = time.time()
        _ = pq.read_table(path)
        rt = time.time() - t0
        return sz, dt, rt, None
    except Exception as e:
        return None, None, None, str(e)[:40]

raw = N * 128
print(f"raw: {raw/1e6:.1f} MB ({raw/N:.0f} B/row)\n")
print(f"{'codec':<14} {'scheme':<18} {'size':>9} {'B/row':>7} {'ratio':>7} {'write':>7} {'read':>7}")
print("-" * 78)

configs = [
    # baseline — no compression, just encoding
    ("none",    None, "plain"),
    ("none",    None, "dict+delta"),
    # fast codecs
    ("snappy",  None, "plain"),
    ("snappy",  None, "dict+delta"),
    ("lz4",     None, "plain"),
    ("lz4",     None, "dict+delta"),
    # zstd sweep
    ("zstd",    1,    "dict+delta"),
    ("zstd",    3,    "dict+delta"),
    ("zstd",    9,    "dict+delta"),
    ("zstd",    22,   "dict+delta"),
    # gzip sweep
    ("gzip",    6,    "dict+delta"),
    ("gzip",    9,    "dict+delta"),
    # brotli sweep
    ("brotli",  4,    "dict+delta"),
    ("brotli",  8,    "dict+delta"),
    ("brotli",  11,   "dict+delta"),
    # exotic: byte-stream-split for random u64s
    ("zstd",    3,    "bss_only"),
    ("zstd",    3,    "dict+delta+bss"),
    ("brotli",  11,   "dict+delta+bss"),
]

best = (None, 10**18)
for codec, lvl, scheme in configs:
    sz, wt, rt, err = bench(codec, lvl, scheme)
    if err:
        print(f"{codec:<14} {scheme:<18} ERROR: {err}")
        continue
    codec_str = f"{codec}({lvl})" if lvl else codec
    ratio = raw / sz
    star = ""
    if sz < best[1] and codec != "none":
        best = ((codec_str, scheme, sz, wt, rt), sz)
        star = " *"
    print(f"{codec_str:<14} {scheme:<18} {sz/1e6:>6.2f} MB {sz/N:>6.2f} {ratio:>6.2f}× {wt:>6.2f}s {rt:>6.2f}s{star}")

(codec, scheme, bsz, bwt, brt), _ = best
print(f"\nbest: {codec} + {scheme} → {bsz/1e6:.2f} MB ({raw/bsz:.2f}×) write={bwt:.2f}s read={brt:.2f}s")

# Projection
days = 365 * 10
raw_tb = 1e9 * 128 * days / 1e12
print(f"\n10yr projection (1B/day): {raw_tb:.0f} TB raw → {raw_tb*bsz/raw:.1f} TB compressed")
