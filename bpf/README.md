# eBPF scripts for observing TigerBeetle vs PostgreSQL

These `bpftrace` scripts were used in the write-up at
<https://backend.how/posts/1b-payments-per-day/>.

**They need a real Linux kernel.** On macOS, run them inside the Podman
machine VM (`podman machine ssh`).

## `io_uring.bt` — submission→completion latency for TigerBeetle

Histograms the per-I/O round-trip latency in microseconds, system-wide.
TigerBeetle is usually the only process doing io_uring on a bench host.

```sh
sudo bpftrace io_uring.bt
```

Output:
```
@completions: 45,819
@submits:     45,945
@latency_us:
[64, 128)     7,785 |@@@@@@@@@@@@@@@@@@@@@@@                             |
[128, 256)   17,467 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@|
[256, 512)   16,158 |@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@    |
```

## `fsync_count.d` — count fsync/fdatasync by fd

Pass the comm name as a positional arg. Records a count per file
descriptor, so you can see which files (usually WAL segments for
postgres) are getting hit.

```sh
sudo bpftrace --unsafe fsync_count.d postgres
```

## `fsync_lat.d` — linear histogram of fsync latency

Pass the comm name. Linear bucket histogram (0–2000 μs, 100 μs wide
buckets) + a final 2000+ bucket. Useful for spotting tail latency.

```sh
sudo bpftrace fsync_lat.d postgres
```

## `raw_sys.d` — count every syscall made by a PID

Pass the target PID. Dumps a map of `syscall_nr → count` at Ctrl-C.

```sh
TB_PID=$(pgrep tigerbeetle)
sudo bpftrace --unsafe raw_sys.d $TB_PID
```

Look up the syscall numbers for your arch:
- `cat /usr/include/asm-generic/unistd.h | grep __NR_`
- or `ausyscall --dump`

On aarch64 Linux you'll typically see:
- `64` = write
- `206` = sendto
- `426` = io_uring_enter
- `74` = fsync
- `83` = fdatasync
