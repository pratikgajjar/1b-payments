tracepoint:syscalls:sys_enter_fsync,tracepoint:syscalls:sys_enter_fdatasync
/ comm == str($1) /
{
  @start[tid] = nsecs;
}

tracepoint:syscalls:sys_exit_fsync,tracepoint:syscalls:sys_exit_fdatasync
/ comm == str($1) /
{
  $lat = (nsecs - @start[tid]) / 1000;
  @latency_us = lhist($lat, 0, 2000, 100);
  delete(@start[tid]);
}
