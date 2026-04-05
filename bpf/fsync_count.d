tracepoint:syscalls:sys_enter_fsync,tracepoint:syscalls:sys_enter_fdatasync
/ comm == str($1) /
{
  @fsyncs[args->fd] = count();
}
