tracepoint:raw_syscalls:sys_enter
/ pid == $1 /
{
  @syscalls[args->id] = count();
}

END {
  print(@syscalls);
}
