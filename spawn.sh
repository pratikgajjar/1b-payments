#!/bin/bash

# Start a new tmux session named "transfers"
SESSION="transfers"
tmux new-session -d -s $SESSION

# Create 10 panes and run transfer commands
for i in {0..9}; do
  LAST_TRANSFER_ID=$((i * 1000000))
  CMD="./transfer -totalTransfer 1000000 -lastTransferId $LAST_TRANSFER_ID"

  if [ $i -eq 0 ]; then
    tmux send-keys "$CMD" C-m
  else
    tmux split-window -h "$CMD"
    tmux select-layout tiled
  fi
done

# Attach to the session
tmux attach-session -t $SESSION
