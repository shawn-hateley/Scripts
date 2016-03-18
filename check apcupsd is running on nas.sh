#!/bin/bash
ps xa | egrep apc[u] > /dev/null
if [ $? -eq 0 ]; then
  echo "Process is running."
else
  echo "Process is not running. Starting..."
  apcupsd
  sleep 5
  ~/startApc.sh
fi