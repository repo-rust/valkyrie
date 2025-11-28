#!/usr/bin/env bash

address=0.0.0.0/9191

echo "Sending few RedisTypes in a loop of 10 iterations"

send_once() {
  # Send PING as SimpleString
  exec 3<>/dev/tcp/$address
    printf "+PING\r\n" >&3
  exec 3>&-

  # Send BulkString
  exec 3<>/dev/tcp/$address
    printf "\$25\r\nHello wonderfull world!!!\r\n" >&3
  exec 3>&-

  # Send Array with mixed types
  exec 3<>/dev/tcp/$address
    printf "*5\r\n\$5\r\nhello\r\n\$-1\r\n\$5\r\nworld\r\n\$8\r\nvalkyrie\r\n\$11\r\nis the best\r\n" >&3
  exec 3>&-
}

#
# Send 100 times requests for Valkyrie server
#
for i in {1..100}; do
  echo "Iteration $i"
  send_once
done
