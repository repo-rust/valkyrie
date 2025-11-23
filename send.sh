#!/usr/bin/env bash

echo "Sending few RedisTypes"

# Send PING as SimpleString
exec 3<>/dev/tcp/127.0.0.1/8080
    printf "+PING\r\n">&3
exec 3>&-

# Send BulkString
exec 3<>/dev/tcp/127.0.0.1/8080
    printf "\$5\r\nHello\r\n">&3
exec 3>&-

done
