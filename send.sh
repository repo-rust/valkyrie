#!/usr/bin/env bash

read -r -p "Input test: " input_text


for _ in {1..5}; do
    exec 3<>/dev/tcp/127.0.0.1/8080
        printf "%s" "$input_text" >&3
    exec 3>&-
done
