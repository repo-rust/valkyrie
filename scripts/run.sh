#!/usr/bin/env bash
set -euo pipefail

cargo build --release

# Choose executable based on OS (Windows -> .exe, Unix-like -> no extension)
exe="valkyrie"
uname_out="$(uname -s 2>/dev/null || echo unknown)"
case "$uname_out" in
  MINGW*|MSYS*|CYGWIN*)
    exe="valkyrie.exe"
    ;;
  *)
    # In some shells on Windows, uname may not reflect Windows; check OS env as fallback
    if [ "${OS:-}" = "Windows_NT" ]; then
      exe="valkyrie.exe"
    fi
    ;;
esac

"./target/release/$exe" --address=127.0.0.1:6379 --tcp-handlers=4 --shards=5
