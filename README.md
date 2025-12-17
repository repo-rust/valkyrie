# Valkyrie Key-Value Store

<img src="assets/logo-horizontal.png" alt="Project Logo" width="700"/>

Valkyrie is a lightweight, high‑performance key‑value store that speaks the Redis Serialization Protocol (RESP). It aims to be simple to run locally, easy to integrate with existing Redis clients, and a clean, idiomatic Rust codebase for learning and experimentation.
Valkyrie is a minimal Redis‑compatible server built in Rust. It accepts connections over TCP and understands a subset of Redis commands over RESP. The storage engine is in‑memory and sharded for scalability on multi‑core systems.

Key properties:
- Redis‑compatible wire protocol (RESP)
- In‑memory, sharded key‑value and list engine
- Async I/O with Tokio
- Structured logging with tracing

Docs
- Protocol: Redis RESP specification
- Tokio runtime
- Clap for CLI parsing
- Tracing for logs

Getting Started
Quickstart (local)
1) Build and run:
```
cargo run --release -- \
  --address=127.0.0.1:6379 --tcp-handlers=4 --shards=5
```

Alternatively, use the helper script (bash):
- On Unix-like systems (Linux/macOS), or on Windows via Git Bash/WSL:
```
scripts/run.sh
```

2) Connect with redis-cli:
```
redis-cli -p 6379 ping
redis-cli -p 6379 set foo bar
redis-cli -p 6379 get foo
```

Additional list command examples:
```
# Push to a list
redis-cli lpush mylist a
redis-cli rpush mylist b c

# Pop from left
redis-cli lpop mylist

# Length and range
redis-cli llen mylist
redis-cli lrange mylist 0 -1

# Blocking pop (timeout in seconds; 0 = block indefinitely)
redis-cli blpop mylist 5
redis-cli blpop list1 list2 5
```

Build from Source

Build:
```
cargo build --release
```

Formatting and linting:
```
cargo fmt
cargo clippy
```

Usage (CLI)
The server binary is named valkyrie (valkyrie.exe on Windows).

Flags:
- --address=<ip:port>
  - Socket address to bind. Default: 127.0.0.1:6379
- --tcp-handlers=<usize>
  - Number of TCP handler threads. Default: usize::MAX (clamped at runtime)
- --shards=<usize>
  - Number of storage shards. Default: usize::MAX (clamped at runtime)

Runtime clamping:
At startup, Valkyrie detects available_parallelism (CPUs). It computes half = max(1, CPUs/2) and clamps both --tcp-handlers and --shards to min(user_value, half).
- If you omit the flags, both values become half by default.
- If you pass a value higher than half, it will be clamped down to half.

Examples:
- Use defaults (auto half):
```
./target/release/valkyrie
```
- Explicit values:
```
./target/release/valkyrie --address=0.0.0.0:6379 --tcp-handlers=4 --shards=5
```

Protocol and Supported Commands
Valkyrie speaks the Redis wire protocol (RESP). The following commands are implemented:

- PING [message]
  - Examples:
    - `redis-cli ping` → PONG
    - `redis-cli ping hello` → hello
- ECHO message
  - Example: `redis-cli echo "hi"` → hi
- SET key value
  - Example: `redis-cli set foo bar` → OK
- GET key
  - Example: `redis-cli get foo` → bar
- LPUSH key value [value ...]
  - Push one or more values to the head (left) of the list
- RPUSH key value [value ...]
  - Push one or more values to the tail (right) of the list
- LPOP key
  - Pop and return the first element of the list
- LLEN key
  - Return the length of the list
- LRANGE key start stop
  - Return a range of elements (e.g., `redis-cli lrange mylist 0 -1`)
- BLPOP key [key ...] timeout
  - Block until an element is available to pop from the left side of any of the given lists.
  - `timeout` is in seconds; `0` means block indefinitely.
  - On timeout, a nil value is returned.
- COMMAND
  - Returns a minimal command metadata placeholder (compatibility)

For details on [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/), see the official Redis protocol spec.

Client Drivers
Because Valkyrie implements the Redis protocol, you can use most Redis client libraries:
- Go: go-redis, redigo
- Rust: redis-rs
- Python: redis-py
- Java: Jedis, Lettuce
- Node.js: ioredis, node-redis
- .NET: StackExchange.Redis

Testing and Tooling
Unit/integration tests:
```
cargo test
```

If you use cargo-nextest:
```
cargo nextest run
```

Contributing
Contributions are welcome! Suggested steps:
1) Fork and clone the repo
2) Create a feature branch
3) Implement changes with tests
4) Run fmt/clippy/tests
5) Open a pull request describing the change

Good first areas:
- Additional Redis command support
- Protocol robustness and error handling
- Observability (structured logs, metrics)
- Performance tuning of the storage engine

Design
High-level architecture:
- Network I/O: Tokio-based TCP server (src/network)
- Request handling: Per-connection async handlers (src/network/connection_handler.rs)
- Protocol: RESP parser/encoder (src/protocol/redis_serialization_protocol.rs)
- Commands: Minimal Redis command set (src/command)
- Storage: In‑memory, sharded engine (src/storage.rs)

Sharding and parallelism:
- Shards and TCP handlers are capped to ~50% of CPU cores by default to reduce contention and leave system headroom.
- Thread affinity support is available via the `affinity` crate (Linux only).

Logging:
- Uses tracing and tracing-subscriber with env-filter. Set RUST_LOG for fine‑grained control.
  - Example: `RUST_LOG=info ./target/release/valkyrie`

Licensing
This project is licensed under the Apache License, Version 2.0. See the LICENSE file for details.

Comparison with Other Databases
- Redis: Valkyrie is protocol‑compatible for a small subset of commands but is not a drop‑in feature replacement. Redis is production‑hardened with a rich command set and ecosystem.
- Embedded KV stores: Valkyrie is a networked server, not an embedded library.
- SQL databases: Valkyrie is a simple KV store and does not provide SQL, transactions, or durability.
