# Valkyrie Key-Value Storage

Valkirye is a key-value storage similar to Redis. Uses [Redis serialization protocol specification](https://redis.io/docs/latest/develop/reference/protocol-spec/) as 
a lightway text-based protocol.

Written in Rust.


## Build, format 7 static analyzer

```bash
cargo build
```

```bash
cargo fmt
```

```bash
cargo clippy
```

## Run unit-tests

```bash
cargo nextest run
```