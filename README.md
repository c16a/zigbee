# zigbee

`zigbee` is a small Zig message broker with two binaries:

| Binary | Purpose |
| --- | --- |
| `zigbee` | Runs the broker server. |
| `zigbee-cli` | Talks to the broker and runs client-side benchmarks. |

## License

MIT. See [`LICENSE`](LICENSE).

## Requirements

- Zig 0.15.2

## Build

```sh
zig build
```

This installs both binaries into `zig-out/bin/`.

## Server

Run the broker with:

```sh
zig build run -- --config zigbee.config.json
```

If `zigbee.config.json` is absent, the server listens on `0.0.0.0:4222`.

Example configuration:

```json
{
  "listen_address": "0.0.0.0:4222"
}
```

## CLI

The CLI connects to a broker with `--server IP:port`. If omitted, it defaults to `127.0.0.1:4222`.
You can place `--server` before the command name as a global flag.

### Commands

| Command | Usage |
| --- | --- |
| `pub` | `zigbee-cli [--server IP:port] pub [--reply subject] <subject> [payload...]` |
| `sub` | `zigbee-cli [--server IP:port] sub [--queue name] [--sid n] [--count n] <subject>` |
| `unsub` | `zigbee-cli [--server IP:port] unsub [--sid n] [--max n]` |
| `request` | `zigbee-cli [--server IP:port] request <subject> [payload...]` |
| `reply` | `zigbee-cli [--server IP:port] reply [--queue name] [--sid n] [--count n] <subject> [payload...]` |
| `ping` | `zigbee-cli [--server IP:port] ping [--count n]` |
| `bench` | `zigbee-cli bench <pub|sub|request|reply|latency> ...` |

### Command Notes

- `pub` publishes a single message.
- `sub` prints incoming messages until `--count` is reached.
- `unsub` sends an unsubscribe control frame for a subscription id.
- `request` publishes a message with a temporary reply inbox and waits for one reply.
- `reply` subscribes to a subject and replies to each incoming request with the provided payload.
- `ping` sends PING/PONG round trips.

## Benchmarking

Benchmarking lives under `zigbee-cli bench`.

| Benchmark | Usage |
| --- | --- |
| `pub` | `zigbee-cli bench pub [flags] <subject>` |
| `sub` | `zigbee-cli bench sub [flags] <subject>` |
| `request` | `zigbee-cli bench request [flags] <subject>` |
| `reply` | `zigbee-cli bench reply [flags] <subject>` |
| `latency` | `zigbee-cli bench latency [flags]` |

### Common Benchmark Flags

| Flag | Meaning |
| --- | --- |
| `--server IP:port` | Broker address. |
| `--clients n` | Number of logical benchmark clients. |
| `--msgs n` | Message count. |
| `--size n` | Payload size in bytes. |
| `--sleep <duration>` | Pause between operations, e.g. `10ms`, `1s`, `250us`. |
| `--no-progress` | Suppress progress chatter. |
| `--multi-subject` | Spread publishes across multiple subjects. |
| `--multi-subject-max n` | Maximum suffix used for `--multi-subject`. |
| `--queue name` | Queue group name for `bench reply`. |

### Benchmark Modes

- `bench pub` publishes as fast as possible.
- `bench sub` receives messages and measures subscription throughput.
- `bench request` sends request/reply traffic and waits for responses.
- `bench reply` runs a queue-backed responder that answers requests.
- `bench latency` runs PING/PONG round trips.

## Examples

```sh
zigbee-cli --server 127.0.0.1:4222 ping
zigbee-cli --server 127.0.0.1:4222 pub foo "hello world"
zigbee-cli --server 127.0.0.1:4222 sub foo
zigbee-cli --server 127.0.0.1:4222 request foo "who is there?"
zigbee-cli --server 127.0.0.1:4222 reply foo "it works"
zigbee-cli --server 127.0.0.1:4222 bench pub foo --clients 4 --msgs 1000000 --size 128
zigbee-cli --server 127.0.0.1:4222 bench sub foo --clients 4 --msgs 1000000
zigbee-cli --server 127.0.0.1:4222 bench request foo --clients 4 --msgs 100000
zigbee-cli --server 127.0.0.1:4222 bench reply foo --clients 4 --msgs 100000 --queue bench
zigbee-cli --server 127.0.0.1:4222 bench latency --msgs 100000
```
