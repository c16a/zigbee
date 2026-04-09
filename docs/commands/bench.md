# `bench`

Usage:

```sh
zigbee-cli bench <pub|sub|request|reply|latency> ...
```

Subcommands:

- [bench pub](../examples/bench-pub.md)
- [bench sub](../examples/bench-sub.md)
- [bench request](../examples/bench-request.md)
- [bench reply](../examples/bench-reply.md)
- [bench latency](../examples/bench-latency.md)

Common flags:

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

Examples:

```sh
zigbee-cli --server 127.0.0.1:4222 bench pub foo --clients 4 --msgs 1000000 --size 128
zigbee-cli --server 127.0.0.1:4222 bench sub foo --clients 4 --msgs 1000000
zigbee-cli --server 127.0.0.1:4222 bench request foo --clients 4 --msgs 100000
zigbee-cli --server 127.0.0.1:4222 bench reply foo --clients 4 --msgs 100000 --queue bench
zigbee-cli --server 127.0.0.1:4222 bench latency --msgs 100000
```
