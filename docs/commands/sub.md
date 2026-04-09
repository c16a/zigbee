# `sub`

Usage:

```sh
zigbee-cli [--server IP:port] sub [--queue name] [--sid n] [--count n] <subject>
```

Behavior:

- Subscribes to `<subject>`.
- Prints each incoming message.
- Stops after `--count` messages if the count is set.

Notes:

- Queue subscriptions use `--queue name`.
- If auth is disabled, the subscription can be sent immediately after connect.

Example:

```sh
zigbee-cli --server 127.0.0.1:4222 sub foo
```
