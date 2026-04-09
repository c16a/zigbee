# `reply`

Usage:

```sh
zigbee-cli [--server IP:port] reply [--queue name] [--sid n] [--count n] <subject> [payload...]
```

Behavior:

- Subscribes to `<subject>`.
- For each incoming request, sends the provided payload back to the request's reply subject.
- With `--queue`, acts as a queue worker.

Example:

```sh
zigbee-cli --server 127.0.0.1:4222 reply foo "it works"
```
