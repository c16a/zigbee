# `pub`

Usage:

```sh
zigbee-cli [--server IP:port] pub [--reply subject] <subject> [payload...]
```

Behavior:

- Publishes one message to `<subject>`.
- If `--reply` is present, the message is sent with a reply subject.
- If no payload words are provided, the payload is empty.

Notes:

- With auth disabled, the CLI can publish without a `CONNECT` step.
- With static zkey auth enabled, the CLI signs the server nonce before publishing.

Example:

```sh
zigbee-cli --server 127.0.0.1:4222 pub foo "hello world"
```
