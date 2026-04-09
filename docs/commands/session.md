# `session`

Usage:

```sh
zigbee-cli [--server IP:port] session
```

Behavior:

- Opens a line-oriented interactive protocol session.
- Automatically reads the server `INFO` frame and sends `CONNECT`.
- If `--zkey-seed` is provided, the CLI signs the server nonce and sends an authenticated `CONNECT`.
- If `--zkey-seed` is not provided, the CLI sends `CONNECT` with `auth_mode = "none"`.
- Prints server frames as they arrive.
- If the server is running with verbose mode enabled, `OK` frames will appear after successful commands.
- Exits when the server sends `CLOSE` or disconnects.

Notes:

- Each Enter key sends one raw protocol line unchanged.
- This is the CLI equivalent of a telnet session against the same broker.

Example:

```sh
zigbee-cli --server 127.0.0.1:4222 session
```
