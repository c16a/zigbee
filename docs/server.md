# Server Reference

Run the broker with:

```sh
zig build run -- --config zigbee.config.json
```

If `zigbee.config.json` is absent, the server listens on `0.0.0.0:4222`.
Verbose acknowledgements are enabled by default, so successful client commands
receive an `OK` frame unless you explicitly disable verbose mode in config.

Connection rule:

- If `auth` is not configured, clients may publish and subscribe immediately after TCP connect.
- If `auth.mode = "static_zkey"` is configured, clients must first send a valid `CONNECT` with `--zkey-seed`-backed auth material.
- Raw telnet-style sessions work directly against the same protocol; when auth is enabled, telnet users type `CONNECT` manually, while `zigbee-cli session` sends it automatically.

Shutdown:

- The server may send a `CLOSE` frame before closing the socket on orderly disconnect paths.

Protocol acknowledgements:

- When verbose mode is enabled, the server sends `OK` after successful commands such as `CONNECT`, `SUB`, `UNSUB`, `PUB`, `PING`, and `PONG`.
- Client tools and telnet sessions should treat `OK` as an acknowledgement, not a separate command.

Example configuration:

```json
{
  "listen_address": "0.0.0.0:4222",
  "auth": {
    "mode": "static_zkey",
    "users": [
      {
        "name": "service-a",
        "public_key": "BASE64_PUBLIC_KEY",
        "allow_publish": ["events.>", "rpc.requests"],
        "allow_subscribe": ["rpc.replies", "updates.>"]
      }
    ]
  }
}
```
