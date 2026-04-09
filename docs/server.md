# Server Reference

Run the broker with:

```sh
zig build run -- --config zigbee.config.json
```

If `zigbee.config.json` is absent, the server listens on `0.0.0.0:4222`.

Connection rule:

- If `auth` is not configured, clients may publish and subscribe immediately after TCP connect.
- If `auth.mode = "static_zkey"` is configured, clients must first send a valid `CONNECT` with `--zkey-seed`-backed auth material.

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

