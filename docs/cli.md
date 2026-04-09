# CLI Reference

The CLI connects to a broker with `--server IP:port`. If omitted, it defaults to `127.0.0.1:4222`.
You can place `--server` and `--zkey-seed` before the command name as global flags.

If the broker requires zkey auth, pass `--zkey-seed path/to/seed.b64` so the client can sign the server nonce.
If the broker does not require auth, `--zkey-seed` is optional and `CONNECT` is not needed before `PUB` or `SUB`.

## Commands

- [pub](commands/pub.md)
- [sub](commands/sub.md)
- [unsub](commands/unsub.md)
- [request](commands/request.md)
- [reply](commands/reply.md)
- [ping](commands/ping.md)
- [bench](commands/bench.md)

