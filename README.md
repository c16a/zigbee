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

## Documentation

- [Docs index](docs/index.md)
- [Server reference](docs/server.md)
- [CLI reference](docs/cli.md)
