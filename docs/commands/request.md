# `request`

## Usage

```sh
zigbee-cli [--server IP:port] request <subject> [payload...]
```

## Behavior

- Creates a temporary inbox.
- Subscribes to the inbox.
- Publishes a request to `<subject>` with the inbox as reply subject.
- Waits for a single reply and prints it.

## Example

```sh
zigbee-cli --server 127.0.0.1:4222 request foo "who is there?"
```
