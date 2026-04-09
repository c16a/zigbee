# `ping`

## Usage

```sh
zigbee-cli [--server IP:port] ping [--count n]
```

## Behavior

- Sends `PING` frames and waits for `PONG`.
- Useful as a connectivity check.

## Example

```sh
zigbee-cli --server 127.0.0.1:4222 ping
```
