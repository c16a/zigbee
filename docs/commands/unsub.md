# `unsub`

## Usage

```sh
zigbee-cli [--server IP:port] unsub [--sid n] [--max n]
```

## Behavior

- Sends an `UNSUB` control frame for the given subscription id.
- If `--max` is set, the unsubscribe is delayed until the given number of messages has been delivered.

## Example

```sh
zigbee-cli --server 127.0.0.1:4222 unsub --sid 1
```
