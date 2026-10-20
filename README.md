# irp

A TCP reverse proxy written in Iris. It spreads client connections across backends round-robin.

Currently it only works with the following services:
- PostgreSQL
- MySQL
- Redis

## Run

```
iris run main.iris
```

Settings are in `config.json`: the listen port, the routing strategy and the backends.
