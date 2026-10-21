# irp

A TCP reverse proxy written in Iris. It spreads client connections across backends round-robin.

Currently it only works with the following services:
- PostgreSQL
- MySQL
- Redis
- MongoDB
- RabbitMQ
- NATS
- Kafka, with one backend only: brokers keep per-cluster state (topic IDs, producer IDs) that clients reuse across connections

## Run

```
iris run src --config-file config/postgres-config.json
```

Settings are in `config/<service>-config.json`: the listen port, the routing strategy and the backends.
