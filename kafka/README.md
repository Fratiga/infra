# Kafka

`compose.yml` levanta Zookeeper + un broker de Kafka + Kafka UI (`:8090`).
El documento de arquitectura sugiere 3 nodos Zookeeper y 3 brokers con
factor de réplica 3 para las evaluaciones — se simplificó a un nodo de cada
uno para desarrollo local; los tópicos y su semántica (particionado por
`sessionId`, DLT) son los mismos independiente del número de brokers.

## Uso

```bash
docker compose -f compose.yml up -d
```

Después, corre `kafka-admin/` una vez para declarar los tópicos
(`sessions.events`, `audit.timeline` y sus `.DLT`):

```bash
cd kafka-admin
./mvnw spring-boot:run
```
