# RabbitMQ

`compose.yml` levanta un único nodo de RabbitMQ con el plugin de management
(UI en `:15672`, AMQP en `:5672`). El documento de arquitectura sugiere un
clúster de 2 nodos con colas espejo para las evaluaciones — se simplificó a
un solo nodo para desarrollo local; la topología (exchanges, colas, DLQ) es
la misma independiente del número de nodos.

## Uso

```bash
docker compose -f compose.yml up -d
```

Después, corre `rabbitmq-admin/` una vez para declarar la topología
(exchanges `cmd.direct`, `cmd.topic`, `cmd.dead.dlx`; colas `q.cmd.email`,
`q.cmd.session`, `q.cmd.certificate` y sus DLQ):

```bash
cd rabbitmq-admin
./mvnw spring-boot:run
```
