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

## Clúster de 2 nodos (`compose.cluster.yml`)

Variante con dos nodos y colas espejo, como pide el documento de arquitectura.
`compose.yml` (un nodo) sigue funcionando igual.

```bash
docker compose -f compose.cluster.yml up -d --build
```

Arranca en orden: `rabbitmq-1` → `rabbitmq-2` (se une solo) → `rabbitmq-policy`
(aplica la política `ha-all`, que replica todas las colas en ambos nodos) →
`rabbitmq-admin` (microservicio administrador: declara exchanges, colas y DLQ al
arrancar y queda corriendo con su API REST). Ya no hace falta ejecutarlo a mano
ni el orden de arranque importa para `ms-edututor-notify`, que además reintenta
si las colas todavía no existen.

### API de administración (`rabbitmq-admin`, puerto 8086)

Solo escucha en `127.0.0.1` del host (no tiene autenticación): se usa desde la
misma instancia o por SSH. Los nombres de exchanges y colas salen de
`edututor.rabbit.*` en su `application.properties`.

| Método y ruta | Qué hace |
|---|---|
| `POST /api/queues` | Crea una cola (`name`, `durable`, `messageTtlMs`, `deadLetterExchange`) |
| `GET /api/queues/{name}` | Mensajes y consumidores de la cola |
| `DELETE /api/queues/{name}` | Elimina la cola |
| `POST /api/queues/{name}/purge` | Vacía la cola |
| `POST /api/exchanges` | Crea un exchange (`name`, `type`: DIRECT, TOPIC o FANOUT) |
| `DELETE /api/exchanges/{name}` | Elimina el exchange |
| `POST /api/bindings` | Enlaza cola y exchange con una ruta |

Valida la entrada (nombres vacíos, caracteres no permitidos, prefijo `amq.`,
TTL no positivo, tipo desconocido → 400) y devuelve 404 si el recurso no existe y
409 si ya existe con otra configuración o es parte de la topología base (las 3
colas, sus DLQ y los 3 exchanges no se pueden borrar).

```bash
curl -X POST http://localhost:8086/api/queues -H "content-type: application/json" \
  -d '{"name":"q.demo","messageTtlMs":60000}'
curl http://localhost:8086/api/queues/q.demo
curl -X DELETE http://localhost:8086/api/queues/q.demo
```

- Nodo 1: AMQP `:5672`, panel `:15672`. Nodo 2: AMQP `:5673`, panel `:15673`.
- Credenciales: `guest`/`guest` (las mismas de antes; se cambian con
  `RABBITMQ_USER` / `RABBITMQ_PASS`).
- Comprobar el clúster: `docker exec rabbitmq-1 rabbitmqctl cluster_status`.

### Conectar los servicios a los dos nodos

`ms-edututor-notify` y `rabbitmq-admin` aceptan `RABBITMQ_ADDRESSES`
(`host:puerto,host:puerto`). Si el primer nodo cae, el cliente se reconecta al
siguiente y sigue consumiendo. En `infra/apps/compose.yml`, servicio `notify`:

```yaml
RABBITMQ_ADDRESSES: 44.208.153.147:5672,44.208.153.147:5673
```

Antes de activarlo hay que desplegar este clúster en `ec2-mq` y abrir el puerto
**5673** en el Security Group `edututor-mq` (hoy solo está el 5672). Sin
`RABBITMQ_ADDRESSES`, `notify` usa `RABBITMQ_HOST`/`RABBITMQ_PORT` como siempre.
Ojo: ambos nodos corren en la misma instancia, así que esto protege contra la
caída de un contenedor o nodo RabbitMQ, no de la máquina.
