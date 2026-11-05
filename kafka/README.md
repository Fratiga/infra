# Kafka

Kafka complementa a RabbitMQ en EduTutor, no lo reemplaza. RabbitMQ sigue
llevando los **comandos** (`cmd.direct` → notify). Kafka lleva los **hechos**
que ya ocurrieron, para que varios servicios los lean a su ritmo y en orden.

```
sessions ──► sessions.events ──┬─► audit   (grupo audit-service-v1)  guarda el historial
  (productor)   3 particiones  │             │ detecta patrones
                key = sessionId│             ▼
                               │        sessions.alerts ──► report (grupo report-alerts-v1)
                               └─► report  (grupo report-service-v1)  guarda las alertas
                                     calcula las métricas
   Lo que no se puede procesar ──► sessions.events.DLT / sessions.alerts.DLT
```

| Tópico | Particiones | Retención | cleanup.policy | Para qué |
|---|---|---|---|---|
| `sessions.events` | 3 | 7 días | `delete` | Hechos de las sesiones (creada, cambió de estado). La key es el `sessionId`: los eventos de una sesión caen en la misma partición y se leen en orden. |
| `sessions.alerts` | 3 | 30 días | `compact,delete` | Alertas de analítica. La key es `estudianteId:regla`: se conserva la última de cada una. |
| `sessions.events.DLT` | 3 | 14 días | `delete` | Eventos que no se pudieron procesar. |
| `sessions.alerts.DLT` | 3 | 14 días | `delete` | Alertas que no se pudieron procesar. |

La única fuente de esta tabla es `kafka-admin/src/main/resources/application.yml`.
Los factores de réplica salen del entorno: 1 en un broker, 3 (con `min.insync.replicas=2`)
en el clúster.

## Formas de levantarlo

| Archivo | Qué levanta | Cuándo |
|---|---|---|
| `compose.yml` | 1 Zookeeper + 1 broker + Kafka UI | Desarrollo local |
| `compose.cluster.yml` | 3 Zookeeper + 3 brokers (RF 3, ISR 2) + kafka-admin + Kafka UI | Evaluación / probar réplicas y caídas |
| `compose.ec2.yml` | 1 Zookeeper + 1 broker anunciando la Elastic IP + `kafka-init` + Kafka UI | Servidor EC2 |
| `compose.ui.yml` | Solo Kafka UI, apuntando a un Kafka que ya existe | Mirar el de EC2 o el de otro equipo |

```bash
# un broker
docker compose -f compose.yml up -d

# clúster de 3 brokers (declara los tópicos solo, con RF 3)
docker compose -f compose.cluster.yml up -d --build

# solo la interfaz, contra cualquier Kafka
KAFKA_BOOTSTRAP=184.192.63.192:9092 docker compose -f compose.ui.yml up -d
```

Kafka UI queda en <http://localhost:8090>. Desde el equipo, el clúster responde en
`localhost:19092`, `19093` y `19094`.

Para probar tolerancia a fallos con el clúster: `docker stop kafka-2` y ver en
Kafka UI cómo cambian el líder y las réplicas en sincronía; las escrituras
siguen funcionando (ISR 2 de 3). `docker start kafka-2` y se pone al día solo.

## kafka-admin (puerto 8087)

Microservicio de administración. Al arrancar **crea o corrige** los tópicos de la
tabla (nunca borra) y expone una API REST. Con el clúster ya viene incluido; con
un solo broker:

```bash
cd kafka-admin
./mvnw spring-boot:run
```

Si se define `KAFKA_ADMIN_TOKEN`, cada llamada debe llevar la cabecera
`X-Admin-Token`. En un servidor debe definirse siempre.

| Operación | Llamada |
|---|---|
| Estado del clúster | `GET /api/cluster` |
| Listar tópicos | `GET /api/topics` |
| Ver un tópico (particiones, líder, réplicas, ISR, configuración, offsets) | `GET /api/topics/{nombre}` |
| Crear | `POST /api/topics` `{"name":"x","partitions":3,"replicationFactor":1,"configs":{"retention.ms":"86400000"}}` |
| Cambiar configuración (retención, cleanup.policy, …) | `PUT /api/topics/{nombre}/config` `{"set":{"retention.ms":"1000"},"unset":["max.message.bytes"]}` |
| Aumentar particiones | `PUT /api/topics/{nombre}/partitions` `{"partitions":6}` |
| Borrar | `DELETE /api/topics/{nombre}` (los tópicos de EduTutor están protegidos: 409) |
| Listar grupos de consumo | `GET /api/consumer-groups` |
| Ver un grupo (miembros, partición asignada, offset, LAG) | `GET /api/consumer-groups/{id}` |
| Reposicionar offsets (`EARLIEST`, `LATEST`, `OFFSET`) | `POST /api/consumer-groups/{id}/offsets/reset` `{"topic":"sessions.events","to":"EARLIEST"}` — solo con el grupo detenido |
| Borrar un grupo | `DELETE /api/consumer-groups/{id}` — solo si está vacío |

Las particiones solo se pueden aumentar (Kafka no permite reducirlas) y una
clave deja de caer siempre en la misma partición al aumentarlas; por eso no se
hace sola.

## Cómo está armado cada servicio

- **Productor** (`ms-edututor-sessions`): `SessionEventPublisher` arma un `SessionEvent`
  tipado, lo serializa a JSON y lo envía con `acks=all` e idempotencia. Si Kafka no
  responde, lo registra y sigue: crear una sesión no depende de que el broker esté arriba.
- **Consumidores** (`ms-edututor-audit`, `ms-edututor-report`): el `@KafkaListener` solo
  recibe el mensaje y lo valida; la lógica está en `AuditService` / `MetricasService` /
  `AlertaService`, que no conocen Kafka y se prueban sin broker.
- **Entrega "al menos una vez"**: el offset se confirma después de procesar cada mensaje,
  y cada servicio ignora un `eventId` que ya aplicó.
- **Errores**: un mensaje que no es JSON o que no pasa la validación va directo a la DLT;
  uno que falla por algo transitorio se reintenta 3 veces (1 s, 2 s, 4 s) y, si sigue
  fallando, va a la DLT con la excepción en las cabeceras `kafka_dlt-exception-*`.
  El consumo del resto sigue sin detenerse.
- **Grupos**: `audit-service-v1` y `report-service-v1` leen los mismos eventos de forma
  independiente; `report-alerts-v1` lee las alertas. `audit` usa 3 hilos (uno por partición);
  `report` usa 1 porque actualiza contadores compartidos.

La pantalla **Auditoría** del portal muestra el offset de cada partición y el LAG de
cada grupo (`GET /api/audit/kafka`); **Analítica** lista las alertas.
