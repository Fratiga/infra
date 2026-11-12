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

## Tuning y pruebas de carga

Dónde se ajusta cada cosa y cómo se mide. Las pruebas de carga usan un tópico
propio (`sessions.loadtest`) para no llenar de eventos falsos la auditoría ni las
métricas, y solo existen cuando `kafka-admin` corre con `KAFKA_LOADTEST_ENABLED=true`
(ya viene activado en `compose.cluster.yml`).

### Qué se ajustó

| Dónde | Parámetro | De fábrica | EduTutor | Para qué |
|---|---|---|---|---|
| Productor (`sessions`) | `batch.size` | 16 KB | **32 KB** | Agrupa más mensajes por viaje de red |
| | `linger.ms` | 0 | **20 ms** | Espera un poco para llenar el lote (el evento no se espera de forma síncrona, así que 20 ms no se notan) |
| | `compression.type` | ninguna | **lz4** | Menos bytes por la red y en disco |
| | `acks` / idempotencia | 1 / no | **all / sí** | No perder ni duplicar eventos al reintentar |
| Tópico `sessions.events` | `retention.ms` | 7 días | 7 días | Tiempo máximo que se conserva un evento |
| | `retention.bytes` | sin límite | **1 GB por partición** | Tope de disco: si se llena antes, borra lo más viejo |
| | `segment.bytes` | 1 GB | **100 MB** | Kafka solo borra segmentos cerrados; segmentos más chicos permiten borrar antes |
| Tópico `sessions.loadtest` | `retention.ms` / `retention.bytes` | 7 días / sin límite | **5 min / 50 MB** | Retención agresiva a propósito, para verla en acción |
| | `segment.ms` / `segment.bytes` | 7 días / 1 GB | **1 min / 10 MB** | Para que haya segmentos cerrados que borrar |
| Broker (`compose*.yml`) | `log.retention.check.interval.ms` | 5 min | **1 min** | Cada cuánto revisa qué borrar |
| | `log.retention.hours` / `bytes` | 168 h / sin límite | 168 h / **1 GB** | Tope para cualquier tópico nuevo que no declare el suyo |

### Cómo medir (con `compose.cluster.yml` arriba)

```bash
# 100.000 mensajes con los valores de fábrica y luego con los de EduTutor
curl -s -X POST "localhost:8087/api/loadtest?count=100000&payloadBytes=200&perfil=COMPARAR"
```

`perfil` puede ser `DEFAULT`, `TUNED` o `COMPARAR`; `batchSize`, `lingerMs` y
`compression` permiten probar otros valores. Los dos perfiles usan `acks=all`, así que
solo cambia lo que se quiere medir.

**Resultado** (clúster de 3 brokers, factor de réplica 3, `acks=all`, 100.000 mensajes de 200 B):

| | De fábrica | EduTutor | Efecto |
|---|---|---|---|
| Duración | 9.314 ms | 4.376 ms | 2,1 veces más rápido |
| Mensajes por segundo | 10.737 | 22.852 | **+113 %** |
| Mensajes por petición | 73 | 197 | 2,7 veces más agrupados |
| Peticiones por segundo | 35 | 15 | **-57 %** menos viajes de red |
| Tamaño comprimido / original | 1,00 | 0,42 | Viaja el 42 % de los bytes |
| Errores | 0 | 0 | |

(Los números dependen de la máquina; lo importante es la diferencia entre columnas.)

### El consumidor lento y el LAG

`kafka-admin` incluye un consumidor que tarda 10 ms por mensaje (como si guardara en una
base de datos), grupo `loadtest-analytics-v1`. Con 1 hilo procesa como máximo 100 mensajes por
segundo, así que ante la ráfaga se queda atrás:

```bash
curl -s localhost:8087/api/consumer-groups/loadtest-analytics-v1
```

Tras lanzar los 200.000 mensajes el LAG fue de 198.704 y bajaba de 100 en 100 por segundo
(2.000 mensajes en 20 s). Es lo que dispararía una alarma en producción.

**Cómo se arregla**: el tópico tiene 3 particiones, así que se pueden repartir entre 3 hilos
(o 3 instancias del servicio). Se cambia en caliente, sin reiniciar nada:

```bash
curl -s -X PUT localhost:8087/api/loadtest/consumer -H 'Content-Type: application/json' -d '{"concurrency":3}'
```

Con 3 hilos se procesaron 6.201 mensajes en 20 s (unos 300 por segundo), tres veces más
rápido. Más hilos que particiones no ayudan: el máximo de consumidores útiles de un grupo
es el número de particiones, y por eso `sessions.events` tiene 3.

### Retención

Después de la ráfaga, el tópico `sessions.loadtest` tenía 200.000 mensajes. Pasados los 5 minutos
de `retention.ms`, la revisión del broker (cada minuto) borró de la partición 0 los
primeros 47.296 mensajes (`offsetInicial` pasó de 0 a 47.296), **aunque el consumidor no los había
leído**:

```bash
curl -s localhost:8087/api/topics/sessions.loadtest
```

Dos cosas importantes de cómo funciona:

- Kafka solo borra **segmentos cerrados**, nunca el activo. Por eso se acotan `segment.ms` y
  `segment.bytes`: con el valor de fábrica (1 GB) un tópico pequeño no borraría nada, por mucho
  que se baje `retention.ms`.
- Si un consumidor está más atrás que lo que queda en el tópico, los mensajes borrados se
  pierden para él (salta al más antiguo disponible). La retención es un tope de disco, no
  una garantía de entrega: por eso hay que vigilar el LAG.
