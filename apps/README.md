# apps (ec2-apps)

`compose.yml` construye y levanta los 6 microservicios de dominio + BFF junto
a una base Oracle XE con un esquema por servicio. Pensado para correr en
`ec2-apps`, con los 6 repos clonados como carpetas hermanas de `infra/`:

```
cloud native/
├── infra/                  (este repo)
├── ms-edututor-bff/
├── ms-edututor-catalog/
├── ms-edututor-sessions/
├── ms-edututor-notify/
├── ms-edututor-audit/
└── ms-edututor-report/
```

## Uso

```bash
cd infra/apps
docker compose up -d --build
```

Oracle tarda 1-2 minutos en el primer arranque (crea los esquemas de
`oracle-init/`); catalog/sessions/audit/report esperan a que su healthcheck
pase antes de arrancar.

## IPs externas

`KAFKA_BOOTSTRAP_SERVERS` y `RABBITMQ_HOST` apuntan a las **Elastic IP** de
`ec2-kafka` y `ec2-mq` (cuenta AWS del compañero) — no cambian entre
reinicios del lab, así que no hay que tocarlas cada vez.

Si algo deja de conectar después de una pausa del lab, el problema no es la
IP: es que la instancia de tu compañero (o los contenedores de Kafka/RabbitMQ
dentro de ella) simplemente no están arriba en ese momento. Pídele que la
inicie y levante `docker compose up -d` de su lado antes de asumir que hay
que cambiar algo acá.

`ec2-apps` en cambio **no** tiene Elastic IP — su IP pública sí cambia cada
vez que se reinicia el lab (hay que actualizar el Security Group con la IP
saliente actual si se restringe por IP, y regenerar el build del frontend
apuntando a la IP nueva si el proxy de API Gateway la usa directamente).
