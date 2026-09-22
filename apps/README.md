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

`KAFKA_BOOTSTRAP_SERVERS` y `RABBITMQ_HOST` apuntan a las IPs públicas de
`ec2-kafka` y `ec2-mq` (cuenta AWS del compañero). **AWS Academy asigna una IP
pública nueva cada vez que se reinicia el Learner Lab** — si algo deja de
conectar después de una pausa del lab, lo primero a revisar es si esas IPs
cambiaron, y actualizar aquí + el Security Group de `ec2-apps` en consecuencia.
