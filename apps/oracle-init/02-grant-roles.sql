-- Separado de 01-create-schemas.sql a proposito: en una prueba real, los
-- GRANT que iban intercalados con cada CREATE USER en un solo archivo no
-- quedaron aplicados (los usuarios se crearon pero sin privilegios, causando
-- ORA-01045 al primer login de cada microservicio). Ejecutarlos en un
-- archivo/paso separado evita ese problema.
GRANT CONNECT, RESOURCE TO edututor_catalog;
GRANT CONNECT, RESOURCE TO edututor_sessions;
GRANT CONNECT, RESOURCE TO edututor_audit;
GRANT CONNECT, RESOURCE TO edututor_report;
