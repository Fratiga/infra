-- Crea un esquema/usuario por microservicio dentro del mismo PDB (XEPDB1).
-- gvenzl/oracle-xe ejecuta este script una sola vez, en el primer arranque
-- (cuando el volumen de datos esta vacio), contra el PDB de la app.
CREATE USER edututor_catalog IDENTIFIED BY "EduTutorCat2026" DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
CREATE USER edututor_sessions IDENTIFIED BY "EduTutorSes2026" DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
CREATE USER edututor_audit IDENTIFIED BY "EduTutorAud2026" DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
CREATE USER edututor_report IDENTIFIED BY "EduTutorRep2026" DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;
