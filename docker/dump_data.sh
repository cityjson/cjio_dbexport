#! /bin/bash

# port 5557 points to the postgres 10 database
PGPASSWORD="cjdb_test" pg_dump -U cjdb_tester -p 5557 -h localhost --dbname="db3dnl" \
  --clean \
  --if-exists \
  --create \
  --no-owner \
  --no-privileges \
  | gzip -9 > /data/3D_basisvoorziening/db3dnl/db3dnl_sample.sql.gz