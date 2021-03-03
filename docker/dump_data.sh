#! /bin/bash

# port 5557 points to the postgres 10 database
#PGPASSWORD="cjdb_test" pg_dump -U cjdb_tester -p 5558 -h localhost --dbname="db3dnl" \
#  --clean \
#  --if-exists \
#  --create \
#  --no-owner \
#  --no-privileges \
#  | gzip -9 > /data/3D_basisvoorziening/db3dnl/db3dnl_sample.sql.gz

docker exec \
db3dnl_postgis_13_30_1 \
  pg_dump \
  -U cjdb_tester \
  -p 5432 \
  -h localhost \
  --dbname="db3dnl" \
  --clean \
  --if-exists \
  --no-owner \
  --no-privileges \
  --create | gzip -c > "/tmp/db3dbag_sample.sql.gz";