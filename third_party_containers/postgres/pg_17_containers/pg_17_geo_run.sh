docker run -d \
  --name pg-geo-routing \
  -e POSTGRES_USER=gedgar \
  -e POSTGRES_PASSWORD=ready2go \
  -e POSTGRES_DB=$POSTGRES_PASSWORD \
  -p 5433:5432 \
  -v /home/gedgar/Postgres_Data/geo:/var/lib/postgresql/data \
  pgrouting/pgrouting:17-3.5-3.8
