docker run -d \
  --name pg-timescale \
  -e POSTGRES_USER=gedgar \
  -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  -e POSTGRES_DB=knowledge_base \
  -p 5434:5432 \
  -v /home/gedgar/Postgres_Data/timescale:/var/lib/postgresql/data \
  timescale/timescaledb:latest-pg17

