docker run -d \
  --name pg-vector \
  -e POSTGRES_USER=gedgar \
  -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
  -e POSTGRES_DB=knowledge_base \
  -p 5432:5432 \
  -v /home/gedgar/Postgres_Data/vector:/var/lib/postgresql/data \
  pgvector/pgvector:pg17


 

