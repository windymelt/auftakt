default:
    docker compose up

initialize-db:
    docker compose exec -T db psql -U myuser --password -d mydb -f /app/db/schema.sql