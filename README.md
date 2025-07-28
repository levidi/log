docker compose exec mongo1 mongosh --port 27017 --quiet --eval "rs.initiate({...})" --json relaxed
mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=rs0