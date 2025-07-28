#!/bin/bash
set -e

echo "Waiting for MongoDB to start..."

until mongosh --host mongo --port 27017 --eval "db.runCommand({ ping: 1 })" > /dev/null 2>&1; do
  sleep 2
  echo "Waiting for MongoDB connection..."
done

echo "Initializing replica set..."

mongosh --host mongo --port 27017 <<EOF
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "mongo:27017" }
  ]
});
EOF

echo "Replica set initialized."