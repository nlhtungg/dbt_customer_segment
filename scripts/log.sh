echo "hello"

# Wait for all containers to be running
containers=(
  spark-master
  spark-worker
  spark-thrift-server
  minio
  hive-postgres
  hive-metastore
  trino
  jupyter
)

echo "Waiting for containers to be running..."
for container in "${containers[@]}"; do
  while ! docker ps --filter "name=$container" --filter "status=running" | grep "$container" > /dev/null; do
    echo "Waiting for $container..."
    sleep 1
  done
  echo "$container is running."
done

docker logs -f spark-master >> logs/spark-master.log 2>&1 &
docker logs -f spark-worker >> logs/spark-worker.log 2>&1 &
docker logs -f spark-thrift-server >> logs/spark-thrift-server.log 2>&1 &
docker logs -f minio >> logs/minio.log 2>&1 &
docker logs -f hive-postgres >> logs/hive-postgres.log 2>&1 &
docker logs -f hive-metastore >> logs/hive-metastore.log 2>&1 &
docker logs -f trino >> logs/trino.log 2>&1 &
docker logs -f jupyter >> logs/jupyter.log 2>&1 &

