#!/bin/sh

# create extended airflow image
docker build . --tag extending_airflow:latest

# setup airflow docker network
docker compose up -d

# minio blob storage setup
mkdir -p minio/data

docker run -d \
   -p 9000:9000 \
   -p 9090:9090 \
   --name minio \
   -v $PWD/minio/data:/data \
   --env-file .env \
   quay.io/minio/minio server /data --console-address ":9090"

sleep 10 # TODO: figure out why connection fails without waiting a few seconds

# create airflow <-> minio connection
docker exec -it $(docker container ls -q --filter "name=ci_data-airflow-webserver*") airflow connections add "minio_conn" \
      --conn-type "S3" \
      --conn-extra '{"aws_access_key_id": ROOTNAME, \
                     "aws_secret_access_key": CHANGEME123, \
                     "host": "http://host.docker.internal:9000"}'

# # create minio buckets
# docker run --rm --link minio:minio -e MINIO_BUCKET=products --entrypoint sh minio/mc -c "\
#   while ! nc -z minio 9000; do echo 'Wait minio to startup...' && sleep 0.1; done; \
#   sleep 5 && \
#   mc config host add myminio http://localhost:9000 ROOTNAME CHANGEME123 && \
#   mc rm -r --force myminio/products || true && \
#   mc mb myminio/products && \
#   mc policy download myminio/products \
# "