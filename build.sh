#!/bin/sh

docker build . --tag extending_airflow:latest

docker compose up -d

mkdir -p minio/data

docker run -d \
   -p 9000:9000 \
   -p 9090:9090 \
   --name minio \
   -v $PWD/minio/data:/data \
   --env-file .env \
   quay.io/minio/minio server /data --console-address ":9090"