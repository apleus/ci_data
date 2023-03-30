#!/bin/sh

# create extended airflow image
docker build . --tag extending_airflow:latest

# setup airflow docker network
docker compose up -d

sleep 10 # TODO: figure out why connection fails without waiting a few seconds

# create airflow <-> s3 connection
docker exec -it $(docker container ls -q --filter "name=ci_data-airflow-webserver*") airflow connections add "s3_conn" \
      --conn-type "S3" \
      --conn-extra '{"aws_access_key_id": ROOTNAME, \
                     "aws_secret_access_key": CHANGEME123}'