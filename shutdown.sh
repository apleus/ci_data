#!/bin/sh

docker compose down
docker stop $(docker container ls -q --filter name=minio)
docker rm $(docker container ls -a -q --filter name=minio)