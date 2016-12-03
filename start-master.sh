#!/usr/bin/env bash
# docker pull epahomov/docker-spark
docker run -d -t -P --name spark_master -v `pwd`/src:/root/src/ spark /start-master.sh "$@"
