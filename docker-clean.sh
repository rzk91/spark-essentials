#!/bin/bash
export MY_POSTGRES_CONTAINER_ID=c847425a5e6c
docker rm -f $(docker ps -aq | grep -v $MY_POSTGRES_CONTAINER_ID)