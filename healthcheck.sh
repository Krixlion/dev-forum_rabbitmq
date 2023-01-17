#!/usr/bin/env bash
until [ "$(docker inspect -f {{.State.Health.Status}} rabbitmq)" == "healthy" ]; do
    echo "Waiting for rabbitmq to boot. "
    echo "Current status: $(docker inspect -f {{.State.Health.Status}} rabbitmq)"
    echo "Sleeping for 10 seconds"
    printf "\n"
    sleep 10;
done;