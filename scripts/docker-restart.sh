#!/bin/bash

unhealthy_containers=$(docker ps --filter "health=unhealthy" --format "{{.Names}}")

if [[ -n "$unhealthy_containers" ]]; then
  echo "Found unhealthy containers: $unhealthy_containers"
  for service in $unhealthy_containers; do
    echo "Restarting service: $service"
    docker-compose restart "$service"
  done
else
  echo "No unhealthy containers found."
fi