#!/bin/bash
#
# Usage:
#   ./test.sh [-c COMMAND] [NAME1 [NAME2 [...]]]

# set -e

if [[ "$1" == "-c" ]]; then
  cmd="$2"
  shift
  shift
else
  cmd="mix test"
fi

if [[ "$@" == "" ]]; then
  services=`docker-compose config --services | xargs echo`
else
  services="$@"
fi

for name in $services; do
  port=`docker inspect --format='{{(index (index .NetworkSettings.Ports "3306/tcp") 0).HostPort}}' myxql_${name}_1`
  echo $name
  MYSQL_TCP_PORT=$port $cmd
done
