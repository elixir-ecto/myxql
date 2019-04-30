#!/bin/bash
set -e
set -x

: "${DB:=mysql:8.0}"
: "${MYSQL_TCP_PORT:=3306}"
: "${MYSQL_SOCKET_DIR:=/tmp/mysqld}"
name="myxql-ci"

mkdir -p $MYSQL_SOCKET_DIR
sudo chmod 777 $MYSQL_SOCKET_DIR

docker pull $DB || true
docker rm --force $name || true
docker run --name $name -p $MYSQL_TCP_PORT:3306 --volume $MYSQL_SOCKET_DIR:/var/run/mysqld -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -d $DB
mysql --version
until mysql -u root --protocol=tcp -e "SELECT @@version;"; do sleep 1; done
mix test $1
docker rm --force $name || true
