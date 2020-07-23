#!/bin/bash
set -e

: "${DB:=mysql:8.0}"
: "${MYSQL_TCP_PORT:=3306}"
: "${MYSQL_SOCKET_DIR:=/tmp/mysqld}"
name="myxql-ci"

sudo mkdir -p $MYSQL_SOCKET_DIR
sudo chmod 777 $MYSQL_SOCKET_DIR

docker pull $DB || true
# docker rm --force $name || true
docker run --name $name -p $MYSQL_TCP_PORT:3306 --volume $MYSQL_SOCKET_DIR:/var/run/mysqld -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -d $DB --innodb_log_file_size=1G
mysql -u root --version
until mysql -u root --protocol=tcp -e "SELECT @@version;"; do sleep 1; done
