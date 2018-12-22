#!/bin/bash
set -e
set -x

if [ -n "$CHECKS_ONLY" ]; then
  mix compile --warnings-as-errors
  mix format --check-formatted
else
  docker pull $DB || true
  sudo chmod 777 -R /var/run/mysqld
  docker run --name mysql -p 3306:3306 --volume /var/run/mysqld:/var/run/mysqld -e MYSQL_ALLOW_EMPTY_PASSWORD=1 -d $DB
  mysql --version
  until mysql -u root -e "SELECT @@version;"; do sleep 1; done
  mix test
fi
