# MyXQL

**TODO: Add description**

## Development

```
# Setup mysql 5.7
docker run --publish=5706:3306 --name mysql5.7 -e MYSQL_ROOT_PASSWORD=secret -d mysql:5.7.23

# Setup mysql 8.0
docker run --publish=8006:3306 --name mysql8.0 -e MYSQL_ROOT_PASSWORD=secret -d mysql:8.0.12 --default-authentication-plugin=mysql_native_password
```
