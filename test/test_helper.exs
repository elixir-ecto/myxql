ExUnit.start()

defmodule TestHelpers do
  def opts() do
    [
      hostname: "127.0.0.1",
      username: "root",
      database: "myxql_test",
      timeout: 5000,
      ssl: false,
      # TODO: need to force TLS v1.1 as MySQL 5.7 otherwise fails, need to document this.
      ssl_opts: [versions: [:"tlsv1.1"]],
      backoff_type: :stop,
      max_restarts: 0
    ]
  end
end

sql = """
DROP DATABASE IF EXISTS myxql_test;
CREATE DATABASE myxql_test;
USE myxql_test;

DROP USER IF EXISTS myxql_test;
CREATE USER myxql_test IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON myxql_test.* TO myxql_test;

DROP USER IF EXISTS mysql_native_password;
CREATE USER mysql_native_password IDENTIFIED WITH mysql_native_password BY 'secret';
GRANT ALL PRIVILEGES ON myxql_test.* TO mysql_native_password;

DROP USER IF EXISTS sha256_password;
CREATE USER sha256_password IDENTIFIED WITH sha256_password BY 'secret';
GRANT ALL PRIVILEGES ON myxql_test.* TO sha256_password;

DROP USER IF EXISTS nopassword;
CREATE USER nopassword;
GRANT ALL PRIVILEGES ON myxql_test.* TO nopassword;

CREATE TABLE integers (x int);

CREATE TABLE uniques (a int UNIQUE);

CREATE TABLE test_types (
  id SERIAL PRIMARY KEY AUTO_INCREMENT,
  my_tinyint TINYINT,
  my_smallint SMALLINT,
  my_mediumint MEDIUMINT,
  my_int INT,
  my_bigint BIGINT,
  my_float FLOAT,
  my_double DOUBLE,
  my_decimal DECIMAL /* same as: DECIMAL(10, 0) */,
  my_decimal52 DECIMAL(5,2),
  my_bit2 BIT(2),
  my_bit8 BIT(8),
  my_date DATE,
  my_time TIME,
  my_time3 TIME(3),
  my_time6 TIME(6),
  my_datetime DATETIME,
  my_datetime3 DATETIME(3),
  my_datetime6 DATETIME(6),
  my_timestamp TIMESTAMP,
  my_year YEAR,
  my_binary3 BINARY(3),
  my_varbinary3 VARBINARY(3),
  my_boolean BOOLEAN,
  my_blob BLOB,
  my_json JSON
);

DROP PROCEDURE IF EXISTS multi;
DELIMITER $$
CREATE PROCEDURE multi()
BEGIN
  SELECT 1;
  SELECT 2;
END$$
DELIMITER ;
"""

argv = ~w(
  --protocol=tcp
  --user=root
) ++ ["-e", sql]

case System.cmd("mysql", argv) do
  {result, 0} ->
    IO.puts(result)

  {result, _} ->
    IO.puts(result)
    exit(:error)
end
