ExUnit.start()

sql = """
DROP DATABASE IF EXISTS myxql_test;
CREATE DATABASE myxql_test;
USE myxql_test;

DROP USER IF EXISTS nopassword;
CREATE USER nopassword;

DROP USER IF EXISTS sha2;
CREATE USER sha2 IDENTIFIED WITH caching_sha2_password BY 'secret';

CREATE TABLE integers (x int);

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
  my_year YEAR,
  my_binary3 BINARY(3),
  my_varbinary3 VARBINARY(3)
)
"""

argv = ~w(
  --defaults-file=#{Path.expand("../.my.cnf", __DIR__)}
  --protocol=tcp
  --port=8006
  myxql_test
) ++ ["-e", sql]

case System.cmd("mysql", argv) do
  {result, 0} ->
    IO.puts(result)

  {result, _} ->
    IO.puts(result)
    exit(:error)
end
