defmodule TestHelper do
  def opts() do
    ssl_opts =
      case System.get_env("FORCE_TLS11") do
        "true" ->
          [versions: [:"tlsv1.1"]]

        nil ->
          []
      end

    [
      hostname: "127.0.0.1",
      username: "root",
      database: "myxql_test",
      timeout: 5000,
      ssl: false,
      ssl_opts: ssl_opts,
      backoff_type: :stop,
      max_restarts: 0,
      pool_size: 1,
      show_sensitive_data_on_connection_error: true
    ]
  end

  def auth_plugins() do
    {:ok, pid} = MyXQL.start_link(opts())

    %{rows: plugins} =
      MyXQL.query!(
        pid,
        "SELECT plugin_name FROM information_schema.plugins WHERE plugin_type = 'authentication'"
      )

    List.flatten(plugins)
  end

  def default_auth_plugin() do
    {:ok, pid} = MyXQL.start_link(opts())

    %MyXQL.Result{rows: [[plugin_name]]} =
      MyXQL.query!(pid, "SELECT plugin FROM mysql.user WHERE user = 'root' LIMIT 1")

    plugin_name
  end

  def mysql(sql) do
    args = ~w(
      --protocol=tcp
      --user=root
    ) ++ ["-e", sql]

    cmd(["mysql" | args])
  end

  def cmd([command | args]) do
    case System.cmd(command, args) do
      {"", 0} ->
        :ok

      {result, 0} ->
        IO.puts(result)

      {result, _} ->
        IO.puts(result)
        exit(:error)
    end
  end
end

TestHelper.mysql("""
DROP DATABASE IF EXISTS myxql_test;
CREATE DATABASE myxql_test;
""")

sha256_password_available? = "sha256_password" in TestHelper.auth_plugins()

if sha256_password_available? do
  TestHelper.mysql("""
  DROP USER IF EXISTS sha256_password;
  CREATE USER sha256_password IDENTIFIED WITH sha256_password;
  ALTER USER sha256_password IDENTIFIED BY 'secret';
  GRANT ALL PRIVILEGES ON myxql_test.* TO sha256_password;
  """)
end

TestHelper.mysql("""
USE myxql_test;
DROP USER IF EXISTS default_auth;
CREATE USER default_auth IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON myxql_test.* TO default_auth;

DROP USER IF EXISTS mysql_native_password;
CREATE USER mysql_native_password IDENTIFIED WITH mysql_native_password;
ALTER USER mysql_native_password IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON myxql_test.* TO mysql_native_password;

DROP USER IF EXISTS nopassword;
CREATE USER nopassword;
GRANT ALL PRIVILEGES ON myxql_test.* TO nopassword;

CREATE TABLE integers (x int);

CREATE TABLE uniques (a int UNIQUE);

CREATE TABLE test_types (
  id SERIAL PRIMARY KEY,
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
""")

exclude = if System.otp_release() >= "19", do: [], else: [:requires_otp_19]

exclude =
  if sha256_password_available? do
    exclude
  else
    [{:auth_plugin, "sha256_password"} | exclude]
  end

exclude =
  case System.get_env("JSON") do
    "false" -> [{:requires_json, true} | exclude]
    _ -> exclude
  end

exclude =
  case System.get_env("SSL") do
    "false" -> [{:requires_ssl, true} | exclude]
    _ -> exclude
  end

ExUnit.start(exclude: exclude)
