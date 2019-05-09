defmodule TestHelper do
  def opts() do
    [
      hostname: "127.0.0.1",
      username: "root",
      database: "myxql_test",
      timeout: 5000,
      ssl_opts: ssl_opts(),
      backoff_type: :stop,
      max_restarts: 0,
      pool_size: 1,
      show_sensitive_data_on_connection_error: true
    ]
  end

  defp ssl_opts() do
    [versions: [:"tlsv1.1"]]
  end

  def setup_server() do
    configure_server()
    create_test_database()
    create_test_users()
    create_test_tables()
  end

  def configure_server() do
    mysql!("""
    -- set packet size to 100mb
    SET GLOBAL max_allowed_packet=#{100_000_000};
    """)
  end

  def create_test_database() do
    mysql!("""
    DROP DATABASE IF EXISTS myxql_test;
    CREATE DATABASE myxql_test;
    """)
  end

  def create_test_users() do
    mysql!("""
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
    """)

    auth_plugins = auth_plugins()

    if "sha256_password" in auth_plugins do
      mysql!("""
      DROP USER IF EXISTS sha256_password;
      CREATE USER sha256_password IDENTIFIED WITH sha256_password;
      ALTER USER sha256_password IDENTIFIED BY 'secret';
      GRANT ALL PRIVILEGES ON myxql_test.* TO sha256_password;
      """)
    end

    if "caching_sha2_password" in auth_plugins do
      mysql("""
      DROP USER IF EXISTS caching_sha2_password;
      CREATE USER caching_sha2_password IDENTIFIED WITH caching_sha2_password;
      ALTER USER caching_sha2_password IDENTIFIED BY 'secret';
      GRANT ALL PRIVILEGES ON myxql_test.* TO caching_sha2_password;
      """)
    end
  end

  def create_test_tables() do
    mysql!("""
    USE myxql_test;

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
      my_decimal DECIMAL, /* same as: DECIMAL(10, 0) */
      my_decimal52 DECIMAL(5,2),

      my_unsigned_tinyint TINYINT UNSIGNED,
      my_unsigned_smallint SMALLINT UNSIGNED,
      my_unsigned_mediumint MEDIUMINT UNSIGNED,
      my_unsigned_int INT UNSIGNED,
      my_unsigned_bigint BIGINT UNSIGNED,
      my_unsigned_float FLOAT UNSIGNED,
      my_unsigned_double DOUBLE UNSIGNED,
      my_unsigned_decimal DECIMAL UNSIGNED,
      my_unsigned_decimal52 DECIMAL(5, 2) UNSIGNED,

      my_enum ENUM('red', 'green', 'blue'),
      my_set SET('red', 'green', 'blue'),
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
      my_mediumblob MEDIUMBLOB,
      my_json JSON,
      my_char CHAR
    );

    DROP PROCEDURE IF EXISTS single_procedure;
    DELIMITER $$
    CREATE PROCEDURE single_procedure()
    BEGIN
      SELECT 1;
    END$$
    DELIMITER ;

    DROP PROCEDURE IF EXISTS multi_procedure;
    DELIMITER $$
    CREATE PROCEDURE multi_procedure()
    BEGIN
      SELECT 1;
      SELECT 2;
    END$$
    DELIMITER ;
    """)
  end

  def auth_plugins() do
    "SELECT plugin_name FROM information_schema.plugins WHERE plugin_type = 'authentication'"
    |> mysql!()
    |> String.split("\n", trim: true)
    |> Enum.drop(1)
  end

  def default_auth_plugin() do
    "SELECT plugin FROM mysql.user WHERE user = 'root' LIMIT 1"
    |> mysql!()
    |> String.split("\n", trim: true)
    |> Enum.at(1)
  end

  def supports_ssl?() do
    mysql!("SELECT @@have_ssl") == "@@have_ssl\nYES\n"
  end

  def supports_json?() do
    sql =
      "CREATE TEMPORARY TABLE myxql_test.test_json (json json); SHOW COLUMNS IN myxql_test.test_json"

    case mysql(sql) do
      {:ok, result} ->
        row =
          result
          |> String.split("\n", trim: true)
          |> Enum.at(1)

        row =~ "json\tjson"

      {:error, _} ->
        false
    end
  end

  def mysql!(sql, options \\ []) do
    case mysql(sql, options) do
      {:ok, result} -> result
      {:error, message} -> exit(message)
    end
  end

  def mysql(sql, options \\ []) do
    args = ~w(
        --protocol=tcp
        --user=root
      ) ++ ["-e", sql]

    cmd(["mysql" | args], options)
  end

  def cmd([command | args], options) do
    options = Keyword.put_new(options, :stderr_to_stdout, true)

    case System.cmd(command, args, options) do
      {result, 0} ->
        {:ok, result}

      {result, _} ->
        {:error, result}
    end
  end

  def excludes() do
    auth_plugins = auth_plugins()

    exclude = []
    exclude = if System.otp_release() >= "19", do: exclude, else: [:requires_otp_19]

    exclude =
      if "sha256_password" in auth_plugins,
        do: exclude,
        else: [{:sha256_password, true} | exclude]

    exclude =
      if "caching_sha2_password" in auth_plugins,
        do: exclude,
        else: [{:caching_sha2_password, true} | exclude]

    exclude =
      case System.get_env("JSON") do
        "false" -> [{:requires_json, true} | exclude]
        _ -> exclude
      end

    exclude =
      if supports_json?() do
        [{:json, false} | exclude]
      else
        [{:json, true} | exclude]
      end

    exclude =
      if supports_ssl?() do
        [{:ssl, false} | exclude]
      else
        [{:ssl, true} | exclude]
      end

    exclude
  end
end

TestHelper.setup_server()
ExUnit.start(exclude: TestHelper.excludes())
