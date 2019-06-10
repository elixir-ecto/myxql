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
    create_user("default_auth", nil, "secret")
    create_user("nopassword", nil, nil)
    create_user("mysql_native", "mysql_native_password", "secret")
    create_user("sha256_password", "sha256_password", "secret")
    create_user("sha256_empty", "sha256_password", nil)
    create_user("caching_sha2_password", "caching_sha2_password", "secret")
  end

  def create_user(username, auth_plugin_name, password) do
    # due to server differences some of these commands may fail but we continue anyway

    mysql("DROP USER #{username}")
    mysql("CREATE USER #{username} #{auth_plugin_name && "IDENTIFIED WITH #{auth_plugin_name};"}")
    mysql("GRANT ALL PRIVILEGES ON myxql_test.* TO #{username};")

    if password do
      flag =
        case auth_plugin_name do
          "sha256_password" -> 2
          _ -> 0
        end

      # works on mysql < 8.0
      sql = "SET old_passwords=#{flag};SET PASSWORD FOR #{username}=PASSWORD('#{password}')"
      mysql(sql)
      # works on mysql >= 5.7
      mysql("ALTER USER #{username} IDENTIFIED BY '#{password}'")
    end
  end

  def create_test_tables() do
    timestamps_with_precision = """
    my_time3 TIME(3),
    my_time6 TIME(6),
    my_datetime3 DATETIME(3),
    my_datetime6 DATETIME(6),
    """

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
      my_bit3 BIT(3),
      my_date DATE,
      my_time TIME,
      my_timestamp TIMESTAMP,
      my_datetime DATETIME,
      #{if supports_timestamp_precision?(), do: timestamps_with_precision, else: ""}
      my_year YEAR,
      my_binary3 BINARY(3),
      my_varbinary3 VARBINARY(3),
      my_boolean BOOLEAN,
      my_blob BLOB,
      my_mediumblob MEDIUMBLOB,
      #{if supports_json?(), do: "my_json JSON,", else: ""}
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

  def available_auth_plugins() do
    "SELECT plugin_name FROM information_schema.plugins WHERE plugin_type = 'authentication'"
    |> mysql!()
    |> String.split("\n", trim: true)
    |> Enum.drop(1)
    |> Enum.map(&String.to_atom/1)
  end

  def supports_ssl?() do
    mysql!("SELECT @@have_ssl") == "@@have_ssl\nYES\n"
  end

  def supports_public_key_exchange?() do
    mysql!("SHOW STATUS LIKE 'Rsa_public_key'") != ""
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

  def supports_timestamp_precision?() do
    case mysql("CREATE TEMPORARY TABLE myxql_test.timestamp_precision (time time(3));") do
      {:ok, _} -> true
      {:error, _} -> false
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
    supported_auth_plugins = [:mysql_native_password, :sha256_password, :caching_sha2_password]
    available_auth_plugins = available_auth_plugins()
    mariadb? = mysql!("select @@version") =~ ~r"mariadb"i

    exclude =
      for plugin <- supported_auth_plugins,
          not (plugin in available_auth_plugins) do
        {plugin, true}
      end

    exclude = [{:requires_otp_19, System.otp_release() < "19"} | exclude]
    exclude = [{:ssl, not supports_ssl?()} | exclude]
    exclude = [{:public_key_exchange, not supports_public_key_exchange?()} | exclude]
    exclude = [{:json, not supports_json?()} | exclude]
    exclude = [{:timestamp_precision, not supports_timestamp_precision?()} | exclude]
    exclude = if mariadb?, do: [{:bit, true} | exclude], else: exclude
    exclude
  end
end

TestHelper.setup_server()
ExUnit.start(exclude: TestHelper.excludes())
