defmodule MyXQLTest do
  use ExUnit.Case, async: true
  import MyXQL.Messages

  @opts [
    hostname: "127.0.0.1",
    port: 8006,
    username: "root",
    password: "secret",
    database: "myxql_test",
    timeout: 5000
  ]

  describe "connect" do
    test "connect and disconnect" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)
      :ok = MyXQL.Protocol.disconnect(conn)
    end

    test "connect with no password" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)
      ok_packet() = MyXQL.Protocol.query(conn, "DROP USER IF EXISTS nopassword")
      ok_packet() = MyXQL.Protocol.query(conn, "CREATE USER nopassword")

      opts = Keyword.merge(@opts, username: "nopassword", password: nil)
      {:ok, _} = MyXQL.Protocol.connect(opts)
    end

    @tag :mysql_8_x
    test "connect with non-default authentication method" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)
      ok_packet() = MyXQL.Protocol.query(conn, "DROP USER IF EXISTS sha2user")

      ok_packet() =
        MyXQL.Protocol.query(conn, "CREATE USER sha2user IDENTIFIED WITH caching_sha2_password")

      opts = Keyword.put(@opts, :username, "sha2user")

      assert {:error, err_packet(error_message: "Client does not support authentication" <> _)} =
               MyXQL.Protocol.connect(opts)
    end

    test "connect with invalid password" do
      assert {:error, err_packet(error_message: "Access denied for user" <> _)} =
               MyXQL.Protocol.connect(Keyword.put(@opts, :password, "bad"))
    end

    test "connect with host down" do
      assert {:error, :econnrefused} = MyXQL.Protocol.connect(Keyword.put(@opts, :port, 9999))
    end
  end

  describe "query" do
    test "simple query" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert resultset(column_definitions: [_, _], rows: [[6, 20]]) =
               MyXQL.Protocol.query(conn, "SELECT 2*3, 4*5")
    end

    test "invalid query" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert err_packet(error_message: "Unknown column 'bad' in 'field list'") =
               MyXQL.Protocol.query(conn, "SELECT bad")
    end

    test "query with multiple rows" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert ok_packet() =
               MyXQL.Protocol.query(conn, "CREATE TABLE IF NOT EXISTS integers (x int)")

      assert ok_packet() = MyXQL.Protocol.query(conn, "TRUNCATE TABLE integers")
      assert ok_packet() = MyXQL.Protocol.query(conn, "INSERT INTO integers VALUES (10)")
      assert ok_packet() = MyXQL.Protocol.query(conn, "INSERT INTO integers VALUES (20)")

      assert resultset(column_definitions: [column_definition41(name: "x")], rows: [[10], [20]]) =
               MyXQL.Protocol.query(conn, "SELECT * FROM integers")
    end
  end

  describe "prepared statements" do
    test "multiple rows" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert com_stmt_prepare_ok(statement_id: statement_id) =
               MyXQL.Protocol.prepare(conn, "SELECT x, x FROM integers")

      assert resultset(rows: rows) = MyXQL.Protocol.execute(conn, statement_id, [])
      assert rows == [[10, 10], [20, 20]]
    end

    test "params" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert com_stmt_prepare_ok(statement_id: statement_id) =
               MyXQL.Protocol.prepare(conn, "SELECT ? * ?")

      assert resultset(rows: rows) = MyXQL.Protocol.execute(conn, statement_id, [2, 3])
      assert rows == [[6]]
    end

    test "invalid prepared statement" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert err_packet(error_message: "Unknown column 'bad' in 'field list'") =
               MyXQL.Protocol.prepare(conn, "SELECT bad")
    end

    @tag :skip
    test "invalid params count" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert com_stmt_prepare_ok(statement_id: statement_id) =
               MyXQL.Protocol.prepare(conn, "SELECT ? * ?")

      MyXQL.Protocol.execute(conn, statement_id, [1, 2, 3])
    end
  end
end
