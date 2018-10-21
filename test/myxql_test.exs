defmodule MyXQLTest do
  use ExUnit.Case, async: true

  @opts TestHelpers.opts()

  describe "connect" do
    test "connect with default auth method and SSL" do
      opts = Keyword.merge(@opts, username: "myxql_test", password: "secret", ssl: true)
      {:ok, conn} = MyXQL.connect(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with default auth method and no SSL" do
      opts = Keyword.merge(@opts, username: "myxql_test", password: "secret", ssl: false)

      case MyXQL.connect(opts) do
        {:ok, conn} ->
          MyXQL.query!(conn, "SELECT 1")

        # if default auth method is e.g. sha256_password then we require SSL
        # so this will never succeed
        {:error, %MyXQL.Error{message: "ERROR 2061 (HY000)" <> _}} ->
          :ok
      end
    end

    test "connect with mysql_native_password" do
      opts = Keyword.merge(@opts, username: "mysql_native_password", password: "secret")
      {:ok, conn} = MyXQL.connect(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with mysql_native_password and bad password" do
      opts = Keyword.merge(@opts, username: "mysql_native_password", password: "bad")

      assert {:error,
              %MyXQL.Error{message: "Access denied for user 'mysql_native_password'" <> _}} =
               MyXQL.connect(opts)
    end

    test "connect with sha256_password and SSL" do
      opts = Keyword.merge(@opts, username: "sha256_password", password: "secret", ssl: true)
      {:ok, conn} = MyXQL.connect(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with sha256_password, SSL and bad password" do
      opts = Keyword.merge(@opts, username: "sha256_password", password: "bad", ssl: true)

      assert {:error, %MyXQL.Error{message: "Access denied for user 'sha256_password'" <> _}} =
               MyXQL.connect(opts)
    end

    test "connect with sha256_password and no SSL" do
      opts = Keyword.merge(@opts, username: "sha256_password", password: "secret", ssl: false)

      assert {:error, %MyXQL.Error{message: "ERROR 2061 (HY000)" <> _}} = MyXQL.connect(opts)
    end

    test "connect with no password" do
      opts = Keyword.merge(@opts, username: "nopassword", password: nil)
      {:ok, conn} = MyXQL.connect(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with bad SSL opts" do
      opts = Keyword.merge(@opts, ssl: true, ssl_opts: [ciphers: [:bad]])

      assert {:error, %MyXQL.Error{message: "Invalid TLS option: {ciphers,[bad]}"}} =
               MyXQL.connect(opts)
    end

    test "connect with host down" do
      opts = Keyword.merge(@opts, port: 9999)
      assert {:error, %MyXQL.Error{message: "connection refused"}} = MyXQL.connect(opts)
    end
  end

  describe "query" do
    test "simple query" do
      {:ok, conn} = MyXQL.connect(@opts)

      assert {:ok, %MyXQL.Result{columns: columns, rows: rows}} =
               MyXQL.query(conn, "SELECT 2*3, 4*5")

      assert columns == ["2*3", "4*5"]
      assert rows == [[6, 20]]
    end

    test "invalid query" do
      {:ok, conn} = MyXQL.connect(@opts)

      assert {:error, %MyXQL.Error{message: "Unknown column 'bad' in 'field list'"}} =
               MyXQL.query(conn, "SELECT bad")
    end

    test "query with multiple rows" do
      {:ok, conn} = MyXQL.connect(@opts)

      MyXQL.query!(conn, "TRUNCATE TABLE integers")
      MyXQL.query!(conn, "INSERT INTO integers VALUES (10), (20)")

      assert {:ok, %MyXQL.Result{columns: [_], rows: [[10], [20]]}} =
               MyXQL.query(conn, "SELECT * FROM integers")
    end
  end

  describe "prepared statements" do
    test "multiple rows" do
      {:ok, conn} = MyXQL.connect(@opts)

      MyXQL.query!(conn, "TRUNCATE TABLE integers")
      MyXQL.query!(conn, "INSERT INTO integers VALUES (10), (20)")

      {:ok, statement_id} = MyXQL.Protocol.prepare(conn, "SELECT x, x FROM integers")
      {:ok, %MyXQL.Result{columns: columns, rows: rows}} = MyXQL.execute(conn, statement_id, [])
      assert columns == ["x", "x"]
      assert rows == [[10, 10], [20, 20]]
    end

    test "params" do
      {:ok, conn} = MyXQL.connect(@opts)

      {:ok, statement_id} = MyXQL.prepare(conn, "SELECT ? * ?")
      {:ok, %MyXQL.Result{rows: rows}} = MyXQL.execute(conn, statement_id, [2, 3])
      assert rows == [[6]]
    end

    test "invalid prepared statement" do
      {:ok, conn} = MyXQL.Protocol.connect(@opts)

      assert {:error, %MyXQL.Error{message: "Unknown column 'bad' in 'field list'"}} =
               MyXQL.prepare(conn, "SELECT bad")
    end
  end
end
