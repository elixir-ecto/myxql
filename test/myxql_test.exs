defmodule MyXQLTest do
  use ExUnit.Case, async: true

  @opts TestHelpers.opts()

  describe "connect" do
    test "connect with no password" do
      opts = Keyword.merge(@opts, username: "nopassword", password: nil)
      {:ok, _} = MyXQL.connect(opts)
    end

    test "connect with non-default authentication method" do
      opts = Keyword.put(@opts, :username, "sha256_password")

      assert {:error, %MyXQL.Error{message: "Client does not support authentication" <> _}} =
               MyXQL.connect(opts)
    end

    test "connect with invalid password" do
      assert {:error, %MyXQL.Error{message: "Access denied for user" <> _}} =
               MyXQL.connect(Keyword.put(@opts, :password, "bad"))
    end

    test "connect with host down" do
      assert {:error, %MyXQL.Error{message: "connection refused"}} =
               MyXQL.connect(Keyword.put(@opts, :port, 9999))
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
