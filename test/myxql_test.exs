defmodule MyXQLTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  @opts TestHelpers.opts()

  describe "connect" do
    test "connect with default auth method and SSL" do
      opts = Keyword.merge(@opts, username: "myxql_test", password: "secret", ssl: true)
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    @tag :skip
    test "connect with default auth method and no SSL" do
      opts = Keyword.merge(@opts, username: "myxql_test", password: "secret", ssl: false)

      case MyXQL.start_link(opts) do
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
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with mysql_native_password and bad password" do
      assert capture_log(fn ->
               opts = Keyword.merge(@opts, username: "mysql_native_password", password: "bad")
               assert_start_and_killed(opts)
             end) =~ "** (MyXQL.Error) Access denied for user 'mysql_native_password'"
    end

    test "connect with sha256_password and SSL" do
      opts = Keyword.merge(@opts, username: "sha256_password", password: "secret", ssl: true)
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with sha256_password, SSL and bad password" do
      assert capture_log(fn ->
               opts =
                 Keyword.merge(@opts, username: "sha256_password", password: "bad", ssl: true)

               assert_start_and_killed(opts)
             end) =~ "** (MyXQL.Error) Access denied for user 'sha256_password'"
    end

    test "connect with sha256_password and no SSL" do
      assert capture_log(fn ->
               opts =
                 Keyword.merge(@opts, username: "sha256_password", password: "secret", ssl: false)

               assert_start_and_killed(opts)
             end) =~ "** (MyXQL.Error)"
    end

    test "connect with no password" do
      opts = Keyword.merge(@opts, username: "nopassword", password: nil)
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with bad SSL opts" do
      assert capture_log(fn ->
               opts = Keyword.merge(@opts, ssl: true, ssl_opts: [ciphers: [:bad]])
               assert_start_and_killed(opts)
             end) =~ "** (MyXQL.Error) Invalid TLS option: {ciphers,[bad]}"
    end

    test "connect with host down" do
      assert capture_log(fn ->
               opts = Keyword.merge(@opts, port: 9999)
               assert_start_and_killed(opts)
             end) =~ "(MyXQL.Error) connection refused"
    end
  end

  describe "query" do
    setup [:connect, :truncate]

    test "simple query", c do
      assert {:ok, %MyXQL.Result{columns: ["2*3", "4*5"], rows: [[6, 20]]}} =
               MyXQL.query(c.conn, "SELECT 2*3, 4*5")
    end

    test "invalid query", c do
      assert {:error, %MyXQL.Error{message: "Unknown column 'bad' in 'field list'"}} =
               MyXQL.query(c.conn, "SELECT bad")
    end

    test "query with multiple rows", c do
      %MyXQL.Result{num_rows: 2} = MyXQL.query!(c.conn, "INSERT INTO integers VALUES (10), (20)")

      assert {:ok, %MyXQL.Result{columns: ["x"], rows: [[10], [20]]}} =
               MyXQL.query(c.conn, "SELECT * FROM integers")
    end

    test "insert many rows", c do
      values = Enum.map_join(1..10_000, ", ", &"(#{&1})")
      result = MyXQL.query!(c.conn, "INSERT INTO integers VALUES " <> values)
      assert result.num_rows == 10_000
    end
  end

  describe "prepared statements" do
    setup [:connect, :truncate]

    test "params", c do
      assert {:ok, %MyXQL.Result{rows: [[6]]}} = MyXQL.query(c.conn, "SELECT ? * ?", [2, 3])
    end

    test "prepare and then execute", c do
      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT ? * ?")

      assert {:ok, %MyXQL.Query{}, %MyXQL.Result{rows: [[6]]}} =
               MyXQL.execute(c.conn, query, [2, 3])
    end

    # TODO:
    # test "prepare and close", c do
    #   {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT ? * ?")
    #   :ok = MyXQL.close(c.conn, query)

    #   assert {:erorr, %MyXQL.Error{message: "closed statement etc" <> _}} = MyXQL.execute(c.conn, query, [2, 3])
    # end

    # TODO:
    # test "execute with invalid number of arguments", c do
    #   assert {:error, %MyXQL.Error{message: message}} = MyXQL.query(c.conn, "SELECT ? * ?", [1])
    #   assert message == "Wrong number of params etc"

    #   assert {:error, %MyXQL.Error{message: message}} = MyXQL.query(c.conn, "SELECT ? * ?", [1, 2, 3])
    #   assert message == "Wrong number of params etc"
    # end

    test "unprepared query is prepared on execute", c do
      query = %MyXQL.Query{statement: "SELECT ? * ?", ref: make_ref()}
      assert {:ok, _query, %MyXQL.Result{rows: [[6]]}} = MyXQL.execute(c.conn, query, [2, 3])
    end

    test "prepared statement from different connection is reprepared", c do
      conn1 = c.conn
      {:ok, query1} = MyXQL.prepare(conn1, "", "SELECT 1")

      {:ok, conn2} = MyXQL.start_link(@opts)
      {:ok, query2, _result} = MyXQL.execute(conn2, query1)
      assert query1.ref != query2.ref
    end
  end

  describe "transactions" do
    setup [:connect, :truncate]

    test "commit", c do
      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")
      result = make_ref()

      {:ok, ^result} =
        MyXQL.transaction(c.conn, fn conn ->
          MyXQL.query!(conn, "INSERT INTO integers VALUES (10)")
          MyXQL.query!(conn, "INSERT INTO integers VALUES (20)")
          result
        end)

      assert %MyXQL.Result{rows: [[2]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")
    end

    test "rollback", c do
      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")
      reason = make_ref()

      {:error, ^reason} =
        MyXQL.transaction(c.conn, fn conn ->
          MyXQL.query!(conn, "INSERT INTO integers VALUES (10)")
          MyXQL.rollback(conn, reason)
        end)

      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")
    end

    test "status", c do
      MyXQL.query!(c.conn, "SELECT 1")
      assert DBConnection.status(c.conn) == :idle

      MyXQL.transaction(c.conn, fn conn ->
        assert DBConnection.status(conn) == :transaction

        assert {:error, %MyXQL.Error{mysql: %{code: 1062}}} =
                 MyXQL.query(conn, "INSERT INTO uniques VALUES (1), (1)")

        # TODO:
        # assert DBConnection.status(conn) == :error
      end)

      assert DBConnection.status(c.conn) == :idle
      MyXQL.query!(c.conn, "SELECT 1")
    end
  end

  defp assert_start_and_killed(opts) do
    Process.flag(:trap_exit, true)

    case MyXQL.start_link(opts) do
      # TODO: see if we can go back to the default 100ms timeout
      {:ok, pid} -> assert_receive {:EXIT, ^pid, :killed}, 500
      {:error, :killed} -> :ok
    end
  end

  defp connect(c) do
    {:ok, conn} = MyXQL.start_link(@opts)
    Map.put(c, :conn, conn)
  end

  defp truncate(c) do
    # TODO: is there a better way? Run in sandbox mode?
    MyXQL.query!(c.conn, "TRUNCATE TABLE integers")
    c
  end
end
