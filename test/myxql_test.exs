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
    test "simple query" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert {:ok, %MyXQL.Result{columns: ["2*3", "4*5"], rows: [[6, 20]]}} =
               MyXQL.query(conn, "SELECT 2*3, 4*5")
    end

    test "invalid query" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert {:error, %MyXQL.Error{message: "Unknown column 'bad' in 'field list'"}} =
               MyXQL.query(conn, "SELECT bad")
    end

    test "query with multiple rows" do
      {:ok, conn} = MyXQL.start_link(@opts)

      try do
        MyXQL.query!(conn, "TRUNCATE TABLE integers")
        %MyXQL.Result{num_rows: 2} = MyXQL.query!(conn, "INSERT INTO integers VALUES (10), (20)")

        assert {:ok, %MyXQL.Result{columns: ["x"], rows: [[10], [20]]}} =
                 MyXQL.query(conn, "SELECT * FROM integers")
      after
        # TODO: is there a better way? Run in sandbox mode?
        MyXQL.query!(conn, "TRUNCATE TABLE integers")
      end
    end
  end

  describe "prepared statements" do
    test "params" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert {:ok, %MyXQL.Result{rows: [[6]]}} = MyXQL.query(conn, "SELECT ? * ?", [2, 3])
    end

    test "prepare and then execute" do
      {:ok, conn} = MyXQL.start_link(@opts)

      {:ok, query} = MyXQL.prepare(conn, "", "SELECT ? * ?")

      assert {:ok, %MyXQL.Query{}, %MyXQL.Result{rows: [[6]]}} =
               MyXQL.execute(conn, query, [2, 3])
    end

    test "prepared statement from different connection is reprepared" do
      {:ok, conn1} = MyXQL.start_link(@opts)
      {:ok, query1} = MyXQL.prepare(conn1, "", "SELECT 1")

      {:ok, conn2} = MyXQL.start_link(@opts)
      {:ok, query2, _result} = MyXQL.execute(conn2, query1)
      assert query1.ref != query2.ref
    end
  end

  describe "transactions" do
    test "commit" do
      {:ok, conn} = MyXQL.start_link(@opts)

      try do
        assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(conn, "SELECT COUNT(1) FROM integers")
        result = make_ref()

        {:ok, ^result} =
          MyXQL.transaction(conn, fn conn ->
            MyXQL.query!(conn, "INSERT INTO integers VALUES (10)")
            MyXQL.query!(conn, "INSERT INTO integers VALUES (20)")
            result
          end)

        assert %MyXQL.Result{rows: [[2]]} = MyXQL.query!(conn, "SELECT COUNT(1) FROM integers")
      after
        MyXQL.query!(conn, "TRUNCATE TABLE integers")
      end
    end

    test "rollback" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(conn, "SELECT COUNT(1) FROM integers")
      reason = make_ref()

      {:error, ^reason} =
        MyXQL.transaction(conn, fn conn ->
          MyXQL.query!(conn, "INSERT INTO integers VALUES (10)")
          MyXQL.rollback(conn, reason)
        end)

      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(conn, "SELECT COUNT(1) FROM integers")
    end

    test "status" do
      {:ok, conn} = MyXQL.start_link(@opts)

      MyXQL.query!(conn, "SELECT 1")
      assert DBConnection.status(conn) == :idle

      MyXQL.transaction(conn, fn conn ->
        assert DBConnection.status(conn) == :transaction

        assert {:error, %MyXQL.Error{mysql: %{code: 1062}}} =
                 MyXQL.query(conn, "INSERT INTO uniques VALUES (1), (1)")

        # assert DBConnection.status(conn) == :error
      end)

      assert DBConnection.status(conn) == :idle
      MyXQL.query!(conn, "SELECT 1")
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
end
