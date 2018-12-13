defmodule MyXQLTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  @opts TestHelpers.opts()

  describe "connect" do
    test "connect with default auth method and SSL" do
      opts = Keyword.merge(@opts, username: "default_auth", password: "secret", ssl: true)
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with default auth method and no SSL" do
      opts = Keyword.merge(@opts, username: "default_auth", password: "secret", ssl: false)

      case default_auth_plugin() do
        "mysql_native_password" ->
          {:ok, conn} = MyXQL.start_link(opts)
          MyXQL.query!(conn, "SELECT 1")

        # requires SSL so this will never succeed
        "caching_sha2_password" ->
          assert capture_log(fn ->
                   assert_start_and_killed(opts)
                 end) =~ "** (MyXQL.Error) ERROR 2061 (HY000)"
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
             end) =~ ~r"\*\* \(MyXQL.Error\) .* 'sha256_password' .* requires secure connection"
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

    @tag requires_otp_19: true
    test "connect using UNIX domain socket is the default" do
      opts =
        @opts
        |> Keyword.delete(:hostname)
        |> Keyword.delete(:port)
        |> Keyword.delete(:socket)

      {:ok, conn} = MyXQL.start_link(opts)
      MyXQL.query!(conn, "SELECT 1")
    end

    @tag requires_otp_19: true
    test "connect using UNIX domain socket" do
      socket = System.get_env("MYSQL_UNIX_PORT") || "/tmp/mysql.sock"

      opts =
        @opts
        |> Keyword.delete(:hostname)
        |> Keyword.delete(:port)
        |> Keyword.merge(socket: socket)

      {:ok, conn} = MyXQL.start_link(opts)
      MyXQL.query!(conn, "SELECT 1")
    end

    @tag requires_otp_19: true
    test "connect using bad UNIX domain socket" do
      opts =
        @opts
        |> Keyword.delete(:hostname)
        |> Keyword.delete(:port)
        |> Keyword.merge(socket: "/bad")

      assert capture_log(fn ->
               assert_start_and_killed(opts)
             end) =~ "** (MyXQL.Error) no such file or directory"
    end
  end

  describe "query" do
    setup [:connect, :truncate]

    test "simple query", c do
      assert {:ok, %MyXQL.Result{columns: ["2*3", "4*5"], rows: [[6, 20]]}} =
               MyXQL.query(c.conn, "SELECT 2*3, 4*5")
    end

    test "iodata in text protocol", c do
      statement = ["SELECT", [" ", ["42"]]]
      assert {:ok, %{rows: [[42]]}} = MyXQL.query(c.conn, statement, [], query_type: :text)
    end

    test "iodata in binary protocol", c do
      statement = ["SELECT", [" ", ["42"]]]
      assert {:ok, %{rows: [[42]]}} = MyXQL.query(c.conn, statement, [], query_type: :binary)
    end

    test "invalid query", c do
      assert {:error, %MyXQL.Error{mysql: %{name: :ER_BAD_FIELD_ERROR}}} =
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

    test "query before and after idle ping" do
      opts = Keyword.merge(@opts, backoff_type: :stop, idle_interval: 1)
      {:ok, pid} = MyXQL.start_link(opts)

      assert {:ok, _} = MyXQL.query(pid, "SELECT 42", [])
      :timer.sleep(20)
      assert {:ok, _} = MyXQL.query(pid, "SELECT 42", [])
      :timer.sleep(20)
      assert {:ok, _} = MyXQL.query(pid, "SELECT 42", [])
    end

    test "text query with invalid number of params", c do
      assert_raise ArgumentError, ~r"parameters must be of length 0 for query", fn ->
        MyXQL.query(c.conn, "SELECT 42", [1], query_type: :text)
      end
    end

    test "binary query with invalid number of params", c do
      assert_raise ArgumentError, ~r"parameters must be of length 2 for query", fn ->
        MyXQL.query(c.conn, "SELECT ? * ?", [1], query_type: :binary)
      end
    end
  end

  describe "prepared statements" do
    setup [:connect, :truncate]

    test "prepare and execute", c do
      assert {:ok, %MyXQL.Result{rows: [[6]]}} = MyXQL.query(c.conn, "SELECT ? * ?", [2, 3])
    end

    test "prepare and then execute", c do
      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT ? * ?")

      assert {:ok, _, %MyXQL.Result{rows: [[6]]}} = MyXQL.execute(c.conn, query, [2, 3])
    end

    test "prepare, execute, close, and execute", c do
      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT 42")
      assert {:ok, _, %MyXQL.Result{rows: [[42]]}} = MyXQL.execute(c.conn, query, [])
      :ok = MyXQL.close(c.conn, query)

      assert {:ok, _, %MyXQL.Result{rows: [[42]]}} = MyXQL.execute(c.conn, query, [])
    end

    test "prepare from different connection and close", c do
      conn1 = c.conn
      {:ok, query1} = MyXQL.prepare(conn1, "", "SELECT 42")

      {:ok, conn2} = MyXQL.start_link(@opts)
      :ok = MyXQL.close(conn2, query1)

      assert {:ok, _, %{rows: [[42]]}} = MyXQL.execute(conn1, query1, [])
    end

    test "prepared statement from different connection is reprepared", c do
      conn1 = c.conn
      {:ok, query1} = MyXQL.prepare(conn1, "", "SELECT 42")

      {:ok, conn2} = MyXQL.start_link(@opts)
      {:ok, _, %{rows: [[42]]}} = MyXQL.execute(conn2, query1)
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
      reason = make_ref()

      {:error, ^reason} =
        MyXQL.transaction(c.conn, fn conn ->
          assert DBConnection.status(conn) == :transaction

          assert {:error, %MyXQL.Error{mysql: %{name: :ER_DUP_ENTRY}}} =
                   MyXQL.query(conn, "INSERT INTO uniques VALUES (1), (1)")

          MyXQL.rollback(conn, reason)
        end)

      assert DBConnection.status(c.conn) == :idle
      MyXQL.query!(c.conn, "SELECT 1")
    end
  end

  describe "stream" do
    setup [:connect, :truncate]

    test "empty", c do
      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers")
          Enum.to_list(stream)
        end)

      assert [%{rows: [], num_rows: 0}] = result

      # try again for the same query
      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers")
          Enum.to_list(stream)
        end)

      assert [%{rows: [], num_rows: 0}] = result
    end

    test "few rows", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")

      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers", [], max_rows: 2)
          Enum.to_list(stream)
        end)

      assert [%{rows: [[1], [2]]}, %{rows: [[3], [4]]}, %{rows: [[5]]}] = result
    end

    test "few rows with no leftovers", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4)")

      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers", [], max_rows: 2)
          Enum.to_list(stream)
        end)

      assert [%{rows: [[1], [2]]}, %{rows: [[3], [4]]}, %{rows: []}] = result
    end

    test "many rows", c do
      values = Enum.map_join(1..10_000, ", ", &"(#{&1})")
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES " <> values)

      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers")
          Enum.to_list(stream)
        end)

      assert 10_000 = result |> Enum.map(&length(&1.rows)) |> Enum.sum()
    end

    test "multiple streams", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")

      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          odd =
            MyXQL.stream(conn, "SELECT * FROM integers WHERE x % 2 != 0", [], max_rows: 2)
            |> Stream.flat_map(& &1.rows)

          even =
            MyXQL.stream(conn, "SELECT * FROM integers WHERE x % 2 = 0", [], max_rows: 2)
            |> Stream.flat_map(& &1.rows)

          Enum.zip(odd, even)
        end)

      assert result == [{[1], [2]}, {[3], [4]}]
    end

    test "bad query", c do
      assert_raise MyXQL.Error, "Unknown column 'bad' in 'field list'", fn ->
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT bad")
          Enum.to_list(stream)
        end)
      end
    end

    test "with prepared query", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")
      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT * FROM integers")

      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, query, [], max_rows: 2)
          Enum.to_list(stream)
        end)

      assert [%{rows: [[1], [2]]}, %{rows: [[3], [4]]}, %{rows: [[5]]}] = result
    end

    test "on another connection", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")

      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT * FROM integers")

      {:ok, conn2} = MyXQL.start_link(@opts)

      assert {:ok, [%{rows: [[1], [2], [3], [4], [5]]}]} =
               MyXQL.transaction(conn2, fn conn ->
                 MyXQL.stream(conn, query) |> Enum.to_list()
               end)
    end
  end

  # TODO:
  # describe "stored procedures" do
  #   setup [:connect, :truncate]

  #   test "multi-resultset", c do
  #     MyXQL.query!(c.conn, "CALL multi();", [], query_type: :text)
  #     |> IO.inspect()
  #   end
  # end

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
    MyXQL.query!(c.conn, "TRUNCATE TABLE integers", [], query_type: :text)
    c
  end

  defp default_auth_plugin() do
    {:ok, pid} = MyXQL.start_link(@opts)

    %MyXQL.Result{rows: [[_, plugin_name]]} =
      MyXQL.query!(pid, "SHOW VARIABLES WHERE variable_name = 'default_authentication_plugin'")

    plugin_name
  end
end
