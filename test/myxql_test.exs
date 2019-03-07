defmodule MyXQLTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  @opts TestHelper.opts()

  describe "connect" do
    @tag requires_ssl: true
    test "connect with default auth method and SSL" do
      opts = Keyword.merge(@opts, username: "default_auth", password: "secret", ssl: true)
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    test "connect with default auth method and no SSL" do
      opts = Keyword.merge(@opts, username: "default_auth", password: "secret", ssl: false)

      case TestHelper.default_auth_plugin() do
        "mysql_native_password" ->
          {:ok, conn} = MyXQL.start_link(opts)
          MyXQL.query!(conn, "SELECT 1")

        # requires SSL so this will never succeed
        "caching_sha2_password" ->
          assert capture_log(fn ->
                   assert_start_and_killed(opts)
                 end) =~ "** (MyXQL.Error) (2061) (CR_AUTH_PLUGIN_ERR)"
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
             end) =~ "** (MyXQL.Error) (1045) (ER_ACCESS_DENIED_ERROR)"
    end

    @tag auth_plugin: "sha256_password"
    test "connect with sha256_password and SSL" do
      opts = Keyword.merge(@opts, username: "sha256_password", password: "secret", ssl: true)
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1")
    end

    @tag auth_plugin: "sha256_password"
    test "connect with sha256_password, SSL and bad password" do
      assert capture_log(fn ->
               opts =
                 Keyword.merge(@opts, username: "sha256_password", password: "bad", ssl: true)

               assert_start_and_killed(opts)
             end) =~ "** (MyXQL.Error) (1045) (ER_ACCESS_DENIED_ERROR)"
    end

    @tag auth_plugin: "sha256_password"
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

    @tag requires_ssl: true
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
    test "connect using default protocol (:socket)" do
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
        |> Keyword.delete(:port)
        |> Keyword.put(:hostname, "intentionally_bad_host")
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

    @tag capture_log: true
    test "connect with SSL but without starting :ssl" do
      Application.stop(:ssl)

      assert_raise RuntimeError,
                   ~r"cannot be established because `:ssl` application is not started",
                   fn ->
                     opts = Keyword.merge(@opts, ssl: true)
                     MyXQL.start_link(opts)
                   end
    after
      Application.start(:ssl)
    end

    test "custom socket options" do
      opts = Keyword.merge(@opts, socket_options: [buffer: 4])
      {:ok, conn} = MyXQL.start_link(opts)

      MyXQL.query!(conn, "SELECT 1, 2, NOW()")
      MyXQL.prepare_execute!(conn, "", "SELECT 1, 2, NOW()")
    end

    test "after_connect callback" do
      pid = self()
      opts = Keyword.merge(@opts, after_connect: fn conn -> send(pid, {:connected, conn}) end)
      MyXQL.start_link(opts)
      assert_receive {:connected, _}
    end

    test "handshake timeout" do
      %{port: port} =
        start_fake_server(fn _ ->
          Process.sleep(:infinity)
        end)

      opts = Keyword.merge(@opts, port: port, handshake_timeout: 5)

      assert capture_log(fn ->
               assert_start_and_killed(opts)
             end) =~ "timed out because it was handshaking for longer than 5ms"
    end
  end

  describe "query" do
    setup [:connect, :truncate]

    test "default to binary protocol", c do
      self = self()
      {:ok, _} = MyXQL.query(c.conn, "SELECT 42", [], log: &send(self, &1))
      assert_received %DBConnection.LogEntry{} = entry
      assert %MyXQL.Query{} = entry.query
    end

    test "binary: query with params", c do
      assert {:ok, %MyXQL.Result{rows: [[6]]}} = MyXQL.query(c.conn, "SELECT ? * ?", [2, 3])
    end

    test "binary: iodata", c do
      statement = ["SELECT", [" ", ["42"]]]

      assert {:ok, %{rows: [[42]]}} =
               MyXQL.query(c.conn, statement, [], query_type: :binary, log: &send(self(), &1))

      assert_received %DBConnection.LogEntry{} = entry
      assert %MyXQL.Query{} = entry.query
    end

    test "text: iodata", c do
      statement = ["SELECT", [" ", ["42"]]]

      assert {:ok, %{rows: [[42]]}} =
               MyXQL.query(c.conn, statement, [], query_type: :text, log: &send(self(), &1))

      assert_received %DBConnection.LogEntry{} = entry
      assert %MyXQL.TextQuery{} = entry.query
    end

    test "non preparable statement", c do
      assert {:ok, %MyXQL.Result{}} = MyXQL.query(c.conn, "BEGIN", [], query_type: :text)

      assert {:error, %MyXQL.Error{mysql: %{code: 1295, name: :ER_UNSUPPORTED_PS}}} =
               MyXQL.query(c.conn, "BEGIN", [], query_type: :binary)

      assert {:ok, %MyXQL.Result{}} =
               MyXQL.query(c.conn, "BEGIN", [], query_type: :binary_then_text)
    end

    for protocol <- [:binary, :text] do
      @protocol protocol

      test "#{@protocol}: invalid query", c do
        assert {:error, %MyXQL.Error{mysql: %{name: :ER_BAD_FIELD_ERROR}}} =
                 MyXQL.query(c.conn, "SELECT bad", [], query_type: @protocol)
      end

      test "#{@protocol}: query with multiple rows", c do
        %MyXQL.Result{num_rows: 2} =
          MyXQL.query!(c.conn, "INSERT INTO integers VALUES (10), (20)", [], query_type: @protocol)

        assert {:ok, %MyXQL.Result{columns: ["x"], rows: [[10], [20]]}} =
                 MyXQL.query(c.conn, "SELECT * FROM integers")
      end

      test "#{@protocol}: many rows", c do
        num = 10_000

        values = Enum.map_join(1..num, ", ", &"(#{&1})")

        result =
          MyXQL.query!(c.conn, "INSERT INTO integers VALUES " <> values, [], query_type: @protocol)

        assert result.num_rows == num

        result = MyXQL.query!(c.conn, "SELECT x FROM integers")
        assert List.flatten(result.rows) == Enum.to_list(1..num)
        assert result.num_rows == num
      end
    end
  end

  describe "prepared queries" do
    setup [:connect, :truncate]

    test "prepare_execute", c do
      assert {:ok, %MyXQL.Query{}, %MyXQL.Result{rows: [[6]]}} =
               MyXQL.prepare_execute(c.conn, "", "SELECT ? * ?", [2, 3])
    end

    test "prepare and then execute", c do
      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT ? * ?")

      assert query.num_params == 2
      assert {:ok, _, %MyXQL.Result{rows: [[6]]}} = MyXQL.execute(c.conn, query, [2, 3])
    end

    test "query is re-prepared if executed after being closed", c do
      {:ok, query1} = MyXQL.prepare(c.conn, "", "SELECT 42")
      assert {:ok, _, %MyXQL.Result{rows: [[42]]}} = MyXQL.execute(c.conn, query1, [])
      :ok = MyXQL.close(c.conn, query1)

      assert {:ok, query2, %MyXQL.Result{rows: [[42]]}} = MyXQL.execute(c.conn, query1, [])
      assert query1.ref != query2.ref
    end

    test "query is re-prepared if executed from different connection", c do
      conn1 = c.conn
      {:ok, query1} = MyXQL.prepare(conn1, "", "SELECT 42")

      {:ok, conn2} = MyXQL.start_link(@opts)
      {:ok, query2, %{rows: [[42]]}} = MyXQL.execute(conn2, query1)
      assert query1.ref != query2.ref
    end

    # This test is just describing existing behaviour, we may want to change it in the future.
    test "prepared query is not re-prepared after schema change", c do
      MyXQL.query!(c.conn, "CREATE TABLE test_prepared_schema_change (x integer)")
      MyXQL.query!(c.conn, "INSERT INTO test_prepared_schema_change VALUES (1)")

      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT * FROM test_prepared_schema_change")
      MyXQL.query!(c.conn, "ALTER TABLE test_prepared_schema_change ADD y integer DEFAULT 2")

      {:ok, query2, result} = MyXQL.execute(c.conn, query)
      assert result.rows == [[1, 2]]
      assert query.ref == query2.ref
    after
      MyXQL.query!(c.conn, "DROP TABLE IF EXISTS test_prepared_schema_change")
    end

    test "invalid number of params", c do
      assert_raise ArgumentError, ~r"parameters must be of length 2 for query", fn ->
        MyXQL.query(c.conn, "SELECT ? * ?", [1])
      end
    end

    test "many rows", c do
      num = 10_000

      values = Enum.map_join(1..num, ", ", &"(#{&1})")
      result = MyXQL.query!(c.conn, "INSERT INTO integers VALUES " <> values)
      assert result.num_rows == num

      {_query, result} = MyXQL.prepare_execute!(c.conn, "", "SELECT x FROM integers")
      assert List.flatten(result.rows) == Enum.to_list(1..num)
      assert result.num_rows == num
    end

    test "named and unnamed queries" do
      {:ok, pid} = MyXQL.start_link(@opts ++ [prepare: :named])
      {:ok, query} = MyXQL.prepare(pid, "1", "SELECT 1")
      {:ok, query2, _} = MyXQL.execute(pid, query)
      assert query.ref == query2.ref
      {:ok, query3, _} = MyXQL.execute(pid, query)
      assert query.ref == query3.ref

      # unnamed queries are closed
      {:ok, query} = MyXQL.prepare(pid, "", "SELECT 1")
      {:ok, query2, _} = MyXQL.execute(pid, query)
      assert query.ref == query2.ref
      {:ok, query3, _} = MyXQL.execute(pid, query)
      assert query2.ref != query3.ref

      {:ok, pid} = MyXQL.start_link(@opts ++ [prepare: :unnamed])
      {:ok, query} = MyXQL.prepare(pid, "1", "SELECT 1")
      {:ok, query2, _} = MyXQL.execute(pid, query)
      assert query.ref == query2.ref
      {:ok, query3, _} = MyXQL.execute(pid, query)
      assert query2.ref != query3.ref
    end

    test "disconnect on errors" do
      Process.flag(:trap_exit, true)

      {:ok, pid} =
        MyXQL.start_link(
          @opts ++ [disconnect_on_error_codes: [:ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION]]
        )

      MyXQL.query!(pid, "SET TRANSACTION READ ONLY")

      assert capture_log(fn ->
               MyXQL.transaction(pid, fn conn ->
                 MyXQL.query(conn, "INSERT INTO integers VALUES (1)")
                 assert_receive {:EXIT, ^pid, :killed}, 500
               end)
             end) =~ "disconnected: ** (MyXQL.Error) (1792) (ER_CANT_EXECUTE_IN_READ_ONLY"
    end
  end

  describe "transactions" do
    setup [:connect, :truncate]

    test "commit", c do
      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")
      self = self()

      {:ok, :success} =
        MyXQL.transaction(
          c.conn,
          fn conn ->
            MyXQL.query!(conn, "INSERT INTO integers VALUES (10)", [], log: &send(self(), &1))
            MyXQL.query!(conn, "INSERT INTO integers VALUES (20)", [], log: &send(self(), &1))
            :success
          end,
          log: &send(self, &1)
        )

      assert %MyXQL.Result{rows: [[2]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")

      assert_receive %DBConnection.LogEntry{} = begin_entry
      assert_receive %DBConnection.LogEntry{} = query1_entry
      assert_receive %DBConnection.LogEntry{} = query2_entry
      assert_receive %DBConnection.LogEntry{} = commit_entry

      assert begin_entry.call == :begin
      assert begin_entry.query == :begin
      assert {:ok, _, %MyXQL.Result{}} = begin_entry.result

      assert query1_entry.call == :prepare_execute
      assert query2_entry.call == :prepare_execute

      assert commit_entry.call == :commit
      assert commit_entry.query == :commit
      assert {:ok, %MyXQL.Result{}} = commit_entry.result
    end

    test "rollback", c do
      self = self()
      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")
      reason = make_ref()

      {:error, ^reason} =
        MyXQL.transaction(
          c.conn,
          fn conn ->
            MyXQL.query!(conn, "INSERT INTO integers VALUES (10)", [], log: &send(self, &1))
            MyXQL.rollback(conn, reason)
          end,
          log: &send(self(), &1)
        )

      assert %MyXQL.Result{rows: [[0]]} = MyXQL.query!(c.conn, "SELECT COUNT(1) FROM integers")

      assert_receive %DBConnection.LogEntry{} = begin_entry
      assert_receive %DBConnection.LogEntry{} = query_entry
      assert_receive %DBConnection.LogEntry{} = rollback_entry

      assert begin_entry.call == :begin
      assert begin_entry.query == :begin

      assert query_entry.call == :prepare_execute

      assert rollback_entry.call == :rollback
      assert rollback_entry.query == :rollback
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
      {:ok, results} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers")
          Enum.to_list(stream)
        end)

      assert [
               %{rows: [], num_rows: 0},
               %{rows: [], num_rows: 0}
             ] = results

      # try again for the same query
      {:ok, results} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers")
          Enum.to_list(stream)
        end)

      assert [
               %{rows: [], num_rows: 0},
               %{rows: [], num_rows: 0}
             ] = results
    end

    test "few rows", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")

      {:ok, results} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers", [], max_rows: 2)
          Enum.to_list(stream)
        end)

      assert [
               %{rows: [], num_rows: 0},
               %{rows: [[1], [2]]},
               %{rows: [[3], [4]]},
               %{rows: [[5]]}
             ] = results
    end

    test "few rows with no leftovers", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4)")

      {:ok, results} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers", [], max_rows: 2)
          Enum.to_list(stream)
        end)

      assert [
               %{rows: [], num_rows: 0},
               %{rows: [[1], [2]]},
               %{rows: [[3], [4]]},
               %{rows: []}
             ] = results
    end

    test "many rows", c do
      num = 10_000

      values = Enum.map_join(1..10_000, ", ", &"(#{&1})")
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES " <> values)

      {:ok, results} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers")
          Enum.to_list(stream)
        end)

      [first | results] = results
      assert %{rows: [], num_rows: 0} = first

      assert length(results) == 21
      assert results |> Enum.map(& &1.rows) |> List.flatten() == Enum.to_list(1..num)
    end

    test "multiple streams", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")

      {:ok, results} =
        MyXQL.transaction(c.conn, fn conn ->
          odd =
            MyXQL.stream(conn, "SELECT * FROM integers WHERE x % 2 != 0", [], max_rows: 2)
            |> Stream.flat_map(& &1.rows)

          even =
            MyXQL.stream(conn, "SELECT * FROM integers WHERE x % 2 = 0", [], max_rows: 2)
            |> Stream.flat_map(& &1.rows)

          Enum.zip(odd, even)
        end)

      assert results == [{[1], [2]}, {[3], [4]}]
    end

    test "bad query", c do
      assert_raise MyXQL.Error, ~r"\(1054\) \(ER_BAD_FIELD_ERROR\)", fn ->
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT bad")
          Enum.to_list(stream)
        end)
      end
    end

    test "invalid params", c do
      assert_raise ArgumentError, ~r"parameters must be of length 2", fn ->
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT ? * ?", [1])
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

      assert [
               %{rows: [], num_rows: 0},
               %{rows: [[1], [2]]},
               %{rows: [[3], [4]]},
               %{rows: [[5]]}
             ] = result
    end

    test "on another connection", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")

      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT * FROM integers")

      {:ok, conn2} = MyXQL.start_link(@opts)

      {:ok, results} =
        MyXQL.transaction(conn2, fn conn ->
          MyXQL.stream(conn, query) |> Enum.to_list()
        end)

      assert [
               %{rows: [], num_rows: 0},
               %{rows: [[1], [2], [3], [4], [5]]}
             ] = results
    end
  end

  describe "stored procedures" do
    setup :connect

    test "text query", c do
      assert %MyXQL.Result{rows: [[1]]} =
               MyXQL.query!(c.conn, "CALL single_procedure()", [], query_type: :text)

      assert %MyXQL.Result{rows: [[1]]} =
               MyXQL.query!(c.conn, "CALL single_procedure()", [], query_type: :text)

      assert_raise RuntimeError, "returning multiple results is not yet supported", fn ->
        assert %MyXQL.Result{rows: [[1]]} = MyXQL.query!(c.conn, "CALL multi_procedure()")
      end
    end

    test "prepared query", c do
      assert {_, %MyXQL.Result{rows: [[1]]}} =
               MyXQL.prepare_execute!(c.conn, "", "CALL single_procedure()")

      assert {_, %MyXQL.Result{rows: [[1]]}} =
               MyXQL.prepare_execute!(c.conn, "", "CALL single_procedure()")

      assert_raise RuntimeError, "returning multiple results is not yet supported", fn ->
        MyXQL.prepare_execute!(c.conn, "", "CALL multi_procedure()")
      end
    end

    test "stream procedure with single result", c do
      statement = "CALL single_procedure()"

      {:ok, result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, statement, [], max_rows: 2)
          Enum.to_list(stream)
        end)

      assert [%MyXQL.Result{rows: [[1]]}] = result
    end

    test "stream procedure with multiple results", c do
      statement = "CALL multi_procedure()"

      assert_raise RuntimeError, "returning multiple results is not yet supported", fn ->
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, statement, [], max_rows: 2)
          Enum.to_list(stream)
        end)
      end
    end
  end

  describe "idle ping" do
    test "query before and after" do
      opts = Keyword.merge(@opts, backoff_type: :stop, idle_interval: 1)
      {:ok, pid} = MyXQL.start_link(opts)

      assert {:ok, _} = MyXQL.query(pid, "SELECT 42")
      Process.sleep(5)
      assert {:ok, _} = MyXQL.query(pid, "SELECT 42")
      Process.sleep(5)
      assert {:ok, _} = MyXQL.query(pid, "SELECT 42")
    end

    test "socket receive timeout" do
      Process.flag(:trap_exit, true)
      opts = Keyword.merge(@opts, backoff_type: :stop, idle_interval: 1, ping_timeout: 0)
      {:ok, pid} = MyXQL.start_link(opts)

      assert capture_log(fn ->
               assert_receive {:EXIT, ^pid, :killed}, 500
             end) =~ "disconnected: ** (MyXQL.Error) Unexpected error: timeout"
    end
  end

  test "warnings" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert %MyXQL.Result{num_warnings: 1, rows: [[nil]]} = MyXQL.query!(conn, "SELECT 1/0")
  end

  defp assert_start_and_killed(opts) do
    Process.flag(:trap_exit, true)

    case MyXQL.start_link(opts) do
      {:ok, pid} -> assert_receive {:EXIT, ^pid, :killed}, 500
      {:error, :killed} -> :ok
    end
  end

  defp connect(c) do
    {:ok, conn} = MyXQL.start_link(@opts)
    Map.put(c, :conn, conn)
  end

  defp truncate(c) do
    MyXQL.query!(c.conn, "TRUNCATE TABLE integers")
    c
  end

  defp start_fake_server(fun) do
    {:ok, listen_socket} = :gen_tcp.listen(0, mode: :binary, active: false)
    {:ok, port} = :inet.port(listen_socket)

    {:ok, pid} =
      Task.start_link(fn ->
        {:ok, accept_socket} = :gen_tcp.accept(listen_socket)
        fun.(%{accept_socket: accept_socket, listen_socket: listen_socket})
      end)

    %{pid: pid, port: port}
  end
end
