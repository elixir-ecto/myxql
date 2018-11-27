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

    # TODO:
    # test "multiple results", c do
    #   assert [%{rows: [[1]]}, %{rows: [[2]]}] = MyXQL.query!(c.conn, "SELECT 1; SELECT 2", [], query_type: :text)
    # end

    test "query before and after idle ping" do
      opts = Keyword.merge(@opts, backoff_type: :stop, idle_interval: 1)
      {:ok, pid} = MyXQL.start_link(opts)

      assert {:ok, _} = MyXQL.query(pid, "SELECT 42", [])
      :timer.sleep(20)
      assert {:ok, _} = MyXQL.query(pid, "SELECT 42", [])
      :timer.sleep(20)
      assert {:ok, _} = MyXQL.query(pid, "SELECT 42", [])
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

    test "prepare and close", c do
      {:ok, query} = MyXQL.prepare(c.conn, "", "SELECT ? * ?")
      :ok = MyXQL.close(c.conn, query)

      assert {:error, %MyXQL.Error{message: "Unknown prepared statement handler" <> _}} =
               MyXQL.execute(c.conn, query, [2, 3])
    end

    test "prepare from different connection and close", c do
      conn1 = c.conn
      {:ok, query1} = MyXQL.prepare(conn1, "", "SELECT ? * ?")

      {:ok, conn2} = MyXQL.start_link(@opts)
      :ok = MyXQL.close(conn2, query1)

      assert {:ok, _, %{rows: [[6]]}} = MyXQL.execute(conn1, query1, [2, 3])
    end

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
      reason = make_ref()

      {:error, ^reason} =
        MyXQL.transaction(c.conn, fn conn ->
          assert DBConnection.status(conn) == :transaction

          assert {:error, %MyXQL.Error{mysql: %{code: 1062}}} =
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

    test "many rows", c do
      values = Enum.map_join(1..10_000, ", ", &"(#{&1})")
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES " <> values)

      {:ok, _result} =
        MyXQL.transaction(c.conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers")
          Enum.to_list(stream)
        end)
    end

    test "multiple streams", c do
      MyXQL.query!(c.conn, "INSERT INTO integers VALUES (1), (2), (3), (4), (5)")

      {:ok, _} =
        MyXQL.transaction(c.conn, fn conn ->
          odd =
            MyXQL.stream(conn, "SELECT * FROM integers WHERE x % 2 != 0", [], max_rows: 2)
            |> Stream.flat_map(& &1.rows)

          even =
            MyXQL.stream(conn, "SELECT * FROM integers WHERE x % 2 = 0", [], max_rows: 2)
            |> Stream.flat_map(& &1.rows)

          assert Enum.zip(odd, even) == [{[1], [2]}, {[3], [4]}]
        end)
    end

    test "bad query" do
      {:ok, conn} = MyXQL.start_link(@opts)

      assert_raise MyXQL.Error, "Unknown column 'bad' in 'field list'", fn ->
        MyXQL.transaction(conn, fn conn ->
          stream = MyXQL.stream(conn, "SELECT bad")
          Enum.to_list(stream)
        end)
      end
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
    # TODO: is there a better way? Run in sandbox mode?
    MyXQL.query!(c.conn, "TRUNCATE TABLE integers")
    c
  end
end
