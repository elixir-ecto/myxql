defmodule MyXQL.ClientTest do
  use ExUnit.Case, async: true
  alias MyXQL.{Client, Protocol}
  import MyXQL.Protocol.{Flags, Records}

  @opts TestHelper.opts()

  describe "connect" do
    @tag public_key_exchange: true
    test "default auth plugin (public key exchange)" do
      opts = [username: "default_auth", password: "secret"] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag ssl: true
    test "default auth plugin (ssl)" do
      opts = [username: "default_auth", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag public_key_exchange: false, ssl: false
    test "default auth plugin (no secure authentication)" do
      opts = [username: "default_auth", password: "secret"] ++ @opts

      case Client.connect(opts) do
        # e.g. mysql_native_password doesn't require secure connection
        {:ok, _} ->
          :ok

        # e.g. sha256_password does
        {:error, err_packet(message: "Access denied" <> _)} ->
          :ok
      end
    end

    test "no password" do
      opts = [username: "nopassword"] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag ssl: true
    test "no password (ssl)" do
      opts = [username: "nopassword", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    # mysql_native_password

    @tag mysql_native_password: true
    test "mysql_native_password" do
      opts = [username: "mysql_native", password: "secret"] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag mysql_native_password: true
    test "mysql_native_password (bad password)" do
      opts = [username: "mysql_native", password: "bad"] ++ @opts
      assert {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag mysql_native_password: true, ssl: true
    test "mysql_native_password (ssl)" do
      opts = [username: "mysql_native", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    # sha256_password

    @tag sha256_password: true, public_key_exchange: true
    test "sha256_password" do
      opts = [username: "sha256_password", password: "secret"] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag sha256_password: true, ssl: true
    test "sha256_password (ssl)" do
      opts = [username: "sha256_password", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag sha256_password: true, public_key_exchange: true
    test "sha256_password (bad password)" do
      opts = [username: "sha256_password", password: "bad"] ++ @opts
      assert {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag sha256_password: true, ssl: true
    test "sha256_password (bad password) (ssl)" do
      opts = [username: "sha256_password", password: "bad", ssl: true] ++ @opts
      assert {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag sha256_password: true, ssl: true
    test "sha256_password (empty password) (ssl)" do
      opts = [username: "sha256_empty", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    # caching_sha2_password

    @tag caching_sha2_password: true, public_key_exchange: true
    test "caching_sha2_password (public key exchange)" do
      opts = [username: "caching_sha2_password", password: "secret"] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag caching_sha2_password: true, ssl: true
    test "caching_sha2_password (ssl)" do
      opts = [username: "caching_sha2_password", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag caching_sha2_password: true
    test "caching_sha2_password (bad password)" do
      opts = [username: "caching_sha2_password", password: "bad"] ++ @opts
      assert {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag caching_sha2_password: true, ssl: true
    test "caching_sha2_password (bad password) (ssl)" do
      opts = [username: "caching_sha2_password", password: "bad", ssl: true] ++ @opts
      assert {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    # other

    @tag ssl: false
    test "client requires ssl but server does not support it" do
      opts = [ssl: true] ++ @opts
      assert {:error, :server_does_not_support_ssl} = Client.connect(opts)
    end
  end

  describe "com_query/2" do
    setup :connect

    test "simple query", %{state: state} do
      {:ok, resultset(rows: rows)} = Client.com_query("SELECT 1024 as a, 2048 as b", state)
      assert rows == [[1024, 2048]]
    end
  end

  describe "com_stmt_prepare/2 + com_stmt_execute/2" do
    setup :connect

    test "no results", %{state: state} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare("select x from integers", state)

      {:ok,
       resultset(num_rows: 0, status_flags: status_flags, rows: rows, column_defs: column_defs)} =
        Client.com_stmt_execute(statement_id, [], :cursor_type_no_cursor, state)

      assert list_status_flags(status_flags) == [
               :server_status_autocommit,
               :server_status_no_index_used
             ]

      assert [column_def(name: "x")] = column_defs
      assert rows == []
    end

    test "no params", %{state: state} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare("select 1024 as a, 2048 as b", state)

      {:ok,
       resultset(num_rows: 1, status_flags: status_flags, rows: rows, column_defs: column_defs)} =
        Client.com_stmt_execute(statement_id, [], :cursor_type_no_cursor, state)

      assert [column_def(name: "a"), column_def(name: "b")] = column_defs
      assert [[1024, 2048]] = rows
      assert list_status_flags(status_flags) == [:server_status_autocommit]
    end

    test "params", %{state: state} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare("select ? as a, ? as b", state)

      {:ok,
       resultset(num_rows: 1, status_flags: status_flags, rows: rows, column_defs: column_defs)} =
        Client.com_stmt_execute(statement_id, [1024, 2048], :cursor_type_no_cursor, state)

      assert [column_def(name: "a"), column_def(name: "b")] = column_defs
      assert [[1024, 2048]] = rows
      assert list_status_flags(status_flags) == [:server_status_autocommit]
    end
  end

  describe "com_stmt_prepare + com_stmt_execute + com_stmt_fetch" do
    setup :connect

    test "with no results", %{state: state} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare("select * from integers", state)

      {:ok,
       resultset(num_rows: 0, status_flags: status_flags, rows: [], column_defs: column_defs)} =
        Client.com_stmt_execute(statement_id, [], :cursor_type_read_only, state)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 0, status_flags: status_flags, rows: [])} =
        Client.com_stmt_fetch(statement_id, column_defs, 5, state)

      refute :server_status_cursor_exists in list_status_flags(status_flags)
      assert :server_status_last_row_sent in list_status_flags(status_flags)
    end

    test "with simple query", %{state: state} do
      values = Enum.map_join(1..4, ", ", &"(#{&1})")
      {:ok, ok_packet()} = Client.com_query("insert into integers values #{values}", state)

      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare("select * from integers", state)

      {:ok,
       resultset(num_rows: 0, status_flags: status_flags, rows: [], column_defs: column_defs)} =
        Client.com_stmt_execute(statement_id, [], :cursor_type_read_only, state)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 2, status_flags: status_flags, rows: [[1], [2]])} =
        Client.com_stmt_fetch(statement_id, column_defs, 2, state)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 2, status_flags: status_flags, rows: [[3], [4]])} =
        Client.com_stmt_fetch(statement_id, column_defs, 2, state)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 0, status_flags: status_flags, rows: [])} =
        Client.com_stmt_fetch(statement_id, column_defs, 5, state)

      refute :server_status_cursor_exists in list_status_flags(status_flags)
      assert :server_status_last_row_sent in list_status_flags(status_flags)

      {:ok, err_packet(code: code)} = Client.com_stmt_fetch(statement_id, column_defs, 2, state)

      assert Protocol.error_code_to_name(code) == :ER_STMT_HAS_NO_OPEN_CURSOR
    end

    test "with stored procedure of single result", %{state: state} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare("CALL single_procedure()", state)

      {:ok, resultset(num_rows: 1, status_flags: status_flags)} =
        Client.com_stmt_execute(statement_id, [], :cursor_type_read_only, state)

      assert list_status_flags(status_flags) == [:server_status_autocommit]
    end

    test "with stored procedure of multiple results", %{state: state} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare("CALL multi_procedure()", state)

      assert {:error, :multiple_results} =
               Client.com_stmt_execute(statement_id, [], :cursor_type_read_only, state)
    end
  end

  describe "recv_packets/4" do
    test "simple" do
      %{port: port} =
        start_fake_server(fn %{accept_socket: sock} ->
          :gen_tcp.send(sock, <<3::24-little, 0, "foo">>)
        end)

      decoder = fn payload, _next_data, :initial ->
        {:halt, payload}
      end

      {:ok, state} = Client.do_connect(Client.Config.new(port: port))
      assert Client.recv_packets(decoder, :initial, state) == {:ok, "foo"}
    end
  end

  defp connect(_) do
    {:ok, state} = Client.connect(@opts)
    {:ok, ok_packet()} = Client.com_query("create temporary table integers (x int)", state)
    {:ok, [state: state]}
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
