defmodule MyXQL.ClientTest do
  use ExUnit.Case, async: true
  alias MyXQL.{Client, Protocol}
  import MyXQL.Protocol.{Flags, Records}

  @opts TestHelper.opts()

  describe "connect" do
    @tag public_key_exchange: true
    test "default auth plugin (public key exchange)" do
      opts = [username: "default_auth", password: "secret"] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.disconnect(client)
    end

    @tag ssl: true
    test "default auth plugin (ssl)" do
      opts = [username: "default_auth", password: "secret", ssl: true] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag public_key_exchange: false, ssl: false
    test "default auth plugin (no secure authentication)" do
      opts = [username: "default_auth", password: "secret"] ++ @opts

      case Client.connect(opts) do
        # e.g. mysql_native_password doesn't require secure connection
        {:ok, client} ->
          Client.com_quit(client)

        # e.g. sha256_password does
        {:error, err_packet(message: "Access denied" <> _)} ->
          :ok
      end
    end

    test "no password" do
      opts = [username: "nopassword"] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)

      opts = [username: "nopassword", password: ""] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag ssl: true
    test "no password (ssl)" do
      opts = [username: "nopassword", ssl: true] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)

      opts = [username: "nopassword", password: ""] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    # mysql_native_password

    @tag mysql_native_password: true
    test "mysql_native_password" do
      opts = [username: "mysql_native", password: "secret"] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag mysql_native_password: true
    test "mysql_native_password (bad password)" do
      opts = [username: "mysql_native", password: "bad"] ++ @opts
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag mysql_native_password: true, ssl: true
    test "mysql_native_password (ssl)" do
      opts = [username: "mysql_native", password: "secret", ssl: true] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    # mysql_clear_password

    test "mysql_clear_password" do
      opts = [username: "mysql_clear", password: "secret", enable_cleartext_plugin: true] ++ @opts
      %{port: port} = start_cleartext_fake_server()
      opts = Keyword.put(opts, :port, port)
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    test "mysql_clear_password (bad password)" do
      opts = [username: "mysql_clear", password: "bad", enable_cleartext_plugin: true] ++ @opts
      %{port: port} = start_cleartext_fake_server()
      opts = Keyword.put(opts, :port, port)
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    # sha256_password

    @tag sha256_password: true, public_key_exchange: true
    test "sha256_password" do
      opts = [username: "sha256_password", password: "secret"] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag sha256_password: true, ssl: true
    test "sha256_password (ssl)" do
      opts = [username: "sha256_password", password: "secret", ssl: true] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag sha256_password: true, public_key_exchange: true
    test "sha256_password (bad password)" do
      opts = [username: "sha256_password", password: "bad"] ++ @opts
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag sha256_password: true, ssl: true
    test "sha256_password (bad password) (ssl)" do
      opts = [username: "sha256_password", password: "bad", ssl: true] ++ @opts
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag sha256_password: true, ssl: true
    test "sha256_password (empty password) (ssl)" do
      opts = [username: "sha256_empty", ssl: true] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    # Try long passwords that force us to apply the scramble multiple times when XORing

    @tag sha256_password: true, public_key_exchange: true
    test "sha256_password (long password)" do
      opts = [username: "sha256_password_long", password: "secretsecretsecretsecret"] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag sha256_password: true, public_key_exchange: true
    test "sha256_password (long password) (bad password)" do
      opts = [username: "sha256_password_long", password: "badbadbadbadbadbadbad"] ++ @opts
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    # caching_sha2_password

    @tag caching_sha2_password: true, public_key_exchange: true
    test "caching_sha2_password (public key exchange)" do
      opts = [username: "caching_sha2_password", password: "secret"] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag caching_sha2_password: true, ssl: true
    test "caching_sha2_password (ssl)" do
      opts = [username: "caching_sha2_password", password: "secret", ssl: true] ++ @opts
      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag caching_sha2_password: true
    test "caching_sha2_password (bad password)" do
      opts = [username: "caching_sha2_password", password: "bad"] ++ @opts
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    @tag caching_sha2_password: true, ssl: true
    test "caching_sha2_password (bad password) (ssl)" do
      opts = [username: "caching_sha2_password", password: "bad", ssl: true] ++ @opts
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    # Try long passwords that force us to apply the scramble multiple times when XORing

    @tag caching_sha2_password: true, public_key_exchange: true
    test "caching_sha2_password (long password) (public key exchange)" do
      opts =
        [username: "caching_sha2_password_long", password: "secretsecretsecretsecret"] ++ @opts

      assert {:ok, client} = Client.connect(opts)
      Client.com_quit(client)
    end

    @tag caching_sha2_password: true
    test "caching_sha2_password (long password) (bad password)" do
      opts = [username: "caching_sha2_password_long", password: "badbadbadbadbadbadbad"] ++ @opts
      {:error, err_packet(message: "Access denied" <> _)} = Client.connect(opts)
    end

    # other

    @tag ssl: false
    test "client requires ssl but server does not support it" do
      opts = [ssl: true] ++ @opts
      assert {:error, :server_does_not_support_ssl} = Client.connect(opts)
    end

    test "default charset" do
      {:ok, client} = Client.connect(@opts)

      {:ok, resultset(rows: [[charset, collation]])} =
        Client.com_query(client, "select @@character_set_connection, @@collation_connection")

      assert charset == "utf8mb4"
      assert collation =~ "utf8mb4_"

      {:ok, resultset(rows: [["hello 😃"]])} = Client.com_query(client, "SELECT 'hello 😃'")
      Client.com_quit(client)
    end

    test "set charset" do
      {:ok, client} = Client.connect([charset: "latin1"] ++ @opts)

      {:ok, resultset(rows: [[charset, collation]])} =
        Client.com_query(client, "select @@character_set_connection, @@collation_connection")

      assert charset == "latin1"
      assert collation == "latin1_swedish_ci"
      Client.com_quit(client)
    end

    test "set charset and collation" do
      {:ok, client} = Client.connect([charset: "latin1", collation: "latin1_general_ci"] ++ @opts)

      {:ok, resultset(rows: [[charset, collation]])} =
        Client.com_query(client, "select @@character_set_connection, @@collation_connection")

      assert charset == "latin1"
      assert collation == "latin1_general_ci"
      Client.com_quit(client)
    end
  end

  describe "com_query/2" do
    setup :connect

    test "simple query", %{client: client} do
      {:ok, resultset(rows: rows)} = Client.com_query(client, "SELECT 1024 as a, 2048 as b")
      assert rows == [[1024, 2048]]
      Client.com_quit(client)
    end
  end

  describe "com_stmt_prepare/2 + com_stmt_execute/2" do
    setup :connect

    test "no results", %{client: client} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "select x from integers")

      {:ok,
       resultset(num_rows: 0, status_flags: status_flags, rows: rows, column_defs: column_defs)} =
        Client.com_stmt_execute(client, statement_id, [], :cursor_type_no_cursor)

      assert list_status_flags(status_flags) == [
               :server_status_autocommit,
               :server_status_no_index_used
             ]

      [column_def(name: "x")] = column_defs
      assert rows == []
      Client.com_quit(client)
    end

    test "no params", %{client: client} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "select 1024 as a, 2048 as b")

      {:ok,
       resultset(num_rows: 1, status_flags: status_flags, rows: rows, column_defs: column_defs)} =
        Client.com_stmt_execute(client, statement_id, [], :cursor_type_no_cursor)

      [column_def(name: "a"), column_def(name: "b")] = column_defs
      assert [[1024, 2048]] = rows
      assert list_status_flags(status_flags) == [:server_status_autocommit]
      Client.com_quit(client)
    end

    test "params", %{client: client} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "select ? as a, ? as b")

      {:ok,
       resultset(num_rows: 1, status_flags: status_flags, rows: rows, column_defs: column_defs)} =
        Client.com_stmt_execute(client, statement_id, [1024, 2048], :cursor_type_no_cursor)

      [column_def(name: "a"), column_def(name: "b")] = column_defs
      assert [[1024, 2048]] = rows
      assert list_status_flags(status_flags) == [:server_status_autocommit]
      Client.com_quit(client)
    end

    test "encode large packets", %{client: client} do
      x = String.duplicate("x", 20_000_000)

      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "select length(?)")

      {:ok, resultset(rows: rows)} =
        Client.com_stmt_execute(client, statement_id, [x], :cursor_type_no_cursor)

      assert rows == [[20_000_000]]
      Client.com_quit(client)
    end
  end

  describe "com_stmt_prepare + com_stmt_execute + com_stmt_fetch" do
    setup :connect

    test "with no results", %{client: client} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "select * from integers")

      {:ok,
       resultset(num_rows: 0, status_flags: status_flags, rows: [], column_defs: column_defs)} =
        Client.com_stmt_execute(client, statement_id, [], :cursor_type_read_only)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 0, status_flags: status_flags, rows: [])} =
        Client.com_stmt_fetch(client, statement_id, column_defs, 5)

      refute :server_status_cursor_exists in list_status_flags(status_flags)
      assert :server_status_last_row_sent in list_status_flags(status_flags)
      Client.com_quit(client)
    end

    test "with simple query", %{client: client} do
      values = Enum.map_join(1..4, ", ", &"(#{&1})")
      {:ok, ok_packet()} = Client.com_query(client, "insert into integers values #{values}")

      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "select * from integers")

      {:ok,
       resultset(num_rows: 0, status_flags: status_flags, rows: [], column_defs: column_defs)} =
        Client.com_stmt_execute(client, statement_id, [], :cursor_type_read_only)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 2, status_flags: status_flags, rows: [[1], [2]])} =
        Client.com_stmt_fetch(client, statement_id, column_defs, 2)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 2, status_flags: status_flags, rows: [[3], [4]])} =
        Client.com_stmt_fetch(client, statement_id, column_defs, 2)

      assert :server_status_cursor_exists in list_status_flags(status_flags)

      {:ok, resultset(num_rows: 0, status_flags: status_flags, rows: [])} =
        Client.com_stmt_fetch(client, statement_id, column_defs, 5)

      refute :server_status_cursor_exists in list_status_flags(status_flags)
      assert :server_status_last_row_sent in list_status_flags(status_flags)

      {:ok, err_packet(code: code)} = Client.com_stmt_fetch(client, statement_id, column_defs, 2)

      assert Protocol.error_code_to_name(code) == :ER_STMT_HAS_NO_OPEN_CURSOR
      Client.com_quit(client)
    end

    test "with stored procedure of single result", %{client: client} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "CALL single_procedure()")

      {:ok, resultset(num_rows: 1, status_flags: status_flags)} =
        Client.com_stmt_execute(client, statement_id, [], :cursor_type_read_only)

      assert list_status_flags(status_flags) == [:server_status_autocommit]
      Client.com_quit(client)
    end

    test "with stored procedure of multiple results", %{client: client} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "CALL multi_procedure()")

      assert {:error, :multiple_results} =
               Client.com_stmt_execute(client, statement_id, [], :cursor_type_read_only)

      Client.com_quit(client)
    end

    test "with stored procedure using a cursor", %{client: client} do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
        Client.com_stmt_prepare(client, "CALL cursor_procedure()")

      {:ok, resultset(num_rows: 1, rows: [[3]])} =
        Client.com_stmt_execute(client, statement_id, [], :cursor_type_read_only)

      # This will be called if, for instance, someone issues the procedure statement from Ecto.Adapters.SQL.query
      {:ok, resultset(num_rows: 1, rows: [[3]])} =
        Client.com_stmt_execute(client, statement_id, [], :cursor_type_no_cursor)

      Client.com_quit(client)
    end
  end

  describe "recv_packets/5" do
    test "simple" do
      %{port: port} =
        start_fake_server(fn %{accept_socket: sock} ->
          :gen_tcp.send(sock, <<3::24-little, 0, "foo">>)
        end)

      decoder = fn payload, _next_data, :initial ->
        {:halt, payload}
      end

      {:ok, client} = Client.do_connect(Client.Config.new(port: port))
      assert Client.recv_packets(client, decoder, :initial, :single) == {:ok, "foo"}
    end
  end

  describe "com_ping/2" do
    test "handles multiple packets" do
      %{port: port} =
        start_fake_server(fn %{accept_socket: sock} ->
          payload1 =
            <<255, 211, 7, 35, 72, 89, 48, 48, 48, 76, 111, 115, 116, 32, 99, 111, 110, 110, 101,
              99, 116, 105, 111, 110, 32, 116, 111, 32, 98, 97, 99, 107, 101, 110, 100, 32, 115,
              101, 114, 118, 101, 114, 46>>

          payload2 =
            <<255, 135, 7, 35, 48, 56, 83, 48, 49, 67, 111, 110, 110, 101, 99, 116, 105, 111, 110,
              32, 107, 105, 108, 108, 101, 100, 32, 98, 121, 32, 77, 97, 120, 83, 99, 97, 108,
              101, 58, 32, 82, 111, 117, 116, 101, 114>>

          :gen_tcp.send(sock, [
            <<byte_size(payload1)::24-little>>,
            0,
            payload1,
            <<byte_size(payload2)::24-little>>,
            1,
            payload2
          ])
        end)

      {:ok, client} = Client.do_connect(Client.Config.new(port: port))

      {:ok, err_packet(message: "Lost connection to backend server.")} =
        Client.com_ping(client, 100)
    end
  end

  defp connect(_) do
    {:ok, client} = Client.connect(@opts)
    {:ok, ok_packet()} = Client.com_query(client, "create temporary table integers (x int)")
    {:ok, [client: client]}
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


  defp start_cleartext_fake_server() do
    start_fake_server(fn %{accept_socket: sock} ->
      initial_handshake =
        <<74, 0, 0, 0, 10, 56, 46, 48, 46, 51, 53, 0, 127, 24, 4, 0, 93, 42, 61, 27, 60,
          38, 85, 12, 0, 255, 255, 255, 2, 0, 255, 223, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 39, 48, 10, 117, 54, 65, 74, 37, 125, 121, 93, 6, 0, 109, 121, 115, 113,
          108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114,
          100, 0>>

      client_auth_response =
         <<98, 0, 0, 1, 10, 162, 11, 0, 255, 255, 255, 0, 45, 0, 0, 0, 0, 0, 0, 0, 0, 0,
           0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 109, 121, 115, 113, 108, 95, 99,
           108, 101, 97, 114, 0, 20, 254, 122, 75, 71, 45, 200, 185, 238, 55, 229, 170,
           5, 207, 204, 65, 246, 243, 144, 91, 183, 109, 121, 120, 113, 108, 95, 116,
           101, 115, 116, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101,
           95, 112, 97, 115, 115, 119, 111, 114, 100, 0>>

      switch_auth_response =
        <<22, 0, 0, 2, 254, 109, 121, 115, 113, 108, 95, 99, 108, 101, 97, 114, 95, 112,
          97, 115, 115, 119, 111, 114, 100, 0>>

      client_switch_auth_response =
        <<7, 0, 0, 3, 115, 101, 99, 114, 101, 116, 0>>

      ok_response =
        <<7, 0, 0, 4, 0, 0, 0, 2, 0, 0, 0>>

      client_quit = <<1, 0, 0, 0, 1>>

      auth_response_invalid =
        <<83, 0, 0, 1, 255, 21, 4, 35, 50, 56, 48, 48, 48, 65, 99, 99, 101, 115, 115,
          32, 100, 101, 110, 105, 101, 100, 32, 102, 111, 114, 32, 117, 115, 101, 114,
          32, 39, 100, 101, 102, 97, 117, 108, 116, 95, 97, 117, 116, 104, 39, 64, 39,
          49, 57, 50, 46, 49, 54, 56, 46, 54, 53, 46, 49, 39, 32, 40, 117, 115, 105,
          110, 103, 32, 112, 97, 115, 115, 119, 111, 114, 100, 58, 32, 89, 69, 83, 41>>

      :gen_tcp.send(sock, initial_handshake)

      case :gen_tcp.recv(sock, 0) do
        {:ok, ^client_auth_response} ->
          :ok = :gen_tcp.send(sock, switch_auth_response)
          {:ok, ^client_switch_auth_response} = :gen_tcp.recv(sock, 0)
          :ok = :gen_tcp.send(sock, ok_response)
          {:ok, ^client_quit} = :gen_tcp.recv(sock, 0)
          :ok = :gen_tcp.send(sock, ok_response)

        {:ok, _other} ->
          :ok = :gen_tcp.send(sock, auth_response_invalid)
      end
    end)
  end
end
