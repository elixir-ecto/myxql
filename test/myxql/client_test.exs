defmodule MyXQL.ClientTest do
  use ExUnit.Case, async: true
  alias MyXQL.{Client, Protocol.ServerErrorCodes}, warn: false
  import MyXQL.Protocol.{Flags, Records}

  @opts TestHelper.opts()

  describe "connect" do
    test "default auth plugin" do
      opts = [username: "default_auth", password: "secret"] ++ @opts
      auth_plugin = TestHelper.default_auth_plugin()

      if auth_plugin in ["sha256_password", "caching_sha2_password"] do
        assert {:error,
                {:auth_plugin_error, {^auth_plugin, "Authentication requires secure connection"}}} =
                 Client.connect(opts)
      else
        assert {:ok, _} = Client.connect(opts)
      end
    end

    @tag :ssl
    test "default auth plugin (ssl)" do
      opts = [username: "default_auth", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    test "no password" do
      opts = [username: "nopassword"] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag :ssl
    test "no password (ssl)" do
      opts = [username: "nopassword", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag :mysql_native_password
    test "mysql_native_password" do
      opts = [username: "mysql_native_password", password: "secret"] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag mysql_native_password: true, ssl: true
    test "mysql_native_password (ssl)" do
      opts = [username: "mysql_native_password", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag :sha256_password
    test "sha256_password" do
      opts = [username: "sha256_password", password: "secret"] ++ @opts
      assert {:error, {:auth_plugin_error, _}} = Client.connect(opts)
    end

    @tag sha256_password: true, ssl: true
    test "sha256_password (ssl)" do
      opts = [username: "sha256_password", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag :caching_sha2_password
    test "caching_sha2_password" do
      opts = [username: "caching_sha2_password", password: "secret"] ++ @opts
      assert {:error, {:auth_plugin_error, _}} = Client.connect(opts)
    end

    @tag caching_sha2_password: true, ssl: true
    test "caching_sha2_password (ssl)" do
      opts = [username: "caching_sha2_password", password: "secret", ssl: true] ++ @opts
      assert {:ok, _} = Client.connect(opts)
    end

    @tag ssl: false
    test "client requires ssl but server does not support it" do
      opts = [ssl: true] ++ @opts
      assert {:error, :server_does_not_support_ssl} = Client.connect(opts)
    end
  end

  describe "com_stmt_execute and com_stmt_fetch" do
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

      assert ServerErrorCodes.code_to_name(code) == :ER_STMT_HAS_NO_OPEN_CURSOR
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

  defp connect(_) do
    {:ok, state} = Client.connect(@opts)
    {:ok, ok_packet()} = Client.com_query("create temporary table integers (x int)", state)
    {:ok, [state: state]}
  end
end
