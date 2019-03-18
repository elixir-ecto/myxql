defmodule MyXQL.ClientTest do
  use ExUnit.Case, async: true
  alias MyXQL.{Client, Protocol.ServerErrorCodes}
  import MyXQL.Protocol.{Flags, Records}

  setup do
    {:ok, state} = Client.connect(TestHelper.opts())
    {:ok, ok_packet()} = Client.com_query("create temporary table integers (x int)", state)
    [state: state]
  end

  describe "com_stmt_execute and com_stmt_fetch" do
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
end
