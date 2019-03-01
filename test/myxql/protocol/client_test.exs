defmodule MyXQL.Protocol.ClientTest do
  use ExUnit.Case, async: true
  alias MyXQL.Protocol.Client
  import MyXQL.Protocol.{Flags, Records}

  test "com_stmt_fetch" do
    {:ok, state} = Client.connect(database: "wojtek")
    {:ok, ok_packet()} = Client.com_query("create temporary table integers (x int)", state)

    values = Enum.map_join(1..4, ", ", &"(#{&1})")

    {:ok, ok_packet(affected_rows: 4)} =
      Client.com_query("insert into integers values #{values}", state)

    {:ok, com_stmt_prepare_ok(statement_id: statement_id)} =
      Client.com_stmt_prepare("select * from integers", state)

    {:ok, resultset(num_rows: 0, status_flags: status_flags, rows: [], column_defs: column_defs)} =
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

    assert {:ok, err_packet(message: "The statement (1) has no open cursor.")} =
             Client.com_stmt_fetch(statement_id, column_defs, 2, state)
  end
end
