defmodule MyXQLTest do
  use ExUnit.Case, async: true
  import MyXQL.Messages

  test "myxql" do
    opts = [
      hostname: "127.0.0.1",
      port: 8006,
      username: "root",
      password: "secret",
      database: "myxql_test",
      timeout: 5000
    ]

    {:ok, conn} = MyXQL.Protocol.connect(opts)

    assert resultset(column_definitions: [_, _], rows: [[6, 20]]) =
             MyXQL.Protocol.query(conn, "SELECT 2*3, 4*5")

    assert ok_packet() = MyXQL.Protocol.query(conn, "CREATE TABLE IF NOT EXISTS integers (x int)")

    assert ok_packet() = MyXQL.Protocol.query(conn, "TRUNCATE TABLE integers")
    assert ok_packet() = MyXQL.Protocol.query(conn, "INSERT INTO integers VALUES (10)")
    assert ok_packet() = MyXQL.Protocol.query(conn, "INSERT INTO integers VALUES (20)")

    assert resultset(column_definitions: [column_definition41(name: "x")], rows: [[10], [20]]) =
             MyXQL.Protocol.query(conn, "SELECT * FROM integers")

    assert com_stmt_prepare_ok(statement_id: statement_id) =
             MyXQL.Protocol.prepare(conn, "SELECT x, x FROM integers")

    assert resultset(rows: rows) = MyXQL.Protocol.execute(conn, statement_id, [])
    assert rows == [[10, 10], [20, 20]]

    assert com_stmt_prepare_ok(statement_id: statement_id) =
             MyXQL.Protocol.prepare(conn, "SELECT ? * ?")

    assert resultset(rows: rows) = MyXQL.Protocol.execute(conn, statement_id, [2, 3])
    assert rows == [[6]]

    assert err_packet(error_message: "Unknown column 'bad' in 'field list'") =
             MyXQL.Protocol.prepare(conn, "SELECT bad")

    assert err_packet(error_message: "Unknown column 'bad' in 'field list'") =
             MyXQL.Protocol.query(conn, "SELECT bad")
  end
end
