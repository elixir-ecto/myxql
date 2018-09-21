defmodule MyxqlTest do
  use ExUnit.Case, async: true
  import Myxql.Messages

  test "myxql" do
    opts = [
      host: "127.0.0.1",
      port: 8006,
      user: "root",
      password: "secret",
      database: "myxql_test",
      timeout: 5000
    ]

    {:ok, conn} = Myxql.Protocol.connect(opts)

    assert resultset(column_definitions: [_, _], rows: [[6, 20]]) =
             Myxql.Protocol.query(conn, "SELECT 2*3, 4*5")

    assert ok_packet() = Myxql.Protocol.query(conn, "CREATE TABLE IF NOT EXISTS integers (x int)")

    assert ok_packet() = Myxql.Protocol.query(conn, "TRUNCATE TABLE integers")
    assert ok_packet() = Myxql.Protocol.query(conn, "INSERT INTO integers VALUES (10)")
    assert ok_packet() = Myxql.Protocol.query(conn, "INSERT INTO integers VALUES (20)")

    assert resultset(column_definitions: [column_definition41(name: "x")], rows: [[10], [20]]) =
             Myxql.Protocol.query(conn, "SELECT * FROM integers")

    assert com_stmt_prepare_ok(statement_id: statement_id) =
             Myxql.Protocol.prepare(conn, "SELECT x, x FROM integers")

    assert resultset(rows: rows) = Myxql.Protocol.execute(conn, statement_id)
    assert rows == [[10, 10], [20, 20]]

    assert err_packet(error_message: "Unknown column 'bad' in 'field list'") =
             Myxql.Protocol.prepare(conn, "SELECT bad")

    assert err_packet(error_message: "Unknown column 'bad' in 'field list'") =
             Myxql.Protocol.query(conn, "SELECT bad")
  end
end
