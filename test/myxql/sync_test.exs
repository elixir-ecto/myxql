# used for tests that affect global state
defmodule MyXQL.SyncTest do
  use ExUnit.Case

  @opts TestHelper.opts()

  test "do not leak statements with :cache_statement and prepare: :unnamed" do
    {:ok, conn} = MyXQL.start_link(@opts ++ [prepare: :unnamed])
    assert prepared_stmt_count() == 0

    MyXQL.query!(conn, "SELECT 42", [], cache_statement: "42")
    assert prepared_stmt_count() == 0
  end

  test "do not leak statements with streams" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    MyXQL.transaction(conn, fn conn ->
      stream = MyXQL.stream(conn, "SELECT 42")
      Enum.to_list(stream)
    end)

    assert prepared_stmt_count() == 0
  end

  test "do not leak statements with prepare+stream" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    {:ok, query} = MyXQL.prepare(conn, "", "SELECT * FROM integers")

    {:ok, [_, _]} =
      MyXQL.transaction(conn, fn conn ->
        Enum.to_list(MyXQL.stream(conn, query))
      end)

    assert prepared_stmt_count() == 0
  end

  test "do not leak statements with prepare+close" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    {:ok, query} = MyXQL.prepare(conn, "", "SELECT * FROM integers")
    :ok = MyXQL.close(conn, query)
    assert prepared_stmt_count() == 0
  end

  defp prepared_stmt_count() do
    [%{"Value" => count}] = TestHelper.mysql!("show global status like 'Prepared_stmt_count'")
    String.to_integer(count)
  end
end
