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

  defp prepared_stmt_count() do
    [%{"Value" => count}] = TestHelper.mysql!("show global status like 'Prepared_stmt_count'")
    String.to_integer(count)
  end
end
