# used for tests that affect global state
defmodule MyXQL.SyncTest do
  use ExUnit.Case

  @opts TestHelper.opts()

  test "do not leak statements with :cache_statement and prepare: :unnamed" do
    {:ok, conn} = MyXQL.start_link(@opts ++ [prepare: :unnamed])
    assert prepared_stmt_count() == 0

    # Multiple queries do not increase statement count
    MyXQL.query!(conn, "SELECT 42", [], cache_statement: "42")
    assert prepared_stmt_count() == 1

    MyXQL.query!(conn, "SELECT 43", [], cache_statement: "43")
    assert prepared_stmt_count() == 1

    # Multiple preparations don't increase statement count
    {:ok, _query} = MyXQL.prepare(conn, "1", "SELECT 1")
    assert prepared_stmt_count() == 1

    {:ok, _query} = MyXQL.prepare(conn, "2", "SELECT 2")
    assert prepared_stmt_count() == 1
  end

  test "do not leak statements with prepare: :named" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    MyXQL.query!(conn, "SELECT 42", [], cache_statement: "42")
    assert prepared_stmt_count() == 1

    MyXQL.query!(conn, "SELECT 1337", [], cache_statement: "1337")
    assert prepared_stmt_count() == 2

    MyXQL.query!(conn, "SELECT 42", [], cache_statement: "42")
    assert prepared_stmt_count() == 2

    MyXQL.query!(conn, "SELECT 43", [])
    assert prepared_stmt_count() == 2
  end

  test "do not leak statements with rebound :cache_statement" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    MyXQL.query!(conn, "SELECT 42", [], cache_statement: "select number")
    assert prepared_stmt_count() == 1

    MyXQL.query!(conn, "SELECT 34", [], cache_statement: "select number")
    assert prepared_stmt_count() == 1
  end

  test "do not leak statements with insert and failed insert with prepare: :named" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    {:ok, _} = MyXQL.query(conn, "INSERT INTO uniques(a) VALUES (1)")
    assert prepared_stmt_count() == 0

    {:error, _} = MyXQL.query(conn, "INSERT INTO uniques(a) VALUES (1)")
    assert prepared_stmt_count() == 0
  end

  test "do not leak statements with insert and failed insert with prepare: :unnamed" do
    {:ok, conn} = MyXQL.start_link(@opts ++ [prepare: :unnamed])
    assert prepared_stmt_count() == 0

    {:ok, _} = MyXQL.query(conn, "INSERT INTO uniques(a) VALUES (2)")
    assert prepared_stmt_count() == 1

    {:error, _} = MyXQL.query(conn, "INSERT INTO uniques(a) VALUES (2)")
    assert prepared_stmt_count() == 1

    MyXQL.query!(conn, "SELECT 123", [], cache_statement: "123")
    assert prepared_stmt_count() == 1
  end

  test "do not leak statements on multiple executions of the same name in prepare_execute" do
    {:ok, conn} = MyXQL.start_link(@opts)
    {:ok, _, _} = MyXQL.prepare_execute(conn, "foo", "SELECT 42")
    {:ok, _, _} = MyXQL.prepare_execute(conn, "foo", "SELECT 42")
    assert prepared_stmt_count() == 1
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
    num = 1500
    values = Enum.map_join(1..num, ", ", &"(#{&1})")
    TestHelper.mysql!("TRUNCATE TABLE #{@opts[:database]}.integers")
    TestHelper.mysql!("INSERT INTO #{@opts[:database]}.integers VALUES " <> values)

    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    {:ok, query} = MyXQL.prepare(conn, "", "SELECT * FROM integers")

    {:ok, [_, _, _, _, _]} =
      MyXQL.transaction(conn, fn conn ->
        Enum.to_list(MyXQL.stream(conn, query))
      end)

    assert prepared_stmt_count() == 0
  end

  test "do not leak statements with prepare+close" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    {:ok, query} = MyXQL.prepare(conn, "", "SELECT 42")
    :ok = MyXQL.close(conn, query)
    assert prepared_stmt_count() == 0
  end

  test "do not leak with single and multiple result queries using the same name" do
    {:ok, conn} = MyXQL.start_link(@opts)
    assert prepared_stmt_count() == 0

    {:ok, _} = MyXQL.prepare_many(conn, "foo", "CALL multi_procedure()")
    assert prepared_stmt_count() == 1

    {:ok, _} = MyXQL.prepare(conn, "foo", "SELECT 42")
    assert prepared_stmt_count() == 1
  end

  defp prepared_stmt_count() do
    [%{"Value" => count}] = TestHelper.mysql!("show global status like 'Prepared_stmt_count'")
    String.to_integer(count)
  end
end
