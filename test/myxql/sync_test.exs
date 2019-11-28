# used for tests that affect global state
defmodule MyXQL.SyncTest do
  use ExUnit.Case

  @opts TestHelper.opts()

  test "do not leak statements with :cache_statement and prepare: :unnamed" do
    {:ok, conn} = MyXQL.start_link(@opts ++ [prepare: :unnamed])
    assert prepared_stmt_count() == 0

    MyXQL.query!(conn, "SELECT 42", [], cache_statement: "42")
    assert prepared_stmt_count() == 1

    MyXQL.query!(conn, "SELECT 1337", [], cache_statement: "69")
    assert prepared_stmt_count() == 1

    MyXQL.query!(conn, "SELECT 42", [], cache_statement: "42")
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

  defp prepared_stmt_count() do
    [%{"Value" => count}] = TestHelper.mysql!("show global status like 'Prepared_stmt_count'")
    String.to_integer(count)
  end

  @tag capture_log: true
  test "connect with SSL but without starting :ssl" do
    Application.stop(:ssl)

    assert_raise RuntimeError,
                 ~r"cannot be established because `:ssl` application is not started",
                 fn ->
                   opts = [ssl: true] ++ @opts
                   MyXQL.start_link(opts)
                 end
  after
    Application.ensure_all_started(:ssl)
  end
end
