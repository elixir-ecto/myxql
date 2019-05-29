defmodule Support.MariaexTests do
  def myxql_to_mariaex(%MyXQL.Query{} = query) do
    map = query |> Map.from_struct() |> Map.drop([:cache, :statement_id])
    struct!(Mariaex.Query, map)
  end

  def myxql_to_mariaex(%MyXQL.Result{} = result) do
    map = result |> Map.from_struct() |> Map.drop([:num_warnings])
    struct!(Mariaex.Result, map)
  end

  def myxql_to_mariaex(%MyXQL.Error{} = error) do
    map = error |> Map.from_struct() |> Map.drop([:mysql, :statement])
    struct!(Mariaex.Error, map)
  end

  def myxql_to_mariaex(tuple) when is_tuple(tuple), do: myxql_to_mariaex_tuple(tuple)
  def myxql_to_mariaex(other), do: other

  defp myxql_to_mariaex_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> myxql_to_mariaex_list([])
    |> List.to_tuple()
  end

  defp myxql_to_mariaex_list([head | tail], acc) do
    myxql_to_mariaex_list(tail, [myxql_to_mariaex(head) | acc])
  end

  defp myxql_to_mariaex_list([], acc), do: Enum.reverse(acc)

  def mariaex_to_myxql(%Mariaex.Query{} = query) do
    map = query |> Map.from_struct() |> Map.drop([:binary_as, :reserved?, :type])
    struct!(MyXQL.Query, map)
  end

  def mariaex_to_myxql(other), do: other

  def mariaex_path() do
    Mix.Project.deps_paths()[:mariaex]
  end

  def enabled?() do
    System.get_env("MARIAEX") == "true"
  end

  def load_test_helper_if_enabled() do
    if enabled?() do
      Code.require_file(Path.join([mariaex_path(), "test", "test_helper.exs"]))
      exclude = ExUnit.configuration()[:exclude]

      extra_exclude = [
        connection_opts: [datetime: :tuples],
        bits: true,
        include_table_name: true,
        stream_text: true,
        geometry: true,
        coder: true
      ]

      ExUnit.configure(exclude: exclude ++ extra_exclude)
    end
  end

  def load_tests_if_enabled() do
    if enabled?() do
      test_files =
        [mariaex_path(), "test", "**/*_test.exs"]
        |> Path.join()
        |> Path.wildcard()

      for path <- test_files do
        Code.require_file(path)
      end
    end
  end
end

defmodule Mariaex do
  import Support.MariaexTests, only: [myxql_to_mariaex: 1, mariaex_to_myxql: 1]

  defdelegate start_link(options), to: MyXQL

  defdelegate transaction(conn, fun), to: MyXQL

  def query(conn, statement, params \\ [], opts \\ []) do
    opts = Keyword.put_new(opts, :query_type, :binary_then_text)

    MyXQL.query(conn, statement, params, opts)
    |> myxql_to_mariaex()
  end

  def query!(conn, statement, params \\ [], opts \\ []) do
    opts = Keyword.put_new(opts, :query_type, :binary_then_text)

    MyXQL.query!(conn, statement, params, opts)
    |> myxql_to_mariaex()
  end

  def prepare(conn, name, statement, opts \\ []) do
    MyXQL.prepare(conn, name, statement, opts)
    |> myxql_to_mariaex()
  end

  def prepare!(conn, name, statement, opts \\ []) do
    MyXQL.prepare!(conn, name, statement, opts)
    |> myxql_to_mariaex()
  end

  def prepare_execute(conn, name, statement, params \\ [], opts \\ []) do
    MyXQL.prepare_execute(conn, name, statement, params, opts)
    |> myxql_to_mariaex()
  end

  def close(conn, query, opts \\ []) do
    MyXQL.close(conn, mariaex_to_myxql(query), opts)
  end

  def execute(conn, query, params \\ [], opts \\ []) do
    MyXQL.execute(conn, mariaex_to_myxql(query), params, opts)
    |> myxql_to_mariaex()
  end

  def execute!(conn, query, params \\ [], opts \\ []) do
    MyXQL.execute!(conn, mariaex_to_myxql(query), params, opts)
    |> myxql_to_mariaex()
  end

  def stream(conn, query, params \\ [], opts \\ []) do
    MyXQL.stream(conn, mariaex_to_myxql(query), params, opts)
    |> Stream.map(&myxql_to_mariaex/1)
  end
end
