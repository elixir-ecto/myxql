defmodule MyXQL do
  def start_link(opts) do
    DBConnection.start_link(MyXQL.Protocol, opts)
  end

  def query(conn, statement, params \\ [], opts \\ [])

  def query(conn, statement, [], opts) when is_binary(statement) or is_list(statement) do
    query = %MyXQL.Query{
      name: "",
      statement: statement,
      type: :text
    }

    DBConnection.execute(conn, query, [], opts)
    |> query_result()
  end

  def query(conn, statement, params, opts) when is_binary(statement) or is_list(statement) do
    query = %MyXQL.Query{
      name: "",
      ref: make_ref(),
      statement: statement,
      type: :binary
    }

    DBConnection.prepare_execute(conn, query, params, opts)
    |> query_result()
  end

  defp query_result({:ok, _query, result}), do: {:ok, result}
  defp query_result({:error, _} = error), do: error

  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  def prepare(conn, name, statement, opts \\ []) do
    query = %MyXQL.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare(conn, query, opts)
  end

  def prepare_execute(conn, name, statement, params \\ [], opts \\ [])
      when is_binary(statement) do
    query = %MyXQL.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  defdelegate execute(conn, query, params \\ [], opts \\ []), to: DBConnection

  def close(conn, %MyXQL.Query{} = query, opts \\ []) do
    case DBConnection.close(conn, query, opts) do
      {:ok, _} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  defdelegate transaction(conn, fun, opts \\ []), to: DBConnection

  @spec rollback(DBConnection.t(), any()) :: no_return()
  defdelegate rollback(conn, reason), to: DBConnection

  def stream(conn, query, params \\ [], opts \\ [])

  def stream(%DBConnection{} = conn, statement, params, opts) when is_binary(statement) do
    query = %MyXQL.Query{name: "", ref: make_ref(), statement: statement}
    stream(conn, query, params, opts)
  end

  def stream(%DBConnection{} = conn, %MyXQL.Query{type: :binary} = query, params, opts) do
    opts = Keyword.put_new(opts, :max_rows, 500)
    DBConnection.prepare_stream(conn, query, params, opts)
  end

  def child_spec(opts) do
    DBConnection.child_spec(MyXQL.Protocol, opts)
  end
end
