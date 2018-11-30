defmodule MyXQL do
  def start_link(opts) do
    DBConnection.start_link(MyXQL.Protocol, default_opts(opts))
  end

  defp default_opts(opts) do
    opts
    |> Keyword.put_new(:hostname, System.get_env("MYSQL_HOST") || "localhost")
    |> Keyword.put_new(:port, String.to_integer(System.get_env("MYSQL_TCP_PORT") || "3306"))
    |> Keyword.put_new(:username, System.get_env("USER"))
    |> Keyword.put_new(:password, nil)
    |> Keyword.put_new(:database, nil)
    |> Keyword.put_new(:skip_database, false)
    |> Keyword.put_new(:ssl, false)
    |> Keyword.put_new(:ssl_opts, [])
    |> Keyword.put_new(:timeout, 5000)
  end

  def query(conn, statement, params \\ [], opts \\ [])
      when is_binary(statement) or is_list(statement) do
    query_type = Keyword.get(opts, :query_type, :binary)
    query = %MyXQL.Query{name: "", ref: make_ref(), statement: statement, type: query_type}
    fun = if query_type == :binary, do: :prepare_execute, else: :execute

    case apply(DBConnection, fun, [conn, query, params, opts]) do
      {:ok, _query, result} ->
        {:ok, result}

      {:error, _} = error ->
        error
    end
  end

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

    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, query, result} ->
        {:ok, query, result}

      {:error, _} = error ->
        error
    end
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

  defdelegate rollback(conn, reason), to: DBConnection

  def stream(conn, query, params \\ [], opts \\ [])

  def stream(%DBConnection{} = conn, statement, params, opts) when is_binary(statement) do
    opts = Keyword.put_new(opts, :max_rows, 500)
    query_type = Keyword.get(opts, :query_type, :binary)
    query = %MyXQL.Query{name: "", ref: make_ref(), statement: statement, type: query_type}

    case query_type do
      :binary ->
        DBConnection.prepare_stream(conn, query, params, opts)

      :text ->
        stream(conn, query, [], opts)
    end
  end

  def child_spec(opts) do
    DBConnection.child_spec(MyXQL.Protocol, opts)
  end
end
