defmodule MyXQL.Connection do
  @moduledoc false

  use DBConnection
  import MyXQL.Protocol.{Flags, Records}
  alias MyXQL.{Client, Cursor, Query, Queries, Protocol, Result, TextQuery, TextQueries}

  defstruct [
    :client,
    cursors: %{},
    disconnect_on_error_codes: [],
    ping_timeout: 15_000,
    prepare: :named,
    queries: nil,
    transaction_status: :idle,
    last_ref: nil
  ]

  @impl true
  def connect(opts) do
    # Trap exits so that DBConnection calls `disconnect` on unexpected shutdowns
    Process.flag(:trap_exit, true)

    prepare = Keyword.get(opts, :prepare, :named)
    ping_timeout = Keyword.get(opts, :ping_timeout, 15_000)
    config = Client.Config.new(opts)

    case Client.connect(config) do
      {:ok, %Client{} = client} ->
        state = %__MODULE__{
          client: client,
          prepare: prepare,
          disconnect_on_error_codes: Keyword.fetch!(opts, :disconnect_on_error_codes),
          ping_timeout: ping_timeout,
          queries: queries_new()
        }

        {:ok, state}

      {:ok, err_packet() = err_packet} ->
        {:error, error(err_packet)}

      {:error, reason} ->
        case config.address do
          {:local, socket} ->
            conn_error(socket, reason)

          host ->
            conn_error("#{host}:#{config.port}", reason)
        end
    end
  end

  @impl true
  def disconnect(_reason, state) do
    :ets.delete(state.queries)
    Client.disconnect(state.client)
  end

  @impl true
  def checkout(state) do
    {:ok, state}
  end

  @impl true
  def handle_prepare(query, opts, state) do
    query = rename_query(state, query)

    if cached_query = queries_get(state, query) do
      {:ok, cached_query, %{state | last_ref: cached_query.ref}}
    else
      case prepare(query, state) do
        {:ok, _, _} = ok ->
          ok

        {:error, %MyXQL.Error{mysql: %{name: :ER_UNSUPPORTED_PS}}, state} = error ->
          if Keyword.get(opts, :query_type) == :binary_then_text do
            query = %MyXQL.TextQuery{statement: query.statement}
            {:ok, query, state}
          else
            error
          end

        other ->
          other
      end
    end
  end

  @impl true
  def handle_execute(%TextQuery{statement: statement} = query, [], _opts, state) do
    Client.com_query(state.client, statement, result_state(query))
    |> result(query, state)
  end

  def handle_execute(%TextQueries{statement: statement} = query, [], _opts, state) do
    Client.com_query(state.client, statement, result_state(query))
    |> result(query, state)
  end

  def handle_execute(query, params, _opts, state) do
    with {:ok, query, state} <- maybe_reprepare(query, state) do
      result =
        Client.com_stmt_execute(
          state.client,
          query.statement_id,
          params,
          :cursor_type_no_cursor,
          result_state(query)
        )

      with {:ok, state} <- maybe_close(query, state) do
        result(result, query, state)
      end
    end
  end

  @impl true
  def handle_close(query, _opts, state) do
    with {:ok, state} <- close(query, state) do
      {:ok, nil, state}
    end
  end

  @impl true
  def ping(state) do
    case Client.com_ping(state.client, state.ping_timeout) do
      {:ok, ok_packet(status_flags: status_flags)} ->
        {:ok, put_status(state, status_flags)}

      {:ok, err_packet() = err_packet} ->
        {:disconnect, error(err_packet), state}

      {:error, reason} ->
        {:disconnect, error(reason), state}
    end
  end

  @impl true
  def handle_begin(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :idle ->
        handle_transaction(:begin, "BEGIN", s)

      :savepoint when status == :transaction ->
        handle_transaction(:begin, "SAVEPOINT myxql_savepoint", s)

      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_commit(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :transaction ->
        handle_transaction(:commit, "COMMIT", s)

      :savepoint when status == :transaction ->
        handle_transaction(:commit, "RELEASE SAVEPOINT myxql_savepoint", s)

      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_rollback(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :transaction ->
        handle_transaction(:rollback, "ROLLBACK", s)

      :savepoint when status == :transaction ->
        with {:ok, _result, s} <-
               handle_transaction(:rollback, "ROLLBACK TO SAVEPOINT myxql_savepoint", s) do
          handle_transaction(:rollback, "RELEASE SAVEPOINT myxql_savepoint", s)
        end

      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_status(_opts, s) do
    {s.transaction_status, s}
  end

  @impl true
  def handle_declare(query, params, _opts, state) do
    with {:ok, query, state} <- maybe_reprepare(query, state) do
      cursor = %Cursor{ref: make_ref()}

      state = %{
        state
        | cursors: Map.put(state.cursors, cursor.ref, {:params, params, query.statement_id})
      }

      {:ok, query, cursor, state}
    end
  end

  @impl true
  def handle_fetch(query, %Cursor{ref: cursor_ref}, opts, state) do
    case Map.fetch!(state.cursors, cursor_ref) do
      {:params, params, statement_id} ->
        fetch_first(%{query | statement_id: statement_id}, cursor_ref, params, opts, state)

      {:column_defs, column_defs, statement_id} ->
        fetch_next(%{query | statement_id: statement_id}, cursor_ref, column_defs, opts, state)
    end
  end

  defp fetch_first(query, cursor_ref, params, _opts, state) do
    case Client.com_stmt_execute(state.client, query.statement_id, params, :cursor_type_read_only) do
      {:ok, resultset(column_defs: column_defs, status_flags: status_flags)} = result ->
        {:ok, _query, result, state} = result(result, query, state)

        cursors =
          Map.put(state.cursors, cursor_ref, {:column_defs, column_defs, query.statement_id})

        state = put_status(%{state | cursors: cursors}, status_flags)

        if has_status_flag?(status_flags, :server_status_cursor_exists) do
          {:cont, result, state}
        else
          {:halt, result, state}
        end

      other ->
        result(other, query, state)
    end
  end

  defp fetch_next(query, _cursor_ref, column_defs, opts, state) do
    max_rows = Keyword.get(opts, :max_rows, 500)
    result = Client.com_stmt_fetch(state.client, query.statement_id, column_defs, max_rows)

    case result do
      {:ok, resultset(status_flags: status_flags)} ->
        with {:ok, _query, result, state} <- result(result, query, state) do
          if has_status_flag?(status_flags, :server_status_cursor_exists) do
            {:cont, result, state}
          else
            true = has_status_flag?(status_flags, :server_status_last_row_sent)
            {:halt, result, state}
          end
        end

      other ->
        result(other, query, state)
    end
  end

  @impl true
  def handle_deallocate(%{name: ""} = query, cursor, _opts, state) do
    state = delete_cursor(cursor, state)

    with {:ok, state} <- close(query, state) do
      {:ok, nil, state}
    end
  end

  def handle_deallocate(query, cursor, _opts, state) do
    state = delete_cursor(cursor, state)

    case Client.com_stmt_reset(state.client, query.statement_id) do
      {:ok, ok_packet(status_flags: status_flags)} ->
        {:ok, nil, put_status(state, status_flags)}

      other ->
        result(other, query, state)
    end
  end

  ## Internals

  defp result({:ok, ok_packet(status_flags: status_flags) = result}, query, state) do
    {:ok, query, format_result(result, state), put_status(state, status_flags)}
  end

  defp result({:ok, resultset(status_flags: status_flags) = result}, query, state) do
    {:ok, query, format_result(result, state), put_status(state, status_flags)}
  end

  defp result({:ok, results}, query, state) when is_list(results) do
    {results, status_flags} =
      Enum.reduce(results, {[], nil}, fn
        result, {results, latest_status_flags} ->
          # Keep status flags from the last query. The results are given
          # to this function in reverse order, so it is the first one.
          if latest_status_flags do
            {[format_result(result, state) | results], latest_status_flags}
          else
            {[format_result(result, state) | results], status_flags(result)}
          end
      end)

    {:ok, query, results, put_status(state, status_flags)}
  end

  defp result({:ok, err_packet() = err_packet}, query, state) do
    exception = error(err_packet, query, state)
    maybe_disconnect(exception, state)
  end

  defp result({:error, :multiple_results}, _query, _state) do
    raise RuntimeError,
          "returning multiple results is not supported from this function. Use MyXQL.query_many/4 and similar functions."
  end

  defp result({:error, reason}, _query, state) do
    {:disconnect, error(reason), state}
  end

  defp conn_error(_address, err_packet() = packet) do
    {:error, error(packet)}
  end

  defp conn_error(address, reason) do
    message = "(#{address}) #{format_reason(reason)} - #{inspect(reason)}"
    {:error, %DBConnection.ConnectionError{message: message}}
  end

  defp format_result(
         ok_packet(
           last_insert_id: last_insert_id,
           affected_rows: affected_rows,
           num_warnings: num_warnings
         ),
         state
       ) do
    %Result{
      connection_id: state.client.connection_id,
      last_insert_id: last_insert_id,
      num_rows: affected_rows,
      num_warnings: num_warnings
    }
  end

  defp format_result(
         resultset(
           column_defs: column_defs,
           num_rows: num_rows,
           rows: rows,
           num_warnings: num_warnings
         ),
         state
       ) do
    columns = Enum.map(column_defs, &column_def(&1, :name))

    %Result{
      connection_id: state.client.connection_id,
      columns: columns,
      num_rows: num_rows,
      rows: rows,
      num_warnings: num_warnings
    }
  end

  defp status_flags(ok_packet(status_flags: status_flags)), do: status_flags
  defp status_flags(resultset(status_flags: status_flags)), do: status_flags

  defp error(reason, %{statement: statement}, state) do
    error(reason, statement, state)
  end

  defp error(reason, statement, state) do
    %MyXQL.Error{} = exception = error(reason)
    %{exception | statement: statement, connection_id: state.client.connection_id}
  end

  defp error(err_packet(code: code, message: message)) do
    name = Protocol.error_code_to_name(code)
    %MyXQL.Error{mysql: %{code: code, name: name}, message: message}
  end

  defp error(reason) do
    %DBConnection.ConnectionError{message: format_reason(reason)}
  end

  defp format_reason(:timeout), do: "timeout"
  defp format_reason(:closed), do: "socket closed"

  defp format_reason({:tls_alert, {:bad_record_mac, _}} = reason) do
    versions = :ssl.versions()[:supported]

    """
    #{:ssl.format_error({:error, reason})}

    You might be using TLS version not supported by the server.
    Protocol versions reported by the :ssl application: #{inspect(versions)}.
    Set `:ssl_opts` in `MyXQL.start_link/1` to force specific protocol versions.
    """
  end

  defp format_reason(reason) when is_atom(reason) do
    List.to_string(:inet.format_error(reason))
  end

  defp format_reason(reason) do
    case :ssl.format_error(reason) do
      ~c"Unexpected error" ++ _ ->
        inspect(reason)

      message ->
        List.to_string(message)
    end
  end

  defp maybe_disconnect(exception, state) do
    %MyXQL.Error{mysql: %{code: code}} = exception

    if code in state.disconnect_on_error_codes do
      {:disconnect, exception, state}
    else
      {:error, exception, state}
    end
  end

  defp handle_transaction(call, statement, state) do
    case Client.com_query(state.client, statement) do
      {:ok, ok_packet()} = ok ->
        {:ok, _query, result, state} = result(ok, call, state)
        {:ok, result, state}

      other ->
        # We convert {:error, exception, state} to {:error, state}
        # so that DBConnection will disconnect during handle_begin/handle_rollback
        # and will attempt to rollback during handle_commit
        with {:error, _exception, state} <- result(other, statement, state) do
          {:error, state}
        end
    end
  end

  defp transaction_status(status_flags) do
    if has_status_flag?(status_flags, :server_status_in_trans) do
      :transaction
    else
      :idle
    end
  end

  defp put_status(state, status_flags) do
    %{state | transaction_status: transaction_status(status_flags)}
  end

  defp rename_query(%{prepare: :force_named}, query),
    do: %{query | name: "force_#{System.unique_integer([:positive])}"}

  defp rename_query(%{prepare: :named}, query),
    do: query

  defp rename_query(%{prepare: :unnamed}, query),
    do: %{query | name: ""}

  defp prepare(query, state) do
    case Client.com_stmt_prepare(state.client, query.statement) do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id, num_params: num_params)} ->
        ref = make_ref()
        query = %{query | num_params: num_params, statement_id: statement_id, ref: ref}
        queries_put(state, query)
        {:ok, query, %{state | last_ref: ref}}

      result ->
        result(result, query, state)
    end
  end

  defp maybe_reprepare(%{ref: ref} = query, %{last_ref: ref} = state), do: {:ok, query, state}

  defp maybe_reprepare(query, state) do
    if cached_query = queries_get(state, query) do
      {:ok, cached_query, state}
    else
      prepare(query, state)
    end
  end

  defp delete_cursor(cursor, state) do
    %{state | cursors: Map.delete(state.cursors, cursor.ref)}
  end

  # Close unnamed queries after executing them
  defp maybe_close(%{name: ""} = query, state), do: close(query, state)
  defp maybe_close(_query, state), do: {:ok, state}

  defp close(%{ref: ref} = query, %{last_ref: ref} = state) do
    close(query, %{state | last_ref: nil})
  end

  defp close(query, state) do
    case Client.com_stmt_close(state.client, query.statement_id) do
      :ok ->
        queries_delete(state, query)
        {:ok, state}

      {:error, reason} ->
        {:disconnect, error(reason), state}
    end
  end

  defp result_state(%TextQuery{}), do: :single
  defp result_state(%TextQueries{}), do: {:many, []}
  defp result_state(%Query{}), do: :single
  defp result_state(%Queries{}), do: {:many, []}

  ## Cache query handling

  defp queries_new(), do: :ets.new(__MODULE__, [:set, :public])

  defp queries_put(%{queries: nil}, _), do: :ok
  defp queries_put(_state, %{name: ""}), do: :ok

  defp queries_put(state, %{cache: :reference} = query) do
    %{
      num_params: num_params,
      statement_id: statement_id,
      ref: ref,
      name: name
    } = query

    try do
      :ets.insert(state.queries, {name, {num_params, statement_id, ref}})
    rescue
      ArgumentError ->
        :ok
    else
      true -> :ok
    end
  end

  defp queries_put(state, %{cache: :statement} = query) do
    %{
      num_params: num_params,
      statement_id: statement_id,
      ref: ref,
      name: name,
      statement: statement
    } = query

    try do
      :ets.insert(state.queries, {name, {statement, num_params, statement_id, ref}})
    rescue
      ArgumentError ->
        :ok
    else
      true -> :ok
    end
  end

  defp queries_delete(%{queries: nil}, _), do: :ok
  defp queries_delete(_state, %{name: ""}), do: :ok

  defp queries_delete(state, %{name: name}) do
    try do
      :ets.delete(state.queries, name)
    rescue
      ArgumentError -> :ok
    else
      true -> :ok
    end
  end

  defp queries_get(%{queries: nil}, _), do: nil
  defp queries_get(_state, %{name: ""}), do: nil

  defp queries_get(state, %{cache: :reference, name: name} = query) do
    try do
      :ets.lookup_element(state.queries, name, 2)
    rescue
      ArgumentError -> nil
    else
      {num_params, statement_id, ref} ->
        %{query | num_params: num_params, statement_id: statement_id, ref: ref}
    end
  end

  defp queries_get(state, %{cache: :statement, name: name, statement: statement} = query) do
    try do
      :ets.lookup_element(state.queries, name, 2)
    rescue
      ArgumentError -> nil
    else
      {^statement, num_params, statement_id, ref} ->
        %{query | num_params: num_params, statement_id: statement_id, ref: ref}

      {_statement, _num_params, statement_id, _ref} ->
        Client.com_stmt_close(state.client, statement_id)
        :ets.delete(state.queries, name)
        nil
    end
  end
end
