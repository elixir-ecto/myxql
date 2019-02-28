defmodule MyXQL.Protocol do
  @moduledoc false

  use DBConnection
  import MyXQL.Protocol.{Flags, Records}
  alias MyXQL.Protocol.Client
  alias MyXQL.{Cursor, Query, TextQuery, Result}

  @disconnect_on_error_codes [
    :ER_MAX_PREPARED_STMT_COUNT_REACHED
  ]

  defstruct [
    :sock,
    :connection_id,
    disconnect_on_error_codes: [],
    ping_timeout: 15_000,
    prepare: :named,
    prepared_statements: %{},
    transaction_status: :idle
  ]

  @impl true
  def connect(opts) do
    prepare = Keyword.get(opts, :prepare, :named)
    ping_timeout = Keyword.get(opts, :ping_timeout, 15_000)

    disconnect_on_error_codes =
      @disconnect_on_error_codes ++ Keyword.get(opts, :disconnect_on_error_codes, [])

    case Client.connect(opts) do
      {:ok, state} ->
        state = %__MODULE__{
          prepare: prepare,
          disconnect_on_error_codes: disconnect_on_error_codes,
          ping_timeout: ping_timeout,
          sock: state.sock,
          connection_id: state.connection_id
        }

        {:ok, state}

      {:error, reason} ->
        {:error, Client.socket_error(reason, %{connection_id: nil})}
    end
  end

  @impl true
  def disconnect(_reason, state) do
    Client.disconnect(state)
  end

  @impl true
  def checkout(state) do
    {:ok, state}
  end

  @impl true
  def checkin(state) do
    {:ok, state}
  end

  @impl true
  def handle_prepare(query, _opts, state) do
    query = if state.prepare == :unnamed, do: %{query | name: ""}, else: query

    with {:ok, query, _statement_id, state} <- prepare(query, state) do
      {:ok, query, state}
    end
  end

  @impl true
  def handle_execute(%Query{} = query, params, _opts, state) do
    with {:ok, query, statement_id, state} <- maybe_reprepare(query, state),
         result = Client.com_stmt_execute(statement_id, params, :cursor_type_no_cursor, state),
         {:ok, query, result, state} <- result(result, query, state) do
      maybe_close(query, statement_id, result, state)
    end
  end

  def handle_execute(%TextQuery{statement: statement} = query, [], _opts, state) do
    Client.com_query(statement, state)
    |> result(query, state)
  end

  @impl true
  def handle_close(%Query{} = query, _opts, state) do
    case fetch_statement_id(state, query) do
      {:ok, statement_id} ->
        state = close(query, statement_id, state)
        {:ok, nil, state}

      :error ->
        {:ok, nil, state}
    end
  end

  @impl true
  def ping(state) do
    with {:ok, ok_packet(status_flags: status_flags)} <- Client.com_ping(state) do
      {:ok, put_status(state, status_flags)}
    else
      {:error, reason} ->
        {:disconnect, Client.socket_error(reason, state), state}
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
    {:ok, _query, statement_id, state} = maybe_reprepare(query, state)

    case Client.com_stmt_execute(statement_id, params, :cursor_type_read_only, state) do
      {:ok, resultset(column_defs: column_defs, status_flags: status_flags)} = result ->
        if has_status_flag?(status_flags, :server_status_cursor_exists) do
          cursor = %Cursor{column_defs: column_defs}
          {:ok, query, cursor, put_status(state, status_flags)}
        else
          result(result, query, state)
        end

      {:ok, _} = result ->
        result(result, query, state)

      {:error, _} = result ->
        result(result, query, state)
    end
  end

  @impl true
  def handle_fetch(_query, %Result{} = result, _opts, s) do
    {:halt, result, s}
  end

  def handle_fetch(query, %Cursor{column_defs: column_defs}, opts, state) do
    max_rows = Keyword.get(opts, :max_rows, 500)
    {:ok, _query, statement_id, state} = maybe_reprepare(query, state)

    with {:ok, resultset(status_flags: status_flags)} = result <-
           Client.com_stmt_fetch(statement_id, column_defs, max_rows, state),
         {:ok, _query, result, state} <- result(result, query, state) do
      if has_status_flag?(status_flags, :server_status_cursor_exists) do
        {:cont, result, state}
      else
        true = has_status_flag?(status_flags, :server_status_last_row_sent)
        {:halt, result, state}
      end
    end
  end

  @impl true
  def handle_deallocate(query, _cursor, _opts, state) do
    case fetch_statement_id(state, query) do
      {:ok, statement_id} ->
        with {:ok, packet} <- Client.com_stmt_reset(statement_id, state) do
          case packet do
            ok_packet(status_flags: status_flags) ->
              {:ok, nil, put_status(state, status_flags)}

            err_packet() = err_packet ->
              {:error, Client.mysql_error(err_packet, query.statement, state), state}
          end
        end

      :error ->
        {:ok, nil, state}
    end
  end

  ## Internals

  defp result(
         {:ok,
          ok_packet(
            last_insert_id: last_insert_id,
            affected_rows: affected_rows,
            status_flags: status_flags,
            warning_count: warning_count
          )},
         query,
         state
       ) do
    result = %Result{
      connection_id: state.connection_id,
      last_insert_id: last_insert_id,
      num_rows: affected_rows,
      num_warnings: warning_count
    }

    {:ok, query, result, put_status(state, status_flags)}
  end

  defp result(
         {:ok,
          resultset(
            column_defs: column_defs,
            row_count: num_rows,
            rows: rows,
            status_flags: status_flags,
            warning_count: warning_count
          )},
         query,
         state
       ) do
    columns = Enum.map(column_defs, &elem(&1, 1))

    result = %Result{
      connection_id: state.connection_id,
      columns: columns,
      num_rows: num_rows,
      rows: rows,
      num_warnings: warning_count
    }

    {:ok, query, result, put_status(state, status_flags)}
  end

  defp result({:ok, err_packet() = err_packet}, query, state) do
    maybe_disconnect(Client.mysql_error(err_packet, query.statement, state), state)
  end

  defp result({:error, :multiple_results}, _query, _state) do
    raise ArgumentError, "expected a single result, got multiple; use MyXQL.stream/4 instead"
  end

  defp result({:error, reason}, _query, state) do
    {:error, Client.socket_error(reason, state), state}
  end

  defp maybe_disconnect(exception, state) do
    %MyXQL.Error{mysql: %{name: error_name}} = exception

    if error_name in state.disconnect_on_error_codes do
      {:disconnect, exception, state}
    else
      {:error, exception, state}
    end
  end

  ## Handshake

  defp handle_transaction(call, statement, state) do
    case Client.com_query(statement, state) do
      {:ok, ok_packet()} = ok ->
        {:ok, _query, result, state} = result(ok, call, state)
        {:ok, result, state}

      {:ok, err_packet() = err_packet} ->
        {:disconnect, Client.mysql_error(err_packet, statement, state), state}
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

  defp put_statement_id(state, %{ref: ref}, statement_id) do
    %{state | prepared_statements: Map.put(state.prepared_statements, ref, statement_id)}
  end

  defp fetch_statement_id(state, %{ref: ref}) do
    Map.fetch(state.prepared_statements, ref)
  end

  defp delete_statement_id(state, %{ref: ref}) do
    %{state | prepared_statements: Map.delete(state.prepared_statements, ref)}
  end

  defp prepare(%Query{ref: ref, statement: statement} = query, state) when is_reference(ref) do
    case Client.com_stmt_prepare(statement, state) do
      {:ok, com_stmt_prepare_ok(statement_id: statement_id, num_params: num_params)} ->
        state = put_statement_id(state, query, statement_id)
        query = %{query | num_params: num_params}
        {:ok, query, statement_id, state}

      result ->
        result(result, query, state)
    end
  end

  defp maybe_reprepare(query, state) do
    case fetch_statement_id(state, query) do
      {:ok, statement_id} ->
        {:ok, query, statement_id, state}

      :error ->
        reprepare(query, state)
    end
  end

  defp reprepare(query, state) do
    query = %Query{query | ref: make_ref()}

    with {:ok, query, statement_id, state} <- prepare(query, state) do
      {:ok, query, statement_id, state}
    end
  end

  # Close unnamed queries after executing them
  defp maybe_close(%Query{name: ""} = query, statement_id, result, state) do
    state = close(query, statement_id, state)
    {:ok, query, result, state}
  end

  defp maybe_close(query, _statement_id, result, state) do
    {:ok, query, result, state}
  end

  defp close(query, statement_id, state) do
    :ok = Client.com_stmt_close(statement_id, state)
    delete_statement_id(state, query)
  end
end
