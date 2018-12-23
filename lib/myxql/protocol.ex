defmodule MyXQL.Protocol do
  @moduledoc false
  use DBConnection
  import MyXQL.Messages
  alias MyXQL.{Cursor, Error, Query, Result}

  defstruct [
    :sock,
    :sock_mod,
    :connection_id,
    transaction_status: :idle,
    prepared_statements: %{},
    cursors: %{}
  ]

  @impl true
  def connect(opts) do
    username =
      Keyword.get(opts, :username, System.get_env("USER") || raise(":username is missing"))

    password = Keyword.get(opts, :password)
    database = Keyword.get(opts, :database)
    ssl? = Keyword.get(opts, :ssl, false)
    ssl_opts = Keyword.get(opts, :ssl_opts, [])

    case do_connect(opts) do
      {:ok, sock} ->
        state = %__MODULE__{sock: sock, sock_mod: :gen_tcp}
        handshake(state, username, password, database, ssl?, ssl_opts)

      {:error, reason} ->
        message = reason |> :inet.format_error() |> List.to_string()
        {:error, %MyXQL.Error{message: message}}
    end
  end

  defp do_connect(opts) do
    {address, port} = address_and_port(opts)
    timeout = Keyword.get(opts, :timeout, 5000)

    # TODO: figure out best recbuf and/or support multiple recvs when they don't fit
    socket_opts = [
      :binary,
      active: false,
      recbuf: 65535
    ]

    :gen_tcp.connect(address, port, socket_opts, timeout)
  end

  defp address_and_port(opts) do
    tcp? = Keyword.has_key?(opts, :hostname) or Keyword.has_key?(opts, :port)

    if tcp? and not Keyword.has_key?(opts, :socket) do
      hostname = Keyword.get(opts, :hostname)
      default_port = String.to_integer(System.get_env("MYSQL_TCP_PORT") || "3306")
      port = Keyword.get(opts, :port, default_port)
      {String.to_charlist(hostname), port}
    else
      socket = Keyword.get(opts, :socket, System.get_env("MYSQL_UNIX_PORT") || "/tmp/mysql.sock")
      {{:local, socket}, 0}
    end
  end

  @impl true
  def disconnect(_reason, s) do
    sock_close(s)
    :ok
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
  def handle_prepare(%Query{ref: ref, type: :binary} = query, _opts, state)
      when is_reference(ref) do
    data = encode_com_stmt_prepare(query.statement)
    {:ok, data} = send_and_recv(state, data)

    case decode_com_stmt_prepare_response(data) do
      com_stmt_prepare_ok(statement_id: statement_id, num_params: num_params) ->
        state = put_statement_id(state, query, statement_id)
        query = %{query | num_params: num_params}
        {:ok, query, state}

      err_packet() = err_packet ->
        {:error, exception(err_packet, query.statement), state}
    end
  end

  defp maybe_reprepare(query, state) do
    case get_statement_id(state, query) do
      {:ok, statement_id} ->
        {:ok, query, statement_id, state}

      :error ->
        reprepare(query, state)
    end
  end

  @impl true
  def handle_execute(%Query{type: :binary} = query, params, _opts, s) do
    with {:ok, query, statement_id, s} <- maybe_reprepare(query, s) do
      data = encode_com_stmt_execute(statement_id, params, :cursor_type_no_cursor)
      {:ok, data} = send_and_recv(s, data)

      case decode_com_stmt_execute_response(data) do
        resultset(column_defs: column_defs, rows: rows, status_flags: status_flags) ->
          columns = Enum.map(column_defs, &elem(&1, 1))
          result = %Result{columns: columns, num_rows: length(rows), rows: rows}
          {:ok, query, result, put_status(s, status_flags)}

        ok_packet(
          status_flags: status_flags,
          affected_rows: affected_rows,
          last_insert_id: last_insert_id,
          info: _info
        ) ->
          result = %Result{
            columns: [],
            rows: nil,
            num_rows: affected_rows,
            last_insert_id: last_insert_id
          }

          {:ok, query, result, put_status(s, status_flags)}
          # Logger.debug("info: #{inspect(info)}")

          {:ok, query, result, put_status(s, status_flags)}

        err_packet() = err_packet ->
          {:error, exception(err_packet, query.statement), s}
      end
    end
  end

  def handle_execute(%Query{type: :text, statement: statement} = query, [], _opts, s) do
    data = encode_com_query(statement)
    {:ok, data} = send_and_recv(s, data)

    case decode_com_query_response(data) do
      ok_packet(last_insert_id: last_insert_id, status_flags: status_flags) ->
        {:ok, query, %MyXQL.Result{last_insert_id: last_insert_id}, put_status(s, status_flags)}

      resultset(
        column_defs: column_defs,
        row_count: num_rows,
        rows: rows,
        status_flags: status_flags
      ) ->
        columns = Enum.map(column_defs, &elem(&1, 1))
        result = %MyXQL.Result{columns: columns, num_rows: num_rows, rows: rows}
        {:ok, query, result, put_status(s, status_flags)}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}, s}
    end
  end

  @impl true
  def handle_close(%Query{} = query, _opts, state) do
    case get_statement_id(state, query) do
      {:ok, statement_id} ->
        data = encode_com_stmt_close(statement_id)
        :ok = sock_send(state, data)
        state = delete_statement_id(state, query)
        {:ok, nil, state}

      :error ->
        {:ok, nil, state}
    end
  end

  @impl true
  def ping(state) do
    case send_and_recv(state, encode_com_ping()) do
      {:ok, data} ->
        packet(payload: payload) = decode_packet(data)
        ok_packet(status_flags: status_flags) = decode_ok_packet(payload)
        {:ok, put_status(state, status_flags)}

      {:error, reason} ->
        message = reason |> :ssl.format_error() |> List.to_string()
        error = %MyXQL.Error{message: message}
        {:disconnect, error, state}
    end
  end

  @impl true
  def handle_begin(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :idle ->
        handle_transaction("BEGIN", s)

      :savepoint when status == :transaction ->
        handle_transaction("SAVEPOINT myxql_savepoint", s)

      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_commit(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :transaction ->
        handle_transaction("COMMIT", s)

      :savepoint when status == :transaction ->
        handle_transaction("RELEASE SAVEPOINT myxql_savepoint", s)

      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_rollback(opts, %{transaction_status: status} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when status == :transaction ->
        handle_transaction("ROLLBACK", s)

      :savepoint when status == :transaction ->
        # TODO: send as one query
        {:ok, _result, s} = handle_transaction("ROLLBACK TO SAVEPOINT myxql_savepoint", s)
        handle_transaction("RELEASE SAVEPOINT myxql_savepoint", s)

      mode when mode in [:transaction, :savepoint] ->
        {status, s}
    end
  end

  @impl true
  def handle_status(_opts, s) do
    {s.transaction_status, s}
  end

  @impl true
  def handle_declare(query, params, _opts, s) do
    {:ok, _query, statement_id, s} = maybe_reprepare(query, s)
    data = encode_com_stmt_execute(statement_id, params, :cursor_type_read_only)
    {:ok, data} = send_and_recv(s, data)

    case decode_com_stmt_execute_response(data) do
      resultset(column_defs: column_defs, rows: [], status_flags: status_flags) ->
        true = :server_status_cursor_exists in list_status_flags(status_flags)
        cursor = %Cursor{column_defs: column_defs}
        {:ok, query, cursor, put_status(s, status_flags)}
    end
  end

  @impl true
  def handle_fetch(query, %Cursor{column_defs: column_defs}, opts, s) do
    max_rows = Keyword.fetch!(opts, :max_rows)
    {:ok, _query, statement_id, s} = maybe_reprepare(query, s)
    data = encode_com_stmt_fetch(statement_id, max_rows, 0)
    {:ok, data} = send_and_recv(s, data)

    case data do
      <<_size::24-little, _seq, 0xFF, rest::binary>> ->
        err_packet() = err_packet = decode_err_packet(<<0xFF>> <> rest)
        {:error, exception(err_packet, query.statement), s}

      _ ->
        {row_count, rows, _warning_count, status_flags} =
          decode_binary_resultset_rows(data, column_defs)

        columns = Enum.map(column_defs, &elem(&1, 1))
        result = %MyXQL.Result{rows: rows, num_rows: row_count, columns: columns}

        if :server_status_cursor_exists in list_status_flags(status_flags) do
          {:cont, result, s}
        else
          {:halt, result, s}
        end
    end
  end

  # TODO: finish up
  @impl true
  def handle_deallocate(_query, %Cursor{}, _opts, s) do
    {:ok, nil, s}
  end

  ## Internals

  defp handshake(state, username, password, database, ssl?, ssl_opts) do
    {:ok, data} = :gen_tcp.recv(state.sock, 0)

    handshake_v10(
      conn_id: conn_id,
      server_version: server_version,
      auth_plugin_name: auth_plugin_name,
      auth_plugin_data1: auth_plugin_data1,
      auth_plugin_data2: auth_plugin_data2,
      status_flags: _status_flags
    ) = MyXQL.Messages.decode_handshake_v10(data)

    state = %{state | connection_id: conn_id}
    sequence_id = 1

    case maybe_upgrade_to_ssl(state, ssl?, ssl_opts, server_version, database, sequence_id) do
      {:ok, state, sequence_id} ->
        auth_plugin_data = <<auth_plugin_data1::binary, auth_plugin_data2::binary>>

        do_handshake(
          state,
          username,
          password,
          auth_plugin_name,
          auth_plugin_data,
          database,
          sequence_id,
          ssl?
        )

      {:error, _} = error ->
        error
    end
  end

  defp do_handshake(
         state,
         username,
         password,
         auth_plugin_name,
         auth_plugin_data,
         database,
         sequence_id,
         ssl?
       ) do
    auth_response = auth_response(auth_plugin_name, password, auth_plugin_data)

    data =
      MyXQL.Messages.encode_handshake_response_41(
        username,
        auth_plugin_name,
        auth_response,
        database,
        ssl?,
        sequence_id
      )

    {:ok, data} = connect_send_and_recv(state, data)

    case decode_handshake_response(data) do
      ok_packet(warning_count: 0) ->
        {:ok, state}

      err_packet() = err_packet ->
        {:error, exception(err_packet, nil)}

      auth_switch_request(plugin_name: plugin_name, plugin_data: plugin_data) ->
        with {:ok, auth_response} <-
               auth_switch_response(plugin_name, password, plugin_data, ssl?) do
          data = encode_packet(auth_response, sequence_id + 2)
          {:ok, data} = connect_send_and_recv(state, data)

          case decode_handshake_response(data) do
            ok_packet(warning_count: 0) ->
              {:ok, state}

            err_packet() = err_packet ->
              {:error, exception(err_packet, nil)}
          end
        end

      :full_auth ->
        if ssl? do
          auth_response = password <> <<0x00>>
          data = encode_packet(auth_response, sequence_id + 2)
          {:ok, data} = connect_send_and_recv(state, data)

          case decode_handshake_response(data) do
            ok_packet(warning_count: 0) ->
              {:ok, state}

            err_packet() = err_packet ->
              {:error, exception(err_packet, nil)}
          end
        else
          message =
            "ERROR 2061 (HY000): Authentication plugin 'caching_sha2_password' reported error: Authentication requires secure connection."

          {:error, %MyXQL.Error{message: message}}
        end
    end
  end

  defp auth_response(_plugin_name, nil, _plugin_data),
    do: nil

  defp auth_response("mysql_native_password", password, plugin_data),
    do: MyXQL.Auth.mysql_native_password(password, plugin_data)

  defp auth_response(plugin_name, password, plugin_data)
       when plugin_name in ["sha256_password", "caching_sha2_password"],
       do: MyXQL.Auth.sha256_password(password, plugin_data)

  defp auth_switch_response(_plugin_name, nil, _plugin_data, _ssl?),
    do: {:ok, <<>>}

  defp auth_switch_response("mysql_native_password", password, plugin_data, _ssl?),
    do: {:ok, MyXQL.Auth.mysql_native_password(password, plugin_data)}

  defp auth_switch_response(plugin_name, password, _plugin_data, ssl?)
       when plugin_name in ["sha256_password", "caching_sha2_password"] do
    if ssl? do
      {:ok, password <> <<0x00>>}
    else
      # https://dev.mysql.com/doc/refman/8.0/en/client-error-reference.html#error_cr_auth_plugin_err
      code = 2061
      name = :CR_AUTH_PLUGIN_ERR
      message = auth_error_message(plugin_name, code, "Authentication requires secure connection")
      {:error, %MyXQL.Error{message: message, mysql: %{code: code, name: name, message: message}}}
    end
  end

  defp auth_error_message(plugin_name, code, message) do
    "ERROR #{code} (HY000): Authentication plugin '#{plugin_name}' reported error: #{message}."
  end

  defp maybe_upgrade_to_ssl(state, true, ssl_opts, server_version, database, sequence_id) do
    data = encode_ssl_request(sequence_id, database)
    :ok = :gen_tcp.send(state.sock, data)
    ssl_opts = default_tls_versions(ssl_opts, server_version)

    case :ssl.connect(state.sock, ssl_opts) do
      {:ok, ssl_sock} ->
        {:ok, %{state | sock: ssl_sock, sock_mod: :ssl}, sequence_id + 1}

      {:error, reason} ->
        message = reason |> :ssl.format_error() |> List.to_string()
        {:error, %MyXQL.Error{message: message}}
    end
  end

  defp maybe_upgrade_to_ssl(
         state,
         false,
         _ssl_opts,
         _server_version,
         _database,
         sequence_id
       ) do
    {:ok, state, sequence_id}
  end

  defp default_tls_versions(ssl_opts, server_version) do
    # MySQL 5.7 cannot handle TLS v1.2 and doesn't fallback to v1.1
    if Version.match?(server_version, "< 8.0.0") do
      Keyword.put_new(ssl_opts, :versions, [:"tlsv1.1"])
    else
      ssl_opts
    end
  end

  # inside connect/1 callback we need to handle timeout ourselves
  defp connect_send_and_recv(state, data) do
    :ok = sock_send(state, data)
    sock_recv(state, 5000)
  end

  defp send_and_recv(state, data) do
    :ok = sock_send(state, data)
    sock_recv(state)
  end

  defp sock_send(%{sock: sock, sock_mod: sock_mod}, data) do
    sock_mod.send(sock, data)
  end

  defp sock_recv(%{sock: sock, sock_mod: sock_mod}, timeout \\ :infinity) do
    sock_mod.recv(sock, 0, timeout)
  end

  defp sock_close(%{sock: sock, sock_mod: sock_mod}) do
    sock_mod.close(sock)
  end

  defp handle_transaction(statement, s) do
    :ok = send_text_query(s, statement)
    transaction_recv(statement, s)
  end

  defp transaction_recv(statement, s) do
    {:ok, data} = sock_recv(s)

    case decode_com_query_response(data) do
      ok_packet(status_flags: status_flags) ->
        if has_status_flag?(status_flags, :server_more_results_exists) do
          transaction_recv(statement, s)
        else
          result = :todo
          {:ok, result, put_status(s, status_flags)}
        end

      err_packet() = err_packet ->
        {:disconnect, exception(err_packet, statement), s}
    end
  end

  defp send_text_query(s, statement) do
    data = encode_com_query(statement)
    sock_send(s, data)
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

  defp put_statement_id(state, %Query{ref: ref}, statement_id) do
    %{state | prepared_statements: Map.put(state.prepared_statements, ref, statement_id)}
  end

  defp get_statement_id(state, %Query{ref: ref}) do
    Map.fetch(state.prepared_statements, ref)
  end

  defp delete_statement_id(state, %Query{ref: ref}) do
    %{state | prepared_statements: Map.delete(state.prepared_statements, ref)}
  end

  defp reprepare(query, state) do
    # TODO: extract common parts instead
    # TODO: return statement_id without additional lookup. Maybe store statement_in on %Query{}?

    with {:ok, query, state} <- handle_prepare(query, [], state) do
      {:ok, statement_id} = get_statement_id(state, query)
      {:ok, query, statement_id, state}
    end
  end

  defp exception(err_packet(error_code: code, error_message: message), statement) do
    name = MyXQL.ServerErrorCodes.code_to_name(code)
    mysql = %{code: code, name: name, message: message}
    %Error{message: message, statement: statement, mysql: mysql}
  end
end
