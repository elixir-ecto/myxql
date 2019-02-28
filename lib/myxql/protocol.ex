defmodule MyXQL.Protocol do
  @moduledoc false

  use DBConnection
  import MyXQL.Protocol.{Messages, Records}
  alias MyXQL.Protocol.{Auth, Client, ServerErrorCodes}
  alias MyXQL.{Cursor, Query, TextQuery, Result}

  @disconnect_on_error_codes [
    :ER_MAX_PREPARED_STMT_COUNT_REACHED
  ]

  @handshake_recv_timeout 5_000

  defstruct [
    :sock,
    :sock_mod,
    :connection_id,
    disconnect_on_error_codes: [],
    ping_timeout: 15_000,
    prepare: :named,
    prepared_statements: %{},
    transaction_status: :idle
  ]

  @impl true
  def connect(opts) do
    username =
      Keyword.get(opts, :username, System.get_env("USER") || raise(":username is missing"))

    password = Keyword.get(opts, :password)
    database = Keyword.get(opts, :database)
    ssl? = Keyword.get(opts, :ssl, false)
    ssl_opts = Keyword.get(opts, :ssl_opts, [])
    prepare = Keyword.get(opts, :prepare, :named)
    connect_timeout = Keyword.get(opts, :connect_timeout, 15_000)
    ping_timeout = Keyword.get(opts, :ping_timeout, 15_000)

    disconnect_on_error_codes =
      @disconnect_on_error_codes ++ Keyword.get(opts, :disconnect_on_error_codes, [])

    case do_connect(opts, connect_timeout) do
      {:ok, sock} ->
        state = %__MODULE__{
          sock: sock,
          sock_mod: :gen_tcp,
          prepare: prepare,
          disconnect_on_error_codes: disconnect_on_error_codes,
          ping_timeout: ping_timeout
        }

        handshake(state, username, password, database, ssl?, ssl_opts, connect_timeout)

      {:error, reason} ->
        {:error, socket_error(reason, %{connection_id: nil})}
    end
  end

  defp do_connect(opts, connect_timeout) do
    {address, port} = address_and_port(opts)
    socket_opts = Keyword.merge([mode: :binary, active: false], opts[:socket_options] || [])
    :gen_tcp.connect(address, port, socket_opts, connect_timeout)
  end

  defp address_and_port(opts) do
    default_protocol =
      if (Keyword.has_key?(opts, :hostname) or Keyword.has_key?(opts, :port)) and
           not Keyword.has_key?(opts, :socket),
         do: :tcp,
         else: :socket

    protocol = Keyword.get(opts, :protocol, default_protocol)

    case protocol do
      :socket ->
        default_socket = System.get_env("MYSQL_UNIX_PORT") || "/tmp/mysql.sock"
        socket = Keyword.get(opts, :socket, default_socket)
        {{:local, socket}, 0}

      :tcp ->
        hostname = Keyword.get(opts, :hostname, "localhost")
        default_port = String.to_integer(System.get_env("MYSQL_TCP_PORT") || "3306")
        port = Keyword.get(opts, :port, default_port)
        {String.to_charlist(hostname), port}
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
  def handle_prepare(query, _opts, state) do
    query = if state.prepare == :unnamed, do: %{query | name: ""}, else: query

    with {:ok, query, _statement_id, state} <- prepare(query, state) do
      {:ok, query, state}
    end
  end

  @impl true
  def handle_execute(%Query{} = query, params, _opts, state) do
    with {:ok, query, statement_id, state} <- maybe_reprepare(query, state),
         {:ok, query, result, state} <- execute_binary(query, params, statement_id, state) do
      maybe_close(query, statement_id, result, state)
    end
  end

  def handle_execute(%TextQuery{} = query, [], _opts, state) do
    execute_text(query, state)
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
    with :ok <- Client.send_com(:com_ping, state),
         {:ok, ok_packet(status_flags: status_flags)} <-
           Client.recv_packet(&decode_generic_response/1, state.ping_timeout, state) do
      {:ok, put_status(state, status_flags)}
    else
      {:error, reason} ->
        {:disconnect, socket_error(reason, state), state}
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
    com = {:com_stmt_execute, statement_id, params, :cursor_type_read_only}

    with :ok <- Client.send_com(com, state) do
      case Client.recv_packets(&decode_com_stmt_execute_response/3, :initial, state) do
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
  end

  @impl true
  def handle_fetch(_query, %Result{} = result, _opts, s) do
    {:halt, result, s}
  end

  def handle_fetch(query, %Cursor{column_defs: column_defs}, opts, state) do
    max_rows = Keyword.get(opts, :max_rows, 500)
    {:ok, _query, statement_id, state} = maybe_reprepare(query, state)

    with :ok <- Client.send_com({:com_stmt_fetch, statement_id, max_rows}, state) do
      case Client.recv_packets(
             &decode_com_stmt_execute_response/3,
             {:rows, column_defs, []},
             state
           ) do
        {:ok, resultset(status_flags: status_flags)} = result ->
          {:ok, _query, result, state} = result(result, query, state)

          if :server_status_cursor_exists in list_status_flags(status_flags) do
            {:cont, result, state}
          else
            true = :server_status_last_row_sent in list_status_flags(status_flags)
            {:halt, result, state}
          end
      end
    end
  end

  @impl true
  def handle_deallocate(query, _cursor, _opts, state) do
    case fetch_statement_id(state, query) do
      {:ok, statement_id} ->
        with :ok <- Client.send_com({:com_stmt_reset, statement_id}, state),
             {:ok, packet} <- Client.recv_packet(&decode_generic_response/1, state) do
          case packet do
            ok_packet(status_flags: status_flags) ->
              {:ok, nil, put_status(state, status_flags)}

            err_packet() = err_packet ->
              {:error, mysql_error(err_packet, query.statement, state), state}
          end
        end

      :error ->
        {:ok, nil, state}
    end
  end

  ## Internals

  defp execute_binary(query, params, statement_id, state) do
    with :ok <-
           Client.send_com(
             {:com_stmt_execute, statement_id, params, :cursor_type_no_cursor},
             state
           ) do
      result = Client.recv_packets(&decode_com_stmt_execute_response/3, :initial, state)
      result(result, query, state)
    end
  end

  defp execute_text(%{statement: statement} = query, state) do
    with :ok <- Client.send_com({:com_query, statement}, state) do
      Client.recv_packets(&decode_com_query_response/3, :initial, state)
      |> result(query, state)
    end
  end

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
    maybe_disconnect(mysql_error(err_packet, query.statement, state), state)
  end

  defp result({:error, :multiple_results}, _query, _state) do
    raise ArgumentError, "expected a single result, got multiple; use MyXQL.stream/4 instead"
  end

  defp result({:error, reason}, _query, state) do
    {:error, socket_error(reason, state), state}
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

  defp handshake(state, username, password, database, ssl?, ssl_opts, connect_timeout) do
    {:ok,
     handshake_v10(
       auth_plugin_data: auth_plugin_data,
       auth_plugin_name: auth_plugin_name,
       capability_flags: capability_flags,
       conn_id: conn_id,
       status_flags: _status_flags
     )} = Client.recv_packet(&decode_handshake_v10/1, @handshake_recv_timeout, state)

    state = %{state | connection_id: conn_id}
    sequence_id = 1

    with :ok <- ensure_capabilities(capability_flags, state),
         {:ok, state, sequence_id} <-
           maybe_upgrade_to_ssl(state, ssl?, ssl_opts, connect_timeout, database, sequence_id) do
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
    end
  end

  defp ensure_capabilities(capability_flags, state) do
    if has_capability_flag?(capability_flags, :client_deprecate_eof) do
      :ok
    else
      exception = %MyXQL.Error{
        connection_id: state.connection_id,
        message: "MyXQL only works with MySQL server 5.7.10+"
      }

      {:error, exception}
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

    payload =
      encode_handshake_response_41(
        username,
        auth_plugin_name,
        auth_response,
        database,
        ssl?
      )

    with :ok <- Client.send_packet(payload, sequence_id, state) do
      case Client.recv_packet(&decode_handshake_response/1, @handshake_recv_timeout, state) do
        {:ok, ok_packet()} ->
          {:ok, state}

        {:ok, err_packet() = err_packet} ->
          {:error, mysql_error(err_packet, nil, state)}

        {:ok, auth_switch_request(plugin_name: plugin_name, plugin_data: plugin_data)} ->
          with {:ok, auth_response} <-
                 auth_switch_response(plugin_name, password, plugin_data, ssl?, state),
               :ok <- Client.send_packet(auth_response, sequence_id + 2, state) do
            case Client.recv_packet(&decode_handshake_response/1, @handshake_recv_timeout, state) do
              {:ok, ok_packet(warning_count: 0)} ->
                {:ok, state}

              {:ok, err_packet() = err_packet} ->
                {:error, mysql_error(err_packet, nil, state)}
            end
          end

        {:ok, :full_auth} ->
          if ssl? do
            auth_response = password <> <<0x00>>

            with :ok <- Client.send_packet(auth_response, sequence_id + 2, state) do
              case Client.recv_packet(
                     &decode_handshake_response/1,
                     @handshake_recv_timeout,
                     state
                   ) do
                {:ok, ok_packet(warning_count: 0)} ->
                  {:ok, state}

                {:ok, err_packet() = err_packet} ->
                  {:error, mysql_error(err_packet, nil, state)}
              end
            end
          else
            auth_plugin_secure_connection_error(auth_plugin_name, state)
          end
      end
    end
  end

  defp auth_response(_plugin_name, nil, _plugin_data),
    do: nil

  defp auth_response("mysql_native_password", password, plugin_data),
    do: Auth.mysql_native_password(password, plugin_data)

  defp auth_response(plugin_name, password, plugin_data)
       when plugin_name in ["sha256_password", "caching_sha2_password"],
       do: Auth.sha256_password(password, plugin_data)

  defp auth_switch_response(_plugin_name, nil, _plugin_data, _ssl?, _state),
    do: {:ok, <<>>}

  defp auth_switch_response("mysql_native_password", password, plugin_data, _ssl?, _state),
    do: {:ok, Auth.mysql_native_password(password, plugin_data)}

  defp auth_switch_response(plugin_name, password, _plugin_data, ssl?, state)
       when plugin_name in ["sha256_password", "caching_sha2_password"] do
    if ssl? do
      {:ok, password <> <<0x00>>}
    else
      auth_plugin_secure_connection_error(plugin_name, state)
    end
  end

  # https://dev.mysql.com/doc/refman/8.0/en/client-error-reference.html#error_cr_auth_plugin_err
  defp auth_plugin_secure_connection_error(plugin_name, state) do
    code = 2061
    name = :CR_AUTH_PLUGIN_ERR

    message =
      "(HY000): Authentication plugin '#{plugin_name}' reported error: Authentication requires secure connection"

    {:error, mysql_error(code, name, message, nil, state)}
  end

  defp maybe_upgrade_to_ssl(state, true, ssl_opts, connect_timeout, database, sequence_id) do
    payload = encode_ssl_request(database)

    case Client.send_packet(payload, sequence_id, state) do
      :ok ->
        case :ssl.connect(state.sock, ssl_opts, connect_timeout) do
          {:ok, ssl_sock} ->
            {:ok, %{state | sock: ssl_sock, sock_mod: :ssl}, sequence_id + 1}

          {:error, {:tls_alert, 'bad record mac'} = reason} ->
            versions = :ssl.versions()[:supported]

            extra_message = """
            You might be using TLS version not supported by the server.
            Protocol versions reported by the :ssl application: #{inspect(versions)}.
            Set `:ssl_opts` in `MyXQL.start_link/1` to force specific protocol
            versions.
            """

            error = socket_error(reason, state)
            {:error, %{error | message: error.message <> "\n\n" <> extra_message}}

          {:error, reason} ->
            {:error, socket_error(reason, state)}
        end

      {:error, reason} ->
        {:error, socket_error(reason, state)}
    end
  end

  defp maybe_upgrade_to_ssl(
         state,
         false,
         _ssl_opts,
         _connect_timeout,
         _database,
         sequence_id
       ) do
    {:ok, state, sequence_id}
  end

  defp sock_close(%{sock: sock, sock_mod: sock_mod}) do
    sock_mod.close(sock)
  end

  defp handle_transaction(call, statement, state) do
    :ok = Client.send_com({:com_query, statement}, state)

    case Client.recv_packet(&decode_generic_response/1, state) do
      {:ok, ok_packet()} = ok ->
        {:ok, _query, result, state} = result(ok, call, state)
        {:ok, result, state}

      {:ok, err_packet() = err_packet} ->
        {:disconnect, mysql_error(err_packet, statement, state), state}
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

  defp prepare(%Query{ref: ref} = query, state) when is_reference(ref) do
    with :ok <- Client.send_com({:com_stmt_prepare, query.statement}, state) do
      case Client.recv_packets(&decode_com_stmt_prepare_response/3, :initial, state) do
        {:ok, com_stmt_prepare_ok(statement_id: statement_id, num_params: num_params)} ->
          state = put_statement_id(state, query, statement_id)
          query = %{query | num_params: num_params}
          {:ok, query, statement_id, state}

        result ->
          result(result, query, state)
      end
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
    # No response is sent back to the client.
    :ok = Client.send_com({:com_stmt_close, statement_id}, state)

    delete_statement_id(state, query)
  end

  defp mysql_error(err_packet(error_code: code, error_message: message), statement, state) do
    name = ServerErrorCodes.code_to_name(code)
    mysql_error(code, name, message, statement, state.connection_id)
  end

  defp mysql_error(code, name, message, statement, connection_id)
       when is_integer(code) and is_atom(name) do
    mysql = %{code: code, name: name}

    %MyXQL.Error{
      connection_id: connection_id,
      message: "(#{code}) (#{name}) " <> message,
      mysql: mysql,
      statement: statement
    }
  end

  defp socket_error(%MyXQL.Error{} = exception, _state) do
    exception
  end

  defp socket_error(reason, state) do
    message = {:error, reason} |> :ssl.format_error() |> List.to_string()
    %MyXQL.Error{connection_id: state.connection_id, message: message, socket: reason}
  end
end
