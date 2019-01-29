defmodule MyXQL.Protocol do
  @moduledoc false
  use DBConnection
  import MyXQL.{Messages, Types}
  alias MyXQL.{Cursor, Query, TextQuery, Result}

  @typep t() :: %__MODULE__{}

  defstruct [
    :sock,
    :sock_mod,
    :connection_id,
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

    case do_connect(opts) do
      {:ok, sock} ->
        state = %__MODULE__{sock: sock, sock_mod: :gen_tcp, prepare: prepare}
        handshake(state, username, password, database, ssl?, ssl_opts)

      {:error, reason} ->
        {:error, socket_error(reason)}
    end
  end

  defp do_connect(opts) do
    {address, port} = address_and_port(opts)
    connect_timeout = Keyword.get(opts, :connect_timeout, 15000)
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
    payload = encode_com_ping()

    with :ok <- send_packet(payload, 0, state) do
      case recv_packet(&decode_generic_response/1, state) do
        {:ok, ok_packet(status_flags: status_flags)} ->
          {:ok, put_status(state, status_flags)}

        {:error, reason} ->
          {:disconnect, socket_error(reason), state}
      end
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
        statement = "ROLLBACK TO SAVEPOINT myxql_savepoint; RELEASE SAVEPOINT myxql_savepoint"
        handle_transaction_multi(statement, s)

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
    payload = encode_com_stmt_execute(statement_id, params, :cursor_type_read_only)

    with :ok <- send_packet(payload, 0, state) do
      case recv_packets(&decode_com_stmt_execute_response/3, :initial, state) do
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
    payload = encode_com_stmt_fetch(statement_id, max_rows)

    with :ok <- send_packet(payload, 0, state) do
      case recv_packets(&decode_com_stmt_execute_response/3, {:rows, column_defs, []}, state) do
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
        payload = encode_com_stmt_reset(statement_id)

        with :ok <- send_packet(payload, 0, state),
             {:ok, packet} <- recv_packet(&decode_generic_response/1, state) do
          case packet do
            ok_packet(status_flags: status_flags) ->
              {:ok, nil, put_status(state, status_flags)}

            err_packet() = err_packet ->
              {:error, mysql_error(err_packet, query.statement), state}
          end
        end

      :error ->
        {:ok, nil, state}
    end
  end

  ## Internals

  # next_data is `""` if there is no more data after parsed packet that we know of.
  # There might still be more data in the socket though, in that case the decoder
  # function needs to return `{:cont, ...}`.
  #
  # Pattern matching on next_data = "" is useful for OK packets etc.
  # Looking at next_data is useful for debugging.
  @typep decoder ::
           (payload :: binary(), next_data :: binary(), state :: term() ->
              {:cont, state :: term()}
              | {:halt, result :: term()}
              | {:error, term()})

  @spec recv_packet((payload :: binary() -> term()), t()) ::
          {:ok, term()} | {:error, :inet.posix() | term()}
  def recv_packet(decoder, state) do
    new_decoder = fn payload, "", nil -> {:halt, decoder.(payload)} end
    recv_packets(new_decoder, nil, state)
  end

  @spec recv_packets(decoder, decoder_state :: term(), %__MODULE__{}) ::
          {:ok, term()} | {:error, :inet.posix() | term()}
  defp recv_packets(decoder, decoder_state, state) do
    case recv_data(state) do
      {:ok, data} ->
        recv_packets(data, decoder, decoder_state, state)

      {:error, _} = error ->
        error
    end
  end

  defp recv_packets(
         <<size::int(3), _seq::int(1), payload::string(size), rest::binary>>,
         decoder,
         decoder_state,
         state
       ) do
    case decoder.(payload, rest, decoder_state) do
      {:cont, decoder_state} ->
        recv_packets(rest, decoder, decoder_state, state)

      {:halt, result} ->
        {:ok, result}

      {:error, _} = error ->
        error
    end
  end

  # If we didn't match on a full packet, receive more data and try again
  defp recv_packets(rest, decoder, decoder_state, state) do
    case recv_data(state) do
      {:ok, data} -> recv_packets(<<rest::binary, data::binary>>, decoder, decoder_state, state)
      {:error, _} = error -> error
    end
  end

  def send_packet(payload, sequence_id, state) do
    data = encode_packet(payload, sequence_id)
    send_data(state, data)
  end

  defp execute_binary(query, params, statement_id, state) do
    payload = encode_com_stmt_execute(statement_id, params, :cursor_type_no_cursor)

    with :ok <- send_packet(payload, 0, state) do
      result = recv_packets(&decode_com_stmt_execute_response/3, :initial, state)
      result(result, query, state)
    end
  end

  defp execute_text(%{statement: statement} = query, state) do
    payload = encode_com_query(statement)

    with :ok <- send_packet(payload, 0, state) do
      recv_packets(&decode_com_query_response/3, :initial, state)
      |> result(query, state)
    end
  end

  defp result(
         {:ok,
          ok_packet(
            last_insert_id: last_insert_id,
            affected_rows: affected_rows,
            status_flags: status_flags
          )},
         query,
         state
       ) do
    result = %MyXQL.Result{
      connection_id: state.connection_id,
      last_insert_id: last_insert_id,
      num_rows: affected_rows
    }

    {:ok, query, result, put_status(state, status_flags)}
  end

  defp result(
         {:ok,
          resultset(
            column_defs: column_defs,
            row_count: num_rows,
            rows: rows,
            status_flags: status_flags
          )},
         query,
         state
       ) do
    columns = Enum.map(column_defs, &elem(&1, 1))

    result = %MyXQL.Result{
      connection_id: state.connection_id,
      columns: columns,
      num_rows: num_rows,
      rows: rows
    }

    {:ok, query, result, put_status(state, status_flags)}
  end

  defp result({:ok, err_packet() = err_packet}, query, state) do
    maybe_disconnect(mysql_error(err_packet, query.statement), state)
  end

  defp result({:error, :multiple_results}, _query, _state) do
    raise ArgumentError, "expected a single result, got multiple; use MyXQL.stream/4 instead"
  end

  defp result({:error, reason}, _query, state) do
    {:error, socket_error(reason), state}
  end

  defp maybe_disconnect(exception, state) do
    %MyXQL.Error{mysql: %{name: error_name}} = exception

    disconnect_on_errors = [
      :ER_MAX_PREPARED_STMT_COUNT_REACHED
    ]

    if error_name in disconnect_on_errors do
      {:disconnect, exception, state}
    else
      {:error, exception, state}
    end
  end

  ## Handshake

  defp handshake(state, username, password, database, ssl?, ssl_opts) do
    {:ok,
     handshake_v10(
       conn_id: conn_id,
       auth_plugin_name: auth_plugin_name,
       auth_plugin_data1: auth_plugin_data1,
       auth_plugin_data2: auth_plugin_data2,
       status_flags: _status_flags
     )} = recv_packet(&decode_handshake_v10/1, state)

    state = %{state | connection_id: conn_id}
    sequence_id = 1

    case maybe_upgrade_to_ssl(state, ssl?, ssl_opts, database, sequence_id) do
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

    payload =
      encode_handshake_response_41(
        username,
        auth_plugin_name,
        auth_response,
        database,
        ssl?
      )

    with :ok <- send_packet(payload, sequence_id, state) do
      case recv_packet(&decode_handshake_response/1, state) do
        {:ok, ok_packet()} ->
          {:ok, state}

        {:ok, err_packet() = err_packet} ->
          {:error, mysql_error(err_packet, nil)}

        {:ok, auth_switch_request(plugin_name: plugin_name, plugin_data: plugin_data)} ->
          with {:ok, auth_response} <-
                 auth_switch_response(plugin_name, password, plugin_data, ssl?),
               :ok <- send_packet(auth_response, sequence_id + 2, state) do
            case recv_packet(&decode_handshake_response/1, state) do
              {:ok, ok_packet(warning_count: 0)} ->
                {:ok, state}

              {:ok, err_packet() = err_packet} ->
                {:error, mysql_error(err_packet, nil)}
            end
          end

        {:ok, :full_auth} ->
          if ssl? do
            auth_response = password <> <<0x00>>

            with :ok <- send_packet(auth_response, sequence_id + 2, state) do
              case recv_packet(&decode_handshake_response/1, state) do
                {:ok, ok_packet(warning_count: 0)} ->
                  {:ok, state}

                {:ok, err_packet() = err_packet} ->
                  {:error, mysql_error(err_packet, nil)}
              end
            end
          else
            auth_plugin_secure_connection_error(auth_plugin_name)
          end
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
      auth_plugin_secure_connection_error(plugin_name)
    end
  end

  # https://dev.mysql.com/doc/refman/8.0/en/client-error-reference.html#error_cr_auth_plugin_err
  defp auth_plugin_secure_connection_error(plugin_name) do
    code = 2061
    name = :CR_AUTH_PLUGIN_ERR

    message =
      "(HY000): Authentication plugin '#{plugin_name}' reported error: Authentication requires secure connection"

    {:error, mysql_error(code, name, message, nil)}
  end

  defp maybe_upgrade_to_ssl(state, true, ssl_opts, database, sequence_id) do
    payload = encode_ssl_request(database)
    data = encode_packet(payload, sequence_id)
    :ok = :gen_tcp.send(state.sock, data)

    case :ssl.connect(state.sock, ssl_opts) do
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

        error = socket_error(reason)
        {:error, %{error | message: error.message <> "\n\n" <> extra_message}}

      {:error, reason} ->
        {:error, socket_error(reason)}
    end
  end

  defp maybe_upgrade_to_ssl(
         state,
         false,
         _ssl_opts,
         _database,
         sequence_id
       ) do
    {:ok, state, sequence_id}
  end

  # # inside connect/1 callback we need to handle timeout ourselves
  # defp connect_send_and_recv(state, data) do
  #   :ok = send_data(state, data)
  #   recv_data(state, 5000)
  # end

  defp send_data(%{sock: sock, sock_mod: sock_mod}, data) do
    case sock_mod.send(sock, data) do
      :ok ->
        :ok

      {:error, reason} ->
        {:error, socket_error(reason)}
    end
  end

  defp recv_data(%{sock: sock, sock_mod: sock_mod}, timeout \\ :infinity) do
    sock_mod.recv(sock, 0, timeout)
  end

  defp sock_close(%{sock: sock, sock_mod: sock_mod}) do
    sock_mod.close(sock)
  end

  defp handle_transaction(statement, state) do
    :ok = send_text_query(state, statement)

    case recv_packet(&decode_generic_response/1, state) do
      {:ok, ok_packet(status_flags: status_flags)} ->
        {:ok, nil, put_status(state, status_flags)}

      {:ok, err_packet() = err_packet} ->
        {:disconnect, mysql_error(err_packet, statement), state}
    end
  end

  defp handle_transaction_multi(statement, state) do
    :ok = send_text_query(state, statement)

    case recv_packets(&decode_multi_results/3, :first, state) do
      {:ok, ok_packet(status_flags: status_flags)} ->
        {:ok, nil, put_status(state, status_flags)}

      {:ok, err_packet() = err_packet} ->
        {:disconnect, mysql_error(err_packet, statement), state}
    end
  end

  defp decode_multi_results(payload, _next, :first) do
    case decode_generic_response(payload) do
      ok_packet(status_flags: status_flags) ->
        true = has_status_flag?(status_flags, :server_more_results_exists)
        {:cont, :next}

      err_packet() = err_packet ->
        {:halt, err_packet}
    end
  end

  defp decode_multi_results(payload, "", :next) do
    {:halt, decode_generic_response(payload)}
  end

  defp send_text_query(state, statement) do
    payload = encode_com_query(statement)
    send_packet(payload, 0, state)
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
    payload = encode_com_stmt_prepare(query.statement)

    with :ok <- send_packet(payload, 0, state) do
      case recv_packets(&decode_com_stmt_prepare_response/3, :initial, state) do
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
    payload = encode_com_stmt_close(statement_id)
    :ok = send_packet(payload, 0, state)

    delete_statement_id(state, query)
  end

  defp mysql_error(err_packet(error_code: code, error_message: message), statement) do
    name = MyXQL.ServerErrorCodes.code_to_name(code)
    mysql_error(code, name, message, statement)
  end

  defp mysql_error(code, name, message, statement) when is_integer(code) and is_atom(name) do
    mysql = %{code: code, name: name}
    %MyXQL.Error{message: "(#{code}) (#{name}) " <> message, mysql: mysql, statement: statement}
  end

  defp socket_error(reason) do
    message = {:error, reason} |> :ssl.format_error() |> List.to_string()
    %MyXQL.Error{message: message, socket: reason}
  end
end
