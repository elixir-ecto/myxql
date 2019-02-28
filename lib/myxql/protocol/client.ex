defmodule MyXQL.Protocol.Client do
  @moduledoc false

  alias MyXQL.Protocol.{Auth, ServerErrorCodes}
  import MyXQL.Protocol.{Messages, Records, Types}

  @handshake_recv_timeout 5_000

  def connect(config) do
    with {:ok, sock} <-
           :gen_tcp.connect(
             config.address,
             config.port,
             config.socket_options,
             config.connect_timeout
           ) do
      state = %{sock: {:gen_tcp, sock}, connection_id: nil}
      handshake(config, state)
    end
  end

  def com_ping(state) do
    with :ok <- send_com(:com_ping, state) do
      recv_packet(&decode_generic_response/1, state.ping_timeout, state)
    end
  end

  def com_query(statement, state) do
    with :ok <- send_com({:com_query, statement}, state) do
      recv_packets(&decode_com_query_response/3, :initial, state)
    end
  end

  def com_stmt_prepare(statement, state) do
    with :ok <- send_com({:com_stmt_prepare, statement}, state) do
      recv_packets(&decode_com_stmt_prepare_response/3, :initial, state)
    end
  end

  def com_stmt_execute(statement_id, params, cursor_type, state) do
    with :ok <- send_com({:com_stmt_execute, statement_id, params, cursor_type}, state) do
      recv_packets(&decode_com_stmt_execute_response/3, :initial, state)
    end
  end

  def com_stmt_fetch(statement_id, column_defs, max_rows, state) do
    with :ok <- send_com({:com_stmt_fetch, statement_id, max_rows}, state) do
      recv_packets(&decode_com_stmt_execute_response/3, {:rows, column_defs, []}, state)
    end
  end

  def com_stmt_reset(statement_id, state) do
    with :ok <- send_com({:com_stmt_reset, statement_id}, state) do
      recv_packet(&decode_generic_response/1, state)
    end
  end

  def com_stmt_close(statement_id, state) do
    # No response is sent back to the client.
    :ok = send_com({:com_stmt_close, statement_id}, state)
  end

  def send_packet(payload, sequence_id, state) do
    data = encode_packet(payload, sequence_id)
    send_data(state, data)
  end

  def disconnect(state) do
    sock_close(state)
  end

  ## Internals

  defp send_com(com, state) do
    payload = encode_com(com)
    send_packet(payload, 0, state)
  end

  defp send_data(%{sock: {sock_mod, sock}}, data) do
    sock_mod.send(sock, data)
  end

  def recv_packet(decoder, timeout \\ :infinity, state) do
    new_decoder = fn payload, "", nil -> {:halt, decoder.(payload)} end
    recv_packets(new_decoder, nil, timeout, state)
  end

  def recv_packets(decoder, decoder_state, timeout \\ :infinity, state) do
    case recv_data(state, timeout) do
      {:ok, data} ->
        recv_packets(data, decoder, decoder_state, timeout, state)

      {:error, _} = error ->
        error
    end
  end

  defp recv_packets(
         <<size::int(3), _seq::int(1), payload::string(size), rest::binary>>,
         decoder,
         decoder_state,
         timeout,
         state
       ) do
    case decoder.(payload, rest, decoder_state) do
      {:cont, decoder_state} ->
        recv_packets(rest, decoder, decoder_state, timeout, state)

      {:halt, result} ->
        {:ok, result}

      {:error, _} = error ->
        error
    end
  end

  # If we didn't match on a full packet, receive more data and try again
  defp recv_packets(rest, decoder, decoder_state, timeout, state) do
    case recv_data(state, timeout) do
      {:ok, data} ->
        recv_packets(<<rest::binary, data::binary>>, decoder, decoder_state, timeout, state)

      {:error, _} = error ->
        error
    end
  end

  defp recv_data(%{sock: {sock_mod, sock}}, timeout) do
    sock_mod.recv(sock, 0, timeout)
  end

  defp sock_close(%{sock: {sock_mod, sock}}) do
    sock_mod.close(sock)
  end

  ## Handshake

  defp handshake(config, state) do
    {:ok,
     handshake_v10(
       auth_plugin_data: auth_plugin_data,
       auth_plugin_name: auth_plugin_name,
       capability_flags: capability_flags,
       conn_id: conn_id,
       status_flags: _status_flags
     )} = recv_packet(&decode_handshake_v10/1, @handshake_recv_timeout, state)

    state = %{state | connection_id: conn_id}
    sequence_id = 1

    with :ok <- ensure_capabilities(capability_flags, state),
         {:ok, state, sequence_id} <-
           maybe_upgrade_to_ssl(
             state,
             config.ssl?,
             config.ssl_opts,
             config.connect_timeout,
             config.database,
             sequence_id
           ) do
      do_handshake(
        state,
        config.username,
        config.password,
        auth_plugin_name,
        auth_plugin_data,
        config.database,
        sequence_id,
        config.ssl?
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

    with :ok <- send_packet(payload, sequence_id, state) do
      case recv_packet(&decode_handshake_response/1, @handshake_recv_timeout, state) do
        {:ok, ok_packet()} ->
          {:ok, state}

        {:ok, err_packet() = err_packet} ->
          {:error, mysql_error(err_packet, nil, state)}

        {:ok, auth_switch_request(plugin_name: plugin_name, plugin_data: plugin_data)} ->
          with {:ok, auth_response} <-
                 auth_switch_response(plugin_name, password, plugin_data, ssl?, state),
               :ok <- send_packet(auth_response, sequence_id + 2, state) do
            case recv_packet(&decode_handshake_response/1, @handshake_recv_timeout, state) do
              {:ok, ok_packet(warning_count: 0)} ->
                {:ok, state}

              {:ok, err_packet() = err_packet} ->
                {:error, mysql_error(err_packet, nil, state)}
            end
          end

        {:ok, :full_auth} ->
          if ssl? do
            auth_response = password <> <<0x00>>

            with :ok <- send_packet(auth_response, sequence_id + 2, state) do
              case recv_packet(
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

    case send_packet(payload, sequence_id, state) do
      :ok ->
        {_, sock} = state.sock

        case :ssl.connect(sock, ssl_opts, connect_timeout) do
          {:ok, ssl_sock} ->
            {:ok, %{state | sock: {:ssl, ssl_sock}}, sequence_id + 1}

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

  def mysql_error(err_packet(error_code: code, error_message: message), statement, state) do
    name = ServerErrorCodes.code_to_name(code)
    mysql_error(code, name, message, statement, state.connection_id)
  end

  def mysql_error(code, name, message, statement, connection_id)
      when is_integer(code) and is_atom(name) do
    mysql = %{code: code, name: name}

    %MyXQL.Error{
      connection_id: connection_id,
      message: "(#{code}) (#{name}) " <> message,
      mysql: mysql,
      statement: statement
    }
  end

  def socket_error(%MyXQL.Error{} = exception, _state) do
    exception
  end

  def socket_error(reason, state) do
    message = {:error, reason} |> :ssl.format_error() |> List.to_string()
    %MyXQL.Error{connection_id: state.connection_id, message: message, socket: reason}
  end
end
