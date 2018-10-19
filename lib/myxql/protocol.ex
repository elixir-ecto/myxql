defmodule MyXQL.Protocol do
  @moduledoc false
  import MyXQL.Messages

  def connect(opts) do
    default_port = String.to_integer(System.get_env("MYSQL_TCP_PORT") || "3306")

    hostname = Keyword.fetch!(opts, :hostname)
    port = Keyword.get(opts, :port, default_port)
    username = Keyword.fetch!(opts, :username)
    password = Keyword.get(opts, :password)
    database = Keyword.fetch!(opts, :database)
    timeout = Keyword.fetch!(opts, :timeout)
    ssl? = Keyword.get(opts, :ssl, false)
    ssl_opts = Keyword.get(opts, :ssl_opts, [])
    socket_opts = [:binary, active: false]

    case :gen_tcp.connect(String.to_charlist(hostname), port, socket_opts, timeout) do
      {:ok, sock} ->
        state = %{sock: sock}
        handshake(state, username, password, database, ssl?, ssl_opts)

      {:error, reason} ->
        message = reason |> :inet.format_error() |> List.to_string()
        {:error, %MyXQL.Error{message: message}}
    end
  end

  def disconnect(conn) do
    :gen_tcp.close(conn.sock)
  end

  def query(conn, statement) do
    data = encode_com_query(statement)
    data = send_and_recv(conn, data)

    case decode_com_query_response(data) do
      ok_packet(last_insert_id: last_insert_id) ->
        {:ok, %MyXQL.Result{last_insert_id: last_insert_id}}

      resultset(column_definitions: column_definitions, rows: rows) ->
        columns = Enum.map(column_definitions, &elem(&1, 1))
        {:ok, %MyXQL.Result{columns: columns, rows: rows}}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end

  def prepare(conn, statement) do
    data = encode_com_stmt_prepare(statement)
    data = send_and_recv(conn, data)

    case decode_com_stmt_prepare_response(data) do
      com_stmt_prepare_ok(statement_id: statement_id) ->
        {:ok, statement_id}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end

  def execute(conn, statement, parameters) do
    data = encode_com_stmt_execute(statement, parameters)
    data = send_and_recv(conn, data)

    case decode_com_stmt_execute_response(data) do
      ok_packet(last_insert_id: last_insert_id) ->
        {:ok, %MyXQL.Result{last_insert_id: last_insert_id}}

      resultset(column_definitions: column_definitions, rows: rows) ->
        columns = Enum.map(column_definitions, &elem(&1, 1))
        {:ok, %MyXQL.Result{columns: columns, rows: rows}}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end

  ## Helpers

  defp handshake(state, username, password, database, ssl?, ssl_opts) do
    {:ok, data} = :gen_tcp.recv(state.sock, 0)

    handshake_v10(
      auth_plugin_name: auth_plugin_name,
      auth_plugin_data1: auth_plugin_data1,
      auth_plugin_data2: auth_plugin_data2
    ) = MyXQL.Messages.decode_handshake_v10(data)

    sequence_id = 1

    case maybe_upgrade_to_ssl(state, ssl?, ssl_opts, sequence_id) do
      {:ok, state, sequence_id} ->
        auth_plugin_data = <<auth_plugin_data1::binary, auth_plugin_data2::binary>>

        do_handshake(
          state,
          username,
          password,
          auth_plugin_name,
          auth_plugin_data,
          database,
          sequence_id
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
         sequence_id
       ) do
    # TODO: MySQL 8.0 defaults to "caching_sha2_password", which we don't support yet,
    #       and will send AuthSwitchRequest which we'll need to handle.
    #       https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
    "mysql_native_password" = auth_plugin_name

    auth_response = if password, do: MyXQL.Utils.mysql_native_password(password, auth_plugin_data)

    data =
      MyXQL.Messages.encode_handshake_response_41(username, auth_response, database, sequence_id)

    data = send_and_recv(state, data)

    case decode_response_packet(data) do
      ok_packet(warnings: 0) ->
        {:ok, state}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end

  defp maybe_upgrade_to_ssl(state, true, ssl_opts, sequence_id) do
    data = encode_ssl_request(sequence_id)
    :ok = :gen_tcp.send(state.sock, data)

    case :ssl.connect(state.sock, ssl_opts) do
      {:ok, ssl_sock} ->
        {:ok, %{state | sock: ssl_sock}, sequence_id + 1}

      {:error, reason} ->
        message = reason |> :ssl.format_error() |> List.to_string()
        {:error, %MyXQL.Error{message: message}}
    end
  end

  defp maybe_upgrade_to_ssl(state, false, _ssl_opts, sequence_id) do
    {:ok, state, sequence_id}
  end

  defp send_and_recv(%{sock: sock}, data) when is_port(sock) do
    :ok = :gen_tcp.send(sock, data)
    {:ok, data} = :gen_tcp.recv(sock, 0)
    data
  end

  defp send_and_recv(%{sock: ssl_sock}, data) do
    :ok = :ssl.send(ssl_sock, data)
    {:ok, data} = :ssl.recv(ssl_sock, 0)
    data
  end
end
