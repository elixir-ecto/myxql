defmodule MyXQL.Protocol do
  @moduledoc false
  import MyXQL.Messages

  def connect(opts) do
    hostname = Keyword.fetch!(opts, :hostname)
    port = Keyword.fetch!(opts, :port)
    username = Keyword.fetch!(opts, :username)
    password = Keyword.fetch!(opts, :password)
    database = Keyword.fetch!(opts, :database)
    timeout = Keyword.fetch!(opts, :timeout)
    socket_opts = [:binary, active: false]

    case :gen_tcp.connect(String.to_charlist(hostname), port, socket_opts, timeout) do
      {:ok, sock} ->
        handshake(sock, username, password, database)

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
    :ok = :gen_tcp.send(conn.sock, data)
    {:ok, data} = :gen_tcp.recv(conn.sock, 0)

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
    :ok = :gen_tcp.send(conn.sock, data)
    {:ok, data} = :gen_tcp.recv(conn.sock, 0)

    case decode_com_stmt_prepare_response(data) do
      com_stmt_prepare_ok(statement_id: statement_id) ->
        {:ok, statement_id}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end

  def execute(conn, statement, parameters) do
    data = encode_com_stmt_execute(statement, parameters)
    :ok = :gen_tcp.send(conn.sock, data)
    {:ok, data} = :gen_tcp.recv(conn.sock, 0)

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

  defp handshake(sock, username, password, database) do
    {:ok, data} = :gen_tcp.recv(sock, 0)

    handshake_v10(
      auth_plugin_name: auth_plugin_name,
      auth_plugin_data1: auth_plugin_data1,
      auth_plugin_data2: auth_plugin_data2
    ) = MyXQL.Messages.decode_handshake_v10(data)

    # TODO: MySQL 8.0 defaults to "caching_sha2_password", which we don't support yet,
    #       and will send AuthSwitchRequest which we'll need to handle.
    #       https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
    "mysql_native_password" = auth_plugin_name

    auth_plugin_data = <<auth_plugin_data1::binary, auth_plugin_data2::binary>>
    auth_response = if password, do: MyXQL.Utils.mysql_native_password(password, auth_plugin_data)

    data = MyXQL.Messages.encode_handshake_response_41(username, auth_response, database)
    :ok = :gen_tcp.send(sock, data)
    {:ok, data} = :gen_tcp.recv(sock, 0)

    case decode_response_packet(data) do
      ok_packet(warnings: 0) ->
        {:ok, %{sock: sock}}

      err_packet(error_message: message) ->
        {:error, %MyXQL.Error{message: message}}
    end
  end
end
