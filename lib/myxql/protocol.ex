defmodule Myxql.Protocol do
  @moduledoc false
  import Myxql.Messages

  def connect(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    user = Keyword.fetch!(opts, :user)
    password = Keyword.fetch!(opts, :password)
    database = Keyword.fetch!(opts, :database)
    timeout = Keyword.fetch!(opts, :timeout)
    socket_opts = [:binary, active: false]

    case :gen_tcp.connect(String.to_charlist(host), port, socket_opts, timeout) do
      {:ok, sock} -> handshake(sock, user, password, database)
      {:error, _} = error -> error
    end
  end

  def query(conn, statement) do
    data = encode_com_query(statement)
    :ok = :gen_tcp.send(conn.sock, data)
    {:ok, data} = :gen_tcp.recv(conn.sock, 0)
    decode_com_query_response(data)
  end

  def prepare(conn, statement) do
    data = encode_com_stmt_prepare(statement)
    :ok = :gen_tcp.send(conn.sock, data)
    {:ok, data} = :gen_tcp.recv(conn.sock, 0)
    decode_com_stmt_prepare_ok(data)
  end

  def execute(conn, statement) do
    data = encode_com_stmt_execute(statement)
    :ok = :gen_tcp.send(conn.sock, data)
    {:ok, data} = :gen_tcp.recv(conn.sock, 0)
    decode_com_stmt_execute_response(data)
  end

  defp handshake(sock, user, password, database) do
    {:ok, data} = :gen_tcp.recv(sock, 0)

    handshake_v10(
      auth_plugin_name: auth_plugin_name,
      auth_plugin_data1: auth_plugin_data1,
      auth_plugin_data2: auth_plugin_data2
    ) = Myxql.Messages.decode_handshake_v10(data)

    # TODO: MySQL 8.0 defaults to "caching_sha2_password", which we don't support yet,
    #       and will send AuthSwitchRequest which we'll need to handle.
    #       https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
    "mysql_native_password" = auth_plugin_name

    auth_plugin_data = <<auth_plugin_data1::binary, auth_plugin_data2::binary>>
    auth_response = Myxql.Utils.mysql_native_password(password, auth_plugin_data)

    data = Myxql.Messages.encode_handshake_response_41(user, auth_response, database)
    :ok = :gen_tcp.send(sock, data)
    {:ok, data} = :gen_tcp.recv(sock, 0)
    ok_packet(warnings: 0) = decode_response_packet(data)
    {:ok, %{sock: sock}}
  end
end
