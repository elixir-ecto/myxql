defmodule MyxqlTest do
  use ExUnit.Case, async: true
  import Myxql.Messages

  test "myxql" do
    host = "127.0.0.1"
    port = 5706
    user = "root"
    password = "secret"
    database = "myxql_test"
    timeout = 5000

    socket_opts = [:binary, active: false]
    {:ok, sock} = :gen_tcp.connect(String.to_charlist(host), port, socket_opts, timeout)
    {:ok, data} = :gen_tcp.recv(sock, 0)

    handshake_v10(
      server_version: "5.7.23",
      auth_plugin_name: "mysql_native_password",
      auth_plugin_data1: auth_plugin_data1,
      auth_plugin_data2: auth_plugin_data2
    ) = Myxql.Messages.decode_handshake_v10(data)

    auth_plugin_data = <<auth_plugin_data1::binary, auth_plugin_data2::binary>>
    auth_response = Myxql.Utils.mysql_native_password(password, auth_plugin_data)

    data = Myxql.Messages.encode_handshake_response_41(user, auth_response, database)
    :ok = :gen_tcp.send(sock, data)
    {:ok, data} = :gen_tcp.recv(sock, 0)
    ok_packet(warnings: 0) = decode_response_packet(data)

    assert resultset(columns: ["2*3", "4*5"], rows: [["6", "20"]]) = query(sock, "SELECT 2*3, 4*5")

    statement = "SELECT plugin_name FROM information_schema.plugins WHERE plugin_type = 'AUTHENTICATION'"
    assert resultset(columns: ["plugin_name"], rows: [["mysql_native_password"], ["sha256_password"]]) = query(sock, statement)

    assert ok_packet() = query(sock, "SET CHARSET 'UTF8'")

    assert err_packet(error_message: "You have an error in your SQL syntax" <> _) = query(sock, "bad")
  end

  defp query(sock, statement) do
    data = encode_com_query(statement)
    :ok = :gen_tcp.send(sock, data)
    {:ok, data} = :gen_tcp.recv(sock, 0)
    decode_com_query_response(data)
  end
end
