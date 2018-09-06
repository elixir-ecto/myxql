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
    :binpp.pprint(data)

    handshake_v10(
      server_version: "5.7.23",
      auth_plugin_name: "mysql_native_password",
      auth_plugin_data1: auth_plugin_data1,
      auth_plugin_data2: auth_plugin_data2
    ) = Myxql.Messages.decode_handshake_v10(data)

    auth_plugin_data = <<auth_plugin_data1::binary, auth_plugin_data2::binary>>
    auth_response = Myxql.Utils.mysql_native_password(password, auth_plugin_data)

    data = Myxql.Messages.encode_handshake_response_41(user, auth_response, database)
    :binpp.pprint(data)
    :ok = :gen_tcp.send(sock, data)

    {:ok, data} = :gen_tcp.recv(sock, 0)
    :binpp.pprint(data)
    ok_packet(warnings: 0) = decode_ok_packet(data)

    data = encode_com_query("SELECT 2*3")
    :binpp.pprint(data)
    :ok = :gen_tcp.send(sock, data)

    {:ok, data} = :gen_tcp.recv(sock, 0)
    :binpp.pprint(data)
    {"2*3", "6"} = decode_com_query_response(data)
  end
end
