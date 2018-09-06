defmodule MyxqlTest do
  use ExUnit.Case, async: true
  import Myxql.Messages

  test "myxql" do
    host = "127.0.0.1"
    port = 5706
    timeout = 5000

    socket_opts = [:binary, active: false]
    {:ok, sock} = :gen_tcp.connect(String.to_charlist(host), port, socket_opts, timeout)

    {:ok, data} = :gen_tcp.recv(sock, 0)
    :binpp.pprint(data)
    handshake_v10(
      server_version: "5.7.23",
      auth_plugin_name: "mysql_native_password"
    ) = Myxql.Messages.decode_handshake_v10(data)
  end
end
