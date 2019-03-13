defmodule MyXQL.Protocol.Config do
  @moduledoc false

  defstruct [
    :address,
    :port,
    :username,
    :password,
    :database,
    :ssl?,
    :ssl_opts,
    :connect_timeout,
    :handshake_timeout,
    :socket_options
  ]

  def new(opts) do
    {address, port} = address_and_port(opts)

    %__MODULE__{
      address: address,
      port: port,
      username:
        Keyword.get(opts, :username, System.get_env("USER")) || raise(":username is missing"),
      password: Keyword.get(opts, :password),
      database: Keyword.get(opts, :database),
      ssl?: Keyword.get(opts, :ssl, false),
      ssl_opts: Keyword.get(opts, :ssl_opts, []),
      connect_timeout: Keyword.get(opts, :connect_timeout, 15_000),
      handshake_timeout: Keyword.get(opts, :handshake_timeout, 15_000),
      socket_options:
        Keyword.merge([mode: :binary, packet: :raw, active: false], opts[:socket_options] || [])
    }
  end

  defp address_and_port(opts) do
    default_protocol =
      if (Keyword.has_key?(opts, :hostname) or Keyword.has_key?(opts, :port)) and
           not Keyword.has_key?(opts, :socket) do
        :tcp
      else
        :socket
      end

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
end
