defmodule MyXQL.Client do
  @moduledoc false

  require Logger
  import MyXQL.{Protocol, Protocol.Records, Protocol.Types}
  alias MyXQL.Protocol.Auth

  defstruct [:sock, :connection_id]

  @sock_opts [mode: :binary, packet: :raw, active: false]

  defmodule Config do
    @moduledoc false

    @default_timeout 15_000

    defstruct [
      :address,
      :port,
      :username,
      :password,
      :database,
      :ssl_opts,
      :connect_timeout,
      :handshake_timeout,
      :socket_options,
      :max_packet_size,
      :charset,
      :collation,
      :enable_cleartext_plugin
    ]

    @sock_opts [mode: :binary, packet: :raw, active: false]

    def new(opts) do
      {address, port} = address_and_port(opts)

      {ssl_opts, opts} =
        case Keyword.pop(opts, :ssl, false) do
          {false, opts} ->
            {nil, opts}

          {true, opts} ->
            case Keyword.pop(opts, :ssl_opts) do
              {nil, _opts} ->
                [cacerts: :public_key.cacerts_get()] ++ default_ssl_opts()

              {ssl_opts, opts} ->
                Logger.warning(":ssl_opts is deprecated, pass opts to :ssl instead")
                {ssl_opts, opts}
            end

          {ssl_opts, opts} when is_list(ssl_opts) ->
            {Keyword.merge(default_ssl_opts(), ssl_opts), opts}
        end

      %__MODULE__{
        address: address,
        port: port,
        username:
          Keyword.get(opts, :username, System.get_env("USER")) || raise(":username is missing"),
        password: nilify(Keyword.get(opts, :password, System.get_env("MYSQL_PWD"))),
        database: Keyword.get(opts, :database),
        ssl_opts: ssl_opts,
        connect_timeout: Keyword.get(opts, :connect_timeout, @default_timeout),
        handshake_timeout: Keyword.get(opts, :handshake_timeout, @default_timeout),
        socket_options: (opts[:socket_options] || []) ++ @sock_opts,
        charset: Keyword.get(opts, :charset),
        collation: Keyword.get(opts, :collation),
        enable_cleartext_plugin: Keyword.get(opts, :enable_cleartext_plugin, false)
      }
    end

    defp default_ssl_opts do
      [
        verify: :verify_peer,
        customize_hostname_check: [
          match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
        ]
      ]
    end

    defp nilify(""), do: nil
    defp nilify(other), do: other

    defp address_and_port(opts) do
      hostname = Keyword.get(opts, :hostname, System.get_env("MYSQL_HOST"))

      default_protocol =
        if (!is_nil(hostname) or Keyword.has_key?(opts, :port)) and
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
          hostname = String.to_charlist(hostname || "localhost")
          default_port = String.to_integer(System.get_env("MYSQL_TCP_PORT") || "3306")
          port = Keyword.get(opts, :port, default_port)
          {hostname, port}
      end
    end
  end

  @default_max_packet_size 16_777_215

  # https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
  # utf8mb4
  @default_charset_code 45

  def connect(%Config{} = config) do
    with {:ok, client} <- do_connect(config) do
      handshake(client, config)
    end
  end

  def connect(opts) when is_list(opts) do
    connect(Config.new(opts))
  end

  def com_ping(client, ping_timeout) do
    with :ok <- send_com(client, :com_ping) do
      recv_packet(client, &decode_generic_response/1, ping_timeout)
    end
  end

  def com_query(client, statement, result_state \\ :single) do
    with :ok <- send_com(client, {:com_query, statement}) do
      recv_packets(client, &decode_com_query_response/3, :initial, result_state)
    end
  end

  def com_stmt_prepare(client, statement) do
    with :ok <- send_com(client, {:com_stmt_prepare, statement}) do
      recv_packets(client, &decode_com_stmt_prepare_response/3, :initial, :single)
    end
  end

  def com_stmt_execute(client, statement_id, params, cursor_type, result_state \\ :single) do
    with :ok <- send_com(client, {:com_stmt_execute, statement_id, params, cursor_type}) do
      recv_packets(client, &decode_com_stmt_execute_response/3, :initial, result_state)
    end
  end

  def com_stmt_fetch(client, statement_id, column_defs, max_rows) do
    with :ok <- send_com(client, {:com_stmt_fetch, statement_id, max_rows}) do
      recv_packets(client, &decode_com_stmt_fetch_response/3, {:initial, column_defs}, :single)
    end
  end

  def com_stmt_reset(client, statement_id) do
    with :ok <- send_com(client, {:com_stmt_reset, statement_id}) do
      recv_packet(client, &decode_generic_response/1)
    end
  end

  def com_stmt_close(client, statement_id) do
    send_com(client, {:com_stmt_close, statement_id})
  end

  def com_quit(client) do
    with :ok <- send_com(client, :com_quit) do
      recv_packet(client, &decode_generic_response/1)
    end
  end

  def disconnect(client) do
    {sock_mod, sock} = client.sock
    sock_mod.close(sock)
    :ok
  end

  def send_com(client, com) do
    payload = encode_com(com)
    send_packet(client, payload, 0)
  end

  def send_recv_packet(client, payload, decoder, sequence_id) do
    with :ok <- send_packet(client, payload, sequence_id) do
      recv_packet(client, decoder)
    end
  end

  def send_packet(client, payload, sequence_id) do
    data = encode_packet(payload, sequence_id, @default_max_packet_size)
    send_data(client, data)
  end

  def send_data(%{sock: {sock_mod, sock}}, data) do
    sock_mod.send(sock, data)
  end

  def recv_packet(client, decoder, timeout \\ :infinity) do
    # even if next packet follows, ignore it
    new_decoder = fn payload, _next_packet, nil -> {:halt, decoder.(payload)} end
    recv_packets(client, new_decoder, nil, :single, timeout)
  end

  def recv_packets(client, decoder, decoder_state, result_state, timeout \\ :infinity) do
    case recv_data(client, timeout) do
      {:ok, data} ->
        recv_packets(data, decoder, decoder_state, result_state, timeout, client)

      {:error, _} = error ->
        error
    end
  end

  def recv_data(%{sock: {sock_mod, sock}}, timeout) do
    sock_mod.recv(sock, 0, timeout)
  end

  ## Internals

  defp recv_packets(data, decode, decoder_state, result_state, timeout, client, partial \\ <<>>)

  defp recv_packets(
         <<size::uint3(), _seq::uint1(), payload::string(size), rest::binary>> = data,
         decoder,
         {:more_results, resultset},
         result_state,
         timeout,
         client,
         partial
       )
       when size < @default_max_packet_size do
    case decode_more_results(<<partial::binary, payload::binary>>, rest, resultset, result_state) do
      {:cont, decoder_state, result_state} ->
        recv_packets(data, decoder, decoder_state, result_state, timeout, client, partial)

      {:halt, result} ->
        {:ok, result}

      {:error, _} = error ->
        error
    end
  end

  defp recv_packets(
         <<size::uint3(), _seq::uint1(), payload::string(size), rest::binary>>,
         decoder,
         decoder_state,
         result_state,
         timeout,
         client,
         partial
       )
       when size < @default_max_packet_size do
    case decoder.(<<partial::binary, payload::binary>>, rest, decoder_state) do
      {:cont, decoder_state} ->
        recv_packets(rest, decoder, decoder_state, result_state, timeout, client)

      {:halt, result} ->
        case result_state do
          :single -> {:ok, result}
          {:many, results} -> {:ok, [result | results]}
        end

      {:error, _} = error ->
        error
    end
  end

  # If the packet size equals max packet size, save the payload, receive
  # more data and try again
  defp recv_packets(
         <<size::uint3(), _seq::uint1(), payload::string(size), rest::binary>>,
         decoder,
         decoder_state,
         result_state,
         timeout,
         client,
         partial
       )
       when size >= @default_max_packet_size do
    recv_packets(
      rest,
      decoder,
      decoder_state,
      result_state,
      timeout,
      client,
      <<partial::binary, payload::binary>>
    )
  end

  # If we didn't match on a full packet, receive more data and try again
  defp recv_packets(rest, decoder, decoder_state, result_state, timeout, client, partial) do
    case recv_data(client, timeout) do
      {:ok, data} ->
        recv_packets(
          <<rest::binary, data::binary>>,
          decoder,
          decoder_state,
          result_state,
          timeout,
          client,
          partial
        )

      {:error, _} = error ->
        error
    end
  end

  @doc false
  def do_connect(config) do
    %{
      address: address,
      port: port,
      socket_options: socket_options,
      connect_timeout: connect_timeout
    } = config

    buffer? = Keyword.has_key?(socket_options, :buffer)
    client = %__MODULE__{connection_id: nil, sock: nil}

    case :gen_tcp.connect(address, port, socket_options ++ @sock_opts, connect_timeout) do
      {:ok, sock} when buffer? ->
        {:ok, %{client | sock: {:gen_tcp, sock}}}

      {:ok, sock} ->
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(sock, [:sndbuf, :recbuf, :buffer])

        buffer = buffer |> max(sndbuf) |> max(recbuf)
        :ok = :inet.setopts(sock, buffer: buffer)
        {:ok, %{client | sock: {:gen_tcp, sock}}}

      other ->
        other
    end
  end

  ## Handshake

  defp handshake(client, config) do
    %{sock: {:gen_tcp, sock}} = client
    timer = start_handshake_timer(config.handshake_timeout, sock)

    case do_handshake(client, config) do
      {:ok, client} ->
        cancel_handshake_timer(timer)
        {:ok, client}

      {:error, reason} ->
        cancel_handshake_timer(timer)
        {:error, reason}
    end
  end

  defp do_handshake(client, config) do
    with {:ok, initial_handshake(conn_id: conn_id) = initial_handshake} <- recv_handshake(client),
         client = %{client | connection_id: conn_id},
         sequence_id = 1,
         {:ok, capability_flags} <- build_capability_flags(config, initial_handshake),
         {:ok, sequence_id, client} <-
           maybe_upgrade_to_ssl(client, config, capability_flags, sequence_id) do
      result =
        handle_handshake_response(
          client,
          config,
          initial_handshake,
          capability_flags,
          sequence_id
        )

      with {:ok, ok_packet()} <- result,
           {:ok, ok_packet()} <- maybe_set_names(client, config) do
        {:ok, client}
      else
        {:ok, %{}} = ok ->
          ok

        {:ok, err_packet() = err_packet} ->
          disconnect(client)
          {:error, err_packet}

        {:error, reason} ->
          disconnect(client)
          {:error, reason}
      end
    end
  end

  defp maybe_set_names(client, %{charset: nil, collation: nil}) do
    {:ok, client}
  end

  defp maybe_set_names(client, %{charset: charset, collation: nil}) when is_binary(charset) do
    com_query(client, "SET NAMES '#{charset}'")
  end

  defp maybe_set_names(client, %{charset: charset, collation: collation})
       when is_binary(charset) and is_binary(collation) do
    com_query(client, "SET NAMES '#{charset}' COLLATE '#{collation}'")
  end

  defp maybe_upgrade_to_ssl(client, %{ssl_opts: nil}, _capability_flags, sequence_id) do
    {:ok, sequence_id, client}
  end

  defp maybe_upgrade_to_ssl(client, %{ssl_opts: ssl_opts} = config, capability_flags, sequence_id) do
    {_, sock} = client.sock

    ssl_opts =
      if is_list(config.address) do
        Keyword.put_new(ssl_opts, :server_name_indication, config.address)
      else
        ssl_opts
      end

    ssl_request =
      ssl_request(
        capability_flags: capability_flags,
        charset: @default_charset_code,
        max_packet_size: @default_max_packet_size
      )

    payload = encode_ssl_request(ssl_request)

    with :ok <- send_packet(client, payload, sequence_id),
         {:ok, ssl_sock} <- :ssl.connect(sock, ssl_opts, config.connect_timeout) do
      {:ok, sequence_id + 1, %{client | sock: {:ssl, ssl_sock}}}
    end
  end

  defp recv_handshake(client) do
    recv_packet(client, &decode_initial_handshake/1)
  end

  defp handle_handshake_response(client, config, initial_handshake, capability_flags, sequence_id) do
    initial_handshake(
      auth_plugin_name: initial_auth_plugin_name,
      auth_plugin_data: initial_auth_plugin_data
    ) = initial_handshake

    auth_response = Auth.auth_response(config, initial_auth_plugin_name, initial_auth_plugin_data)

    handshake_response =
      handshake_response_41(
        capability_flags: capability_flags,
        username: config.username,
        auth_plugin_name: initial_auth_plugin_name,
        auth_response: auth_response,
        database: config.database,
        charset: @default_charset_code,
        max_packet_size: @default_max_packet_size
      )

    payload = encode_handshake_response_41(handshake_response)

    case send_recv_packet(client, payload, &decode_auth_response/1, sequence_id) do
      {:ok, auth_switch_request(plugin_name: auth_plugin_name, plugin_data: auth_plugin_data)} ->
        auth_response = Auth.auth_response(config, auth_plugin_name, auth_plugin_data)

        case send_recv_packet(client, auth_response, &decode_auth_response/1, sequence_id + 2) do
          {:ok, :full_auth} ->
            perform_full_auth(client, config, auth_plugin_name, auth_plugin_data, sequence_id + 2)

          {:ok, auth_more_data(data: public_key)} ->
            perform_public_key_auth(
              client,
              config.password,
              public_key,
              auth_plugin_data,
              sequence_id + 4
            )

          other ->
            other
        end

      {:ok, :full_auth} ->
        perform_full_auth(
          client,
          config,
          initial_auth_plugin_name,
          initial_auth_plugin_data,
          sequence_id
        )

      {:ok, auth_more_data(data: public_key)} ->
        perform_public_key_auth(
          client,
          config.password,
          public_key,
          initial_auth_plugin_data,
          sequence_id + 2
        )

      other ->
        other
    end
  end

  defp perform_public_key_auth(client, password, public_key, auth_plugin_data, sequence_id) do
    auth_response = Auth.encrypt_sha_password(password, public_key, auth_plugin_data)
    send_recv_packet(client, auth_response, &decode_auth_response/1, sequence_id)
  end

  defp perform_full_auth(client, config, "caching_sha2_password", auth_plugin_data, sequence_id) do
    auth_response =
      if config.ssl_opts do
        [config.password, 0]
      else
        # request public key
        <<2>>
      end

    case send_recv_packet(client, auth_response, &decode_auth_response/1, sequence_id + 2) do
      {:ok, auth_more_data(data: public_key)} ->
        perform_public_key_auth(
          client,
          config.password,
          public_key,
          auth_plugin_data,
          sequence_id + 4
        )

      other ->
        other
    end
  end

  defp start_handshake_timer(:infinity, _), do: :infinity

  defp start_handshake_timer(timeout, sock) do
    args = [timeout, self(), sock]
    {:ok, tref} = :timer.apply_after(timeout, __MODULE__, :handshake_shutdown, args)
    {:timer, tref}
  end

  @doc false
  def handshake_shutdown(timeout, pid, sock) do
    if Process.alive?(pid) do
      Logger.error(fn ->
        [
          inspect(__MODULE__),
          " (",
          inspect(pid),
          ") timed out because it was handshaking for longer than ",
          to_string(timeout) | "ms"
        ]
      end)

      :gen_tcp.shutdown(sock, :read_write)
    end
  end

  def cancel_handshake_timer(:infinity), do: :ok

  def cancel_handshake_timer({:timer, tref}) do
    {:ok, _} = :timer.cancel(tref)
    :ok
  end
end
