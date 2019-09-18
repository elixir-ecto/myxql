defmodule MyXQL.Client do
  @moduledoc false

  require Logger
  import MyXQL.{Protocol, Protocol.Records, Protocol.Types}
  alias MyXQL.Protocol.Auth

  defmodule Config do
    @moduledoc false

    @default_timeout 15_000

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
      :socket_options,
      :max_packet_size,
      :charset,
      :collation
    ]

    def new(opts) do
      {address, port} = address_and_port(opts)

      %__MODULE__{
        address: address,
        port: port,
        username:
          Keyword.get(opts, :username, System.get_env("USER")) || raise(":username is missing"),
        password: nilify(Keyword.get(opts, :password, System.get_env("MYSQL_PWD"))),
        database: Keyword.get(opts, :database),
        ssl?: Keyword.get(opts, :ssl, false),
        ssl_opts: Keyword.get(opts, :ssl_opts, []),
        connect_timeout: Keyword.get(opts, :connect_timeout, @default_timeout),
        handshake_timeout: Keyword.get(opts, :handshake_timeout, @default_timeout),
        socket_options:
          Keyword.merge([mode: :binary, packet: :raw, active: false], opts[:socket_options] || []),
        charset: Keyword.get(opts, :charset),
        collation: Keyword.get(opts, :collation)
      }
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

  def connect(opts) when is_list(opts) do
    connect(Config.new(opts))
  end

  def connect(%Config{} = config) do
    with {:ok, state} <- do_connect(config) do
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
      recv_packets(&decode_com_stmt_fetch_response/3, {:initial, column_defs}, state)
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

  def disconnect(%{sock: {sock_mod, sock}}) do
    sock_mod.close(sock)
    :ok
  end

  def send_com(com, state) do
    payload = encode_com(com)
    send_packet(payload, 0, state)
  end

  def send_recv_packet(payload, decoder, sequence_id, sock) do
    with :ok <- send_packet(payload, sequence_id, sock) do
      recv_packet(decoder, sock)
    end
  end

  def send_packet(payload, sequence_id, state) do
    data = encode_packet(payload, sequence_id, @default_max_packet_size)
    send_data(state, data)
  end

  def send_data(%{sock: {sock_mod, sock}}, data) do
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

  def recv_data(%{sock: {sock_mod, sock}}, timeout) do
    sock_mod.recv(sock, 0, timeout)
  end

  ## Internals

  defp recv_packets(
         <<size::uint3, _seq::uint1, payload::string(size), rest::binary>>,
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

  @doc false
  def do_connect(config) do
    %{
      address: address,
      port: port,
      socket_options: socket_options,
      connect_timeout: connect_timeout
    } = config

    buffer? = Keyword.has_key?(socket_options, :buffer)
    state = %{connection_id: nil, sock: nil}

    case :gen_tcp.connect(address, port, socket_options, connect_timeout) do
      {:ok, sock} when buffer? ->
        {:ok, Map.put(state, :sock, {:gen_tcp, sock})}

      {:ok, sock} ->
        {:ok, [sndbuf: sndbuf, recbuf: recbuf, buffer: buffer]} =
          :inet.getopts(sock, [:sndbuf, :recbuf, :buffer])

        buffer = buffer |> max(sndbuf) |> max(recbuf)
        :ok = :inet.setopts(sock, buffer: buffer)
        {:ok, %{state | sock: {:gen_tcp, sock}}}

      other ->
        other
    end
  end

  ## Handshake

  defp handshake(config, %{sock: {:gen_tcp, sock}} = state) do
    timer = start_handshake_timer(config.handshake_timeout, sock)

    case do_handshake(config, state) do
      {:ok, state} ->
        cancel_handshake_timer(timer)
        {:ok, state}

      {:error, reason} ->
        cancel_handshake_timer(timer)
        {:error, reason}
    end
  end

  defp do_handshake(config, state) do
    with {:ok, initial_handshake(conn_id: conn_id) = initial_handshake} <- recv_handshake(state),
         state = %{state | connection_id: conn_id},
         sequence_id = 1,
         {:ok, capability_flags} <- build_capability_flags(config, initial_handshake),
         {:ok, sequence_id, state} <-
           maybe_upgrade_to_ssl(config, capability_flags, sequence_id, state) do
      result =
        handle_handshake_response(
          config,
          initial_handshake,
          capability_flags,
          sequence_id,
          state
        )

      with {:ok, ok_packet()} <- result,
           {:ok, ok_packet()} <- maybe_set_names(config, state) do
        {:ok, state}
      else
        {:ok, %{}} = ok ->
          ok

        {:ok, err_packet() = err_packet} ->
          disconnect(state)
          {:error, err_packet}

        {:error, reason} ->
          disconnect(state)
          {:error, reason}
      end
    end
  end

  defp maybe_set_names(%{charset: nil, collation: nil}, state) do
    {:ok, state}
  end

  defp maybe_set_names(%{charset: charset, collation: nil}, state) when is_binary(charset) do
    com_query("SET NAMES '#{charset}'", state)
  end

  defp maybe_set_names(%{charset: charset, collation: collation}, state)
       when is_binary(charset) and is_binary(collation) do
    com_query("SET NAMES '#{charset}' COLLATE '#{collation}'", state)
  end

  defp maybe_upgrade_to_ssl(%{ssl?: true} = config, capability_flags, sequence_id, state) do
    {_, sock} = state.sock

    ssl_request =
      ssl_request(
        capability_flags: capability_flags,
        charset: @default_charset_code,
        max_packet_size: @default_max_packet_size
      )

    payload = encode_ssl_request(ssl_request)

    with :ok <- send_packet(payload, sequence_id, state),
         {:ok, ssl_sock} <- :ssl.connect(sock, config.ssl_opts, config.connect_timeout) do
      {:ok, sequence_id + 1, %{state | sock: {:ssl, ssl_sock}}}
    end
  end

  defp maybe_upgrade_to_ssl(%{ssl?: false}, _capability_flags, sequence_id, state) do
    {:ok, sequence_id, state}
  end

  defp recv_handshake(state) do
    recv_packet(&decode_initial_handshake/1, state)
  end

  defp handle_handshake_response(config, initial_handshake, capability_flags, sequence_id, state) do
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

    case send_recv_packet(payload, &decode_auth_response/1, sequence_id, state) do
      {:ok, auth_switch_request(plugin_name: auth_plugin_name, plugin_data: auth_plugin_data)} ->
        auth_response = Auth.auth_response(config, auth_plugin_name, initial_auth_plugin_data)

        case send_recv_packet(auth_response, &decode_auth_response/1, sequence_id + 2, state) do
          {:ok, :full_auth} ->
            perform_full_auth(config, auth_plugin_name, auth_plugin_data, sequence_id + 2, state)

          {:ok, auth_more_data(data: public_key)} ->
            perform_public_key_auth(
              config.password,
              public_key,
              auth_plugin_data,
              sequence_id + 4,
              state
            )

          other ->
            other
        end

      {:ok, :full_auth} ->
        perform_full_auth(
          config,
          initial_auth_plugin_name,
          initial_auth_plugin_data,
          sequence_id,
          state
        )

      {:ok, auth_more_data(data: public_key)} ->
        perform_public_key_auth(
          config.password,
          public_key,
          initial_auth_plugin_data,
          sequence_id + 2,
          state
        )

      other ->
        other
    end
  end

  defp perform_public_key_auth(password, public_key, auth_plugin_data, sequence_id, state) do
    auth_response = Auth.encrypt_sha_password(password, public_key, auth_plugin_data)
    send_recv_packet(auth_response, &decode_auth_response/1, sequence_id, state)
  end

  defp perform_full_auth(config, "caching_sha2_password", auth_plugin_data, sequence_id, state) do
    auth_response =
      if config.ssl? do
        [config.password, 0]
      else
        # request public key
        <<2>>
      end

    case send_recv_packet(auth_response, &decode_auth_response/1, sequence_id + 2, state) do
      {:ok, auth_more_data(data: public_key)} ->
        perform_public_key_auth(
          config.password,
          public_key,
          auth_plugin_data,
          sequence_id + 4,
          state
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
