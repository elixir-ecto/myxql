defmodule MyXQL.Protocol.Client do
  @moduledoc false

  import MyXQL.Protocol.{Messages, Types}

  # next_data is `""` if there is no more data after parsed packet that we know of.
  # There might still be more data in the socket though, in that case the decoder
  # function needs to return `{:cont, ...}`.
  #
  # Pattern matching on next_data = "" is useful for OK packets etc.
  # Looking at next_data is useful for debugging.
  @typep decoder ::
           (payload :: binary(), next_data :: binary(), state :: term() ->
              {:cont, state :: term()}
              | {:halt, result :: term()}
              | {:error, term()})

  @typep socket_reason() :: :inet.posix() | {:tls_alert, term()} | :timeout

  @spec recv_packet((payload :: binary() -> term()), timeout(), state :: term) ::
          {:ok, term()} | {:error, socket_reason()}
  def recv_packet(decoder, timeout \\ :infinity, state) do
    new_decoder = fn payload, "", nil -> {:halt, decoder.(payload)} end
    recv_packets(new_decoder, nil, timeout, state)
  end

  @spec recv_packets(decoder, decoder_state :: term(), timeout(), state :: term) ::
          {:ok, term()} | {:error, socket_reason()}
  def recv_packets(decoder, decoder_state, timeout \\ :infinity, state) do
    case recv_data(state, timeout) do
      {:ok, data} ->
        recv_packets(data, decoder, decoder_state, timeout, state)

      {:error, _} = error ->
        error
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

  defp send_data(%{sock: sock, sock_mod: sock_mod}, data) do
    sock_mod.send(sock, data)
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

  defp recv_data(%{sock: sock, sock_mod: sock_mod}, timeout) do
    sock_mod.recv(sock, 0, timeout)
  end

  defp sock_close(%{sock: sock, sock_mod: sock_mod}) do
    sock_mod.close(sock)
  end
end
