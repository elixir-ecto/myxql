defmodule Myxql.Messages do
  @moduledoc false
  import Record
  use Bitwise

  @max_packet_size 65536

  # https://dev.mysql.com/doc/internals/en/capability-flags.html
  @client_connect_with_db 0x00000008
  @client_protocol_41 0x00000200
  @client_deprecate_eof 0x01000000

  # https://dev.mysql.com/doc/internals/en/mysql-packet.html
  defrecord :packet, [:payload_length, :sequence_id, :payload]

  def decode_packet(data) do
    <<payload_length::size(24), sequence_id::size(8), payload::binary>> = data
    packet(payload_length: payload_length, sequence_id: sequence_id, payload: payload)
  end

  def encode_packet(payload, sequence_id) do
    payload_length = byte_size(payload)
    <<payload_length::little-size(24), sequence_id::little-size(8), payload::binary>>
  end

  # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  # TODO:
  # - handle lenenc integers for last_insert_id and last_insert_id
  # - investigate using CLIENT_SESSION_TRACK & SERVER_SESSION_STATE_CHANGED capabilities
  defrecord :ok_packet, [:affected_rows, :last_insert_id, :status_flags, :warnings]

  def decode_ok_packet(data) do
    packet(payload: payload) = decode_packet(data)

    <<
      0,
      affected_rows::size(8),
      last_insert_id::size(8),
      status_flags::size(16),
      warnings::size(16)
    >> = payload

    ok_packet(
      affected_rows: affected_rows,
      last_insert_id: last_insert_id,
      status_flags: status_flags,
      warnings: warnings
    )
  end

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
  defrecord :handshake_v10, [
    :protocol_version,
    :server_version,
    :conn_id,
    :auth_plugin_data1,
    :capability_flags1,
    :character_set,
    :status_flags,
    :capability_flags2,
    :auth_plugin_data_length,
    :auth_plugin_data2,
    :auth_plugin_name
  ]

  def decode_handshake_v10(data) do
    packet(payload: payload) = decode_packet(data)
    protocol_version = 10
    <<^protocol_version, rest::binary>> = payload
    [server_version, rest] = :binary.split(rest, <<0>>)

    <<
      conn_id::size(32),
      auth_plugin_data1::8-bytes,
      0,
      capability_flags1::size(16),
      character_set::size(8),
      status_flags::size(16),
      capability_flags2::size(16),
      auth_plugin_data_length::size(8),
      0::size(80),
      rest::binary
    >> = rest

    take = auth_plugin_data_length - 8 - 1
    <<auth_plugin_data2::binary-size(take), 0, auth_plugin_name::binary>> = rest
    [auth_plugin_name, ""] = :binary.split(auth_plugin_name, <<0>>)

    handshake_v10(
      protocol_version: protocol_version,
      server_version: server_version,
      conn_id: conn_id,
      auth_plugin_data1: auth_plugin_data1,
      capability_flags1: capability_flags1,
      character_set: character_set,
      status_flags: status_flags,
      capability_flags2: capability_flags2,
      auth_plugin_data_length: auth_plugin_data_length,
      auth_plugin_data2: auth_plugin_data2,
      auth_plugin_name: auth_plugin_name
    )
  end

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
  defrecord :handshake_response_41, [
    :capability_flags,
    :max_packet_size,
    :character_set,
    :username,
    :auth_response,
    :database
  ]

  def encode_handshake_response_41(user, auth_response, database) do
    capability_flags = @client_connect_with_db ||| @client_protocol_41 ||| @client_deprecate_eof
    charset = 8

    payload = <<
      capability_flags::little-integer-size(32),
      @max_packet_size::little-integer-size(32),
      <<charset::integer>>,
      String.duplicate(<<0>>, 23)::binary,
      user::binary,
      0,
      <<20>>,
      auth_response::binary,
      database::binary,
      0
    >>

    encode_packet(payload, 1)
  end
end
