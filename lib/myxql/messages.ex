defmodule Myxql.Messages do
  @moduledoc false
  import Record

  # https://dev.mysql.com/doc/internals/en/mysql-packet.html
  defrecord :packet, [:payload_length, :sequence_id, :payload]

  def decode_packet(data) do
    <<payload_length::size(24), sequence_id::size(8), payload::binary>> = data
    packet(payload_length: payload_length, sequence_id: sequence_id, payload: payload)
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
end
