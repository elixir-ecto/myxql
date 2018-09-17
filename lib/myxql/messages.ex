defmodule Myxql.Messages do
  @moduledoc false
  import Record
  use Bitwise

  # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  # OK packets are indicating EOF (instead of separate EOF packet)
  # from this version.
  @min_server_version Version.parse!("5.7.5")

  @max_packet_size 65536

  # https://dev.mysql.com/doc/internals/en/capability-flags.html
  @client_connect_with_db 0x00000008
  @client_protocol_41 0x00000200
  @client_deprecate_eof 0x01000000

  # https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
  # TODO: check notes above
  def decode_length_encoded_integer(binary) do
    {integer, ""} = take_length_encoded_integer(binary)
    integer
  end

  def take_length_encoded_integer(<<int::size(8), rest::binary>>) when int < 251, do: {int, rest}
  def take_length_encoded_integer(<<0xFC, int::size(16), rest::binary>>), do: {int, rest}
  def take_length_encoded_integer(<<0xFD, int::size(24), rest::binary>>), do: {int, rest}
  def take_length_encoded_integer(<<0xFE, int::size(64), rest::binary>>), do: {int, rest}

  # https://dev.mysql.com/doc/internals/en/string.html
  def decode_length_encoded_string(binary) do
    {_size, rest} = take_length_encoded_integer(binary)
    rest
  end

  def take_length_encoded_string(binary) do
    {size, rest} = take_length_encoded_integer(binary)
    <<string::bytes-size(size), rest::binary>> = rest
    {string, rest}
  end

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

  # https://dev.mysql.com/doc/internals/en/generic-response-packets.html
  def decode_response_packet(data) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      <<0x00, _::binary>> -> decode_ok_packet(payload)
      <<0xFF, _::binary>> -> decode_err_packet(payload)
    end
  end

  # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  # TODO:
  # - handle lenenc integers for last_insert_id and last_insert_id
  # - investigate using CLIENT_SESSION_TRACK & SERVER_SESSION_STATE_CHANGED capabilities
  defrecord :ok_packet, [:affected_rows, :last_insert_id, :status_flags, :warnings]

  def decode_ok_packet(data) do
    <<
      0,
      affected_rows::size(8),
      last_insert_id::size(8),
      status_flags::size(16),
      warnings::size(16),
      _::binary
    >> = data

    ok_packet(
      affected_rows: affected_rows,
      last_insert_id: last_insert_id,
      status_flags: status_flags,
      warnings: warnings
    )
  end

  # https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
  defrecord :err_packet, [:error_code, :sql_state_marker, :sql_state, :error_message]

  def decode_err_packet(data) do
    <<
      0xFF,
      error_code::size(16),
      sql_state_marker::bytes-size(1),
      sql_state::bytes-size(5),
      error_message::binary
    >> = data

    err_packet(
      error_code: error_code,
      sql_state_marker: sql_state_marker,
      sql_state: sql_state,
      error_message: error_message
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

    if Version.compare(server_version, @min_server_version) == :lt do
      raise "minimum supported server version is #{@min_server_version}, got: #{server_version}"
    end

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
    user = <<user::binary, 0>>
    database = <<database::binary, 0>>

    payload = <<
      capability_flags::little-integer-size(32),
      @max_packet_size::little-integer-size(32),
      <<charset::integer>>,
      String.duplicate(<<0>>, 23)::binary,
      user::binary,
      byte_size(auth_response),
      auth_response::binary,
      database::binary
    >>

    sequence_id = 1
    encode_packet(payload, sequence_id)
  end

  # https://dev.mysql.com/doc/internals/en/com-query.html
  def encode_com_query(query) do
    com_query = 0x03
    sequence_id = 0
    encode_packet(<<com_query::integer, query::binary>>, sequence_id)
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
  defrecord :resultset, [:column_count, :column_definitions, :rows]

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-COM_QUERY_Response
  def decode_com_query_response(data) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      <<0x00, _::binary>> ->
        decode_ok_packet(payload)

      <<0xFF, _::binary>> ->
        decode_err_packet(payload)

      <<column_count::size(8), rest::binary>> ->
        {column_definitions, rest} = decode_column_definitions(rest, column_count, [])
        rows = decode_resultset_rows(rest, column_definitions, [])
        resultset(column_count: column_count, column_definitions: column_definitions, rows: rows)
    end
  end

  defp decode_column_definition41(data) do
    packet(payload: payload) = decode_packet(data)

    <<
      3,
      "def",
      rest::binary
    >> = payload

    {_schema, rest} = take_length_encoded_string(rest)
    {_table, rest} = take_length_encoded_string(rest)
    {_org_table, rest} = take_length_encoded_string(rest)
    {name, rest} = take_length_encoded_string(rest)
    {_org_name, rest} = take_length_encoded_string(rest)

    <<
      0x0C,
      _character_set::2-bytes,
      _column_length::size(32),
      column_type::1-bytes,
      _flags::2-bytes,
      _decimals::1-bytes,
      0::size(16),
      rest::binary
    >> = rest

    {{name, column_type}, rest}
  end

  defp decode_column_definitions(data, column_count, acc) when column_count > 0 do
    {column_name, rest} = decode_column_definition41(data)
    decode_column_definitions(rest, column_count - 1, [column_name | acc])
  end

  defp decode_column_definitions(rest, 0, acc) do
    {Enum.reverse(acc), rest}
  end

  defp decode_resultset_rows(data, column_definitions, acc) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      <<0xFE, _warning_count::size(16), _status_flags::size(16), 0::size(16)>> ->
        Enum.reverse(acc)

      _ ->
        {row, rest} = decode_resultset_row(payload, column_definitions, [])
        decode_resultset_rows(rest, column_definitions, [row | acc])
    end
  end

  defp decode_resultset_row(data, [{_name, type} | tail], acc) do
    <<
      value_size::size(8),
      value::bytes-size(value_size),
      rest::binary
    >> = data

    decode_resultset_row(rest, tail, [decode_value(value, type) | acc])
  end

  defp decode_resultset_row(rest, [], acc) do
    {Enum.reverse(acc), rest}
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
  # TODO: handle remaining types
  defp decode_value(value, <<type>>) when type in [0x01, 0x02, 0x03, 0x08] do
    String.to_integer(value)
  end
  defp decode_value(value, _type) do
    value
  end
end
