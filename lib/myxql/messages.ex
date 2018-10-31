defmodule MyXQL.Messages do
  @moduledoc false
  import Record
  use Bitwise
  alias MyXQL.Types, as: T

  # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  # OK packets are indicating EOF (instead of separate EOF packet)
  # from this version.
  #
  # We support first GA version, 5.7.9, see: https://dev.mysql.com/doc/relnotes/mysql/5.7/en/
  #
  # TODO: consider supporting at least 5.7.10 as prior versions only support TLS v1,
  # maybe we should require at least v1.1.
  # https://dev.mysql.com/doc/refman/5.7/en/encrypted-connection-protocols-ciphers.html
  @min_server_version Version.parse!("5.7.9")

  @max_packet_size 65536

  # https://dev.mysql.com/doc/internals/en/capability-flags.html
  # TODO: double-check https://github.com/elixir-ecto/ecto/blob/v3.0.0-rc.1/integration_test/cases/type.exs#L336:L337,
  #       found row vs affected row
  @capability_flags %{
    client_long_password: 0x00000001,
    client_found_rows: 0x00000002,
    client_long_flag: 0x00000004,
    client_connect_with_db: 0x00000008,
    client_no_schema: 0x00000010,
    client_compress: 0x00000020,
    client_odbc: 0x00000040,
    client_local_files: 0x00000080,
    client_ignore_space: 0x00000100,
    client_protocol_41: 0x00000200,
    client_interactive: 0x00000400,
    client_ssl: 0x00000800,
    client_ignore_sigpipe: 0x00001000,
    client_transactions: 0x00002000,
    client_reserved: 0x00004000,
    client_secure_connection: 0x00008000,
    client_multi_statements: 0x00010000,
    client_multi_results: 0x00020000,
    client_ps_multi_results: 0x00040000,
    client_plugin_auth: 0x00080000,
    client_connect_attrs: 0x00100000,
    client_plugin_auth_lenenc_client_data: 0x00200000,
    client_can_handle_expired_passwords: 0x00400000,
    client_session_track: 0x00800000,
    client_deprecate_eof: 0x01000000
  }

  def has_capability_flag?(flags, name) do
    value = Map.fetch(@capability_flags, name)
    flags &&& value == value
  end

  def set_capability_flags(flags \\ 0, names) do
    Enum.reduce(names, flags, & &2 ||| Map.fetch!(@capability_flags, &1))
  end

  defp capability_flags(database, ssl?) do
    set_capability_flags([
      :client_protocol_41, :client_deprecate_eof, :client_plugin_auth,
      :client_secure_connection, :client_found_rows, :client_multi_statements
    ])
    |> maybe_put_flag(Map.fetch!(@capability_flags, :client_connect_with_db), !is_nil(database))
    |> maybe_put_flag(Map.fetch!(@capability_flags, :client_ssl), ssl?)
  end

  defp maybe_put_flag(flags, flag, true), do: flags ||| flag
  defp maybe_put_flag(flags, _flag, false), do: flags

  # https://dev.mysql.com/doc/internals/en/status-flags.html
  @status_flags %{
    server_status_in_trans: 0x0001,
    server_status_autocommit: 0x0002,
    server_more_results_exists: 0x0008,
    server_status_no_good_index_used: 0x0010,
    server_status_no_index_used: 0x0020,
    server_status_cursor_exists: 0x0040,
    server_status_last_row_sent: 0x0080,
    server_status_db_dropped: 0x0100,
    server_status_no_backslash_escapes: 0x0200,
    server_status_metadata_changed: 0x0400,
    server_query_was_slow: 0x0800,
    server_ps_out_params: 0x1000,
    server_status_in_trans_readonly: 0x2000,
    server_session_state_changed: 0x4000
  }

  def has_status_flag?(flags, name) do
    value = Map.fetch!(@status_flags, name)
    (flags &&& value) == value
  end

  ###########################################################
  # Basic packets
  #
  # https://dev.mysql.com/doc/internals/en/mysql-packet.html
  ###########################################################

  # https://dev.mysql.com/doc/internals/en/mysql-packet.html
  defrecord :packet, [:payload_length, :sequence_id, :payload]

  def decode_packet(data) do
    <<payload_length::size(24), sequence_id::size(8), payload::binary>> = data
    packet(payload_length: payload_length, sequence_id: sequence_id, payload: payload)
  end

  def take_packet(data) do
    <<
      payload_length::little-integer-size(24),
      _sequence_id::size(8),
      payload::bytes-size(payload_length),
      rest::binary
    >> = data

    {payload, rest}
  end

  def encode_packet(payload, sequence_id) do
    payload_length = byte_size(payload)
    <<payload_length::little-size(24), sequence_id::little-size(8), payload::binary>>
  end

  # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  # TODO:
  # - handle lenenc integers for last_insert_id and last_insert_id
  # - investigate using CLIENT_SESSION_TRACK & SERVER_SESSION_STATE_CHANGED capabilities
  defrecord :ok_packet, [:affected_rows, :last_insert_id, :status_flags, :warning_count]

  def decode_ok_packet(data) do
    <<
      0,
      affected_rows::little-8,
      last_insert_id::little-8,
      status_flags::little-16,
      warning_count::little-16,
      _::binary
    >> = data

    ok_packet(
      affected_rows: affected_rows,
      last_insert_id: last_insert_id,
      status_flags: status_flags,
      warning_count: warning_count
    )
  end

  # https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
  defrecord :err_packet, [:error_code, :sql_state_marker, :sql_state, :error_message]

  def decode_err_packet(data) do
    <<
      0xFF,
      error_code::16-little,
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

  ##############################################################
  # Connection Phase
  #
  # https://dev.mysql.com/doc/internals/en/connection-phase.html
  ##############################################################

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
    {server_version, rest} = T.take_null_terminated_string(rest)

    if Version.compare(server_version, @min_server_version) == :lt do
      raise "minimum supported server version is #{@min_server_version}, got: #{server_version}"
    end

    <<
      conn_id::32-little,
      auth_plugin_data1::8-bytes,
      0,
      capability_flags1::16-little,
      character_set::8,
      status_flags::16-little,
      capability_flags2::16-little,
      auth_plugin_data_length::8,
      0::8*10,
      rest::binary
    >> = rest

    take = auth_plugin_data_length - 8
    <<auth_plugin_data2::binary-size(take), auth_plugin_name::binary>> = rest
    auth_plugin_data2 = T.decode_null_terminated_string(auth_plugin_data2)
    auth_plugin_name = T.decode_null_terminated_string(auth_plugin_name)

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

  def encode_handshake_response_41(
        username,
        auth_plugin_name,
        auth_response,
        database,
        ssl?,
        sequence_id
      ) do
    capability_flags = capability_flags(database, ssl?)

    charset = 8
    username = <<username::binary, 0>>
    database = if database, do: <<database::binary, 0x00>>, else: ""
    auth_plugin_name = <<auth_plugin_name::binary, 0x00>>

    auth_response =
      if auth_response do
        <<byte_size(auth_response), auth_response::binary>>
      else
        <<0>>
      end

    payload = <<
      capability_flags::32-little,
      @max_packet_size::32-little,
      charset,
      0::8*23,
      username::binary,
      auth_response::binary,
      database::binary,
      auth_plugin_name::binary
    >>

    encode_packet(payload, sequence_id)
  end

  def encode_ssl_request(sequence_id, database) do
    capability_flags = capability_flags(database, true)
    charset = 8

    payload = <<
      capability_flags::32-little,
      @max_packet_size::32-little,
      charset,
      0::8*23
    >>

    encode_packet(payload, sequence_id)
  end

  def decode_handshake_response(data) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      <<0x00, _::binary>> -> decode_ok_packet(payload)
      <<0xFF, _::binary>> -> decode_err_packet(payload)
      <<0xFE, _::binary>> -> decode_auth_switch_request(payload)
      <<0x01, 0x04>> -> :full_auth
    end
  end

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
  defrecord :auth_switch_request, [:plugin_name, :plugin_data]

  def decode_auth_switch_request(<<0xFE, rest::binary>>) do
    {plugin_name, rest} = T.take_null_terminated_string(rest)
    {plugin_data, ""} = T.take_null_terminated_string(rest)

    auth_switch_request(plugin_name: plugin_name, plugin_data: plugin_data)
  end

  #################################################################
  # Text & Binary Protocol
  #
  # https://dev.mysql.com/doc/internals/en/text-protocol.html
  # https://dev.mysql.com/doc/internals/en/prepared-statements.html
  #################################################################

  # https://dev.mysql.com/doc/internals/en/com-query.html
  def encode_com_query(query) do
    encode_com(0x03, query)
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html#packet-COM_STMT_PREPARE
  def encode_com_stmt_prepare(query) do
    encode_com(0x16, query)
  end

  defp encode_com(command, binary) do
    sequence_id = 0
    encode_packet(<<command::integer, binary::binary>>, sequence_id)
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
  # https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
  #
  # both text & binary resultset have the same columns shape, but different rows
  defrecord :resultset, [:column_count, :column_definitions, :rows, :warning_count, :status_flags]

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-COM_QUERY_Response
  def decode_com_query_response(data) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      <<0x00, _::binary>> ->
        decode_ok_packet(payload)

      <<0xFF, _::binary>> ->
        decode_err_packet(payload)

      # TODO: column_count is lenenc_int, not just int
      <<column_count::size(8), rest::binary>> ->
        {column_definitions, rest} = decode_column_definitions(rest, column_count, [])

        {rows, warning_count, status_flags} =
          decode_text_resultset_rows(rest, column_definitions, [])

        resultset(
          column_count: column_count,
          column_definitions: column_definitions,
          rows: rows,
          warning_count: warning_count,
          status_flags: status_flags
        )
    end
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html

  def decode_com_stmt_prepare_response(data) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      <<0x00, _::binary>> ->
        decode_com_stmt_prepare_ok(payload)

      <<0xFF, _::binary>> ->
        decode_err_packet(payload)
    end
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
  defrecord :com_stmt_prepare_ok, [:statement_id]

  def decode_com_stmt_prepare_ok(data) do
    <<
      0,
      statement_id::32-little,
      _num_columns::16-little,
      _num_params::16-little,
      0,
      _warning_count::16-little,
      _rest::binary
    >> = data

    com_stmt_prepare_ok(statement_id: statement_id)
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
  def encode_com_stmt_execute(statement_id, params) do
    command = 0x17

    # TODO: cursors
    # CURSOR_TYPE_NO_CURSOR  0x00
    # CURSOR_TYPE_READ_ONLY  0x01
    # CURSOR_TYPE_FOR_UPDATE 0x02
    # CURSOR_TYPE_SCROLLABLE 0x04
    flags = 0x00

    # Always 0x01
    iteration_count = 0x01

    # TODO: handle null_bitmap
    null_bitmap = 0

    new_params_bound_flag = 1
    {types, values} = params |> Enum.map(&T.encode_value/1) |> Enum.unzip()

    # TODO: find out and document why types are null-terminated
    types = for t <- types, do: <<t, 0>>, into: ""
    values = for v <- values, do: <<v::binary>>, into: ""

    payload = <<
      command,
      statement_id::little-integer-size(32),
      flags::size(8),
      iteration_count::little-integer-size(32),
      null_bitmap::8,
      new_params_bound_flag::8,
      types::binary,
      values::binary
    >>

    sequence_id = 0
    encode_packet(payload, sequence_id)
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-execute-response.html
  # TODO: similar to decode_com_query_response, except for decode_binary_resultset_rows
  def decode_com_stmt_execute_response(data) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      <<0x00, _::binary>> ->
        decode_ok_packet(payload)

      <<0xFF, _::binary>> ->
        decode_err_packet(payload)

      # TODO: column_count is lenenc_int, not int
      <<column_count::size(8), rest::binary>> ->
        {column_definitions, rest} = decode_column_definitions(rest, column_count, [])

        {rows, warning_count, status_flags} =
          decode_binary_resultset_rows(rest, column_definitions, [])

        resultset(
          column_count: column_count,
          column_definitions: column_definitions,
          rows: rows,
          warning_count: warning_count,
          status_flags: status_flags
        )
    end
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
  defrecord :column_definition41, [:name, :type]

  defp decode_column_definition41(data) do
    packet(payload: payload) = decode_packet(data)

    <<
      3,
      "def",
      rest::binary
    >> = payload

    {_schema, rest} = T.take_length_encoded_string(rest)
    {_table, rest} = T.take_length_encoded_string(rest)
    {_org_table, rest} = T.take_length_encoded_string(rest)
    {name, rest} = T.take_length_encoded_string(rest)
    {_org_name, rest} = T.take_length_encoded_string(rest)

    <<
      0x0C,
      _character_set::2-bytes,
      _column_length::32-little,
      <<type>>,
      _flags::2-bytes,
      _decimals::1-bytes,
      0::8*2,
      rest::binary
    >> = rest

    {column_definition41(name: name, type: type), rest}
  end

  defp decode_column_definitions(data, column_count, acc) when column_count > 0 do
    {column_name, rest} = decode_column_definition41(data)
    decode_column_definitions(rest, column_count - 1, [column_name | acc])
  end

  defp decode_column_definitions(rest, 0, acc) do
    {Enum.reverse(acc), rest}
  end

  defp decode_text_resultset_rows(data, column_definitions, acc) do
    packet(payload: payload) = decode_packet(data)

    case payload do
      # EOF packet
      <<0xFE, warning_count::16-little, status_flags::16-little, 0::8*2>> ->
        {Enum.reverse(acc), warning_count, status_flags}

      _ ->
        {row, rest} = decode_text_resultset_row(payload, column_definitions, [])
        decode_text_resultset_rows(rest, column_definitions, [row | acc])
    end
  end

  defp decode_text_resultset_row(data, [column_definition41(type: type) | tail], acc) do
    case data do
      <<value_size::size(8), value::bytes-size(value_size), rest::binary>> ->
        decode_text_resultset_row(rest, tail, [T.decode_text_value(value, type) | acc])

      <<0xFB, rest::binary>> ->
        decode_text_resultset_row(rest, tail, [nil | acc])
    end
  end

  defp decode_text_resultset_row(rest, [], acc) do
    {Enum.reverse(acc), rest}
  end

  def take_binary_resultset_row(data) do
    <<
      payload_length::little-integer-24,
      _sequence_id::8,
      payload::bytes-size(payload_length),
      rest::binary
    >> = data

    {payload, rest}
  end

  # https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
  defp decode_binary_resultset_rows(data, column_definitions, acc) do
    case take_packet(data) do
      # EOF packet
      {<<0xFE, warning_count::size(16), status_flags::size(16), 0::size(16)>>, ""} ->
        {Enum.reverse(acc), warning_count, status_flags}

      {payload, rest} ->
        <<0, _null_bitmap, values::binary>> = payload
        row = decode_binary_resultset_row(values, column_definitions, [])
        decode_binary_resultset_rows(rest, column_definitions, [row | acc])
    end
  end

  defp decode_binary_resultset_row(values, [column_definition | column_definitions], acc) do
    column_definition41(type: type) = column_definition
    {value, rest} = T.take_binary_value(values, type)
    decode_binary_resultset_row(rest, column_definitions, [value | acc])
  end

  defp decode_binary_resultset_row("", [], acc) do
    Enum.reverse(acc)
  end
end
