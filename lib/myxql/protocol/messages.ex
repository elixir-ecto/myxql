defmodule MyXQL.Protocol.Messages do
  @moduledoc false
  import MyXQL.Protocol.{Flags, Records, Types}
  alias MyXQL.Protocol.Values
  use Bitwise

  @max_packet_size 65536

  defp capability_flags(database, ssl?) do
    put_capability_flags([
      :client_protocol_41,
      :client_deprecate_eof,
      :client_plugin_auth,
      :client_secure_connection,
      :client_found_rows,
      :client_multi_results,
      :client_transactions
    ])
    |> maybe_put_capability_flag(:client_connect_with_db, !is_nil(database))
    |> maybe_put_capability_flag(:client_ssl, ssl?)
  end

  defp maybe_put_capability_flag(flags, name, true), do: put_capability_flags(flags, [name])
  defp maybe_put_capability_flag(flags, _name, false), do: flags

  # https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet
  character_sets = %{
    utf8_general_ci: 0x21
  }

  for {name, code} <- character_sets do
    def character_set_name_to_code(unquote(name)), do: unquote(code)
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
  @cursor_types %{
    cursor_type_no_cursor: 0x00,
    cursor_type_read_only: 0x01,
    cursor_type_for_update: 0x02,
    cursor_type_scrollable: 0x04
  }

  ###########################################################
  # Basic packets
  #
  # https://dev.mysql.com/doc/internals/en/mysql-packet.html
  ###########################################################

  def encode_packet(payload, sequence_id) do
    payload_length = IO.iodata_length(payload)
    [<<payload_length::int(3), sequence_id::int(1)>>, payload]
  end

  def decode_generic_response(<<0x00, rest::binary>>) do
    {affected_rows, rest} = take_int_lenenc(rest)
    {last_insert_id, rest} = take_int_lenenc(rest)

    <<
      status_flags::int(2),
      num_warnings::int(2),
      info::binary
    >> = rest

    ok_packet(
      affected_rows: affected_rows,
      last_insert_id: last_insert_id,
      status_flags: status_flags,
      num_warnings: num_warnings,
      info: info
    )
  end

  def decode_generic_response(
        <<0xFF, error_code::int(2), _sql_state_marker::string(1), _sql_state::string(5),
          error_message::binary>>
      ) do
    err_packet(error_code: error_code, error_message: error_message)
  end

  # Note: header is last argument to allow binary optimization
  def decode_generic_response(<<rest::binary>>, header) do
    decode_generic_response(<<header, rest::binary>>)
  end

  ##############################################################
  # Connection Phase
  #
  # https://dev.mysql.com/doc/internals/en/connection-phase.html
  ##############################################################

  def decode_initial_handshake(payload) do
    protocol_version = 10
    <<^protocol_version, rest::binary>> = payload
    {server_version, rest} = take_string_nul(rest)

    <<
      conn_id::int(4),
      auth_plugin_data1::string(8),
      0,
      capability_flags1::int(2),
      character_set::int(1),
      status_flags::int(2),
      capability_flags2::int(2),
      auth_plugin_data_length::int(1),
      _::int(10),
      rest::binary
    >> = rest

    take = auth_plugin_data_length - 8
    <<auth_plugin_data2::binary-size(take), auth_plugin_name::binary>> = rest
    auth_plugin_data2 = decode_string_nul(auth_plugin_data2)
    auth_plugin_name = decode_string_nul(auth_plugin_name)

    <<capability_flags::int(4)>> = <<capability_flags1::int(2), capability_flags2::int(2)>>

    initial_handshake(
      server_version: server_version,
      conn_id: conn_id,
      auth_plugin_name: auth_plugin_name,
      auth_plugin_data: auth_plugin_data1 <> auth_plugin_data2,
      capability_flags: capability_flags,
      character_set: character_set,
      status_flags: status_flags
    )
  end

  def encode_handshake_response_41(
        username,
        auth_plugin_name,
        auth_response,
        database,
        ssl?
      ) do
    capability_flags = capability_flags(database, ssl?)
    auth_response = if auth_response, do: encode_string_lenenc(auth_response), else: <<0>>
    database = if database, do: <<database::binary, 0x00>>, else: ""

    <<
      capability_flags::int(4),
      @max_packet_size::int(4),
      character_set_name_to_code(:utf8_general_ci),
      0::int(23),
      <<username::binary, 0x00>>,
      auth_response::binary,
      database::binary,
      (<<auth_plugin_name::binary, 0x00>>)
    >>
  end

  def encode_ssl_request(database) do
    capability_flags = capability_flags(database, true)

    <<
      capability_flags::int(4),
      @max_packet_size::int(4),
      character_set_name_to_code(:utf8_general_ci),
      0::int(23)
    >>
  end

  def decode_handshake_response(<<header, rest::binary>>) when header in [0x00, 0xFF] do
    decode_generic_response(rest, header)
  end

  def decode_handshake_response(<<0x01, 0x04>>) do
    :full_auth
  end

  def decode_handshake_response(<<0xFE, rest::binary>>) do
    {plugin_name, rest} = take_string_nul(rest)
    {plugin_data, ""} = take_string_nul(rest)

    auth_switch_request(plugin_name: plugin_name, plugin_data: plugin_data)
  end

  #################################################################
  # Text & Binary Protocol
  #
  # https://dev.mysql.com/doc/internals/en/text-protocol.html
  # https://dev.mysql.com/doc/internals/en/prepared-statements.html
  #################################################################

  # https://dev.mysql.com/doc/internals/en/com-ping.html
  def encode_com(:com_ping) do
    <<0x0E>>
  end

  # https://dev.mysql.com/doc/internals/en/com-query.html
  def encode_com({:com_query, query}) do
    [0x03, query]
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html#packet-COM_STMT_PREPARE
  def encode_com({:com_stmt_prepare, query}) do
    [0x16, query]
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-close.html
  def encode_com({:com_stmt_close, statement_id}) do
    [0x19, <<statement_id::int(4)>>]
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-reset.html
  def encode_com({:com_stmt_reset, statement_id}) do
    [0x1A, <<statement_id::int(4)>>]
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
  def encode_com({:com_stmt_execute, statement_id, params, cursor_type}) do
    command = 0x17
    flags = Map.fetch!(@cursor_types, cursor_type)

    # Always 0x01
    iteration_count = 0x01

    new_params_bound_flag = 1
    {null_bitmap, types, values} = encode_params(params)

    <<
      command,
      statement_id::int(4),
      flags::size(8),
      iteration_count::int(4),
      null_bitmap::bitstring,
      new_params_bound_flag::int(1),
      types::binary,
      values::binary
    >>
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html
  def encode_com({:com_stmt_fetch, statement_id, num_rows}) do
    <<
      0x1C,
      statement_id::int(4),
      num_rows::int(4)
    >>
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-COM_QUERY_Response
  def decode_com_query_response(<<header, rest::binary>>, "", :initial)
      when header in [0x00, 0xFF] do
    {:halt, decode_generic_response(rest, header)}
  end

  def decode_com_query_response(payload, next_data, state) do
    decode_resultset(payload, next_data, state, &Values.decode_text_row/2)
  end

  def decode_com_stmt_prepare_response(
        <<0x00, statement_id::int(4), num_columns::int(2), num_params::int(2), 0,
          num_warnings::int(2)>>,
        next_data,
        :initial
      ) do
    result =
      com_stmt_prepare_ok(
        statement_id: statement_id,
        num_columns: num_columns,
        num_params: num_params,
        num_warnings: num_warnings
      )

    if num_columns + num_params > 0 do
      {:cont, {result, num_columns + num_params}}
    else
      "" = next_data
      {:halt, result}
    end
  end

  def decode_com_stmt_prepare_response(<<rest::binary>>, "", :initial) do
    {:halt, decode_generic_response(rest)}
  end

  # for now, we're simply consuming column_definition packets for params and columns,
  # we might decode them in the future.
  def decode_com_stmt_prepare_response(_payload, next_data, {com_stmt_prepare_ok, packets_left}) do
    if packets_left > 1 do
      {:cont, {com_stmt_prepare_ok, packets_left - 1}}
    else
      "" = next_data
      {:halt, com_stmt_prepare_ok}
    end
  end

  defp encode_params(params) do
    null_type = 0x06

    {count, null_bitmap, types, values} =
      Enum.reduce(params, {0, 0, <<>>, <<>>}, fn
        value, {idx, null_bitmap, types, values} ->
          null_value = if value == nil, do: 1, else: 0
          null_bitmap = null_bitmap ||| null_value <<< idx

          if value == nil do
            {idx + 1, null_bitmap, <<types::binary, null_type, unsigned_flag(value)>>, values}
          else
            {type, binary} = Values.encode_binary_value(value)

            {idx + 1, null_bitmap, <<types::binary, type, unsigned_flag(value)>>,
             <<values::binary, binary::binary>>}
          end
      end)

    null_bitmap_size = div(count + 7, 8)
    {<<null_bitmap::int(null_bitmap_size)>>, types, values}
  end

  defp unsigned_flag(value) when is_integer(value) and value >= 1 <<< 63, do: 0x80
  defp unsigned_flag(_), do: 0x00

  # https://dev.mysql.com/doc/internals/en/com-stmt-execute-response.html
  def decode_com_stmt_execute_response(<<header, rest::binary>>, "", :initial)
      when header in [0x00, 0xFF] do
    {:halt, decode_generic_response(rest, header)}
  end

  def decode_com_stmt_execute_response(payload, next_data, state) do
    decode_resultset(payload, next_data, state, &Values.decode_binary_row/2)
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-fetch-response.html
  def decode_com_stmt_fetch_response(<<0xFF, rest::binary>>, "", {:initial, _column_defs}) do
    {:halt, decode_generic_response(rest, 0xFF)}
  end

  def decode_com_stmt_fetch_response(payload, next_data, {:initial, column_defs}) do
    decode_com_stmt_fetch_response(payload, next_data, {:rows, column_defs, []})
  end

  def decode_com_stmt_fetch_response(payload, next_data, state) do
    decode_resultset(payload, next_data, state, &Values.decode_binary_row/2)
  end

  def decode_column_def(<<3, "def", rest::binary>>) do
    {_schema, rest} = take_string_lenenc(rest)
    {_table, rest} = take_string_lenenc(rest)
    {_org_table, rest} = take_string_lenenc(rest)
    {name, rest} = take_string_lenenc(rest)
    {_org_name, rest} = take_string_lenenc(rest)

    <<
      0x0C,
      _character_set::int(2),
      _column_length::int(4),
      <<type>>,
      flags::int(2),
      _decimals::int(1),
      0::int(2)
    >> = rest

    column_def(
      name: name,
      type: type,
      flags: flags,
      unsigned?: has_column_flag?(flags, :unsigned_flag)
    )
  end

  defp decode_resultset(payload, _next_data, :initial, _row_decoder) do
    {:cont, {:column_defs, decode_int_lenenc(payload), []}}
  end

  defp decode_resultset(payload, _next_data, {:column_defs, num_columns, acc}, _row_decoder) do
    column_def = decode_column_def(payload)
    acc = [column_def | acc]

    if num_columns > 1 do
      {:cont, {:column_defs, num_columns - 1, acc}}
    else
      {:cont, {:rows, Enum.reverse(acc), []}}
    end
  end

  defp decode_resultset(
         <<0xFE, 0, 0, status_flags::int(2), num_warnings::int(2)>>,
         _next_data,
         {:rows, column_defs, acc},
         _row_decoder
       ) do
    resultset =
      resultset(
        column_defs: column_defs,
        num_rows: length(acc),
        rows: Enum.reverse(acc),
        num_warnings: num_warnings,
        status_flags: status_flags
      )

    if has_status_flag?(status_flags, :server_more_results_exists) do
      {:cont, {:trailing_ok_packet, resultset}}
    else
      {:halt, resultset}
    end
  end

  defp decode_resultset(payload, _next_data, {:rows, column_defs, acc}, row_decoder) do
    row = row_decoder.(payload, column_defs)
    {:cont, {:rows, column_defs, [row | acc]}}
  end

  defp decode_resultset(payload, "", {:trailing_ok_packet, resultset}, _row_decoder) do
    ok_packet(status_flags: status_flags) = decode_generic_response(payload)
    {:halt, resultset(resultset, status_flags: status_flags)}
  end

  defp decode_resultset(_payload, _next_data, {:trailing_ok_packet, _resultset}, _row_decoder) do
    {:error, :multiple_results}
  end
end
