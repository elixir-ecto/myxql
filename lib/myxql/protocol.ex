defmodule MyXQL.Protocol do
  @moduledoc false

  import MyXQL.Protocol.{Flags, Records, Types}
  alias MyXQL.Protocol.Values
  import Bitwise

  defdelegate error_code_to_name(code), to: MyXQL.Protocol.ServerErrorCodes, as: :code_to_name

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

  def encode_packet(payload, sequence_id, max_packet_size) do
    payload_length = IO.iodata_length(payload)
    encode_packet(payload, payload_length, sequence_id, max_packet_size)
  end

  defp encode_packet(payload, payload_size, sequence_id, max_packet_size) do
    if payload_size > max_packet_size do
      <<new_payload::size(max_packet_size)-binary, rest::binary>> = IO.iodata_to_binary(payload)
      rest_size = payload_size - max_packet_size
      next_sequence_id = if sequence_id < 255, do: sequence_id + 1, else: 0

      [
        encode_packet(new_payload, max_packet_size, sequence_id, max_packet_size),
        encode_packet(rest, rest_size, next_sequence_id, max_packet_size)
      ]
    else
      [<<payload_size::uint3(), sequence_id::uint1()>>, payload]
    end
  end

  def decode_generic_response(<<0x00, rest::bits>>) do
    decode_ok_packet_body(rest)
  end

  def decode_generic_response(<<0xFF, rest::bits>>) do
    decode_err_packet_body(rest)
  end

  defp decode_ok_packet_body(rest) do
    {affected_rows, rest} = take_int_lenenc(rest)
    {last_insert_id, rest} = take_int_lenenc(rest)

    <<
      status_flags::uint2(),
      num_warnings::uint2(),
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

  defp decode_err_packet_body(
         <<code::uint2(), _sql_state_marker::string(1), _sql_state::string(5), message::bits>>
       ) do
    err_packet(code: code, message: message)
  end

  def decode_eof_packet(<<0xFE, rest::binary>>) do
    decode_eof_packet_body(rest)
  end

  defp decode_eof_packet_body(<<num_warnings::uint2(), status_flags::uint2()>>) do
    eof_packet(
      status_flags: status_flags,
      num_warnings: num_warnings
    )
  end

  defp decode_connect_err_packet_body(<<code::uint2(), message::bits>>) do
    err_packet(code: code, message: message)
  end

  ##############################################################
  # Connection Phase
  #
  # https://dev.mysql.com/doc/internals/en/connection-phase.html
  ##############################################################

  def decode_initial_handshake(<<10, rest::binary>>) do
    {server_version, rest} = take_string_nul(rest)

    <<
      conn_id::uint4(),
      auth_plugin_data1::string(8),
      0,
      capability_flags1::uint2(),
      charset::uint1(),
      status_flags::uint2(),
      capability_flags2::uint2(),
      rest::binary
    >> = rest

    <<capability_flags::uint4()>> = <<capability_flags1::uint2(), capability_flags2::uint2()>>
    # all set in servers since MySQL 4.1
    required_capabilities = [:client_protocol_41, :client_plugin_auth, :client_secure_connection]

    with :ok <- ensure_capabilities(capability_flags, required_capabilities) do
      <<
        auth_plugin_data_length::uint1(),
        _::uint(10),
        rest::binary
      >> = rest

      take = max(13, auth_plugin_data_length - 8)
      <<auth_plugin_data2::binary-size(take), auth_plugin_name::binary>> = rest
      auth_plugin_data2 = decode_string_nul(auth_plugin_data2)
      auth_plugin_name = decode_string_nul(auth_plugin_name)
      auth_plugin_data = auth_plugin_data1 <> auth_plugin_data2

      initial_handshake(
        server_version: server_version,
        conn_id: conn_id,
        auth_plugin_name: auth_plugin_name,
        auth_plugin_data: auth_plugin_data,
        capability_flags: capability_flags,
        charset: charset,
        status_flags: status_flags
      )
    end
  end

  def decode_initial_handshake(<<0xFF, rest::bits>>) do
    decode_connect_err_packet_body(rest)
  end

  defp filter_capabilities(allowed_flags, requested_flags) do
    requested_capabilities = list_capability_flags(requested_flags)

    Enum.reduce(requested_capabilities, requested_flags, fn name, acc ->
      if has_capability_flag?(allowed_flags, name) do
        acc
      else
        remove_capability_flag(acc, name)
      end
    end)
  end

  defp ensure_capabilities(capability_flags, names) do
    Enum.reduce_while(names, :ok, fn name, _acc ->
      if has_capability_flag?(capability_flags, name) do
        {:cont, :ok}
      else
        {:halt, {:error, {:server_missing_capability, name}}}
      end
    end)
  end

  def build_capability_flags(config, initial_handshake) do
    initial_handshake(capability_flags: server_capability_flags) = initial_handshake

    client_capability_flags =
      put_capability_flags([
        :client_protocol_41,
        :client_plugin_auth,
        :client_secure_connection,
        :client_found_rows,
        :client_multi_results,
        :client_multi_statements,
        # set by servers since 4.0
        :client_transactions
      ])
      |> maybe_put_capability_flag(:client_connect_with_db, !is_nil(config.database))
      |> maybe_put_capability_flag(:client_ssl, is_list(config.ssl_opts))

    if config.ssl_opts && !has_capability_flag?(server_capability_flags, :client_ssl) do
      {:error, :server_does_not_support_ssl}
    else
      client_capability_flags =
        filter_capabilities(server_capability_flags, client_capability_flags)

      {:ok, client_capability_flags}
    end
  end

  defp maybe_put_capability_flag(flags, name, true), do: put_capability_flags(flags, [name])
  defp maybe_put_capability_flag(flags, _name, false), do: flags

  def encode_handshake_response_41(
        handshake_response_41(
          capability_flags: capability_flags,
          max_packet_size: max_packet_size,
          charset: charset,
          username: username,
          auth_plugin_name: auth_plugin_name,
          auth_response: auth_response,
          database: database
        )
      ) do
    auth_response = if auth_response, do: encode_string_lenenc(auth_response), else: <<0>>
    database = if database, do: <<database::binary, 0x00>>, else: ""

    <<
      capability_flags::uint4(),
      max_packet_size::uint4(),
      charset,
      0::uint(23),
      <<username::binary, 0x00>>,
      auth_response::binary,
      database::binary,
      (<<auth_plugin_name::binary, 0x00>>)
    >>
  end

  def encode_ssl_request(
        ssl_request(
          capability_flags: capability_flags,
          max_packet_size: max_packet_size,
          charset: charset
        )
      ) do
    <<
      capability_flags::uint4(),
      max_packet_size::uint4(),
      charset,
      0::uint(23)
    >>
  end

  def decode_auth_response(<<0x00, rest::binary>>) do
    decode_ok_packet_body(rest)
  end

  def decode_auth_response(<<0xFF, rest::binary>>) do
    decode_err_packet_body(rest)
  end

  def decode_auth_response(<<0x01, 0x04>>) do
    :full_auth
  end

  def decode_auth_response(<<0x01, rest::binary>>) do
    auth_more_data(data: rest)
  end

  def decode_auth_response(<<0xFE, rest::binary>>) do
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

  # https://dev.mysql.com/doc/internals/en/com-quit.html
  def encode_com(:com_quit) do
    <<0x01>>
  end

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
    [0x19, <<statement_id::uint4()>>]
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-reset.html
  def encode_com({:com_stmt_reset, statement_id}) do
    [0x1A, <<statement_id::uint4()>>]
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
  def encode_com({:com_stmt_execute, statement_id, params, cursor_type}) when is_list(params) do
    params = encode_params(params)
    encode_com({:com_stmt_execute, statement_id, params, cursor_type})
  end

  def encode_com({:com_stmt_execute, statement_id, params, cursor_type}) when is_binary(params) do
    command = 0x17
    flags = Map.fetch!(@cursor_types, cursor_type)

    # Always 0x01
    iteration_count = 0x01

    <<
      command,
      statement_id::uint4(),
      flags::uint1(),
      iteration_count::uint4(),
      params::binary
    >>
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-fetch.html
  def encode_com({:com_stmt_fetch, statement_id, num_rows}) do
    <<
      0x1C,
      statement_id::uint4(),
      num_rows::uint4()
    >>
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-COM_QUERY_Response
  def decode_com_query_response(<<0x00, rest::binary>>, "", :initial) do
    {:halt, decode_ok_packet_body(rest)}
  end

  def decode_com_query_response(<<0xFF, rest::binary>>, "", :initial) do
    {:halt, decode_err_packet_body(rest)}
  end

  def decode_com_query_response(payload, next_data, state) do
    decode_resultset(payload, next_data, state, &Values.decode_text_row/2)
  end

  def decode_com_stmt_prepare_response(
        <<0x00, statement_id::uint4(), num_columns::uint2(), num_params::uint2(), 0,
          num_warnings::uint2()>>,
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

    cond do
      num_params > 0 ->
        {:cont, {result, :params, num_params, num_columns}}

      num_columns > 0 ->
        {:cont, {result, :columns, num_columns}}

      true ->
        "" = next_data
        {:halt, result}
    end
  end

  def decode_com_stmt_prepare_response(<<rest::binary>>, "", :initial) do
    {:halt, decode_generic_response(rest)}
  end

  # for now, we're simply consuming column_definition packets for params and columns,
  # we might decode them in the future.

  def decode_com_stmt_prepare_response(
        payload,
        _next_data,
        {com_stmt_prepare_ok, :params, num_params, num_columns}
      ) do
    if num_params > 0 do
      column_def() = decode_column_def(payload)
      {:cont, {com_stmt_prepare_ok, :params, num_params - 1, num_columns}}
    else
      eof_packet() = decode_eof_packet(payload)

      if num_columns > 0 do
        {:cont, {com_stmt_prepare_ok, :columns, num_columns}}
      else
        {:halt, com_stmt_prepare_ok}
      end
    end
  end

  def decode_com_stmt_prepare_response(
        payload,
        next_data,
        {com_stmt_prepare_ok, :columns, num_columns}
      ) do
    if num_columns > 0 do
      column_def() = decode_column_def(payload)
      {:cont, {com_stmt_prepare_ok, :columns, num_columns - 1}}
    else
      "" = next_data
      eof_packet() = decode_eof_packet(payload)
      {:halt, com_stmt_prepare_ok}
    end
  end

  def encode_params([]) do
    <<>>
  end

  def encode_params(params) when is_list(params) do
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
            type = Values.type_atom_to_code(type)

            {idx + 1, null_bitmap, <<types::binary, type, unsigned_flag(value)>>,
             <<values::binary, binary::binary>>}
          end
      end)

    null_bitmap_size = div(count + 7, 8)
    new_params_bound_flag = 1

    <<null_bitmap::uint(null_bitmap_size), new_params_bound_flag::uint1(), types::binary,
      values::binary>>
  end

  defp unsigned_flag(value) when is_integer(value) and value >= 1 <<< 63, do: 0x80
  defp unsigned_flag(_), do: 0x00

  # https://dev.mysql.com/doc/internals/en/com-stmt-execute-response.html
  def decode_com_stmt_execute_response(<<0x00, rest::binary>>, "", :initial) do
    {:halt, decode_ok_packet_body(rest)}
  end

  def decode_com_stmt_execute_response(<<0xFF, rest::binary>>, "", :initial) do
    {:halt, decode_err_packet_body(rest)}
  end

  def decode_com_stmt_execute_response(payload, next_data, state) do
    decode_resultset(payload, next_data, state, &Values.decode_binary_row/2)
  end

  # https://dev.mysql.com/doc/internals/en/com-stmt-fetch-response.html
  def decode_com_stmt_fetch_response(<<0xFF, rest::binary>>, "", {:initial, _column_defs}) do
    {:halt, decode_err_packet_body(rest)}
  end

  def decode_com_stmt_fetch_response(payload, next_data, {:initial, column_defs}) do
    decode_com_stmt_fetch_response(payload, next_data, {:rows, column_defs, 0, []})
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
      _character_set::uint2(),
      column_length::uint4(),
      type::uint1(),
      flags::uint2(),
      _decimals::uint1(),
      0::uint2()
    >> = rest

    column_def(
      name: name,
      type: Values.type_code_to_atom(type),
      length: column_length,
      flags: flags,
      unsigned?: has_column_flag?(flags, :unsigned_flag)
    )
  end

  def decode_more_results(payload, "", resultset, result_state) do
    ok_packet(status_flags: status_flags) = decode_generic_response(payload)

    case result_state do
      :single ->
        {:halt, resultset(resultset, status_flags: status_flags)}

      {:many, results} ->
        {:halt, [resultset(resultset, status_flags: status_flags) | results]}
    end
  end

  def decode_more_results(_payload, _next_data, _resultset, :single) do
    {:error, :multiple_results}
  end

  def decode_more_results(_payload, _next_data, resultset, {:many, results}) do
    {:cont, :initial, {:many, [resultset | results]}}
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
      {:cont, {:column_defs_eof, Enum.reverse(acc)}}
    end
  end

  defp decode_resultset(
         <<0xFE, num_warnings::uint2(), status_flags::uint2()>>,
         next_data,
         {:column_defs_eof, column_defs},
         _row_decoder
       ) do
    if has_status_flag?(status_flags, :server_status_cursor_exists) and
         not has_status_flag?(status_flags, :server_more_results_exists) do
      "" = next_data

      {:halt,
       resultset(
         column_defs: column_defs,
         num_rows: 0,
         rows: [],
         num_warnings: num_warnings,
         status_flags: status_flags
       )}
    else
      {:cont, {:rows, column_defs, 0, []}}
    end
  end

  defp decode_resultset(
         <<0xFE, num_warnings::uint2(), status_flags::uint2()>>,
         _next_data,
         {:rows, column_defs, num_rows, acc},
         _row_decoder
       ) do
    resultset =
      resultset(
        column_defs: column_defs,
        num_rows: num_rows,
        rows: Enum.reverse(acc),
        num_warnings: num_warnings,
        status_flags: status_flags
      )

    if has_status_flag?(status_flags, :server_more_results_exists) do
      {:cont, {:more_results, resultset}}
    else
      {:halt, resultset}
    end
  end

  defp decode_resultset(<<0xFF, rest::bits>>, _, _, _) do
    {:halt, decode_err_packet_body(rest)}
  end

  defp decode_resultset(payload, _next_data, {:rows, column_defs, num_rows, acc}, row_decoder) do
    row = row_decoder.(payload, column_defs)
    {:cont, {:rows, column_defs, num_rows + 1, [row | acc]}}
  end
end
