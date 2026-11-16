defmodule MyXQL.Protocol.Records do
  @moduledoc false

  import Record

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
  defrecord :ok_packet, [:affected_rows, :last_insert_id, :status_flags, :num_warnings, :info]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
  defrecord :eof_packet, [:status_flags, :num_warnings]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
  defrecord :err_packet, [:code, :message]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake.html
  defrecord :initial_handshake, [
    :auth_plugin_data,
    :auth_plugin_name,
    :capability_flags,
    :charset,
    :conn_id,
    :server_version,
    :status_flags
  ]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html
  defrecord :handshake_response_41, [
    :capability_flags,
    :max_packet_size,
    :charset,
    :username,
    :database,
    :auth_plugin_name,
    :auth_response
  ]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html
  defrecord :ssl_request, [:capability_flags, :max_packet_size, :charset]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html
  defrecord :auth_switch_request, [:plugin_name, :plugin_data]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_more_data.html
  defrecord :auth_more_data, [:data]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response
  defrecord :com_stmt_prepare_ok, [:statement_id, :num_columns, :num_params, :num_warnings]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html
  #
  # both text & binary resultset have the same columns shape, but different rows
  defrecord :resultset, [
    :column_defs,
    :num_rows,
    :rows,
    :num_warnings,
    :status_flags
  ]

  # https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html#sect_protocol_com_query_response_text_resultset_column_definition_41
  defrecord :column_def, [:name, :type, :length, :flags, :unsigned?]
end
