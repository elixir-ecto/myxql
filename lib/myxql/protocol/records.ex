defmodule MyXQL.Protocol.Records do
  @moduledoc false

  import Record

  # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  defrecord :ok_packet, [:affected_rows, :last_insert_id, :status_flags, :num_warnings, :info]

  # https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
  defrecord :eof_packet, [:status_flags, :num_warnings]

  # https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
  defrecord :err_packet, [:code, :message]

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
  defrecord :initial_handshake, [
    :auth_plugin_data,
    :auth_plugin_name,
    :capability_flags,
    :charset,
    :conn_id,
    :server_version,
    :status_flags
  ]

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
  defrecord :handshake_response_41, [
    :capability_flags,
    :max_packet_size,
    :charset,
    :username,
    :database,
    :auth_plugin_name,
    :auth_response
  ]

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::SSLRequest
  defrecord :ssl_request, [:capability_flags, :max_packet_size, :charset]

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
  defrecord :auth_switch_request, [:plugin_name, :plugin_data]

  # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthMoreData
  defrecord :auth_more_data, [:data]

  # https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK
  defrecord :com_stmt_prepare_ok, [:statement_id, :num_columns, :num_params, :num_warnings]

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
  # https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
  #
  # both text & binary resultset have the same columns shape, but different rows
  defrecord :resultset, [
    :column_defs,
    :num_rows,
    :rows,
    :num_warnings,
    :status_flags
  ]

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
  defrecord :column_def, [:name, :type, :length, :flags, :unsigned?]
end
