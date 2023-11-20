defmodule MyXQL.Protocol.Flags do
  @moduledoc false

  import Bitwise

  # https://dev.mysql.com/doc/internals/en/capability-flags.html
  @capability_flags [
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
  ]

  def has_capability_flag?(flags, name), do: has_flag?(@capability_flags, flags, name)

  def remove_capability_flag(flags, name), do: remove_flag(@capability_flags, flags, name)

  def put_capability_flags(flags \\ 0, names), do: put_flags(@capability_flags, flags, names)

  def list_capability_flags(flags), do: list_flags(@capability_flags, flags)

  # https://dev.mysql.com/doc/internals/en/status-flags.html
  @status_flags [
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
  ]

  def has_status_flag?(flags, name), do: has_flag?(@status_flags, flags, name)

  def list_status_flags(flags), do: list_flags(@status_flags, flags)

  # Column flags (non-internal)
  # https://dev.mysql.com/doc/dev/mysql-server/8.0.11/group__group__cs__column__definition__flags.html
  @column_flags [
    not_null_flag: 0x0001,
    pri_key_flag: 0x0002,
    unique_key_flag: 0x0004,
    multiple_key_flag: 0x0008,
    blob_flag: 0x0010,
    unsigned_flag: 0x0020,
    zerofill_flag: 0x0040,
    binary_flag: 0x0080,
    enum_flag: 0x0100,
    auto_increment_flag: 0x0200,
    timestamp_flag: 0x0400,
    set_flag: 0x0800,
    no_default_value_flag: 0x1000,
    on_update_now_flag: 0x2000,
    num_flag: 0x4000
  ]

  def has_column_flag?(flags, name), do: has_flag?(@column_flags, flags, name)

  def list_column_flags(flags), do: list_flags(@column_flags, flags)

  defp has_flag?(all_flags, flags, name) do
    value = Keyword.fetch!(all_flags, name)
    (flags &&& value) == value
  end

  defp put_flags(all_flags, flags, names) do
    Enum.reduce(names, flags, &(&2 ||| Keyword.fetch!(all_flags, &1)))
  end

  defp remove_flag(all_flags, flags, name) do
    value = Keyword.fetch!(all_flags, name)
    flags &&& ~~~value
  end

  def list_flags(all_flags, flags) do
    all_flags
    |> Keyword.keys()
    |> Enum.filter(&has_flag?(all_flags, flags, &1))
  end
end
