defmodule MyXQL.RowValue do
  @moduledoc false
  import MyXQL.Types
  import MyXQL.Messages, only: [column_def: 1]
  use Bitwise

  # Text & Binary row value encoding/decoding
  #
  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
  # https://dev.mysql.com/doc/refman/8.0/en/data-types.html

  # TIMESTAMP vs DATETIME
  #
  # https://dev.mysql.com/doc/refman/8.0/en/datetime.html
  # MySQL converts TIMESTAMP values from the current time zone to UTC for
  # storage, and back from UTC to the current time zone for retrieval. (This
  # does not occur for other types such as DATETIME.)
  #
  # Comparing to Postgres we have:
  # MySQL TIMESTAMP is equal to Postgres TIMESTAMP WITH TIME ZONE
  # MySQL DATETIME  is equal to Postgres TIMESTAMP [WITHOUT TIME ZONE]

  @mysql_type_tiny 0x01
  @mysql_type_short 0x02
  @mysql_type_long 0x03
  @mysql_type_float 0x04
  @mysql_type_double 0x05
  # https://dev.mysql.com/doc/internals/en/null-bitmap.html
  # @mysql_type_null 0x06
  @mysql_type_timestamp 0x07
  @mysql_type_longlong 0x08
  @mysql_type_int24 0x09
  @mysql_type_date 0x0A
  @mysql_type_time 0x0B
  @mysql_type_datetime 0x0C
  @mysql_type_year 0x0D
  @mysql_type_varchar 0x0F
  # @mysql_type_bit 0x10
  @mysql_type_json 0xF5
  @mysql_type_newdecimal 0xF6
  # @mysql_type_enum 0xF7
  # @mysql_type_set 0xF8
  # @mysql_type_tiny_blob 0xF9
  # @mysql_type_medium_blob 0xFA
  @mysql_type_long_blob 0xFB
  @mysql_type_blob 0xFC
  @mysql_type_var_string 0xFD
  @mysql_type_string 0xFE
  # @mysql_type_geometry 0xFF

  @typep type() :: non_neg_integer()

  # Text values

  def decode_text_row(values, column_defs) do
    column_types = Enum.map(column_defs, &elem(&1, 2))
    decode_text_row(values, column_types, [])
  end

  # null value
  defp decode_text_row(<<0xFB, rest::binary>>, [_type | tail], acc) do
    decode_text_row(rest, tail, [nil | acc])
  end

  defp decode_text_row(<<values::binary>>, [type | tail], acc) do
    {string, rest} = take_string_lenenc(values)
    value = decode_text_value(string, type)
    decode_text_row(rest, tail, [value | acc])
  end

  defp decode_text_row("", _column_type, acc) do
    Enum.reverse(acc)
  end

  @spec decode_text_value(term(), type()) :: term()
  def decode_text_value(value, type)
      when type in [
             @mysql_type_tiny,
             @mysql_type_short,
             @mysql_type_long,
             @mysql_type_longlong,
             @mysql_type_int24,
             @mysql_type_year
           ] do
    String.to_integer(value)
  end

  def decode_text_value(value, type) when type in [@mysql_type_float, @mysql_type_double] do
    String.to_float(value)
  end

  # Note: MySQL implements `NUMERIC` as `DECIMAL`s
  def decode_text_value(value, @mysql_type_newdecimal) do
    Decimal.new(value)
  end

  def decode_text_value(value, @mysql_type_date) do
    Date.from_iso8601!(value)
  end

  def decode_text_value(value, @mysql_type_time) do
    Time.from_iso8601!(value)
  end

  def decode_text_value(value, @mysql_type_datetime) do
    NaiveDateTime.from_iso8601!(value)
  end

  def decode_text_value(value, @mysql_type_timestamp) do
    value
    |> NaiveDateTime.from_iso8601!()
    |> DateTime.from_naive!("Etc/UTC")
  end

  def decode_text_value(value, type)
      when type in [
             @mysql_type_varchar,
             @mysql_type_var_string,
             @mysql_type_string,
             @mysql_type_blob,
             @mysql_type_long_blob
             # @mysql_type_bit
           ] do
    value
  end

  def decode_text_value(value, @mysql_type_json) do
    MyXQL.json_library().decode!(value)
  end

  # Binary values

  def decode_binary_row(payload, column_defs) do
    size = div(length(column_defs) + 7 + 2, 8)
    <<0x00, null_bitmap::int(size), values::binary>> = payload
    null_bitmap = null_bitmap >>> 2
    decode_binary_row(values, null_bitmap, column_defs, [])
  end

  defp decode_binary_row(<<rest::binary>>, null_bitmap, [_column_def | tail], acc)
       when (null_bitmap &&& 1) == 1 do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [nil | acc])
  end

  defp decode_binary_row(
         <<value::signed-int(1), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_tiny, unsigned?: false) | tail],
         acc
       ) do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::unsigned-int(1), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_tiny, unsigned?: true) | tail],
         acc
       ) do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::signed-int(2), rest::binary>>,
         null_bitmap,
         [column_def(type: type, unsigned?: false) | tail],
         acc
       )
       when type in [@mysql_type_short, @mysql_type_year] do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::unsigned-int(2), rest::binary>>,
         null_bitmap,
         [column_def(type: type, unsigned?: true) | tail],
         acc
       )
       when type in [@mysql_type_short, @mysql_type_year] do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::signed-int(4), rest::binary>>,
         null_bitmap,
         [column_def(type: type, unsigned?: false) | tail],
         acc
       )
       when type in [@mysql_type_long, @mysql_type_int24] do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::unsigned-int(4), rest::binary>>,
         null_bitmap,
         [column_def(type: type, unsigned?: true) | tail],
         acc
       )
       when type in [@mysql_type_long, @mysql_type_int24] do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::signed-int(8), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_longlong, unsigned?: false) | tail],
         acc
       ) do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::unsigned-int(8), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_longlong, unsigned?: true) | tail],
         acc
       ) do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::little-signed-float-size(32), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_float) | tail],
         acc
       ) do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<value::little-signed-float-size(64), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_double) | tail],
         acc
       ) do
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<data::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_newdecimal) | tail],
         acc
       ) do
    {string, rest} = take_string_lenenc(data)
    value = Decimal.new(string)
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(<<data::binary>>, null_bitmap, [column_def(type: type) | tail], acc)
       when type in [
              @mysql_type_var_string,
              @mysql_type_string,
              @mysql_type_blob,
              @mysql_type_long_blob
              # @mysql_type_bit
            ] do
    {value, rest} = take_string_lenenc(data)
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<4, year::int(2), month::int(1), day::int(1), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_date) | tail],
         acc
       ) do
    {:ok, value} = Date.new(year, month, day)
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<8, 0::int(1), 0::int(4), hour::int(1), minute::int(1), second::int(1), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_time) | tail],
         acc
       ) do
    {:ok, value} = Time.new(hour, minute, second)
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<12, 0::int(1), 0::int(4), hour::int(1), minute::int(1), second::int(1),
           microsecond::int(4), rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_time) | tail],
         acc
       ) do
    {:ok, value} = Time.new(hour, minute, second, {microsecond, 6})
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<0, rest::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_time) | tail],
         acc
       ) do
    value = ~T[00:00:00]
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<4, year::int(2), month::int(1), day::int(1), rest::binary>>,
         null_bitmap,
         [column_def(type: type) | tail],
         acc
       )
       when type in [@mysql_type_datetime, @mysql_type_timestamp] do
    value = new_datetime(type, year, month, day, 0, 0, 0, {0, 0})
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<7, year::int(2), month::int(1), day::int(1), hour::int(1), minute::int(1),
           second::int(1), rest::binary>>,
         null_bitmap,
         [column_def(type: type) | tail],
         acc
       )
       when type in [@mysql_type_datetime, @mysql_type_timestamp] do
    value = new_datetime(type, year, month, day, hour, minute, second, {0, 0})
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<11, year::int(2), month::int(1), day::int(1), hour::int(1), minute::int(1),
           second::int(1), microsecond::int(4), rest::binary>>,
         null_bitmap,
         [column_def(type: type) | tail],
         acc
       )
       when type in [@mysql_type_datetime, @mysql_type_timestamp] do
    value = new_datetime(type, year, month, day, hour, minute, second, {microsecond, 6})
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row(
         <<data::binary>>,
         null_bitmap,
         [column_def(type: @mysql_type_json) | tail],
         acc
       ) do
    {json, rest} = take_string_lenenc(data)
    value = MyXQL.json_library().decode!(json)
    decode_binary_row(rest, null_bitmap >>> 1, tail, [value | acc])
  end

  defp decode_binary_row("", _null_bitmap, [], acc) do
    Enum.reverse(acc)
  end

  @spec encode_binary_value(term()) :: {type(), term()}
  def encode_binary_value(value)
      when is_integer(value) and value >= -1 <<< 63 and value < 1 <<< 64 do
    {@mysql_type_longlong, <<value::signed-int(8)>>}
  end

  def encode_binary_value(value) when is_float(value) do
    {@mysql_type_double, <<value::little-signed-float-size(64)>>}
  end

  def encode_binary_value(%Decimal{} = value) do
    string = Decimal.to_string(value, :normal)
    {@mysql_type_newdecimal, <<byte_size(string), string::binary>>}
  end

  def encode_binary_value(%Date{year: year, month: month, day: day}) do
    {@mysql_type_date, <<4, year::int(2), month::int(1), day::int(1)>>}
  end

  def encode_binary_value(%Time{} = time), do: encode_binary_time(time)

  def encode_binary_value(%NaiveDateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(%DateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(binary) when is_binary(binary) do
    {@mysql_type_var_string, encode_string_lenenc(binary)}
  end

  # def encode_binary_value(bitstring) when is_bitstring(bitstring) do
  #   {@mysql_type_bit, bitstring}
  # end

  def encode_binary_value(true) do
    {@mysql_type_tiny, <<1>>}
  end

  def encode_binary_value(false) do
    {@mysql_type_tiny, <<0>>}
  end

  def encode_binary_value(term) when is_list(term) or is_map(term) do
    string = MyXQL.json_library().encode!(term)
    {@mysql_type_var_string, encode_string_lenenc(string)}
  end

  def encode_binary_value(other) do
    raise ArgumentError, "query has invalid parameter #{inspect(other)}"
  end

  ## Time/DateTime

  # MySQL supports negative time and days, we don't.
  # See: https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME

  defp encode_binary_time(%Time{hour: 0, minute: 0, second: 0, microsecond: {0, 0}}) do
    {@mysql_type_time, <<0>>}
  end

  defp encode_binary_time(%Time{hour: hour, minute: minute, second: second, microsecond: {0, 0}}) do
    {@mysql_type_time, <<8, 0::int(1), 0::int(4), hour::int(1), minute::int(1), second::int(1)>>}
  end

  defp encode_binary_time(%Time{
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _}
       }) do
    {@mysql_type_time,
     <<12, 0::int(1), 0::int(4), hour::int(1), minute::int(1), second::int(1),
       microsecond::int(4)>>}
  end

  defp encode_binary_datetime(%NaiveDateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {0, 0}
       }) do
    {@mysql_type_datetime,
     <<7, year::int(2), month::int(1), day::int(1), hour::int(1), minute::int(1), second::int(1)>>}
  end

  defp encode_binary_datetime(%NaiveDateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _}
       }) do
    {@mysql_type_datetime,
     <<11, year::int(2), month::int(1), day::int(1), hour::int(1), minute::int(1), second::int(1),
       microsecond::int(4)>>}
  end

  defp encode_binary_datetime(%DateTime{
         year: year,
         month: month,
         day: day,
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _},
         time_zone: "Etc/UTC"
       }) do
    {@mysql_type_datetime,
     <<11, year::int(2), month::int(1), day::int(1), hour::int(1), minute::int(1), second::int(1),
       microsecond::int(4)>>}
  end

  defp encode_binary_datetime(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  defp new_datetime(@mysql_type_datetime, year, month, day, hour, minute, second, microsecond) do
    {:ok, naive_datetime} = NaiveDateTime.new(year, month, day, hour, minute, second, microsecond)
    naive_datetime
  end

  defp new_datetime(@mysql_type_timestamp, year, month, day, hour, minute, second, microsecond) do
    new_datetime(@mysql_type_datetime, year, month, day, hour, minute, second, microsecond)
    |> DateTime.from_naive!("Etc/UTC")
  end
end
