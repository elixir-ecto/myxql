defmodule MyXQL.Types do
  @moduledoc false
  use Bitwise

  #########################################################
  # Basic types
  #
  # https://dev.mysql.com/doc/internals/en/basic-types.html
  #########################################################

  # https://dev.mysql.com/doc/internals/en/integer.html#fixed-length-integer
  defmacro int(size) do
    quote do
      little - integer - size(unquote(size)) - unit(8)
    end
  end

  # https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
  def encode_int_lenenc(int) when int < 251, do: <<int>>
  def encode_int_lenenc(int) when int < 0xFFFF, do: <<0xFC, int::int(2)>>
  def encode_int_lenenc(int) when int < 0xFFFFFF, do: <<0xFD, int::int(3)>>
  def encode_int_lenenc(int) when int < 0xFFFFFFFFFFFFFFFF, do: <<0xFE, int::int(8)>>

  def decode_int_lenenc(binary) do
    {integer, ""} = take_int_lenenc(binary)
    integer
  end

  def take_int_lenenc(<<int::8, rest::binary>>) when int < 251, do: {int, rest}
  def take_int_lenenc(<<0xFC, int::int(2), rest::binary>>), do: {int, rest}
  def take_int_lenenc(<<0xFD, int::int(3), rest::binary>>), do: {int, rest}
  def take_int_lenenc(<<0xFE, int::int(8), rest::binary>>), do: {int, rest}

  # https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::LengthEncodedString
  def encode_string_lenenc(binary) when is_binary(binary) do
    size = encode_int_lenenc(byte_size(binary))
    <<size::binary, binary::binary>>
  end

  def decode_string_lenenc(binary) do
    {_size, rest} = take_int_lenenc(binary)
    rest
  end

  def take_string_lenenc(binary) do
    {size, rest} = take_int_lenenc(binary)
    <<string::bytes-size(size), rest::binary>> = rest
    {string, rest}
  end

  # https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString
  def decode_string_nul(binary) do
    {string, ""} = take_string_nul(binary)
    string
  end

  def take_string_nul(binary) do
    [string, rest] = :binary.split(binary, <<0>>)
    {string, rest}
  end

  # Text & Binary
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
  @mysql_type_bit 0x10
  @mysql_type_json 0xF5
  @mysql_type_newdecimal 0xF6
  # @mysql_type_enum 0xF7
  # @mysql_type_set 0xF8
  # @mysql_type_tiny_blob 0xF9
  # @mysql_type_medium_blob 0xFA
  # @mysql_type_long_blob 0xFB
  @mysql_type_blob 0xFC
  @mysql_type_var_string 0xFD
  @mysql_type_string 0xFE
  # @mysql_type_geometry 0xFF

  # Text values

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
    NaiveDateTime.from_iso8601!(value)
  end

  def decode_text_value(value, type)
      when type in [
             @mysql_type_varchar,
             @mysql_type_var_string,
             @mysql_type_string,
             @mysql_type_blob,
             @mysql_type_bit
           ] do
    value
  end

  def decode_text_value(value, @mysql_type_json) do
    Jason.decode!(value)
  end

  # Binary values

  def take_binary_value(value, null_bitmap, type) do
    if (null_bitmap &&& 1) == 1 do
      {nil, value}
    else
      take_binary_value(value, type)
    end
  end

  def take_binary_value(<<value::signed-int(1), rest::binary>>, @mysql_type_tiny) do
    {value, rest}
  end

  def take_binary_value(<<value::signed-int(2), rest::binary>>, type)
      when type in [@mysql_type_short, @mysql_type_year] do
    {value, rest}
  end

  def take_binary_value(<<value::signed-int(4), rest::binary>>, @mysql_type_long) do
    {value, rest}
  end

  def take_binary_value(<<value::signed-int(8), rest::binary>>, @mysql_type_longlong) do
    {value, rest}
  end

  def take_binary_value(<<value::signed-int(4), rest::binary>>, @mysql_type_int24) do
    {value, rest}
  end

  def take_binary_value(<<value::little-signed-float-size(32), rest::binary>>, @mysql_type_float) do
    {value, rest}
  end

  def take_binary_value(<<value::little-signed-float-size(64), rest::binary>>, @mysql_type_double) do
    {value, rest}
  end

  def take_binary_value(data, @mysql_type_newdecimal) do
    {string, rest} = take_string_lenenc(data)
    decimal = Decimal.new(string)
    {decimal, rest}
  end

  def take_binary_value(
        <<4, year::int(2), month::int(1), day::int(1), rest::binary>>,
        @mysql_type_date
      ) do
    {:ok, date} = Date.new(year, month, day)
    {date, rest}
  end

  def take_binary_value(binary, @mysql_type_time), do: take_binary_time(binary)

  def take_binary_value(binary, type) when type in [@mysql_type_datetime, @mysql_type_timestamp],
    do: take_binary_datetime(binary)

  def take_binary_value(data, type)
      when type in [@mysql_type_var_string, @mysql_type_string, @mysql_type_blob, @mysql_type_bit] do
    take_string_lenenc(data)
  end

  def take_binary_value(data, @mysql_type_json) do
    {json, rest} = take_string_lenenc(data)
    value = Jason.decode!(json)
    {value, rest}
  end

  def encode_binary_value(value) when is_integer(value) do
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

  def encode_binary_value(bitstring) when is_bitstring(bitstring) do
    {@mysql_type_bit, bitstring}
  end

  def encode_binary_value(true) do
    {@mysql_type_tiny, <<1>>}
  end

  def encode_binary_value(false) do
    {@mysql_type_tiny, <<0>>}
  end

  ## Time/DateTime

  # MySQL supports negative time and days, we don't.
  # See: https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME
  defp take_binary_time(
         <<8, 0::int(1), 0::int(4), hour::int(1), minute::int(1), second::int(1), rest::binary>>
       ) do
    {:ok, time} = Time.new(hour, minute, second)
    {time, rest}
  end

  defp take_binary_time(
         <<12, 0::int(1), 0::int(4), hour::int(1), minute::int(1), second::int(1),
           microsecond::int(4), rest::binary>>
       ) do
    {:ok, time} = Time.new(hour, minute, second, {microsecond, 6})
    {time, rest}
  end

  defp take_binary_time(<<0, rest::binary>>) do
    {~T[00:00:00], rest}
  end

  defp take_binary_datetime(<<4, year::int(2), month::int(1), day::int(1), rest::binary>>) do
    {:ok, naive_datetime} = NaiveDateTime.new(year, month, day, 0, 0, 0)
    {naive_datetime, rest}
  end

  defp take_binary_datetime(
         <<7, year::int(2), month::int(1), day::int(1), hour::int(1), minute::int(1),
           second::int(1), rest::binary>>
       ) do
    {:ok, naive_datetime} = NaiveDateTime.new(year, month, day, hour, minute, second)
    {naive_datetime, rest}
  end

  defp take_binary_datetime(
         <<11, year::int(2), month::int(1), day::int(1), hour::int(1), minute::int(1),
           second::int(1), microsecond::int(4), rest::binary>>
       ) do
    {:ok, naive_datetime} =
      NaiveDateTime.new(year, month, day, hour, minute, second, {microsecond, 6})

    {naive_datetime, rest}
  end

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
end
