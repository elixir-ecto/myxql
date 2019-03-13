defmodule MyXQL.Protocol.Values do
  @moduledoc false

  use Bitwise
  import MyXQL.Protocol.Types
  import MyXQL.Protocol.Records, only: [column_def: 1]

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

  types = [
    mysql_type_tiny: 0x01,
    mysql_type_short: 0x02,
    mysql_type_long: 0x03,
    mysql_type_float: 0x04,
    mysql_type_double: 0x05,
    # https://dev.mysql.com/doc/internals/en/null-bitmap.html
    mysql_type_null: 0x06,
    mysql_type_timestamp: 0x07,
    mysql_type_longlong: 0x08,
    mysql_type_int24: 0x09,
    mysql_type_date: 0x0A,
    mysql_type_time: 0x0B,
    mysql_type_datetime: 0x0C,
    mysql_type_year: 0x0D,
    mysql_type_varchar: 0x0F,
    mysql_type_bit: 0x10,
    mysql_type_json: 0xF5,
    mysql_type_newdecimal: 0xF6,
    mysql_type_enum: 0xF7,
    mysql_type_set: 0xF8,
    mysql_type_tiny_blob: 0xF9,
    mysql_type_medium_blob: 0xFA,
    mysql_type_long_blob: 0xFB,
    mysql_type_blob: 0xFC,
    mysql_type_var_string: 0xFD,
    mysql_type_string: 0xFE,
    mysql_type_geometry: 0xFF
  ]

  for {atom, code} <- types do
    def type_code_to_atom(unquote(code)), do: unquote(atom)
    def type_atom_to_code(unquote(atom)), do: unquote(code)
  end

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

  def decode_text_value(value, type)
      when type in [
             :mysql_type_tiny,
             :mysql_type_short,
             :mysql_type_long,
             :mysql_type_longlong,
             :mysql_type_int24,
             :mysql_type_year
           ] do
    String.to_integer(value)
  end

  def decode_text_value(value, type) when type in [:mysql_type_float, :mysql_type_double] do
    String.to_float(value)
  end

  # Note: MySQL implements `NUMERIC` as `DECIMAL`s
  def decode_text_value(value, :mysql_type_newdecimal) do
    Decimal.new(value)
  end

  def decode_text_value(value, :mysql_type_date) do
    Date.from_iso8601!(value)
  end

  def decode_text_value(value, :mysql_type_time) do
    Time.from_iso8601!(value)
  end

  def decode_text_value(value, :mysql_type_datetime) do
    NaiveDateTime.from_iso8601!(value)
  end

  def decode_text_value(value, :mysql_type_timestamp) do
    value
    |> NaiveDateTime.from_iso8601!()
    |> DateTime.from_naive!("Etc/UTC")
  end

  def decode_text_value(value, type)
      when type in [
             :mysql_type_varchar,
             :mysql_type_var_string,
             :mysql_type_string,
             :mysql_type_blob,
             :mysql_type_long_blob
             # :mysql_type_bit
           ] do
    value
  end

  def decode_text_value(value, :mysql_type_json) do
    MyXQL.json_library().decode!(value)
  end

  # Binary values

  def encode_binary_value(value)
      when is_integer(value) and value >= -1 <<< 63 and value < 1 <<< 64 do
    {:mysql_type_longlong, <<value::int8>>}
  end

  def encode_binary_value(value) when is_float(value) do
    {:mysql_type_double, <<value::64-little-signed-float>>}
  end

  def encode_binary_value(%Decimal{} = value) do
    string = Decimal.to_string(value, :normal)
    {:mysql_type_newdecimal, <<byte_size(string), string::binary>>}
  end

  def encode_binary_value(%Date{year: year, month: month, day: day}) do
    {:mysql_type_date, <<4, year::uint2, month::uint1, day::uint1>>}
  end

  def encode_binary_value(%Time{} = time), do: encode_binary_time(time)

  def encode_binary_value(%NaiveDateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(%DateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(binary) when is_binary(binary) do
    {:mysql_type_var_string, encode_string_lenenc(binary)}
  end

  # def encode_binary_value(bitstring) when is_bitstring(bitstring) do
  #   {:mysql_type_bit, bitstring}
  # end

  def encode_binary_value(true) do
    {:mysql_type_tiny, <<1>>}
  end

  def encode_binary_value(false) do
    {:mysql_type_tiny, <<0>>}
  end

  def encode_binary_value(term) when is_list(term) or is_map(term) do
    string = MyXQL.json_library().encode!(term)
    {:mysql_type_var_string, encode_string_lenenc(string)}
  end

  def encode_binary_value(other) do
    raise ArgumentError, "query has invalid parameter #{inspect(other)}"
  end

  ## Time/DateTime

  # MySQL supports negative time and days, we don't.
  # See: https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME

  defp encode_binary_time(%Time{hour: 0, minute: 0, second: 0, microsecond: {0, 0}}) do
    {:mysql_type_time, <<0>>}
  end

  defp encode_binary_time(%Time{hour: hour, minute: minute, second: second, microsecond: {0, 0}}) do
    {:mysql_type_time, <<8, 0::uint1, 0::uint4, hour::uint1, minute::uint1, second::uint1>>}
  end

  defp encode_binary_time(%Time{
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _}
       }) do
    {:mysql_type_time,
     <<12, 0::uint1, 0::uint4, hour::uint1, minute::uint1, second::uint1, microsecond::uint4>>}
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
    {:mysql_type_datetime,
     <<7, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1>>}
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
    {:mysql_type_datetime,
     <<11, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
       microsecond::uint4>>}
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
    {:mysql_type_datetime,
     <<11, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
       microsecond::uint4>>}
  end

  defp encode_binary_datetime(%DateTime{} = datetime) do
    raise ArgumentError, "#{inspect(datetime)} is not in UTC"
  end

  def decode_binary_row(<<payload::bits>>, column_defs) do
    size = div(length(column_defs) + 7 + 2, 8)
    <<0x00, null_bitmap::uint(size), values::bits>> = payload
    null_bitmap = null_bitmap >>> 2
    types = Enum.map(column_defs, &column_def_to_type/1)
    decode_binary_row(values, null_bitmap, types, [])
  end

  defp column_def_to_type(column_def(type: type, unsigned?: unsigned?)),
    do: column_def_to_type(type, unsigned?)

  defp column_def_to_type(:mysql_type_tiny, true), do: :uint1
  defp column_def_to_type(:mysql_type_tiny, false), do: :int1
  defp column_def_to_type(:mysql_type_short, true), do: :uint2
  defp column_def_to_type(:mysql_type_short, false), do: :int2
  defp column_def_to_type(:mysql_type_long, true), do: :uint4
  defp column_def_to_type(:mysql_type_long, false), do: :int4
  defp column_def_to_type(:mysql_type_int24, true), do: :uint4
  defp column_def_to_type(:mysql_type_int24, false), do: :int4
  defp column_def_to_type(:mysql_type_longlong, true), do: :uint8
  defp column_def_to_type(:mysql_type_longlong, false), do: :int8
  defp column_def_to_type(:mysql_type_year, _), do: :uint2
  defp column_def_to_type(:mysql_type_float, _), do: :float
  defp column_def_to_type(:mysql_type_double, _), do: :double
  defp column_def_to_type(:mysql_type_timestamp, _), do: :datetime
  defp column_def_to_type(:mysql_type_date, _), do: :date
  defp column_def_to_type(:mysql_type_time, _), do: :time
  defp column_def_to_type(:mysql_type_datetime, _), do: :naive_datetime
  defp column_def_to_type(:mysql_type_newdecimal, _), do: :decimal
  defp column_def_to_type(:mysql_type_json, _), do: :json
  defp column_def_to_type(:mysql_type_blob, _), do: :binary
  defp column_def_to_type(:mysql_type_long_blob, _), do: :binary
  defp column_def_to_type(:mysql_type_var_string, _), do: :binary
  defp column_def_to_type(:mysql_type_string, _), do: :binary

  defp decode_binary_row(<<rest::bits>>, null_bitmap, [_type | t], acc)
       when (null_bitmap &&& 1) == 1 do
    decode_binary_row(rest, null_bitmap >>> 1, t, [nil | acc])
  end

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:binary | t], acc),
    do: decode_string_lenenc(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int1 | t], acc),
    do: decode_int1(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint1 | t], acc),
    do: decode_uint1(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int2 | t], acc),
    do: decode_int2(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint2 | t], acc),
    do: decode_uint2(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int4 | t], acc),
    do: decode_int4(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint4 | t], acc),
    do: decode_uint4(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:int8 | t], acc),
    do: decode_int8(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:uint8 | t], acc),
    do: decode_uint8(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:float | t], acc),
    do: decode_float(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:double | t], acc),
    do: decode_double(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:decimal | t], acc),
    do: decode_decimal(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:json | t], acc),
    do: decode_json(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:date | t], acc),
    do: decode_date(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:time | t], acc),
    do: decode_time(r, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:naive_datetime | t], acc),
    do: decode_datetime(r, null_bitmap, t, acc, :naive_datetime)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:datetime | t], acc),
    do: decode_datetime(r, null_bitmap, t, acc, :datetime)

  defp decode_binary_row(<<>>, _null_bitmap, [], acc) do
    Enum.reverse(acc)
  end

  defp decode_int1(<<v::int1, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint1(<<v::uint1, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int2(<<v::int2, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint2(<<v::uint2, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int4(<<v::int4, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint4(<<v::uint4, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int8(<<v::int8, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint8(<<v::uint8, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_float(<<v::32-signed-little-float, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_double(<<v::64-signed-little-float, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  # in theory it's supposed to be a `string_lenenc` field. However since MySQL decimals
  # maximum precision is 65 digits, the size of the string will always fir on one byte.
  defp decode_decimal(<<n::uint1, string::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [Decimal.new(string) | acc])

  defp decode_date(<<4, year::uint2, month::uint1, day::uint1, r::bits>>, null_bitmap, t, acc) do
    v = %Date{year: year, month: month, day: day}
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(
         <<8, 0, 0::uint4, hour::uint1, minute::uint1, second::uint1, r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    v = %Time{hour: hour, minute: minute, second: second}
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(
         <<12, 0, 0::uint4, hour::uint1, minute::uint1, second::uint1, microsecond::uint4,
           r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    v = %Time{hour: hour, minute: minute, second: second, microsecond: {microsecond, 6}}
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(<<0, r::bits>>, null_bitmap, t, acc) do
    v = ~T[00:00:00]
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<4, year::uint2, month::uint1, day::uint1, r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, 0, 0, 0, {0, 0})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<7, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
           r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, hour, minute, second, {0, 0})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<11, year::uint2, month::uint1, day::uint1, hour::uint1, minute::uint1, second::uint1,
           microsecond::uint4, r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, hour, minute, second, {microsecond, 6})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp new_datetime(:datetime, year, month, day, hour, minute, second, microsecond) do
    %DateTime{
      year: year,
      month: month,
      day: day,
      hour: hour,
      minute: minute,
      second: second,
      microsecond: microsecond,
      std_offset: 0,
      time_zone: "Etc/UTC",
      utc_offset: 0,
      zone_abbr: "UTC"
    }
  end

  defp new_datetime(:naive_datetime, year, month, day, hour, minute, second, microsecond) do
    %NaiveDateTime{
      year: year,
      month: month,
      day: day,
      hour: hour,
      minute: minute,
      second: second,
      microsecond: microsecond
    }
  end

  defp decode_string_lenenc(<<n::int1, v::string(n), r::bits>>, null_bitmap, t, acc) when n < 251,
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_string_lenenc(<<0xFC, n::uint2, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_string_lenenc(<<0xFD, n::uint4, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_string_lenenc(<<0xFE, n::uint8, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_json(<<n::int1, v::string(n), r::bits>>, null_bitmap, t, acc) when n < 251,
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFC, n::uint2, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFD, n::uint4, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFE, n::uint8, v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(string), do: MyXQL.json_library().decode!(string)
end
