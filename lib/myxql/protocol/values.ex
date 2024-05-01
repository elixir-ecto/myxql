defmodule MyXQL.Protocol.Values do
  @moduledoc false

  import Bitwise
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
    mysql_type_geometry: 0xFF,

    # Internal types below, we should never get them but if we do, at least we'll get a nicer error message.
    # https://dev.mysql.com/doc/internals/en/com-query-response.html#fnref_internal
    mysql_type_newdate: 0x0E,
    mysql_type_timestamp2: 0x11,
    mysql_type_datetime2: 0x12,
    mysql_type_date2: 0x13
  ]

  for {atom, code} <- types do
    def type_code_to_atom(unquote(code)), do: unquote(atom)
    def type_atom_to_code(unquote(atom)), do: unquote(code)
  end

  defp column_def_to_type(column_def(type: :mysql_type_tiny, unsigned?: true)), do: :uint1
  defp column_def_to_type(column_def(type: :mysql_type_tiny, unsigned?: false)), do: :int1
  defp column_def_to_type(column_def(type: :mysql_type_short, unsigned?: true)), do: :uint2
  defp column_def_to_type(column_def(type: :mysql_type_short, unsigned?: false)), do: :int2
  defp column_def_to_type(column_def(type: :mysql_type_long, unsigned?: true)), do: :uint4
  defp column_def_to_type(column_def(type: :mysql_type_long, unsigned?: false)), do: :int4
  defp column_def_to_type(column_def(type: :mysql_type_int24, unsigned?: true)), do: :uint4
  defp column_def_to_type(column_def(type: :mysql_type_int24, unsigned?: false)), do: :int4
  defp column_def_to_type(column_def(type: :mysql_type_longlong, unsigned?: true)), do: :uint8
  defp column_def_to_type(column_def(type: :mysql_type_longlong, unsigned?: false)), do: :int8
  defp column_def_to_type(column_def(type: :mysql_type_year)), do: :uint2
  defp column_def_to_type(column_def(type: :mysql_type_float)), do: :float
  defp column_def_to_type(column_def(type: :mysql_type_double)), do: :double
  defp column_def_to_type(column_def(type: :mysql_type_timestamp)), do: :datetime
  defp column_def_to_type(column_def(type: :mysql_type_date)), do: :date
  defp column_def_to_type(column_def(type: :mysql_type_time)), do: :time
  defp column_def_to_type(column_def(type: :mysql_type_datetime)), do: :naive_datetime
  defp column_def_to_type(column_def(type: :mysql_type_newdecimal)), do: :decimal
  defp column_def_to_type(column_def(type: :mysql_type_json)), do: :json
  defp column_def_to_type(column_def(type: :mysql_type_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_tiny_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_medium_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_long_blob)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_var_string)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_string)), do: :binary
  defp column_def_to_type(column_def(type: :mysql_type_bit, length: length)), do: {:bit, length}
  defp column_def_to_type(column_def(type: :mysql_type_null)), do: :null
  defp column_def_to_type(column_def(type: :mysql_type_geometry)), do: :geometry

  # Text values

  def decode_text_row(values, column_defs) do
    types = Enum.map(column_defs, &column_def_to_type/1)
    decode_text_row(values, types, [])
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
             :uint1,
             :uint2,
             :uint4,
             :uint8,
             :int1,
             :int2,
             :int4,
             :int8
           ] do
    String.to_integer(value)
  end

  def decode_text_value(value, type) when type in [:float, :double] do
    if String.contains?(value, ".") do
      String.to_float(value)
    else
      String.to_integer(value) * 1.0
    end
  end

  # Note: MySQL implements `NUMERIC` as `DECIMAL`s
  def decode_text_value(value, :decimal) do
    Decimal.new(value)
  end

  def decode_text_value("0000-00-00", :date) do
    :zero_date
  end

  def decode_text_value(value, :date) do
    Date.from_iso8601!(value)
  end

  def decode_text_value(value, :time) do
    Time.from_iso8601!(value)
  end

  def decode_text_value("0000-00-00 00:00:00", :naive_datetime) do
    :zero_datetime
  end

  def decode_text_value(value, :naive_datetime) do
    NaiveDateTime.from_iso8601!(value)
  end

  def decode_text_value("0000-00-00 00:00:00", :datetime) do
    :zero_datetime
  end

  def decode_text_value(value, :datetime) do
    value
    |> NaiveDateTime.from_iso8601!()
    |> DateTime.from_naive!("Etc/UTC")
  end

  def decode_text_value(value, :binary) do
    value
  end

  def decode_text_value(value, :json) do
    json_library().decode!(value)
  end

  def decode_text_value(value, {:bit, size}) do
    decode_bit(value, size)
  end

  def decode_text_value(value, :geometry) do
    decode_geometry(value)
  end

  # Binary values

  def encode_binary_value(value)
      when is_integer(value) and value >= -1 <<< 63 and value < 1 <<< 64 do
    {:mysql_type_longlong, <<value::int8()>>}
  end

  def encode_binary_value(value) when is_float(value) do
    {:mysql_type_double, <<value::64-little-signed-float>>}
  end

  def encode_binary_value(%Decimal{} = value) do
    string = Decimal.to_string(value, :normal)
    # per manual "The maximum number of digits for DECIMAL is 65" so we can
    # encode this directly instead of going through encode_string_lenenc/1
    {:mysql_type_newdecimal, <<byte_size(string), string::binary>>}
  end

  def encode_binary_value(%Date{year: year, month: month, day: day}) do
    {:mysql_type_date, <<4, year::uint2(), month::uint1(), day::uint1()>>}
  end

  def encode_binary_value(:zero_date) do
    {:mysql_type_date, <<4, 0::uint2(), 0::uint1(), 0::uint1()>>}
  end

  def encode_binary_value(%Time{} = time), do: encode_binary_time(time)

  def encode_binary_value(%NaiveDateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(%DateTime{} = datetime), do: encode_binary_datetime(datetime)

  def encode_binary_value(:zero_datetime) do
    encode_binary_datetime(%NaiveDateTime{
      year: 0,
      month: 0,
      day: 0,
      hour: 0,
      minute: 0,
      second: 0,
      microsecond: {0, 0}
    })
  end

  def encode_binary_value(binary) when is_binary(binary) do
    {:mysql_type_var_string, encode_string_lenenc(binary)}
  end

  def encode_binary_value(bitstring) when is_bitstring(bitstring) do
    size = bit_size(bitstring)
    pad = 8 - rem(size, 8)
    bitstring = <<0::size(pad), bitstring::bitstring>>
    {:mysql_type_var_string, encode_string_lenenc(bitstring)}
  end

  def encode_binary_value(true) do
    {:mysql_type_tiny, <<1>>}
  end

  def encode_binary_value(false) do
    {:mysql_type_tiny, <<0>>}
  end

  if Code.ensure_loaded?(Geo) do
    def encode_binary_value(%Geo.Point{} = geo), do: encode_geometry(geo)
    def encode_binary_value(%Geo.MultiPoint{} = geo), do: encode_geometry(geo)
    def encode_binary_value(%Geo.LineString{} = geo), do: encode_geometry(geo)
    def encode_binary_value(%Geo.MultiLineString{} = geo), do: encode_geometry(geo)
    def encode_binary_value(%Geo.Polygon{} = geo), do: encode_geometry(geo)
    def encode_binary_value(%Geo.MultiPolygon{} = geo), do: encode_geometry(geo)
    def encode_binary_value(%Geo.GeometryCollection{} = geo), do: encode_geometry(geo)
  end

  def encode_binary_value(term) when is_list(term) or is_map(term) do
    string = json_library().encode!(term)
    {:mysql_type_var_string, encode_string_lenenc(string)}
  end

  def encode_binary_value(other) do
    raise ArgumentError, "query has invalid parameter #{inspect(other)}"
  end

  if Code.ensure_loaded?(Geo) do
    defp encode_geometry(geo) do
      srid = geo.srid || 0
      binary = %{geo | srid: nil} |> Geo.WKB.encode_to_iodata(:ndr) |> IO.iodata_to_binary()
      {:mysql_type_var_string, encode_string_lenenc(<<srid::uint4(), binary::binary>>)}
    end
  end

  ## Time/DateTime

  # MySQL supports negative time and days, we don't.
  # See: https://dev.mysql.com/doc/internals/en/binary-protocol-value.html#packet-ProtocolBinary::MYSQL_TYPE_TIME

  defp encode_binary_time(%Time{hour: 0, minute: 0, second: 0, microsecond: {0, 0}}) do
    {:mysql_type_time, <<0>>}
  end

  defp encode_binary_time(%Time{hour: hour, minute: minute, second: second, microsecond: {0, 0}}) do
    {:mysql_type_time,
     <<8, 0::uint1(), 0::uint4(), hour::uint1(), minute::uint1(), second::uint1()>>}
  end

  defp encode_binary_time(%Time{
         hour: hour,
         minute: minute,
         second: second,
         microsecond: {microsecond, _}
       }) do
    {:mysql_type_time,
     <<12, 0::uint1(), 0::uint4(), hour::uint1(), minute::uint1(), second::uint1(),
       microsecond::uint4()>>}
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
     <<7, year::uint2(), month::uint1(), day::uint1(), hour::uint1(), minute::uint1(),
       second::uint1()>>}
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
     <<11, year::uint2(), month::uint1(), day::uint1(), hour::uint1(), minute::uint1(),
       second::uint1(), microsecond::uint4()>>}
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
     <<11, year::uint2(), month::uint1(), day::uint1(), hour::uint1(), minute::uint1(),
       second::uint1(), microsecond::uint4()>>}
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

  defp decode_binary_row(<<rest::bits>>, null_bitmap, [_type | t], acc)
       when (null_bitmap &&& 1) == 1 do
    decode_binary_row(rest, null_bitmap >>> 1, t, [nil | acc])
  end

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:binary | t], acc),
    do: decode_string_lenenc(r, null_bitmap, t, acc, & &1)

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

  defp decode_binary_row(<<r::bits>>, null_bitmap, [{:bit, size} | t], acc),
    do: decode_bit(r, size, null_bitmap, t, acc)

  defp decode_binary_row(<<r::bits>>, null_bitmap, [:geometry | t], acc),
    do: decode_string_lenenc(r, null_bitmap, t, acc, &decode_geometry/1)

  defp decode_binary_row(<<>>, _null_bitmap, [], acc) do
    Enum.reverse(acc)
  end

  if Code.ensure_loaded?(Geo) do
    # https://dev.mysql.com/doc/refman/8.0/en/gis-data-formats.html#gis-internal-format
    defp decode_geometry(<<srid::uint4(), r::bits>>) do
      srid = if srid == 0, do: nil, else: srid
      r |> Geo.WKB.decode!() |> Map.put(:srid, srid)
    end
  else
    defp decode_geometry(_) do
      raise """
      encoding/decoding geometry types requires :geo package, add:

          {:geo, "~> 3.4"}

      to your mix.exs and run `mix deps.compile --force myxql`.
      """
    end
  end

  defp decode_int1(<<v::int1(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint1(<<v::uint1(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int2(<<v::int2(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint2(<<v::uint2(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int4(<<v::int4(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint4(<<v::uint4(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_int8(<<v::int8(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_uint8(<<v::uint8(), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_float(<<v::32-signed-little-float, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  defp decode_double(<<v::64-signed-little-float, r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])

  # in theory it's supposed to be a `string_lenenc` field. However since MySQL decimals
  # maximum precision is 65 digits, the size of the string will always fir on one byte.
  defp decode_decimal(<<n::uint1(), string::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [Decimal.new(string) | acc])

  defp decode_date(
         <<4, year::uint2(), month::uint1(), day::uint1(), r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    v = %Date{year: year, month: month, day: day}
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_date(<<0, r::bits>>, null_bitmap, t, acc) do
    v = :zero_date
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(
         <<8, is_negative, days::uint4(), hours::uint1(), minutes::uint1(), seconds::uint1(),
           r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    v = time(is_negative, days, hours, minutes, seconds, {0, 0})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(
         <<12, is_negative, days::uint4(), hours::uint1(), minutes::uint1(), seconds::uint1(),
           microseconds::uint4(), r::bits>>,
         null_bitmap,
         t,
         acc
       ) do
    v = time(is_negative, days, hours, minutes, seconds, {microseconds, 6})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_time(<<0, r::bits>>, null_bitmap, t, acc) do
    v = ~T[00:00:00]
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp time(0, 0, hours, minutes, seconds, microsecond) do
    %Time{hour: hours, minute: minutes, second: seconds, microsecond: microsecond}
  end

  defp time(is_negative, days, hours, minutes, seconds, microseconds) do
    sign = if is_negative == 0, do: "", else: "-"
    days = if days == 0, do: "", else: "#{days}d "
    time = time(0, 0, hours, minutes, seconds, microseconds)
    string = sign <> days <> to_string(time)

    raise ArgumentError,
          "cannot decode \"#{string}\" as time" <>
            ", negative or >= 24:00:00 values are not supported"
  end

  defp decode_datetime(
         <<4, year::uint2(), month::uint1(), day::uint1(), r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, 0, 0, 0, {0, 0})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<7, year::uint2(), month::uint1(), day::uint1(), hour::uint1(), minute::uint1(),
           second::uint1(), r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, hour, minute, second, {0, 0})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<11, year::uint2(), month::uint1(), day::uint1(), hour::uint1(), minute::uint1(),
           second::uint1(), microsecond::uint4(), r::bits>>,
         null_bitmap,
         t,
         acc,
         type
       ) do
    v = new_datetime(type, year, month, day, hour, minute, second, {microsecond, 6})
    decode_binary_row(r, null_bitmap >>> 1, t, [v | acc])
  end

  defp decode_datetime(
         <<0, r::bits>>,
         null_bitmap,
         t,
         acc,
         _type
       ) do
    v = :zero_datetime
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

  defp decode_string_lenenc(<<n::uint1(), v::string(n), r::bits>>, null_bitmap, t, acc, decoder)
       when n < 251,
       do: decode_binary_row(r, null_bitmap >>> 1, t, [decoder.(v) | acc])

  defp decode_string_lenenc(
         <<0xFC, n::uint2(), v::string(n), r::bits>>,
         null_bitmap,
         t,
         acc,
         decoder
       ),
       do: decode_binary_row(r, null_bitmap >>> 1, t, [decoder.(v) | acc])

  defp decode_string_lenenc(
         <<0xFD, n::uint3(), v::string(n), r::bits>>,
         null_bitmap,
         t,
         acc,
         decoder
       ),
       do: decode_binary_row(r, null_bitmap >>> 1, t, [decoder.(v) | acc])

  defp decode_string_lenenc(
         <<0xFE, n::uint8(), v::string(n), r::bits>>,
         null_bitmap,
         t,
         acc,
         decoder
       ),
       do: decode_binary_row(r, null_bitmap >>> 1, t, [decoder.(v) | acc])

  defp decode_json(<<n::uint1(), v::string(n), r::bits>>, null_bitmap, t, acc) when n < 251,
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFC, n::uint2(), v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFD, n::uint3(), v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(<<0xFE, n::uint8(), v::string(n), r::bits>>, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_json(v) | acc])

  defp decode_json(string), do: json_library().decode!(string)

  defp json_library() do
    Application.get_env(:myxql, :json_library, Jason)
  end

  defp decode_bit(<<n::uint1(), v::string(n), r::bits>>, size, null_bitmap, t, acc) when n < 251,
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(<<0xFC, n::uint2(), v::string(n), r::bits>>, size, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(<<0xFD, n::uint3(), v::string(n), r::bits>>, size, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(<<0xFE, n::uint8(), v::string(n), r::bits>>, size, null_bitmap, t, acc),
    do: decode_binary_row(r, null_bitmap >>> 1, t, [decode_bit(v, size) | acc])

  defp decode_bit(binary, size) do
    pad = 8 - rem(size, 8)
    <<0::size(pad), bitstring::size(size)-bits>> = binary
    bitstring
  end
end
