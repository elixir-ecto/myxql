defmodule MyXQL.Types do
  @moduledoc false

  #########################################################
  # Basic types
  #
  # https://dev.mysql.com/doc/internals/en/basic-types.html
  #########################################################

  # https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
  # TODO: check notes above
  def decode_length_encoded_integer(binary) do
    {integer, ""} = take_length_encoded_integer(binary)
    integer
  end

  def take_length_encoded_integer(<<int::size(8), rest::binary>>) when int < 251, do: {int, rest}
  def take_length_encoded_integer(<<0xFC, int::size(16), rest::binary>>), do: {int, rest}
  def take_length_encoded_integer(<<0xFD, int::size(24), rest::binary>>), do: {int, rest}
  def take_length_encoded_integer(<<0xFE, int::size(64), rest::binary>>), do: {int, rest}

  # https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::LengthEncodedString
  def decode_length_encoded_string(binary) do
    {_size, rest} = take_length_encoded_integer(binary)
    rest
  end

  def take_length_encoded_string(binary) do
    {size, rest} = take_length_encoded_integer(binary)
    <<string::bytes-size(size), rest::binary>> = rest
    {string, rest}
  end

  # https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::NulTerminatedString
  def take_null_terminated_string(binary) do
    [string, rest] = :binary.split(binary, <<0>>)
    {string, rest}
  end

  def decode_null_terminated_string(binary) do
    {string, ""} = take_null_terminated_string(binary)
    string
  end

  # Text & Binary
  #
  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
  # https://dev.mysql.com/doc/refman/8.0/en/data-types.html

  @mysql_type_tiny 0x01
  @mysql_type_short 0x02
  @mysql_type_long 0x03
  @mysql_type_float 0x04
  @mysql_type_double 0x05
  # @mysql_type_null 0x06
  # @mysql_type_timestamp 0x07
  @mysql_type_longlong 0x08
  @mysql_type_int24 0x09
  @mysql_type_date 0x0A
  @mysql_type_time 0x0B
  @mysql_type_datetime 0x0C
  @mysql_type_year 0x0D
  @mysql_type_varchar 0x0F
  # @mysql_type_bit 0x10
  @mysql_type_newdecimal 0xF6
  # @mysql_type_enum 0xF7
  # @mysql_type_set 0xF8
  # @mysql_type_tiny_blob 0xF9
  # @mysql_type_medium_blob 0xFA
  # @mysql_type_long_blob 0xFB
  # @mysql_type_blob 0xFC
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

  # TODO: can we even support bits in *text* protocol?
  # def decode_text_value(value, <<@mysql_type_bit>>) do
  #   value
  # end

  def decode_text_value(value, @mysql_type_date) do
    Date.from_iso8601!(value)
  end

  def decode_text_value(value, @mysql_type_time) do
    Time.from_iso8601!(value)
  end

  def decode_text_value(value, @mysql_type_datetime) do
    NaiveDateTime.from_iso8601!(value)
  end

  def decode_text_value(value, @mysql_type_varchar) do
    value
  end

  def decode_text_value(value, @mysql_type_var_string) do
    value
  end

  def decode_text_value(value, @mysql_type_string) do
    value
  end

  # Binary values

  def take_binary_value(data, @mysql_type_long) do
    <<value::little-integer-size(32), rest::binary>> = data
    {value, rest}
  end

  def take_binary_value(data, @mysql_type_longlong) do
    <<value::little-integer-size(64), rest::binary>> = data
    {value, rest}
  end

  def encode_value(value) when is_integer(value) do
    {0x08, <<value::little-integer-size(64)>>}
  end
end
