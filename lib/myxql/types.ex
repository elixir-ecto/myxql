defmodule MyXQL.Types do
  @moduledoc false

  #########################################################
  # Data types
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

  # https://dev.mysql.com/doc/internals/en/string.html
  def decode_length_encoded_string(binary) do
    {_size, rest} = take_length_encoded_integer(binary)
    rest
  end

  def take_length_encoded_string(binary) do
    {size, rest} = take_length_encoded_integer(binary)
    <<string::bytes-size(size), rest::binary>> = rest
    {string, rest}
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
  # https://dev.mysql.com/doc/refman/8.0/en/data-types.html
  def decode_text_value(value, type)

  # MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG, MYSQL_TYPE_INT24, MYSQL_TYPE_YEAR
  def decode_text_value(value, <<type>>) when type in [0x01, 0x02, 0x03, 0x08, 0x09, 0x0D] do
    String.to_integer(value)
  end

  # MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE
  def decode_text_value(value, <<type>>) when type in [0x04, 0x05] do
    String.to_float(value)
  end

  # MYSQL_TYPE_NEWDECIMAL
  # Note: MySQL implements `NUMERIC` as `DECIMAL`s
  def decode_text_value(value, <<0xF6>>) do
    Decimal.new(value)
  end

  # TODO: can we even support bits in *text* protocol?
  # # MYSQL_TYPE_BIT
  # def decode_text_value(value, <<0x10>>) do
  #   value
  # end

  # MYSQL_TYPE_DATE
  def decode_text_value(value, <<0x0A>>) do
    Date.from_iso8601!(value)
  end

  # MYSQL_TYPE_TIME
  def decode_text_value(value, <<0x0B>>) do
    Time.from_iso8601!(value)
  end

  # MYSQL_TYPE_DATETIME
  def decode_text_value(value, <<0x0C>>) do
    NaiveDateTime.from_iso8601!(value)
  end

  # MYSQL_TYPE_VARCHAR
  def decode_text_value(value, <<0x0F>>) do
    value
  end

  # MYSQL_TYPE_VAR_STRING
  def decode_text_value(value, <<0xFD>>) do
    value
  end

  # MYSQL_TYPE_STRING
  def decode_text_value(value, <<0xFE>>) do
    value
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
  # TODO: try unifying this with decode_text_value/2
  def take_binary_value(data, <<0x03>>) do
    <<value::little-integer-size(32), rest::binary>> = data
    {value, rest}
  end

  def take_binary_value(data, <<0x08>>) do
    <<value::little-integer-size(64), rest::binary>> = data
    {value, rest}
  end

  def encode_value(value) when is_integer(value) do
    {0x08, <<value::little-integer-size(64)>>}
  end
end
