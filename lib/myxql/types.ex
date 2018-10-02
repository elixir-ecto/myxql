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
  # MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG
  def decode_value(value, <<type>>) when type in [0x01, 0x02, 0x03, 0x08] do
    String.to_integer(value)
  end

  # MYSQL_TYPE_VARCHAR
  def decode_value(value, 0x0F) do
    value
  end

  # TODO: handle remaining types
  def decode_value(value, _type) do
    value
  end

  # https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
  # TODO: try unifying this with decode_value/2
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
