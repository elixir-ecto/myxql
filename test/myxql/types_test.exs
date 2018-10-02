defmodule MyXQL.TypesTest do
  use ExUnit.Case, async: true
  import MyXQL.Types

  test "length_encoded_integer" do
    assert decode_length_encoded_integer(<<100>>) == 100
    assert decode_length_encoded_integer(<<0xFC, 0, 252>>) == 252
    assert decode_length_encoded_integer(<<0xFD, 1, 0, 0>>) == 65536
    assert decode_length_encoded_integer(<<0xFE, 0, 0, 0, 0, 1, 0, 0, 0>>) == 16_777_216
  end

  test "length_encoded_string" do
    assert decode_length_encoded_string(<<3, "aaa">>) == "aaa"

    string = String.duplicate("a", 252)
    assert decode_length_encoded_string(<<0xFC, 0, 252, string::binary>>) == string

    assert take_length_encoded_string(<<3, "aaab">>) == {"aaa", "b"}
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
