defmodule MyXQL.Protocol.Types do
  @moduledoc false
  use Bitwise

  # https://dev.mysql.com/doc/internals/en/basic-types.html

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

  def take_int_lenenc(<<int::int(1), rest::binary>>) when int < 251, do: {int, rest}
  def take_int_lenenc(<<0xFC, int::int(2), rest::binary>>), do: {int, rest}
  def take_int_lenenc(<<0xFD, int::int(3), rest::binary>>), do: {int, rest}
  def take_int_lenenc(<<0xFE, int::int(8), rest::binary>>), do: {int, rest}

  # https://dev.mysql.com/doc/internals/en/string.html#packet-Protocol::FixedLengthString
  defmacro string(size) do
    quote do
      bytes - size(unquote(size))
    end
  end

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
    <<string::string(size), rest::binary>> = rest
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
end
