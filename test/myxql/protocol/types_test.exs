defmodule MyXQL.Protocol.TypesTest do
  use ExUnit.Case, async: true
  import MyXQL.Protocol.Types
  import Bitwise

  test "int_lenenc" do
    assert decode_int_lenenc(<<100>>) == 100
    assert decode_int_lenenc(<<0xFC, 0x10, 0x27>>) == 10_000
    assert decode_int_lenenc(<<0xFD, 0, 0, 1>>) == 65536
    assert decode_int_lenenc(<<0xFE, 0, 0, 0, 0, 1, 0, 0, 0>>) == 4_294_967_296
  end

  test "string_lenenc" do
    assert decode_string_lenenc(<<3, "aaa">>) == "aaa"

    string = String.duplicate("a", 252)
    assert decode_string_lenenc(<<0xFC, 0, 252, string::binary>>) == string

    assert take_string_lenenc(<<3, "aaab">>) == {"aaa", "b"}
  end
end
