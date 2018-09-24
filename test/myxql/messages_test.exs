defmodule MyXQL.MessagesTest do
  use ExUnit.Case, async: true
  import MyXQL.Messages

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
end
