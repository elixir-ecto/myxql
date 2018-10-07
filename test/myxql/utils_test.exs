defmodule MyXQL.TypesTest do
  use ExUnit.Case, async: true
  import MyXQL.Utils
  use Bitwise

  test "mysql_native_password/2" do
    expected = <<237, 1, 22, 201, 143, 231, 205, 149, 164, 183, 127, 144, 178, 207, 8, 5, 193, 65, 98, 211>>
    assert mysql_native_password("password", "password") == expected
  end
end
