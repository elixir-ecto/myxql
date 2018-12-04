defmodule MyXQL.ErrorCodesTest do
  use ExUnit.Case, async: true

  test "code_to_name/1" do
    assert MyXQL.ErrorCodes.code_to_name(1062) == :ER_DUP_ENTRY
    assert MyXQL.ErrorCodes.code_to_name(999_999) == nil
  end
end
