defmodule MyXQL.Protocol do
  @moduledoc false

  defdelegate error_code_to_name(code), to: MyXQL.Protocol.ServerErrorCodes, as: :code_to_name
end
