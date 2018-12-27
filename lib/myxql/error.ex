defmodule MyXQL.Error do
  defexception [:message, :statement, :mysql, :erlang]
end
