defmodule MyXQL.Error do
  defexception [:message, :statement, :mysql]
end
