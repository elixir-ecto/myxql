defmodule MyXQL.Error do
  defexception [:message, :query, :mysql]
end
