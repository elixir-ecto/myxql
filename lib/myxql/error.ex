defmodule MyXQL.Error do
  defexception [:message, :statement, :mysql, :erlang]

  @type t :: %__MODULE__{
          message: String.t(),
          statement: iodata() | nil,
          mysql: %{code: integer(), name: atom()} | nil,
          erlang: :inet.posix() | term() | nil
        }
end
