defmodule MyXQL.Error do
  defexception [
    :message,
    :mysql,
    :socket,
    :statement
  ]

  @type t :: %__MODULE__{
          message: String.t(),
          mysql: %{code: integer(), name: atom()} | nil,
          socket: :inet.posix() | term() | nil,
          statement: iodata() | nil
        }
end
