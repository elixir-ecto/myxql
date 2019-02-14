defmodule MyXQL.Error do
  defexception [
    :connection_id,
    :message,
    :mysql,
    :socket,
    :statement
  ]

  @type t :: %__MODULE__{
          connection_id: non_neg_integer() | nil,
          message: String.t(),
          mysql: %{code: integer(), name: atom()} | nil,
          socket: :inet.posix() | term() | nil,
          statement: iodata() | nil
        }
end
