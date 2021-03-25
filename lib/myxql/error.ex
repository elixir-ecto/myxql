defmodule MyXQL.Error do
  defexception [
    :connection_id,
    :message,
    :mysql,
    :statement
  ]

  @type t :: %__MODULE__{
          connection_id: non_neg_integer() | nil,
          message: String.t(),
          mysql: %{code: integer(), name: atom() | nil} | nil,
          statement: iodata() | nil
        }

  @impl true
  def message(%{mysql: %{code: code, name: nil}, message: message}) do
    "(#{code}) " <> message
  end

  def message(%{mysql: %{code: code, name: name}, message: message}) do
    "(#{code}) (#{name})" <> message
  end
end
