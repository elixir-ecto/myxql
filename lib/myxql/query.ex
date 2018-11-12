defmodule MyXQL.Query do
  defstruct [:statement, :ref, name: "", type: :binary]

  defimpl DBConnection.Query do
    def parse(query, _opts) do
      # TODO: send iodata to the socket instead
      %{query | statement: IO.iodata_to_binary(query.statement)}
    end

    def describe(query, _opts) do
      query
    end

    def encode(_query, params, _opts) do
      params
    end

    def decode(_query, result, _opts) do
      result
    end
  end

  defimpl String.Chars do
    def to_string(%{statement: statement}) do
      statement
    end
  end
end
