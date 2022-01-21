defmodule MyXQL.TextQuery do
  @moduledoc false

  defstruct [:statement]
end

defmodule MyXQL.TextQueries do
  @moduledoc false

  defstruct [:statement]
end

defimpl DBConnection.Query, for: [MyXQL.TextQuery, MyXQL.TextQueries] do
  def parse(query, _opts), do: query

  def describe(query, _opts), do: query

  def encode(_query, [], _opts), do: []

  def encode(_query, params, _opts) do
    raise ArgumentError, "text queries cannot use parameters, got: #{inspect(params)}"
  end

  def decode(_query, result, _opts), do: result
end

defimpl String.Chars, for: [MyXQL.TextQuery, MyXQL.TextQueries] do
  def to_string(%{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
