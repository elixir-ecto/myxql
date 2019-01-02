defmodule MyXQL.Query do
  @moduledoc """
  Query struct returned from a successfully prepared query.

  Its public fields are:

    * `:name` - The name of the prepared statement;
    * `:num_params` - The number of parameter placeholders;
    * `:ref` - A reference used to identify prepared queries;
    * `:statement` - The prepared statement

  ## Prepared queries

  Once a query is prepared with `MyXQL.prepare/4`, the
  returned query will have its `ref` field set to a reference.
  When `MyXQL.execute/4` is called with the prepared query,
  it always returns a query. If the `ref` field in the query
  given to `execute` and the one returned are the same, it
  means the cached prepared query was used. If the `ref` field
  is not the same, it means the query had to be re-prepared.
  """

  @type t :: %__MODULE__{
          columns: [String.t()],
          name: iodata(),
          num_params: non_neg_integer(),
          ref: reference(),
          statement: iodata()
        }

  defstruct columns: [],
            name: "",
            num_params: nil,
            ref: nil,
            statement: nil

  defimpl DBConnection.Query do
    def parse(query, _opts) do
      query
    end

    def describe(query, _opts) do
      query
    end

    def encode(%{num_params: num_params} = query, params, _opts)
        when num_params != length(params) do
      raise ArgumentError,
            "parameters must be of length #{num_params} for query #{inspect(query)}"
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
      IO.iodata_to_binary(statement)
    end
  end
end
