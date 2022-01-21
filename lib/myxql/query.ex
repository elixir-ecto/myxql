defmodule MyXQL.Query do
  @moduledoc """
  A struct for a prepared statement that returns a single result.

  For the struct returned from a query that returns multiple
  results, see `MyXQL.Queries`.

  Its public fields are:

    * `:name` - The name of the prepared statement;
    * `:num_params` - The number of parameter placeholders;
    * `:statement` - The prepared statement

  ## Named and Unnamed Queries

  Named queries are identified by the non-empty value in `:name` field
  and are meant to be re-used.

  Unnamed queries, with `:name` equal to `""`, are automatically closed
  after being executed.
  """

  @type t :: %__MODULE__{
          name: iodata(),
          cache: :reference | :statement,
          num_params: non_neg_integer(),
          statement: iodata()
        }

  defstruct name: "",
            cache: :reference,
            num_params: nil,
            ref: nil,
            statement: nil,
            statement_id: nil
end

defmodule MyXQL.Queries do
  @moduledoc """
  A struct for a prepared statement that returns multiple results.

  An example use case is a stored procedure with multiple `SELECT`
  statements.

  Its public fields are:

    * `:name` - The name of the prepared statement;
    * `:num_params` - The number of parameter placeholders;
    * `:statement` - The prepared statement

  ## Named and Unnamed Queries

  Named queries are identified by the non-empty value in `:name` field
  and are meant to be re-used.

  Unnamed queries, with `:name` equal to `""`, are automatically closed
  after being executed.
  """

  @type t :: %__MODULE__{
          name: iodata(),
          cache: :reference | :statement,
          num_params: non_neg_integer(),
          statement: iodata()
        }

  defstruct name: "",
            cache: :reference,
            num_params: nil,
            ref: nil,
            statement: nil,
            statement_id: nil
end

defimpl DBConnection.Query, for: [MyXQL.Query, MyXQL.Queries] do
  def parse(query, _opts) do
    query
  end

  def describe(query, _opts) do
    query
  end

  def encode(%{ref: nil} = query, _params, _opts) do
    raise ArgumentError, "query #{inspect(query)} has not been prepared"
  end

  def encode(%{num_params: nil} = query, _params, _opts) do
    raise ArgumentError, "query #{inspect(query)} has not been prepared"
  end

  def encode(%{num_params: num_params} = query, params, _opts)
      when num_params != length(params) do
    message =
      "expected params count: #{inspect(num_params)}, got values: #{inspect(params)}" <>
        " for query: #{inspect(query)}"

    raise ArgumentError, message
  end

  def encode(_query, params, _opts) do
    MyXQL.Protocol.encode_params(params)
  end

  def decode(_query, result, _opts) do
    result
  end
end

defimpl String.Chars, for: [MyXQL.Query, MyXQL.Queries] do
  def to_string(%{statement: statement}) do
    IO.iodata_to_binary(statement)
  end
end
