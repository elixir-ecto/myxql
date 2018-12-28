defmodule MyXQL.Query do
  defstruct name: "",
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
      statement
    end
  end
end
