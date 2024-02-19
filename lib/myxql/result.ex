defmodule MyXQL.Result do
  @moduledoc """
  Result struct returned from any successful query.

  Its public fields are:

    * `:columns` - The column names;
    * `:connection_id` - The connection ID;
    * `:last_insert_id` - The ID of the last inserted row;
    * `:num_rows` - The number of fetched or affected rows;
    * `:rows` - The result set. A list of lists, each inner list corresponding to a
      row, each element in the inner list corresponds to a column;
    * `:num_warnings` - The number of warnings

  ## Warnings

  Depending on SQL MODE, a given query may error or just return warnings.
  If `result.num_warnings` is non-zero it means there were warnings and they can be
  retrieved by making another query:

      MyXQL.query!(conn, "SHOW WARNINGS", [], query_type: :text)

  """

  @type t :: %__MODULE__{
          columns: [String.t()] | nil,
          connection_id: pos_integer(),
          last_insert_id: term() | nil,
          num_rows: non_neg_integer() | nil,
          rows: [[term()]] | nil,
          num_warnings: non_neg_integer()
        }

  defstruct [
    :columns,
    :connection_id,
    :last_insert_id,
    :num_rows,
    :rows,
    :num_warnings
  ]
end

if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: MyXQL.Result do
    def init(%{columns: columns}) when columns in [nil, []] do
      {:rows, %{columns: [], count: 0}, []}
    end

    def init(result) do
      {columns, _} =
        Enum.map_reduce(result.columns, %{}, fn column, counts ->
          counts = Map.update(counts, column, 1, &(&1 + 1))

          case counts[column] do
            1 -> {column, counts}
            n -> {"#{column}_#{n}", counts}
          end
        end)

      {:rows, %{columns: columns, count: result.num_rows}, result.rows}
    end
  end
end
