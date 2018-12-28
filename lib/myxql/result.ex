defmodule MyXQL.Result do
  @moduledoc """
  Result struct returned from any successful query.

  Its public fields are:

    * `:columns` - The column names;
    * `:last_insert_id` - The ID of the last inserted row;
    * `:num_rows` - The number of fetched or affected rows;
    * `:rows` - The result set. A list of lists, each inner list corresponding to a
      row, each element in the inner list corresponds to a column

  """

  @type t :: %__MODULE__{
          columns: [String.t()] | nil,
          last_insert_id: term() | nil,
          num_rows: non_neg_integer() | nil,
          rows: [[term()]] | nil
        }

  defstruct [
    :columns,
    :last_insert_id,
    :num_rows,
    :rows
  ]
end
