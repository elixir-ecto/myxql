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
  def message(e) do
    if map = e.mysql do
      IO.iodata_to_binary([
        [?(, Integer.to_string(map.code), ?)],
        build_name(map),
        e.message,
        build_query(e.statement)
      ])
    else
      e.message
    end
  end

  defp build_name(%{name: nil}), do: [?\s]
  defp build_name(%{name: name}), do: [?\s, ?(, Atom.to_string(name), ?), ?\s]

  defp build_query(nil), do: []
  defp build_query(query_statement), do: ["\n\n    query: ", query_statement]
end
