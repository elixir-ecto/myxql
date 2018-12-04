defmodule MyXQL.Errmsg.Parser do
  @moduledoc false

  def parse(errmsg_path) do
    state = %{number: nil, codes: %{}}

    errmsg_path
    |> File.stream!()
    |> Enum.reduce(state, &parse(&1, &2))
  end

  defp parse("start-error-number " <> string, state) do
    number = string |> String.trim() |> String.to_integer()
    %{state | number: number - 1}
  end

  defp parse("ER_" <> _ = string, state) do
    parse_error_string(string, state)
  end

  defp parse("WARN_" <> _ = string, state) do
    parse_error_string(string, state)
  end

  defp parse("OBSOLETE_" <> _, state) do
    update_in(state.number, &(&1 + 1))
  end

  defp parse(_, state) do
    state
  end

  defp parse_error_string(string, state) do
    [name | _] = String.split(String.trim(string), " ")
    name = String.to_atom(name)

    number = state.number + 1

    state
    |> put_in([:number], number)
    |> put_in([:codes, number], name)
  end
end

defmodule MyXQL.Errmsg do
  @moduledoc false

  # Downloaded from: https://github.com/mysql/mysql-server/tree/mysql-8.0.13
  # References:
  # - https://dev.mysql.com/doc/refman/8.0/en/server-error-reference.html
  # - https://dev.mysql.com/doc/refman/8.0/en/error-message-components.html
  @external_resource errcodes_path = Path.join(__DIR__, "errmsg-utf8.txt")

  @spec code_to_name(integer()) :: atom()
  def code_to_name(code)

  for {code, name} <- MyXQL.Errmsg.Parser.parse(errcodes_path).codes do
    def code_to_name(unquote(code)), do: unquote(name)
  end

  def code_to_name(_), do: nil
end
