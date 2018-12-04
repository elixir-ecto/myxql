# Usage: mix run build_error_codes.exs
#
# References:
# - https://dev.mysql.com/doc/refman/8.0/en/server-error-reference.html
# - https://dev.mysql.com/doc/refman/8.0/en/error-message-components.html

defmodule MyXQL.ErrorCodes.Parser do
  @moduledoc false

  def parse(errmsg_path) do
    state = %{number: nil, codes: []}

    errmsg_path
    |> File.stream!()
    |> Enum.reduce(state, &parse(&1, &2))
    |> Map.get(:codes)
    |> Enum.reverse()
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
    number = state.number + 1
    [name | _] = String.split(String.trim(string), " ")
    name = String.to_atom(name)
    %{state | number: number, codes: [{number, name} | state.codes]}
  end
end

url = "https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.0.13/share/errmsg-utf8.txt"
path = Path.basename(url)
{_, 0} = System.cmd("curl", ~w(-O #{url}))
codes = MyXQL.ErrorCodes.Parser.parse(path)

code = """
# Do not edit manually, see build_error_codes.exs at the root.

defmodule MyXQL.ErrorCodes do
  @moduledoc false

  codes = #{inspect(codes, limit: :infinity)}

  @spec code_to_name(integer()) :: atom()
  def code_to_name(code)

  for {code, name} <- codes do
    def code_to_name(unquote(code)), do: unquote(name)
  end

  def code_to_name(_), do: nil
end

"""

File.write!("lib/myxql/error_codes.ex", Code.format_string!(code))
