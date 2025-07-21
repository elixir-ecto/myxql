defmodule MyXQL.Protocol.ServerErrorCodes do
  @moduledoc false

  # TODO: remove when we require Elixir v1.10+
  codes_from_config =
    if Version.match?(System.version(), ">= 1.10.0") do
      Application.compile_env(:myxql, :extra_error_codes, [])
    else
      apply(Application, :get_env, [:myxql, :extra_error_codes, []])
    end

  default_codes = [
    {1005, :ER_CANT_CREATE_TABLE},
    {1006, :ER_CANT_CREATE_DB},
    {1007, :ER_DB_CREATE_EXISTS},
    {1008, :ER_DB_DROP_EXISTS},
    {1045, :ER_ACCESS_DENIED_ERROR},
    {1046, :ER_NO_DB_ERROR},
    {1049, :ER_BAD_DB_ERROR},
    {1050, :ER_TABLE_EXISTS_ERROR},
    {1051, :ER_BAD_TABLE_ERROR},
    {1062, :ER_DUP_ENTRY},
    {1146, :ER_NO_SUCH_TABLE},
    {1207, :ER_READ_ONLY_TRANSACTION},
    {1243, :ER_UNKNOWN_PREPARED_STATEMENT_HANDLER},
    {1295, :ER_UNSUPPORTED_PS},
    {1421, :ER_STMT_HAS_NO_OPEN_CURSOR},
    {1451, :ER_ROW_IS_REFERENCED_2},
    {1452, :ER_NO_REFERENCED_ROW_2},
    {1461, :ER_MAX_PREPARED_STMT_COUNT_REACHED},
    {1644, :ER_SIGNAL_EXCEPTION},
    {1792, :ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION},
    {1836, :ER_READ_ONLY_MODE},
    {3819, :ER_CHECK_CONSTRAINT_VIOLATED}
  ]

  codes = default_codes ++ codes_from_config

  for {code, name} <- Enum.uniq(codes) do
    def name_to_code(unquote(name)), do: unquote(code)

    def code_to_name(unquote(code)), do: unquote(name)
  end

  def name_to_code(_), do: nil

  def code_to_name(_), do: nil
end
