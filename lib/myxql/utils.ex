defmodule MyXQL.Utils do
  @moduledoc false
  use Bitwise

  # https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
  @spec mysql_native_password(binary(), binary()) :: binary()
  def mysql_native_password(password, auth_plugin_data)
      when is_binary(password) and is_binary(auth_plugin_data) do
    password_sha1 = :crypto.hash(:sha, password)

    bxor_binary(
      password_sha1,
      :crypto.hash(:sha, auth_plugin_data <> :crypto.hash(:sha, password_sha1))
    )
  end

  defp bxor_binary(<<l::160>>, <<r::160>>), do: <<(l ^^^ r)::160>>
end
