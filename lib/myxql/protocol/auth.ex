defmodule MyXQL.Protocol.Auth do
  @moduledoc false
  use Bitwise

  # https://dev.mysql.com/doc/internals/en/secure-password-authentication.html
  @spec mysql_native_password(binary(), binary()) :: binary()
  def mysql_native_password(password, auth_plugin_data) do
    sha_hash(:sha, password, auth_plugin_data)
  end

  @spec sha256_password(binary(), binary()) :: binary()
  def sha256_password(password, auth_plugin_data) do
    sha_hash(:sha256, password, auth_plugin_data)
  end

  ## Helpers

  defp sha_hash(type, password, auth_plugin_data)
       when is_binary(password) and is_binary(auth_plugin_data) do
    password_sha = :crypto.hash(type, password)

    bxor_binary(
      password_sha,
      :crypto.hash(type, auth_plugin_data <> :crypto.hash(type, password_sha))
    )
  end

  defp bxor_binary(<<l::160>>, <<r::160>>), do: <<l ^^^ r::160>>
  defp bxor_binary(<<l::256>>, <<r::256>>), do: <<l ^^^ r::256>>
end
