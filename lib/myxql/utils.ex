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

  @spec bxor_binary(binary(), binary(), binary()) :: binary()
  def bxor_binary(left, right, result \\ "")
  def bxor_binary("", "", result), do: result
  def bxor_binary(<<l::integer(), left::binary()>>, <<r::integer(), right::binary()>>, result) do
    bxor_binary(left, right, <<result::binary(), l ^^^ r>>)
  end
end
