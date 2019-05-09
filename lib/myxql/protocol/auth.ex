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

  def encrypt_sha_password(password, public_key, auth_plugin_data) do
    password = password <> <<0>>
    xor = :crypto.exor(password, :binary.part(auth_plugin_data, 0, byte_size(password)))
    [entry] = :public_key.pem_decode(public_key)
    public_key = :public_key.pem_entry_decode(entry)
    :public_key.encrypt_public(xor, public_key, rsa_pad: :rsa_pkcs1_oaep_padding)
  end

  def auth_response(config, auth_plugin_name, initial_auth_plugin_data) do
    cond do
      config.password == nil ->
        ""

      auth_plugin_name == "mysql_native_password" ->
        mysql_native_password(config.password, initial_auth_plugin_data)

      auth_plugin_name == "sha256_password" and config.ssl? ->
        config.password <> <<0>>

      auth_plugin_name == "sha256_password" and not config.ssl? ->
        <<1>>

      auth_plugin_name == "caching_sha2_password" ->
        sha256_password(config.password, initial_auth_plugin_data)
    end
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
