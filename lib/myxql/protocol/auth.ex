defmodule MyXQL.Protocol.Auth do
  @moduledoc false

  # TODO: remove when we require Elixir v1.10+
  require Bitwise

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

    xor =
      auth_plugin_data
      |> pad_auth_plugin_data(byte_size(password))
      |> :crypto.exor(password)

    [entry] = :public_key.pem_decode(public_key)
    public_key = :public_key.pem_entry_decode(entry)
    :public_key.encrypt_public(xor, public_key, rsa_pad: :rsa_pkcs1_oaep_padding)
  end

  def auth_response(config, auth_plugin_name, initial_auth_plugin_data) do
    cond do
      config.password == nil ->
        ""

      auth_plugin_name == "mysql_clear_password" and config.enable_cleartext_plugin ->
        config.password <> <<0>>

      auth_plugin_name == "mysql_native_password" ->
        mysql_native_password(config.password, initial_auth_plugin_data)

      auth_plugin_name == "sha256_password" ->
        if config.ssl_opts do
          config.password <> <<0>>
        else
          <<1>>
        end

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

  defp bxor_binary(<<l::160>>, <<r::160>>), do: <<Bitwise.bxor(l, r)::160>>
  defp bxor_binary(<<l::256>>, <<r::256>>), do: <<Bitwise.bxor(l, r)::256>>

  # Repeat str as needed and truncate final string to target_len
  # E.g. "foobar", 12 -> "foobarfoobar"
  # E.g. "foobar", 15 -> "foobarfoobarfoo"
  defp pad_auth_plugin_data(str, target_len),
    do: do_pad_auth_plugin_data(str, byte_size(str), target_len)

  defp do_pad_auth_plugin_data(_str, _str_len, 0), do: ""
  defp do_pad_auth_plugin_data(str, str_len, target_len) when str_len == target_len, do: str

  defp do_pad_auth_plugin_data(str, str_len, target_len) when str_len > target_len,
    do: :binary.part(str, 0, target_len)

  defp do_pad_auth_plugin_data(str, str_len, target_len),
    do: str <> do_pad_auth_plugin_data(str, str_len, target_len - str_len)
end
