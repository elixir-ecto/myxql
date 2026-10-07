defmodule MyXQL.Protocol.AuthTest do
  use ExUnit.Case, async: true

  alias MyXQL.Protocol.Auth

  @nonce <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20>>

  describe "mysql_native_password/2" do
    test "scrambles with the nonce prepended to the double hash" do
      # XOR(SHA1(password), SHA1(nonce <> SHA1(SHA1(password))))
      sha = :crypto.hash(:sha, "secret")

      expected =
        :crypto.exor(sha, :crypto.hash(:sha, @nonce <> :crypto.hash(:sha, sha)))

      assert Auth.mysql_native_password("secret", @nonce) == expected
      assert Base.encode16(expected, case: :lower) == "b32bb3a583e1340c0a1108d58b1be49781ad8c2f"
    end
  end

  describe "caching_sha2_password/2" do
    test "scrambles with the nonce appended to the double hash" do
      # XOR(SHA256(password), SHA256(SHA256(SHA256(password)) <> nonce))
      sha = :crypto.hash(:sha256, "secret")

      expected =
        :crypto.exor(sha, :crypto.hash(:sha256, :crypto.hash(:sha256, sha) <> @nonce))

      assert Auth.caching_sha2_password("secret", @nonce) == expected

      assert Base.encode16(expected, case: :lower) ==
               "746ebe205d56a0707acb3e796e834e0dd7b1d61743b26bd5202c7a623230c7c9"
    end

    # Regression: caching_sha2_password used to reuse the mysql_native_password
    # helper, which prepends the nonce instead of appending it. MySQL hides that
    # by falling back to the full authentication exchange, but servers that
    # verify the scramble eagerly (e.g. ProxySQL) reject it outright.
    test "does not prepend the nonce like mysql_native_password does" do
      sha = :crypto.hash(:sha256, "secret")

      nonce_first =
        :crypto.exor(sha, :crypto.hash(:sha256, @nonce <> :crypto.hash(:sha256, sha)))

      refute Auth.caching_sha2_password("secret", @nonce) == nonce_first
    end
  end
end
