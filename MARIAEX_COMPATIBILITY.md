# Mariaex Compatibility

[Mariaex](https://github.com/xerions/mariaex) is a popular MySQL driver for Elixir.

Below is a list of differences that should ease the transition between drivers.

Note, even though MyXQL may not support a given feature right now (or does it differently) it
doesn't necessarily preclude changing that in the future.

## Differences between MyXQL master and Mariaex v0.9.1

Connection:

  * MyXQL supports MySQL Pluggable Authentication system and ships with `mysql_native_password`, `sha256_password`, and `caching_sha2_password` authentication methods.

    Note: Mariaex added pluggable authentication support on master.

  * MyXQL defaults to using UNIX domain socket for connecting to the server. Forcing TCP with default options can be done by doing: `MyXQL.start_link(protocol: :tcp)`. Mariaex defaults to TCP.

  * MyXQL does not support `:sock_type` option and uses `:protocol` option instead

  * MyXQL does not support `:skip_database` option, set `database: nil` instead

  * MyXQL does not support `:datetime` option and only works with Elixir calendar types (and not Erlang-style tuples)

  * MyXQL does not support `:timeout` option which specifies default timeout for queries, pass
    explicit `:timeout` when invoking `MyXQL.query`, `MyXQL.execute` etc instead

  * MyXQL does not support `:encoder`, `:decoder`, `sync_connect`, `formatter`, `:parameters`, and `:insecure_auth` options

Queries:

  * Mariaex.query/4 function uses `query_type: nil` option to try executing binary query and if
    that fails, fallback to using a text query. MyXQL uses `query_type: :binary_then_text` for
    that instead.

    Mariaex defaults to `query_type: nil`, MyXQL defaults to `query_type: :binary`.

  * MyXQL represents bit type `B'101'` as `<<1::1, 0::0, 1::1>>` (`<<5::size(3)>>`), Mariaex represents it as `<<5>>`

  * MyXQL does not support `:type_names`, `result_types`, `:decode`, `:encode_mapper`,
    `:decode_mapper`, `:include_table_name`, and `:binary_as` options

  * Mariaex has incomplete support for executing queries with multiple statements (e.g. `SELECT 1; SELECT 2`); MyXQL does not support them at all and will return an SQL error

Error struct:

  * `MyXQL.Error` struct contains `:mysql` field for MySQL errors (e.g.: `%{mysql: %{code: 1062, name: :ER_DUP_ENTRY}}`)

    Mariaex.Error contains a similar field called `:mariadb` with `:code` and `:message` fields

  * `MyXQL.Error` does not have `:tag`, `:action`, and `:reason` fields
