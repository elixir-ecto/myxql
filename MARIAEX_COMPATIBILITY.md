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

  * MyXQL does not support `:charset` option, UTF-8 (`utf8_general_ci`) is always used instead

  * MyXQL does not support `:encoder`, `:decoder`, `sync_connect`, `formatter`, `:parameters`, and `:insecure_auth` options

Queries:

  * Mariaex.query/4 function defaults to using binary protocol (prepared statements) and if that fails (some statements are not preparable), it falls back to the text protocol.

    MyXQL.query/4, on the other hand, uses the text protocol on empty params list, and otherwise it uses the binary protocol.

    Note: MyXQL behaviour is subject to change and for compatibility reasons we may follow Mariaex
    suite.

  * MyXQL does not support `BIT`, `ENUM`, `SET` and geometry types

  * MyXQL does not support `:type_names`, `result_types`, `:decode`, `:encode_mapper`,
    `:decode_mapper`, `:include_table_name`, and `:binary_as` options

  * Mariaex has incomplete support for executing queries with multiple statements (e.g. `SELECT 1; SELECT 2`); MyXQL does not support them at all and will return an SQL error

Error struct:

  * MyXQL.Error struct contains `:mysql` field for MySQL errors (e.g.: `%{mysql: %{code: 1062, name: :ER_DUP_ENTRY}}`) and `:socket` field for socket errors (e.g.: `%{socket: :nxdomain}`) which should make it easier for users to understand and handle the errors.
    Mariaex.Error constains a similar field called `:mariadb` with `:code` and `:message` fields

  * MyXQL.Error does not have `:tag`, `:action`, and `:reason` fields
