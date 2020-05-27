# Changelog

## v0.4.1 (2020-05-27)

Bug fixes:

* Handle socket errors when closing statements
* Delete cursor ref on deallocate

## v0.4.0 (2020-03-30)

Enhancements:

* Support receiving packets larger than 16MB

Bug fixes:

* Require `:ssl` & `:public_key` applications

* Enforce prepare names to be unique

* Do not leak statements on multiple executions of the same name in `prepare_execute`

* Do not leak statements with rebound `:cache_statement`

## v0.3.4 (2020-03-19)

* Close statements after query errors

## v0.3.3 (2020-02-10)

* Fix bug when decoding invalid json path error

## v0.3.2 (2020-01-29)

* Improve error messages and docs on unsupported time values

## v0.3.1 (2019-11-28)

Bug fixes:

* Revert: Re-use repeated prepared statements in `:unnamed` mode

## v0.3.0 (2019-11-25)

### Enhancements

* Re-use repeated prepared statements in `:unnamed` mode

* Geometry types support

### Bug fixes

* Fix `mysql_native_authentication` on `auth_switch_request`

* Handle multiple packets on ping

## v0.2.10 (2019-10-29)

### Enhancements

* Add `prepare: :force_named` option

### Bug fixes

* Fix prepared queries leaks

## v0.2.9 (2019-09-18)

### Bug fixes

* Disconnect on handshake errors

## v0.2.8 (2019-09-14)

### Bug fixes

* Encode params in MyXQL.Query and gracefully handle encoding errors

## v0.2.7 (2019-09-11)

### Enhancements

* Decode "zero dates" as `:zero_date` and `:zero_datetime`. Note, this only works on specific
  server `sql_mode` settings, it's disabled by default in servers and in general not recommended.

* Support `MYSQL_HOST` and `MYSQL_PWD` env variables

* Add `:charset` and `:collation` options to `start_link/2`

## v0.2.6 (2019-07-04)

### Bug fixes

* Fix VARCHAR handling
* Fix TINY, MEDIUM, and LONG BLOBs handling
* Fix dialyzer errors

## v0.2.5 (2019-07-04)

### Bug fixes

* Fix encoding large packets
* Consider `""` as empty password
* Raise better error messages on "zero" dates

## v0.2.4 (2019-06-10)

### Enhancements

* Add statement cache
* Support BIT data type
* Encode packets larger than `max_packet_size`

### Bug fixes

* Fix selecting nulls in binary protocol
* Raise error when :ssl is required and not started in `child_spec/1`

## v0.2.3 (2019-05-23)

### Enhancements

* Default charset to utf8mb4
* Raise error when server does not support required capabilities
* Implement public key exchange for sha auth methods
* Support older MySQL versions (tested against 5.5 and 5.6)
* Change `MyXQL.start_option/0` to use `:ssl.tls_client_option/0` type

### Bug fixes

* Handle error packet on handshake

## v0.2.2 (2019-04-05)

### Bug fixes

* Fix documentation and typespec for the `:ssl_opts` option
* Hide undocumented `ref` field from the `MyXQL.Query.t` type
* Fix decoding MEDIUMBLOBs

## v0.2.1 (2019-03-27)

### Bug fixes

* Use `DBConnection.ConnectionError` for transport errors and disconnect the connection
* Remove `:socket` from `MyXQL.Error` as `DBConnection.ConnectionError` is used for that instead
* Improve error message on invalid socket path

## v0.2.0 (2019-03-18)

### Enhancements

* Add `:disconnect_on_error_codes` option to `MyXQL.start_link/1`
* Add `:ping_timeout` option to `MyXQL.start_link/1`
* Add `:handshake_timeout` option to `MyXQL.start_link/1`
* Add `:num_warnings` field to `MyXQL.Result`
* Add `:connection_id` field to `MyXQL.Error`
* Add `query_type: :binary | :binary_then_text | :text` option to `MyXQL.query/4`
* Improve handshake socket errors handling
* Raise error when both :username option and USER env are missing
* Add `MARIAEX_COMPATIBILITY.md` page

### Bug fixes

* Actually disconnect on `ER_MAX_PREPARED_STMT_COUNT_REACHED`
* Encode JSON values in binary protocol
* Gracefully error when server does not support `CLIENT_DEPRECATE_EOF`
* Use `:connect_timeout` option for connecting to SSL socket

### Backwards incompatible changes

* Use binary protocol by default in `MyXQL.query/4`
* Return an additional leading result in `MyXQL.stream/4`.
  The leading result is of executing the query but not yet fetching data.
* Remove `:ssl` from application list

## v0.1.1 (2019-01-24)

* Raise better error message when a parameter cannot be encoded
* Raise a better error when query to be executed is not prepared
* Raise error on multiple results and point to instead using `MyXQL.stream/4`
* Fix streaming inserts
* Fix transaction handling for savepoints
* Disconnect the connection on `ER_MAX_PREPARED_STMT_COUNT_REACHED` error
* Add MySQL error code and name to exception message
* Add `:prepare` connection option

## v0.1.0 (2019-01-22)

* Initial release
