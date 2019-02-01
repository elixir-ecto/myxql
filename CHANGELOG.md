# Changelog

## HEAD

* Actually disconnect on `ER_MAX_PREPARED_STMT_COUNT_REACHED`
* Add `:disconnect_on_error_codes` option
* Add `:ping_timeout` option
* Use `:connect_timeout` option for connecting to SSL socket
* Improve handshake socket errors handling

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
