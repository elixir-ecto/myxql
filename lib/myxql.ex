defmodule MyXQL do
  @moduledoc """
  MySQL driver for Elixir.
  """

  @type conn() :: DBConnection.conn()

  @type start_option() ::
          {:protocol, :socket | :tcp}
          | {:socket, Path.t()}
          | {:socket_options, [:gen_tcp.connect_option()]}
          | {:hostname, String.t()}
          | {:port, :inet.port_number()}
          | {:database, String.t() | nil}
          | {:username, String.t()}
          | {:password, String.t() | nil}
          | {:charset, String.t() | nil}
          | {:collation, String.t() | nil}
          | {:ssl, boolean | [:ssl.tls_client_option()]}
          | {:connect_timeout, timeout()}
          | {:handshake_timeout, timeout()}
          | {:ping_timeout, timeout()}
          | {:prepare, :force_named | :named | :unnamed}
          | {:disconnect_on_error_codes, [atom()]}
          | {:enable_cleartext_plugin, boolean()}
          | DBConnection.start_option()

  @type option() :: DBConnection.option()

  @type query_option() ::
          option()
          | {:query_type, :binary | :binary_then_text | :text}
          | {:cache_statement, iodata()}

  @type stream_option() :: option() | {:max_rows, pos_integer()}

  @doc """
  Starts the connection process and connects to a MySQL server.

  ## Options

    * `:protocol` - Set to `:socket` for using UNIX domain socket, or `:tcp` for TCP
      (default: `:socket`)

      Connecting using UNIX domain socket is the preferred method. If `:hostname` or `:port` is
      set, protocol defaults to `:tcp` unless `:socket` is set too.

    * `:socket` - Connect to MySQL via UNIX domain socket in the given path
      (default: `MYSQL_UNIX_PORT` env variable, then `"/tmp/mysql.sock"`)

    * `:socket_options` - Options to be given to the underlying socket, applies to both TCP and
      UNIX sockets. See `:gen_tcp.connect/3` for more information.  (default: `[]`)

    * `:hostname` - Server hostname (default: `MYSQL_HOST` env variable, then `"localhost"`)

    * `:port` - Server port (default: `MYSQL_TCP_PORT` env variable, then `3306`)

    * `:database` - Database (default: `nil`)

    * `:username` - Username (default: `USER` env variable)

    * `:password` - Password (default: `MYSQL_PWD` env variable, then `nil`)

    * `:charset` - A connection charset. On connection handshake, the charset is set to `utf8mb4`,
      but if this option is set, an additional `SET NAMES <charset> [COLLATE <collation>]` query
      will be executed after establishing the connection. `COLLATE` will be added if `:collation`
      is set. (default: `nil`)

    * `:collation` - A connection collation. Must be given with `:charset` option, and if set
      it overwrites the default collation for the given charset. (default: `nil`)

    * `:ssl` - Enables SSL. Setting it to `true` enables SSL without server certificate verification,
      which emits a warning. Instead, prefer to set it to a keyword list, with either
      `:cacerts` or `:cacertfile` set to a CA trust store, to enable server certificate
      verification. (default: `false`)

    * `:connect_timeout` - Socket connect timeout in milliseconds (default:
      `15_000`)

    * `:handshake_timeout` - Connection handshake timeout in milliseconds (default: `15_000`)

    * `:ping_timeout` - Socket receive timeout when idle in milliseconds (default:
      `15_000`). See `c:DBConnection.ping/1` for more information

    * `:prepare` - How to cache prepared queries. Queries can be named or unnamed. Named
      queries are cached, unnamed queries are never cache by default. The possible values
      for this option are:

        * `:named` - cache only named queries
        * `:unnamed` - treat all queries as unnamed (i.e. nothing is ever cached)
        * `:force_named` - treat all queries as named (i.e. everything is cached)

      Note that MySQL has a global limit on the number of prepared queries. So if you
      enable `:force_named` in production, you may cache more queries than allowed by
      MySQL, leading to disconnections and user errors. Use `:force_named` only in a
      controlled environment, such as `:test`, and in `:prod` only if you are monitoring
      the prepare statement count of your databases (such as using a dashboard or
      setting alarm handlers)

    * `:disconnect_on_error_codes` - List of error code integers or atoms that when encountered
      will disconnect the connection. See "Disconnecting on Errors" section below for more
      information.

    * `:enable_cleartext_plugin` - Set to `true` to send password as cleartext (default: `false`)

   The given options are passed down to DBConnection, some of the most commonly used ones are
   documented below:

    * `:after_connect` - A function to run after the connection has been established, either a
      1-arity fun, a `{module, function, args}` tuple, or `nil` (default: `nil`)

    * `:pool` - The pool module to use, defaults to built-in pool provided by DBconnection

    * `:pool_size` - The size of the pool

  See `DBConnection.start_link/2` for more information and a full list of available options.

  ## Examples

  Start connection using the default configuration (UNIX domain socket):

      iex> {:ok, pid} = MyXQL.start_link([])
      {:ok, #PID<0.69.0>}

  Start connection over TCP:

      iex> {:ok, pid} = MyXQL.start_link(protocol: :tcp)
      {:ok, #PID<0.69.0>}

  Start connection with SSL using CA certificate file:

      iex> {:ok, pid} = MyXQL.start_link(ssl: [cacertfile: System.fetch_env!("DB_CA_CERT_FILE")])
      {:ok, #PID<0.69.0>}

  Run a query after connection has been established:

      iex> {:ok, pid} = MyXQL.start_link(after_connect: &MyXQL.query!(&1, "SET time_zone = '+00:00'"))
      {:ok, #PID<0.69.0>}

  ## Disconnecting on errors

  Sometimes the connection becomes unusable. For example, services such as AWS Aurora support
  failover which means the database you are currently connected to may suddenly become
  read-only. An attempt to do any write operation, such as INSERT/UPDATE/DELETE will lead to
  errors such as:

      ** (MyXQL.Error) (1792) (ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION) Cannot execute statement in a READ ONLY transaction.

  Luckily, you can instruct MyXQL to disconnect in such cases by using the following configuration:

      disconnect_on_error_codes: [:ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION]

  This cause the connection process to attempt to reconnect according to the backoff configuration.

  MyXQL automatically disconnects the connection on the following error codes and they don't have
  to be configured:

    * `:ER_MAX_PREPARED_STMT_COUNT_REACHED`

  You can pass error codes as integers too:

      disconnect_on_error_codes: [1792]

  ## Error codes

  MyXQL maintains a mapping of integers/atoms for commonly used errors. You can add additional
  ones by adding the following compile-time configuration:

      config :myxql, :extra_error_codes, [
        {1048, :ER_BAD_NULL_ERROR}
      ]

  After adding the configuration, MyXQL needs to be recompiled. It can be done with:

      $ mix deps.clean myxql --build

  To convert error code integers to names you can use `perror` command-line utility that ships
  with MySQL client installation, e.g.:

      bash$ perror 1792
      MySQL error code 1792 (ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION): Cannot execute statement in a READ ONLY transaction.

  """
  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(options) do
    options = ensure_valid_error_codes!(options)
    DBConnection.start_link(MyXQL.Connection, options)
  end

  defmacrop is_iodata(data) do
    quote do
      is_list(unquote(data)) or is_binary(unquote(data))
    end
  end

  @doc """
  Runs a query that returns a single result.

  ## Text queries and prepared statements

  MyXQL supports MySQL's two ways of executing queries:

    * text protocol - queries are sent as text

    * binary protocol - used by prepared statements

      The query statement is still sent as text, however it may contain placeholders for parameter
      values.

      Prepared statements have following benefits:

        * better performance: less overhead when parsing the query by the DB engine
        * better performance: binary protocol for encoding parameters and decoding result sets is more efficient
        * protection against SQL injection attacks

      The drawbacks of prepared statements are:

        * not all statements are preparable
        * requires two roundtrips to the DB server: one for preparing the statement and one for executing it.
          This can be alleviated by holding on to prepared statement and executing it multiple times.

  ## Options

    * `:query_type` - use `:binary` for binary protocol (prepared statements), `:binary_then_text` to attempt
      executing a binary query and if that fails fallback to executing a text query, and `:text` for text protocol
      (default: `:binary`)

    * `:cache_statement` - caches the query with the given name. Opposite to the `name` option
      given to `prepare/4`, if the cache statement name is reused with a different, the previous
      query is automatically closed

  Options are passed to `DBConnection.execute/4` for text protocol, and
  `DBConnection.prepare_execute/4` for binary protocol. See their documentation for all available
  options.

  ## Examples

      iex> MyXQL.query(conn, "CREATE TABLE posts (id serial, title text)")
      {:ok, %MyXQL.Result{}}

      iex> MyXQL.query(conn, "INSERT INTO posts (title) VALUES ('title 1')")
      {:ok, %MyXQL.Result{last_insert_id: 1, num_rows: 1}}

      iex> MyXQL.query(conn, "INSERT INTO posts (title) VALUES (?)", ["title 2"])
      {:ok, %MyXQL.Result{last_insert_id: 2, num_rows: 1}}

  """
  @spec query(conn, iodata, list, [query_option()]) ::
          {:ok, MyXQL.Result.t()} | {:error, Exception.t()}
  def query(conn, statement, params \\ [], options \\ []) when is_iodata(statement) do
    name = options[:cache_statement]
    query_type = options[:query_type] || :binary

    cond do
      name != nil ->
        statement = IO.iodata_to_binary(statement)
        query = %MyXQL.Query{name: name, statement: statement, cache: :statement}
        do_query(conn, query, params, options)

      query_type in [:binary, :binary_then_text] ->
        query = %MyXQL.Query{name: "", statement: statement}
        do_query(conn, query, params, options)

      query_type == :text ->
        query = %MyXQL.TextQuery{statement: statement}
        do_query(conn, query, params, options)
    end
  end

  @doc """
  Runs a query that returns a single result.

  Returns `%MyXQL.Result{}` on success, or raises an exception if there was an error.

  See `query/4`.
  """
  @spec query!(conn, iodata, list, [query_option()]) :: MyXQL.Result.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Runs a query that returns multiple results.

  A query may return multiple results if it is a text
  query with statements separated by semicolons or a stored
  procedure. Any prepared statement that is not a stored
  procedure is not allowed to return multiple results and will
  return an error.

  For more information on text queries and prepared statements,
  see `query/4`.

  ## Options

    * `:query_type` - Use `:binary` for binary protocol (prepared statements), `:binary_then_text` to attempt
      executing a binary query and if that fails fallback to executing a text query, and `:text` for text protocol
      (default: `:binary`).

    * `:cache_statement` - Caches the query with the given name. If the cache statement
      name is reused with a different statement, the previous query is automatically closed.

  Options are passed to `DBConnection.execute/4` for text protocol, and
  `DBConnection.prepare_execute/4` for binary protocol. See their documentation for all available
  options.

  ## Examples

      iex> MyXQL.query_many(conn, "SELECT 1; SELECT 2;", [], query_type: :text)
      {:ok, [%MyXQL.Result{rows: [[1]]}, %MyXQL.Result{rows: [[2]]}]}

  """
  @spec query_many(conn, iodata, list, [query_option()]) ::
          {:ok, [MyXQL.Result.t()]} | {:error, Exception.t()}
  def query_many(conn, statement, params \\ [], options \\ []) when is_iodata(statement) do
    name = options[:cache_statement]
    query_type = options[:query_type] || :binary

    cond do
      name != nil ->
        statement = IO.iodata_to_binary(statement)
        query = %MyXQL.Queries{name: name, statement: statement, cache: :statement}
        do_query(conn, query, params, options)

      query_type in [:binary, :binary_then_text] ->
        query = %MyXQL.Queries{name: "", statement: statement}
        do_query(conn, query, params, options)

      query_type == :text ->
        query = %MyXQL.TextQueries{statement: statement}
        do_query(conn, query, params, options)
    end
  end

  @doc """
  Runs a query that returns multiple results.

  Returns `[%MyXQL.Result{}]` on success, or raises an exception if there was an error.

  See `query_many/4`.
  """
  @spec query_many!(conn, iodata, list, [query_option()]) :: [MyXQL.Result.t()]
  def query_many!(conn, statement, params \\ [], opts \\ []) do
    case query_many(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  defp do_query(conn, %MyXQL.Query{} = query, params, options),
    do: DBConnection.prepare_execute(conn, query, params, options) |> query_result()

  defp do_query(conn, %MyXQL.TextQuery{} = query, params, options),
    do: DBConnection.execute(conn, query, params, options) |> query_result()

  defp do_query(conn, %MyXQL.Queries{} = query, params, options),
    do: DBConnection.prepare_execute(conn, query, params, options) |> query_result()

  defp do_query(conn, %MyXQL.TextQueries{} = query, params, options),
    do: DBConnection.execute(conn, query, params, options) |> query_result()

  defp query_result({:ok, _query, result}), do: {:ok, result}
  defp query_result({:error, _} = error), do: error

  @doc """
  Prepares a query that returns a single result to be later executed.

  To execute the query, call `execute/4`. To close the query, call `close/3`.
  If a name is given, the name must be unique per query, as the name is cached
  but the statement isn't. If a new statement is given to an old name, the old
  statement will be the one effectively used.

  ## Options

  Options are passed to `DBConnection.prepare/3`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQL.prepare(conn, "", "SELECT ? * ?")
      iex> {:ok, %MyXQL.Result{rows: [row]}} = MyXQL.execute(conn, query, [2, 3])
      iex> row
      [6]

  """
  @spec prepare(conn(), iodata(), iodata(), [option()]) ::
          {:ok, MyXQL.Query.t()} | {:error, Exception.t()}
  def prepare(conn, name, statement, opts \\ []) when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Query{name: name, statement: statement}
    DBConnection.prepare(conn, query, opts)
  end

  @doc """
  Prepares a query that returns a single result.

  Returns `%MyXQL.Query{}` on success, or raises an exception if there was an error.

  See `prepare/4`.
  """
  @spec prepare!(conn(), iodata(), iodata(), [option()]) :: MyXQL.Query.t()
  def prepare!(conn, name, statement, opts \\ []) when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Query{name: name, statement: statement}
    DBConnection.prepare!(conn, query, opts)
  end

  @doc """
  Prepares a query that returns multiple results to be later executed.

  A prepared statement may return multiple results if it is a stored procedure.
  Any other type of prepared statement is not allowed to return multiple results
  and will return an error.

  To execute the query, call `execute_many/4`. To close the query, call `close/3`.
  If a name is given, the name must be unique per query, as the name is cached.
  If a new statement uses an old name, the old statement will be closed.

  ## Options

  Options are passed to `DBConnection.prepare/3`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQL.prepare_many(conn, "", "CALL multi_procedure()")
      iex> {:ok, [%MyXQL.Result{rows: [row1]}, %MyXQL.Result{rows: [row2]}]} = MyXQL.execute_many(conn, query, [2, 3])
      iex> row1
      [2]
      iex> row2
      [3]

  """
  @spec prepare_many(conn(), iodata(), iodata(), [option()]) ::
          {:ok, MyXQL.Queries.t()} | {:error, Exception.t()}
  def prepare_many(conn, name, statement, opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Queries{name: name, statement: statement}
    DBConnection.prepare(conn, query, opts)
  end

  @doc """
  Prepares a query that returns multiple results.

  Returns `%MyXQL.Queries{}` on success, or raises an exception if there was an error.

  See `prepare_many/4`.
  """
  @spec prepare_many!(conn(), iodata(), iodata(), [option()]) :: MyXQL.Queries.t()
  def prepare_many!(conn, name, statement, opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Queries{name: name, statement: statement}
    DBConnection.prepare!(conn, query, opts)
  end

  @doc """
  Prepares and executes a query that returns a single result, in a single step.

  ## Options

  Options are passed to `DBConnection.prepare_execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, _query, %MyXQL.Result{rows: [row]}} = MyXQL.prepare_execute(conn, "", "SELECT ? * ?", [2, 3])
      iex> row
      [6]

  """
  @spec prepare_execute(conn, iodata, iodata, list, keyword()) ::
          {:ok, MyXQL.Query.t(), MyXQL.Result.t()} | {:error, Exception.t()}
  def prepare_execute(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Query{name: name, statement: statement}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  @doc """
  Prepares and executes a query that returns a single result, in a single step.

  Returns `{%MyXQL.Query{}, %MyXQL.Result{}}` on success, or raises an exception if there was
  an error.

  See: `prepare_execute/5`.
  """
  @spec prepare_execute!(conn, iodata, iodata, list, [option()]) ::
          {MyXQL.Query.t(), MyXQL.Result.t()}
  def prepare_execute!(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Query{name: name, statement: statement}
    DBConnection.prepare_execute!(conn, query, params, opts)
  end

  @doc """
  Prepares and executes a query that returns multiple results, in a single step.

  A prepared statement may return multiple results if it is a stored procedure.
  Any other type of prepared statement is not allowed to return multiple results
  and will return an error.

  ## Options

  Options are passed to `DBConnection.prepare_execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, _, [%MyXQL.Result{rows: [row1]}, %MyXQL.Result{rows: [row2]}]} = MyXQL.prepare_execute(conn, "", "CALL multi_procedure()")
      iex> row1
      [2]
      iex> row2
      [3]

  """
  @spec prepare_execute_many(conn, iodata, iodata, list, keyword()) ::
          {:ok, MyXQL.Queries.t(), [MyXQL.Result.t()]} | {:error, Exception.t()}
  def prepare_execute_many(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Queries{name: name, statement: statement}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  @doc """
  Prepares and executes a query that returns multiple results, in a single step.

  Returns `{%MyXQL.Queries{}, [%MyXQL.Result{}]}` on success, or raises an exception if there was
  an error.

  See: `prepare_execute_many/5`.
  """
  @spec prepare_execute_many!(conn, iodata, iodata, list, [option()]) ::
          {MyXQL.Queries.t(), [MyXQL.Result.t()]}
  def prepare_execute_many!(conn, name, statement, params \\ [], opts \\ [])
      when is_iodata(name) and is_iodata(statement) do
    query = %MyXQL.Queries{name: name, statement: statement}
    DBConnection.prepare_execute!(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query that returns a single result.

  ## Options

  Options are passed to `DBConnection.execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQL.prepare(conn, "", "SELECT ? * ?")
      iex> {:ok, %MyXQL.Result{rows: [row]}} = MyXQL.execute(conn, query, [2, 3])
      iex> row
      [6]

  """
  @spec execute(conn(), MyXQL.Query.t(), list(), [option()]) ::
          {:ok, MyXQL.Query.t(), MyXQL.Result.t()} | {:error, Exception.t()}
  def execute(conn, %MyXQL.Query{} = query, params \\ [], opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query that returns a single result.

  Returns `%MyXQL.Result{}` on success, or raises an exception if there was an error.

  See: `execute/4`.
  """
  @spec execute!(conn(), MyXQL.Query.t(), list(), keyword()) :: MyXQL.Result.t()
  def execute!(conn, %MyXQL.Query{} = query, params \\ [], opts \\ []) do
    DBConnection.execute!(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query that returns multiple results.

  ## Options

  Options are passed to `DBConnection.execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQL.prepare_many(conn, "", "CALL multi_procedure()")
      iex> {:ok, [%MyXQL.Result{rows: [row1]}, %MyXQL.Result{rows: [row2]}]} = MyXQL.execute_many(conn, query)
      iex> row1
      [2]
      iex> row2
      [3]

  """
  @spec execute_many(conn(), MyXQL.Queries.t(), list(), [option()]) ::
          {:ok, MyXQL.Queries.t(), [MyXQL.Result.t()]} | {:error, Exception.t()}
  def execute_many(conn, %MyXQL.Queries{} = query, params \\ [], opts \\ []) do
    DBConnection.execute(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query that returns multiple results.

  Returns `[%MyXQL.Result{}]` on success, or raises an exception if there was an error.

  See: `execute_many/4`.
  """
  @spec execute_many!(conn(), MyXQL.Queries.t(), list(), keyword()) :: [MyXQL.Result.t()]
  def execute_many!(conn, %MyXQL.Queries{} = query, params \\ [], opts \\ []) do
    DBConnection.execute!(conn, query, params, opts)
  end

  @doc """
  Closes a prepared query.

  Returns `:ok` on success, or raises an exception if there was an error.

  ## Options

  Options are passed to `DBConnection.close/3`, see it's documentation for
  all available options.
  """
  @spec close(conn(), MyXQL.Query.t() | MyXQL.Queries.t(), [option()]) :: :ok
  def close(conn, query, opts \\ [])

  def close(conn, %MyXQL.Query{} = query, opts) do
    case DBConnection.close(conn, query, opts) do
      {:ok, _} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  def close(conn, %MyXQL.Queries{} = query, opts) do
    case DBConnection.close(conn, query, opts) do
      {:ok, _} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Acquire a lock on a connection and run a series of requests inside a
  transaction. The result of the transaction fun is return inside an `:ok`
  tuple: `{:ok, result}`.

  To use the locked connection call the request with the connection
  reference passed as the single argument to the `fun`. If the
  connection disconnects all future calls using that connection
  reference will fail.

  `rollback/2` rolls back the transaction and causes the function to
  return `{:error, reason}`.

  `transaction/3` can be nested multiple times if the connection
  reference is used to start a nested transaction. The top level
  transaction function is the actual transaction.

  ## Options

  Options are passed to `DBConnection.transaction/3`, see it's documentation for
  all available options.

  ## Examples

      {:ok, result} =
        MyXQL.transaction(pid, fn conn  ->
          MyXQL.query!(conn, "SELECT title FROM posts")
        end)

  """
  @spec transaction(conn, (DBConnection.t() -> result), [option()]) ::
          {:ok, result} | {:error, any}
        when result: var
  defdelegate transaction(conn, fun, opts \\ []), to: DBConnection

  @doc """
  Rollback a transaction, does not return.

  Aborts the current transaction. If inside multiple `transaction/3`
  functions, bubbles up to the top level.

  ## Example

      {:error, :oops} =
        MyXQL.transaction(pid, fn conn  ->
          MyXQL.rollback(conn, :oops)
          IO.puts "never reaches here!"
        end)

  """
  @spec rollback(DBConnection.t(), any()) :: no_return()
  defdelegate rollback(conn, reason), to: DBConnection

  @doc """
  Returns a stream for a query on a connection.

  Stream consumes memory in chunks of at most `max_rows` rows (see Options).
  This is useful for processing _large_ datasets.

  A stream must be wrapped in a transaction and may be used as an `Enumerable`.

  ## Options

    * `:max_rows` - Maximum numbers of rows in a result (default: `500`)

  Options are passed to `DBConnection.stream/4`, see it's documentation for
  other available options.

  ## Examples

      {:ok, results} =
        MyXQL.transaction(pid, fn conn ->
          stream = MyXQL.stream(conn, "SELECT * FROM integers", [], max_rows: max_rows)
          Enum.to_list(stream)
        end)

  Suppose the `integers` table contains rows: 1, 2, 3, 4 and `max_rows` is set to `2`.
  We'll get following results:

      # The first item is result of executing the query and has no rows data
      Enum.at(results, 0)
      #=> %MyXQL.Result{num_rows: 0, ...}

      # The second item is result of fetching rows 1 & 2
      Enum.at(results, 1)
      #=> %MyXQL.Result{num_rows: 2, rows: [[1], [2]]}

      # The third item is result of fetching rows 3 & 4
      Enum.at(results, 2)
      #=> %MyXQL.Result{num_rows: 2, rows: [[3], [4]]}

  Because the total number of fetched rows happens to be divisible by our chosen `max_rows`,
  there might be more data on the server so another fetch attempt is made.
  Because in this case there weren't any more rows, the final result has 0 rows:

      Enum.at(results, 3)
      #=> %MyXQL.Result{num_rows: 0}

  However, if the table contained only 3 rows, the 3rd result would contain:

      Enum.at(results, 2)
      #=> %MyXQL.Result{num_rows: 1, rows: [[3]]}

  And that would be the last result in the stream.
  """
  @spec stream(DBConnection.t(), iodata | MyXQL.Query.t(), list, [stream_option()]) ::
          DBConnection.PrepareStream.t()
  def stream(conn, query, params \\ [], opts \\ [])

  def stream(%DBConnection{} = conn, statement, params, opts) when is_iodata(statement) do
    query = %MyXQL.Query{name: "", statement: statement, num_params: length(params)}
    opts = Keyword.put_new(opts, :max_rows, 500)
    DBConnection.prepare_stream(conn, query, params, opts)
  end

  def stream(%DBConnection{} = conn, %MyXQL.Query{} = query, params, opts) do
    opts = Keyword.put_new(opts, :max_rows, 500)
    DBConnection.stream(conn, query, params, opts)
  end

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  @spec child_spec([start_option()]) :: :supervisor.child_spec()
  def child_spec(options) do
    options = ensure_valid_error_codes!(options)
    DBConnection.child_spec(MyXQL.Connection, options)
  end

  @doc """
  Returns the configured JSON library.

  To customize the JSON library, include the following in your `config/config.exs`:

      config :myxql, json_library: SomeJSONModule

  Defaults to `Jason`.
  """
  @spec json_library() :: module()
  def json_library() do
    Application.fetch_env!(:myxql, :json_library)
  end

  ## Helpers

  defp ensure_valid_error_codes!(opts) do
    default_error_codes = [
      :ER_MAX_PREPARED_STMT_COUNT_REACHED
    ]

    codes = default_error_codes ++ Keyword.get(opts, :disconnect_on_error_codes, [])

    codes =
      for code <- codes do
        if is_integer(code) do
          code
        else
          integer = MyXQL.Protocol.ServerErrorCodes.name_to_code(code)

          unless integer do
            raise """
            #{inspect(code)} is not a recognized error code

            To solve this, you can either:

              - pass an integer error code

              - make it recognizable by adding it to `config :myxql, extra_error_codes: [...]`
                (See "Error codes" in MyXQL.start_link/1 documentation for more information).
            """
          end

          integer
        end
      end

    Keyword.put(opts, :disconnect_on_error_codes, codes)
  end
end
