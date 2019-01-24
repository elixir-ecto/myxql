defmodule MyXQL do
  @moduledoc """
  MySQL driver for Elixir.
  """

  @type conn :: DBConnection.conn()

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

    * `:hostname` - Server hostname (default: `"localhost"`)

    * `:port` - Server port (default: `MYSQL_TCP_PORT` env variable, then `3306`)

    * `:database` - Database (default: `nil`)

    * `:username` - Username (default: `USER` env variable)

    * `:password` - Password (default: `nil`)

    * `:ssl` - Set to `true` if SSL should be used (default: `false`)

    * `:ssl_options` - A list of SSL options, see `:ssl.connect/2` (default: `[]`)

    * `:pool` - The pool module to use (default: `DBConnection.ConnectionPool`)

      See the pool documentation for more options. The default `:pool_size` for
      the default pool is `1`. If you set a different pool, this option must be
      included with all requests contacting the pool

    * `:connect_timeout` - Socket connect timeout in milliseconds (default:
      `15_000`)

  MyXQL uses the `DBConnection` library and supports all `DBConnection`
  options like `:pool_size`, `:after_connect` etc. See `DBConnection.start_link/2`
  for more information.

  ## Examples

  Start connection using the default configuration (UNIX domain socket):

      iex> {:ok, pid} = MyXQL.start_link([])
      {:ok, #PID<0.69.0>}

  Start connection over TCP:

      iex> {:ok, pid} = MyXQL.start_link(protocol: :tcp)
      {:ok, #PID<0.69.0>}

  Run a query after connection has been established:

      iex> {:ok, pid} = MyXQL.start_link(after_connect: &MyXQL.query!(&1, "SET time_zone = '+00:00'"))
      {:ok, #PID<0.69.0>}

  """
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, MyXQL.Error.t()}
  def start_link(opts) do
    ensure_deps_started!(opts)
    DBConnection.start_link(MyXQL.Protocol, opts)
  end

  @doc """
  Runs a query.

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

  The `query/4` function, when called with empty list of parameters uses the text protocol, otherwise uses the binary protocol.

  To force using binary protocol, use `prepare_execute/5`.

  ## Multiple results

  If a query returns multiple results (the query has multiple statements or is calling a procedure that returns multiple results)
  an error is raised. If a query may return multiple results it's recommended to use `stream/4` instead.

  ## Options

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
  @spec query(conn, iodata, list, keyword()) ::
          {:ok, MyXQL.Result.t()} | {:error, MyXQL.Error.t()}
  def query(conn, statement, params \\ [], opts \\ [])

  def query(conn, statement, [], opts) when is_binary(statement) or is_list(statement) do
    query = %MyXQL.TextQuery{statement: statement}

    DBConnection.execute(conn, query, [], opts)
    |> query_result()
  end

  def query(conn, statement, params, opts) when is_binary(statement) or is_list(statement) do
    prepare_execute(conn, "", statement, params, opts)
    |> query_result()
  end

  defp query_result({:ok, _query, result}), do: {:ok, result}
  defp query_result({:error, _} = error), do: error

  @doc """
  Runs a query.

  Returns `%MyXQL.Result{}` on success, or raises an exception if there was an error.

  See `query/4`.
  """
  @spec query!(conn, iodata, list, keyword()) :: MyXQL.Result.t()
  def query!(conn, statement, params \\ [], opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  @doc """
  Prepares a query to be later executed.

  To execute the query, call `execute/4`. To close the query, call `close/3`.

  ## Options

  Options are passed to `DBConnection.prepare/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQL.prepare(conn, "", "SELECT ? * ?")
      iex> {:ok, %MyXQL.Result{rows: [row]}} = MyXQL.execute(conn, query, [2, 3])
      iex> row
      [6]

  """
  @spec prepare(conn(), iodata(), iodata(), keyword()) ::
          {:ok, MyXQL.Query.t()} | {:error, MyXQL.Error.t()}
  def prepare(conn, name, statement, opts \\ []) do
    query = %MyXQL.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare(conn, query, opts)
  end

  @doc """
  Prepares a query.

  Returns `%MyXQL.Query{}` on success, or raises an exception if there was an error.

  See `prepare/4`.
  """
  @spec prepare!(conn(), iodata(), iodata(), keyword()) :: MyXQL.Query.t()
  def prepare!(conn, name, statement, opts \\ []) do
    query = %MyXQL.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare!(conn, query, opts)
  end

  @doc """
  Prepares and executes a query in a single step.

  ## Multiple results

  If a query returns multiple results (e.g. it's calling a procedure that returns multiple results)
  an error is raised. If a query may return multiple results it's recommended to use `stream/4` instead.

  ## Options

  Options are passed to `DBConnection.prepare_execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, _query, %MyXQL.Result{rows: [row]}} = MyXQL.prepare_execute(conn, "", "SELECT ? * ?", [2, 3])
      iex> row
      [6]

  """
  @spec prepare_execute(conn, iodata, iodata, list, keyword()) ::
          {:ok, MyXQL.Query.t(), MyXQL.Result.t()} | {:error, MyXQL.Error.t()}
  def prepare_execute(conn, name, statement, params \\ [], opts \\ [])
      when is_binary(statement) or is_list(statement) do
    query = %MyXQL.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  @doc """
  Prepares and executes a query in a single step.

  Returns `{%MyXQL.Query{}, %MyXQL.Result{}}` on success, or raises an exception if there was
  an error.

  See: `prepare_execute/5`.
  """
  @spec prepare_execute!(conn, iodata, iodata, list, keyword()) ::
          {MyXQL.Query.t(), MyXQL.Result.t()}
  def prepare_execute!(conn, name, statement, params \\ [], opts \\ [])
      when is_binary(statement) or is_list(statement) do
    query = %MyXQL.Query{name: name, statement: statement, ref: make_ref()}
    DBConnection.prepare_execute!(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query.

  ## Options

  Options are passed to `DBConnection.execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = MyXQL.prepare(conn, "", "SELECT ? * ?")
      iex> {:ok, %MyXQL.Result{rows: [row]}} = MyXQL.execute(conn, query, [2, 3])
      iex> row
      [6]

  """
  @spec execute(conn(), MyXQL.Query.t(), list(), keyword()) ::
          {:ok, MyXQL.Result.t()} | {:error, MyXQL.Error.t()}
  defdelegate execute(conn, query, params \\ [], opts \\ []), to: DBConnection

  @doc """
  Executes a prepared query.

  Returns `%MyXQL.Result{}` on success, or raises an exception if there was an error.

  See: `execute/4`.
  """
  @spec execute!(conn(), MyXQL.Query.t(), list(), keyword()) :: MyXQL.Result.t()
  defdelegate execute!(conn, query, params \\ [], opts \\ []), to: DBConnection

  @doc """
  Closes a prepared query.

  Returns `:ok` on success, or raises an exception if there was an error.

  ## Options

  Options are passed to `DBConnection.close/3`, see it's documentation for
  all available options.
  """
  @spec close(conn(), MyXQL.Query.t(), keyword()) :: :ok
  def close(conn, %MyXQL.Query{} = query, opts \\ []) do
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
  @spec transaction(conn, (DBConnection.t() -> result), keyword()) ::
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
          stream = MyXQL.stream(conn, "SELECT * FROM posts")
          Enum.to_list(stream)
        end)

  """
  @spec stream(DBConnection.t(), iodata | MyXQL.Query.t(), list, keyword()) ::
          DBConnection.PrepareStream.t()
  def stream(conn, query, params \\ [], opts \\ [])

  def stream(%DBConnection{} = conn, statement, params, opts)
      when is_binary(statement) or is_list(statement) do
    query = %MyXQL.Query{
      name: "",
      ref: make_ref(),
      statement: statement,
      num_params: length(params)
    }

    stream(conn, query, params, opts)
  end

  def stream(%DBConnection{} = conn, %MyXQL.Query{} = query, params, opts) do
    opts = Keyword.put_new(opts, :max_rows, 500)
    DBConnection.prepare_stream(conn, query, params, opts)
  end

  @doc """
  Returns a supervisor child specification for a DBConnection pool.
  """
  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    DBConnection.child_spec(MyXQL.Protocol, opts)
  end

  @doc """
  Returns the configured JSON library.

  To customize the JSON library, including the following
  in your `config/config.exs`:

      config :myxql, :json_library, SomeJSONModule

  Defaults to `Jason`.
  """
  @spec json_library() :: module()
  def json_library() do
    Application.get_env(:myxql, :json_library, Jason)
  end

  ## Helpers

  defp ensure_deps_started!(opts) do
    if Keyword.get(opts, :ssl, false) and
         not List.keymember?(:application.which_applications(), :ssl, 0) do
      raise """
      SSL connection cannot be established because `:ssl` application is not started,
      you can add it to `:extra_applications` in your `mix.exs`:

          def application() do
            [extra_applications: [:ssl]]
          end

      """
    end
  end
end
