# MyXQL

[![Build Status](https://travis-ci.org/elixir-ecto/myxql.svg?branch=master)](https://travis-ci.org/elixir-ecto/myxql)

MySQL driver for Elixir.

Documentation: <http://hexdocs.pm/myxql>

## Features

  * Automatic decoding and encoding of Elixir values to and from MySQL text and binary protocols
  * Supports transactions, prepared queries, streaming, pooling and more via [DBConnection](https://github.com/elixir-ecto/db_connection)
  * Supports MySQL 5.5+, 8.0, and MariaDB 10.3
  * Supports `mysql_native_password`, `sha256_password` (\*), and `caching_sha2_password` (\*)
    authentication plugins

\* These authentication methods require either an SSL connection or the server must exchange its public key during handshake.

## Usage

Add `:myxql` to your dependencies:

```elixir
def deps() do
  [
    {:myxql, "~> 0.3.0"}
  ]
end
```

Make sure you are using the latest version!

```elixir
iex> {:ok, pid} = MyXQL.start_link(username: "root")
iex> MyXQL.query!(pid, "CREATE DATABASE IF NOT EXISTS blog")

iex> {:ok, pid} = MyXQL.start_link(username: "root", database: "blog")
iex> MyXQL.query!(pid, "CREATE TABLE posts IF NOT EXISTS (id serial primary key, title text)")

iex> MyXQL.query!(pid, "INSERT INTO posts (`title`) VALUES ('Post 1')")
%MyXQL.Result{columns: nil, connection_id: 11204,, last_insert_id: 1, num_rows: 1, num_warnings: 0, rows: nil}

iex> MyXQL.query(pid, "INSERT INTO posts (`title`) VALUES (?), (?)", ["Post 2", "Post 3"])
%MyXQL.Result{columns: nil, connection_id: 11204, last_insert_id: 2, num_rows: 2, num_warnings: 0, rows: nil}

iex> MyXQL.query(pid, "SELECT * FROM posts")
{:ok,
 %MyXQL.Result{
   columns: ["id", "title"],
   connection_id: 11204,
   last_insert_id: nil,
   num_rows: 3,
   num_warnings: 0,
   rows: [[1, "Post 1"], [2, "Post 2"], [3, "Post 3"]]
 }}
```

It's recommended to start MyXQL under supervision tree:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {MyXQL, username: "root", name: :myxql}
    ]

    Supervisor.start_link(children, opts)
  end
end
```

and then we can refer to it by its `:name`:

```elixir
iex> MyXQL.query!(:myxql, "SELECT NOW()").rows
[[~N[2018-12-28 13:42:31]]]
```

## Mariaex Compatibility

See [Mariaex Compatibility](https://github.com/elixir-ecto/myxql/blob/master/MARIAEX_COMPATIBILITY.md) page for transition between drivers.

## Data representation

```
MySQL                Elixir
-----                ------
NULL                 nil
bool                 1 | 0
int                  42
float                42.0
decimal              #Decimal<42.0> *
date                 ~D[2013-10-12] **
time                 ~T[00:37:14] ***
datetime             ~N[2013-10-12 00:37:14] ***, ****
timestamp            ~U[2013-10-12 00:37:14Z] ****
json                 %{"foo" => "bar"} *****
char                 "é"
text                 "myxql"
binary               <<1, 2, 3>>
bit                  <<1::size(1), 0::size(1)>>
point, polygon, ...  %Geo.Point{coordinates: {0.0, 1.0}}, ... ******
```

\* See [Decimal](https://github.com/ericmj/decimal)

\*\* When using SQL mode that allows them, MySQL "zero" dates and datetimes are represented as `:zero_date` and `:zero_datetime` respectively.

\*\*\* Negative or >= 24:00:00 values are not supported

\*\*\*\* Datetime fields are represented as `NaiveDateTime`, however a UTC `DateTime` can be used for encoding as well

\*\*\*\*\* MySQL added a native JSON type in version 5.7.8, if you're using earlier versions,
remember to use TEXT column for your JSON field.

\*\*\*\*\*\* See "Geometry support" section below

## JSON support

MyXQL comes with JSON support via the [Jason](https://github.com/michalmuskala/jason) library.

To use it, add `:jason` to your dependencies:

```elixir
{:jason, "~> 1.0"}
```

You can customize it to use another library via the `:json_library` configuration:

```elixir
config :myxql, :json_library, SomeJSONModule
```

## Geometry support

MyXQL comes with Geometry types support via the [Geo](https://github.com/bryanjos/geo) package.

To use it, add `:geo` to your dependencies:

```elixir
{:geo, "~> 3.3"}
```

Note, some structs like `%Geo.PointZ{}` does not have equivalent on the MySQL server side and thus
shouldn't be used.

If you're using MyXQL geometry types with Ecto and need to for example accept a WKT format as user
input, consider implementing an [custom Ecto type](https://hexdocs.pm/ecto/Ecto.Type.html).

## Contributing

Run tests:

```
git clone git@github.com:elixir-ecto/myxql.git
cd myxql
mix deps.get
mix test
```

See [`scripts/ci.sh`](scripts/ci.sh) and [`scripts/test-versions.sh`](scripts/test-versions.sh) for scripts used to test against different server versions.

## License

The source code is under Apache License 2.0.

Copyright (c) 2018 Plataformatec \
Copyright (c) 2020 Dashbit

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
