defmodule MyXQL do
  def connect(opts) do
    MyXQL.Protocol.connect(opts)
  end

  def query(conn, statement) do
    MyXQL.Protocol.query(conn, statement)
  end

  def query!(conn, statement) do
    case query(conn, statement) do
      {:ok, result} -> result
      {:error, exception} -> raise exception
    end
  end

  def prepare(conn, statement) do
    MyXQL.Protocol.prepare(conn, statement)
  end

  def execute(conn, statement_id, params \\ []) do
    MyXQL.Protocol.execute(conn, statement_id, params)
  end
end
