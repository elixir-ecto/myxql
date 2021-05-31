defmodule InfileTest do
  use ExUnit.Case, async: true

  @opts TestHelper.opts()
  describe "infile" do
    setup [:connect, :truncate, :enable_infile]

    test "load data without specifically enabling gives error", c do
      assert_raise RuntimeError, fn ->
               MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile", [], query_type: :text )
      end
    end

    test "load data gives error if not allowed on server", c do
      MyXQL.query!(c.conn, "SET GLOBAL local_infile = 0", [], query_type: :text )

      r = MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile", [], query_type: :text, local_infile: "/tmp/foo" )
      assert {:error, %MyXQL.Error{mysql: %{code: 1148, name: :ER_NOT_ALLOWED_COMMAND}}} = r
    end

    test "load data works with utf8", c do
      s = stream_data("1,test: ½¾\n2,testing2\n" )

      r = MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile CHARACTER SET utf8mb4 FIELDS TERMINATED BY ','", [], query_type: :text, local_infile: s)

      assert {:ok, %MyXQL.Result{num_rows: 2, num_warnings: 0, rows: nil}} = r
      
      {:ok,r} = MyXQL.query(c.conn, "SELECT * FROM test_infile", [])
      assert [[1, "test: ½¾"], [2, "testing2"]]==r.rows
    end

    test "load data works with latin1", c do
      latin1_bytes = <<189,190 >> # fraction 1/2 then 3/4
      #s = stream_data("1,test: " <> latin1_bytes <> "\n2,testing2\n") # the stream here doesn't work for some reason...
      s = write_data_to_temp("1,test: " <> latin1_bytes <> "\n2,testing2\n")

      r = MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile CHARACTER SET latin1 FIELDS TERMINATED BY ','", [], query_type: :text, local_infile: s)

      assert {:ok, %MyXQL.Result{num_rows: 2, num_warnings: 0, rows: nil}} = r
      
      {:ok,r} = MyXQL.query(c.conn, "SELECT * FROM test_infile", [])
      assert [[1, "test: ½¾"], [2, "testing2"]]==r.rows

    end

    test "load data works when passed a filepath", c do
      tmp_filepath = write_data_to_temp("17,testing\n")

      r = MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile FIELDS TERMINATED BY ','", [], query_type: :text, local_infile: tmp_filepath)

      assert {:ok, %MyXQL.Result{num_rows: 1, num_warnings: 0, rows: nil}} = r
    end

    test "load data works when passed an Enumerable", c do
      s = stream_data("1,testing\n2,testing2\n")

      r = MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile FIELDS TERMINATED BY ','", [], query_type: :text, local_infile: s)

      assert {:ok, %MyXQL.Result{num_rows: 2, num_warnings: 0, rows: nil}} = r
    end

    test "load data respects configured max packet size", c do
      s = stream_data("1,testing\n2,testing2\n3,testing3\n4,testing4\n")

      MyXQL.query(c.conn, "set max_allowed_packet = 10", [], query_type: :text, local_infile: s)

      r = MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile FIELDS TERMINATED BY ','", [], query_type: :text, local_infile: s)

      assert {:ok, %MyXQL.Result{num_rows: 4, num_warnings: 0, rows: nil}} = r
    end

    test "load data can handle more than 256 packet files", c do
      lines = for i <- 1..10000, do: "#{i},testing\n"

      s = write_data_to_temp(lines)

      MyXQL.query(c.conn, "set max_allowed_packet = 10", [], query_type: :text, local_infile: s)

      r = MyXQL.query(c.conn, "LOAD DATA LOCAL INFILE 'foo' INTO TABLE test_infile FIELDS TERMINATED BY ','", [], query_type: :text, local_infile: s)

      assert {:ok, %MyXQL.Result{num_rows: 10000, num_warnings: 0, rows: nil}} = r
    end

  end

  defp write_data_to_temp(data) do
    dir = System.tmp_dir!()
    tmp_file = Path.join(dir, "testdata")
    File.write!(tmp_file, data)
    tmp_file
  end

  defp stream_data(contents) do
    Stream.resource(
      fn ->
        {:ok, s} = StringIO.open(contents)
        {s, :initial}
      end,
      fn
        {s,:eof} -> {:halt, s}
        {s, :initial} -> {[IO.read(s, :all)] , {s,:eof}}
      end,
        fn s -> StringIO.close(s) end
    )
  end

  defp connect(c) do
    {:ok, conn} = MyXQL.start_link(@opts)
    Map.put(c, :conn, conn)
  end

  defp enable_infile(c) do
    MyXQL.query!(c.conn, "SET GLOBAL local_infile = 1", [], query_type: :text )
    c
  end

  defp truncate(c) do
    MyXQL.query!(c.conn, "TRUNCATE TABLE test_infile")
    c
  end
end
