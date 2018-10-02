defmodule MyXQL.TypesTest do
  use ExUnit.Case, async: true
  import MyXQL.Types
  import MyXQL.Messages
  use Bitwise

  test "length_encoded_integer" do
    assert decode_length_encoded_integer(<<100>>) == 100
    assert decode_length_encoded_integer(<<0xFC, 0, 252>>) == 252
    assert decode_length_encoded_integer(<<0xFD, 1, 0, 0>>) == 65536
    assert decode_length_encoded_integer(<<0xFE, 0, 0, 0, 0, 1, 0, 0, 0>>) == 16_777_216
  end

  test "length_encoded_string" do
    assert decode_length_encoded_string(<<3, "aaa">>) == "aaa"

    string = String.duplicate("a", 252)
    assert decode_length_encoded_string(<<0xFC, 0, 252, string::binary>>) == string

    assert take_length_encoded_string(<<3, "aaab">>) == {"aaa", "b"}
  end

  describe "end-to-end" do
    setup [:connect]

    test "MYSQL_TYPE_TINY", c do
      assert_roundtrip(c, "my_tinyint", -127)
      assert_roundtrip(c, "my_tinyint", 127)
    end

    test "MYSQL_TYPE_SHORT - SQL SMALLINT", c do
      assert_roundtrip(c, "my_smallint", -32767)
      assert_roundtrip(c, "my_smallint", 32767)
    end

    test "MYSQL_TYPE_LONG - SQL MEDIUMINT, SQL INT", c do
      assert_roundtrip(c, "my_mediumint", -8388608)
      assert_roundtrip(c, "my_mediumint", 8388607)

      assert_roundtrip(c, "my_int", -2147483647)
      assert_roundtrip(c, "my_int", 2147483647)
    end

    test "MYSQL_TYPE_LONGLONG - SQL BIGINT", c do
      assert_roundtrip(c, "my_bigint", -1 <<< 63)
      assert_roundtrip(c, "my_bigint", (1 <<< 63) - 1)
    end

    test "MYSQL_TYPE_FLOAT", c do
      assert_roundtrip(c, "my_float", -13.37)
      assert_roundtrip(c, "my_float", 13.37)
    end

    test "MYSQL_TYPE_DOUBLE", c do
      assert_roundtrip(c, "my_double", -13.37)
      assert_roundtrip(c, "my_double", 13.37)
    end

    test "MYSQL_TYPE_NEWDECIMAL - SQL DECIMAL", c do
      assert_roundtrip(c, "my_decimal", Decimal.new(-13))
      assert insert_and_get(c, "my_decimal", "-13.37") == Decimal.new(-13)

      assert_roundtrip(c, "my_decimal52", Decimal.new("-999.99"))
      assert_roundtrip(c, "my_decimal52", Decimal.new("999.99"))
    end

    # TODO:
    # test "MYSQL_TYPE_BIT" do
    #   assert_roundtrip(c, "my_bit2", 1)
    #   assert_roundtrip(c, "my_bit8", 1)
    # end

    test "MYSQL_TYPE_DATE", c do
      assert_roundtrip(c, "my_date", ~D[1999-12-31])
    end

    test "MYSQL_TYPE_TIME", c do
      assert_roundtrip(c, "my_time", ~T[09:10:20])
      assert insert_and_get(c, "my_time", ~T[09:10:20.123]) == ~T[09:10:20]

      assert_roundtrip(c, "my_time3", ~T[09:10:20.000])
      assert_roundtrip(c, "my_time3", ~T[09:10:20.123])
      assert insert_and_get(c, "my_time3", ~T[09:10:20]) == ~T[09:10:20.000]

      assert_roundtrip(c, "my_time6", ~T[09:10:20.123456])
    end

    test "MYSQL_TYPE_DATETIME", c do
      assert_roundtrip(c, "my_datetime", ~N[1999-12-31 09:10:20])
      assert insert_and_get(c, "my_datetime", ~N[1999-12-31 09:10:20.123]) == ~N[1999-12-31 09:10:20]

      assert_roundtrip(c, "my_datetime3", ~N[1999-12-31 09:10:20.000])
      assert_roundtrip(c, "my_datetime3", ~N[1999-12-31 09:10:20.123])
      assert insert_and_get(c, "my_datetime3", ~N[1999-12-31 09:10:20]) == ~N[1999-12-31 09:10:20.000]

      assert_roundtrip(c, "my_datetime6", ~N[1999-12-31 09:10:20.123456])
    end

    test "MYSQL_TYPE_YEAR", c do
      assert_roundtrip(c, "my_year", 1999)
    end

    test "MYSQL_TYPE_STRING - SQL BINARY", c do
      assert_roundtrip(c, "my_binary3", <<1, 2, 3>>)
    end

    test "MYSQL_TYPE_VAR_STRING - SQL VARBINARY", c do
      assert_roundtrip(c, "my_varbinary3", <<1, 2, 3>>)
    end
  end

  defp connect(_) do
    opts = [
      hostname: "127.0.0.1",
      port: 8006,
      username: "root",
      password: "secret",
      database: "myxql_test",
      timeout: 5000
    ]

    {:ok, conn} = MyXQL.Protocol.connect(opts)

    ok_packet() = MyXQL.Protocol.query(conn, "DROP TABLE IF EXISTS test_types")
    ok_packet() = MyXQL.Protocol.query(conn, """
    CREATE TABLE test_types (
      id SERIAL PRIMARY KEY AUTO_INCREMENT,
      my_tinyint TINYINT,
      my_smallint SMALLINT,
      my_mediumint MEDIUMINT,
      my_int INT,
      my_bigint BIGINT,
      my_float FLOAT,
      my_double DOUBLE,
      my_decimal DECIMAL /* same as: DECIMAL(10, 0) */,
      my_decimal52 DECIMAL(5,2),
      my_bit2 BIT(2),
      my_bit8 BIT(8),
      my_date DATE,
      my_time TIME,
      my_time3 TIME(3),
      my_time6 TIME(6),
      my_datetime DATETIME,
      my_datetime3 DATETIME(3),
      my_datetime6 DATETIME(6),
      my_year YEAR,
      my_binary3 BINARY(3),
      my_varbinary3 VARBINARY(3)
    )
    """)

    {:ok, conn: conn}
  end

  defp assert_roundtrip(c, field, value) do
    assert insert_and_get(c, field, value) == value
  end

  defp insert_and_get(c, field, value) do
    id = insert(c, field, value)
    get(c, field, id)
  end

  defp insert(c, field, value) do
    ok_packet(last_insert_id: id) = MyXQL.Protocol.query(c.conn, "INSERT INTO test_types (`#{field}`) VALUES ('#{value}')")
    id
  end

  defp get(c, field, id) do
    resultset(rows: [[value]]) = MyXQL.Protocol.query(c.conn, "SELECT `#{field}` FROM test_types WHERE id = '#{id}'")
    value
  end
end
