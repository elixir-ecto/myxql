defmodule MyXQL.Protocol.ValueTest do
  use ExUnit.Case, async: true
  use Bitwise
  @default_mode "STRICT_TRANS_TABLES"

  unless macro_exported?(Kernel, :sigil_U, 2) do
    defmacrop sigil_U({:<<>>, _, [string]}, _) do
      Macro.escape(DateTime.from_naive!(NaiveDateTime.from_iso8601!(string), "Etc/UTC"))
    end
  end

  for protocol <- [:text, :binary] do
    @protocol protocol

    describe "#{@protocol} protocol" do
      setup context do
        mode = Map.get(context, :mode, @default_mode)
        connect(protocol: @protocol, mode: mode)
      end

      test "MYSQL_TYPE_TINY", c do
        assert_roundtrip(c, "my_tinyint", -128)
        assert_roundtrip(c, "my_tinyint", 127)
        assert_out_of_range(c, "my_tinyint", -129)
        assert_out_of_range(c, "my_tinyint", 128)

        assert_roundtrip(c, "my_unsigned_tinyint", 0)
        assert_roundtrip(c, "my_unsigned_tinyint", 255)
        assert_out_of_range(c, "my_unsigned_tinyint", -1)
        assert_out_of_range(c, "my_unsigned_tinyint", 256)
      end

      test "MYSQL_TYPE_SHORT - SQL SMALLINT", c do
        assert_roundtrip(c, "my_smallint", -32768)
        assert_roundtrip(c, "my_smallint", 32767)
        assert_out_of_range(c, "my_smallint", -32769)
        assert_out_of_range(c, "my_smallint", 32768)

        assert_roundtrip(c, "my_unsigned_smallint", 0)
        assert_roundtrip(c, "my_unsigned_smallint", 65535)
        assert_out_of_range(c, "my_unsigned_smallint", -1)
        assert_out_of_range(c, "my_unsigned_smallint", 65536)
      end

      test "MYSQL_TYPE_LONG - SQL INT", c do
        assert_roundtrip(c, "my_int", -2_147_483_648)
        assert_roundtrip(c, "my_int", 2_147_483_647)
        assert_out_of_range(c, "my_int", -2_147_483_649)
        assert_out_of_range(c, "my_int", 2_147_483_648)

        assert_roundtrip(c, "my_unsigned_int", 0)
        assert_roundtrip(c, "my_unsigned_int", 4_294_967_295)
        assert_out_of_range(c, "my_unsigned_int", -1)
        assert_out_of_range(c, "my_unsigned_int", 4_294_967_296)
      end

      test "MYSQL_TYPE_INT24 - SQL MEDIUMINT", c do
        assert_roundtrip(c, "my_mediumint", -8_388_608)
        assert_roundtrip(c, "my_mediumint", 8_388_607)
        assert_out_of_range(c, "my_mediumint", -8_388_609)
        assert_out_of_range(c, "my_mediumint", 8_388_608)

        assert_roundtrip(c, "my_unsigned_mediumint", 0)
        assert_roundtrip(c, "my_unsigned_mediumint", 16_777_215)
        assert_out_of_range(c, "my_unsigned_mediumint", -1)
        assert_out_of_range(c, "my_unsigned_mediumint", 16_777_216)
      end

      test "MYSQL_TYPE_LONGLONG - SQL BIGINT", c do
        assert_roundtrip(c, "my_bigint", -1 <<< 63)
        assert_roundtrip(c, "my_bigint", (1 <<< 63) - 1)
        assert_out_of_range(c, "my_bigint", 1 <<< 63)

        assert_roundtrip(c, "my_unsigned_bigint", 0)
        assert_out_of_range(c, "my_unsigned_bigint", -1)
      end

      # can't make these assertions for binary protocol as we crash when we try to encode these
      # values, so inserting them is not possible
      if @protocol == :text do
        test "MYSQL_TYPE_LONGLONG - out of range", c do
          assert_out_of_range(c, "my_bigint", (-1 <<< 63) - 1)
          assert_out_of_range(c, "my_unsigned_bigint", 1 <<< 64)
        end
      end

      test "MYSQL_TYPE_FLOAT", c do
        assert Float.round(insert_and_get(c, "my_float", -13.37), 2) == -13.37
        assert Float.round(insert_and_get(c, "my_float", 13.37), 2) == 13.37

        assert Float.round(insert_and_get(c, "my_unsigned_float", 13.37), 2) == 13.37
      end

      test "MYSQL_TYPE_DOUBLE", c do
        assert_roundtrip(c, "my_double", -13.37)
        assert_roundtrip(c, "my_double", 13.37)

        assert_roundtrip(c, "my_unsigned_double", 13.37)
      end

      test "MYSQL_TYPE_DATE", c do
        assert_roundtrip(c, "my_date", ~D[1999-12-31])
      end

      test "MYSQL_TYPE_TIME", c do
        assert_roundtrip(c, "my_time", ~T[09:10:20])
        assert insert_and_get(c, "my_time", ~T[09:10:20.123]) == ~T[09:10:20]
      end

      @tag timestamp_precision: true
      test "MYSQL_TYPE_TIME precision", c do
        assert_roundtrip(c, "my_time6", ~T[09:10:20.123456])
      end

      test "MYSQL_TYPE_DATETIME", c do
        assert_roundtrip(c, "my_datetime", ~N[1999-12-31 09:10:20])

        assert insert_and_get(c, "my_datetime", ~N[1999-12-31 09:10:20.123]) ==
                 ~N[1999-12-31 09:10:20]
      end

      @tag mode: "ALLOW_INVALID_DATES"
      test "MYSQL_TYPE_TIMESTAMP - Zero timestamp", c do
        assert query!(c, "SELECT TIMESTAMP '0000-00-00 00:00:00'").rows == [[:zero_datetime]]
      end

      @tag mode: "ALLOW_INVALID_DATES"
      test "MYSQL_TYPE_DATE - Zero date", c do
        assert query!(c, "SELECT DATE '0000-00-00'").rows == [[:zero_date]]
      end

      @tag timestamp_precision: true
      test "MYSQL_TYPE_DATETIME precision", c do
        assert_roundtrip(c, "my_datetime6", ~N[1999-12-31 09:10:20.123456])
      end

      test "MYSQL_TYPE_DATETIME - time zones", c do
        id = insert(c, "my_datetime", ~N[1999-12-31 09:10:20])
        query!(c, "SET time_zone = '+08:00'")
        assert get(c, "my_datetime", id) == ~N[1999-12-31 09:10:20]
      end

      test "MYSQL_TYPE_DATETIME - down casting DateTime to NaiveDateTime", c do
        ndt = ~N[1999-12-31 09:10:20]
        datetime = DateTime.from_naive!(ndt, "Etc/UTC")
        assert insert_and_get(c, "my_datetime", datetime) == ndt
      end

      if @protocol == :binary do
        test "MYSQL_TYPE_DATETIME - non-UTC datetimes", c do
          ndt = ~N[1999-12-31 09:10:20]
          datetime = %{DateTime.from_naive!(ndt, "Etc/UTC") | time_zone: "Europe/Warsaw"}

          assert_raise ArgumentError, ~r"is not in UTC", fn ->
            insert(c, "my_datetime", datetime)
          end
        end
      end

      test "MYSQL_TYPE_TIMESTAMP", c do
        assert_roundtrip(c, "my_timestamp", ~U[1999-12-31 09:10:20Z])
      end

      test "MYSQL_TYPE_TIMESTAMP - time zones", c do
        query!(c, "SET time_zone = '+00:00'")
        id = insert(c, "my_timestamp", ~U[1999-12-31 09:10:20Z])
        query!(c, "SET time_zone = '+08:00'")
        assert get(c, "my_timestamp", id) == ~U[1999-12-31 17:10:20Z]
      end

      test "MYSQL_TYPE_YEAR", c do
        assert_roundtrip(c, "my_year", 1999)
      end

      @tag bit: true
      test "MYSQL_TYPE_BIT", c do
        assert_roundtrip(c, "my_bit3", <<1::1, 0::1, 1::1>>)
        assert_roundtrip(c, "my_bit3", <<1::1, 0::1, 0::1>>)
        assert_roundtrip(c, "my_bit3", <<0::1, 0::1, 1::1>>)
      end

      test "MYSQL_TYPE_NEWDECIMAL - SQL DECIMAL", c do
        assert_roundtrip(c, "my_decimal", Decimal.new(-13))
        assert insert_and_get(c, "my_decimal", Decimal.new("-13.37")) == Decimal.new(-13)

        assert_roundtrip(c, "my_unsigned_decimal", Decimal.new(0))
        assert insert_and_get(c, "my_decimal", Decimal.new("13.37")) == Decimal.new(13)

        assert_roundtrip(c, "my_decimal52", Decimal.new("-999.99"))
        assert_roundtrip(c, "my_decimal52", Decimal.new("999.99"))
        assert_roundtrip(c, "my_unsigned_decimal52", Decimal.new("999.99"))
      end

      test "MYSQL_TYPE_ENUM", c do
        assert_roundtrip(c, "my_enum", "red")
      end

      test "MYSQL_TYPE_SET", c do
        assert_roundtrip(c, "my_set", "red,green")
      end

      test "MYSQL_TYPE_BLOB", c do
        assert_roundtrip(c, "my_blob", <<1, 2, 3>>)
      end

      test "MYSQL_TYPE_TINY_BLOB", c do
        assert_roundtrip(c, "my_tinyblob", <<1, 2, 3>>)
      end

      test "MYSQL_TYPE_MEDIUM_BLOB", c do
        blob = String.duplicate("a", 1000)
        assert_roundtrip(c, "my_mediumblob", blob)

        blob = String.duplicate("a", 1_000_000)
        assert_roundtrip(c, "my_mediumblob", blob)

        blob = String.duplicate("a", 16_777_000)
        assert_roundtrip(c, "my_mediumblob", blob)
      end

      test "MYSQL_TYPE_LONG_BLOB", c do
        blob = String.duplicate("a", 16_777_000)
        assert_roundtrip(c, "my_longblob", blob)
      end

      test "MYSQL_TYPE_VAR_STRING - SQL VARBINARY", c do
        assert_roundtrip(c, "my_varbinary3", <<1, 2, 3>>)
      end

      test "MYSQL_TYPE_VARCHAR", c do
        assert_roundtrip(c, "my_varchar3", <<1, 2, 3>>)
      end

      test "MYSQL_TYPE_STRING - SQL BINARY", c do
        assert_roundtrip(c, "my_binary3", <<1, 2, 3>>)
      end

      test "MYSQL_TYPE_NULL", c do
        assert_roundtrip(c, ~w(my_tinyint my_binary3 my_smallint my_varbinary3 my_int), [
          nil,
          "foo",
          nil,
          "bar",
          nil
        ])
      end

      test "MYSQL_TYPE_NULL - just nulls", c do
        assert_roundtrip(c, ~w(my_smallint my_mediumint my_int), [nil, nil, nil])
      end

      test "MYSQL_TYPE_NULL - select", c do
        assert query!(c, "SELECT null").rows == [[nil]]
      end

      test "BOOLEAN", c do
        assert insert_and_get(c, "my_boolean", true) == 1
        assert insert_and_get(c, "my_boolean", false) == 0
      end

      @tag json: true
      test "JSON", c do
        assert_roundtrip(c, "my_json", [])
        assert_roundtrip(c, "my_json", [1, [2, 3]])
        assert_roundtrip(c, "my_json", %{})
        assert_roundtrip(c, "my_json", %{"a" => ["foo", 42]})
      end

      test "CHAR", c do
        assert_roundtrip(c, "my_char", "é")
      end
    end
  end

  describe "text & binary discrepancies" do
    test "floats" do
      # 13.37 is returned as 13.3699... in binary protocol and conversely
      # 13.3699 is returned as 13.37 in text protocol.
      assert_discrepancy("my_float",
        text: 13.37,
        binary: 13.369999885559082
      )
    end

    @tag timestamp_precision: true
    test "timestamps" do
      assert_discrepancy("my_time3",
        text: ~T[09:10:20.000],
        binary: ~T[09:10:20]
      )

      assert_discrepancy("my_time3",
        text: ~T[09:10:20.123],
        binary: ~T[09:10:20.123000]
      )

      assert_discrepancy("my_datetime3",
        text: ~N[1999-12-31 09:10:20.000],
        binary: ~N[1999-12-31 09:10:20]
      )

      assert_discrepancy("my_datetime3",
        text: ~N[1999-12-31 09:10:20.123],
        binary: ~N[1999-12-31 09:10:20.123000]
      )
    end
  end

  defp connect(c) do
    after_connect = fn conn ->
      mode = Keyword.get(c, :mode, @default_mode)
      MyXQL.query!(conn, "SET SESSION sql_mode = '#{mode}'")
    end

    {:ok, conn} = MyXQL.start_link([after_connect: after_connect] ++ TestHelper.opts())
    Keyword.put(c, :conn, conn)
  end

  defp assert_roundtrip(c, field, value) do
    assert insert_and_get(c, field, value) == value
    value
  end

  defp assert_out_of_range(c, field, value) do
    assert_raise MyXQL.Error,
                 "(1264) (ER_WARN_DATA_OUT_OF_RANGE) Out of range value for column '#{field}' at row 1",
                 fn ->
                   insert_and_get(c, field, value)
                 end
  end

  defp insert_and_get(c, field, value) do
    id = insert(c, field, value)
    get(c, field, id)
  end

  defp assert_discrepancy(field, text: expected_text, binary: expected_binary)
       when is_binary(field) do
    c = [protocol: :text] |> connect() |> Map.new()
    assert_roundtrip(c, field, expected_text)
    assert insert_and_get(c, field, expected_binary) == expected_text

    c = [protocol: :binary] |> connect() |> Map.new()
    assert_roundtrip(c, field, expected_binary)
    assert insert_and_get(c, field, expected_text) == expected_binary
  end

  defp insert(%{protocol: :text} = c, fields, values) when is_list(fields) and is_list(values) do
    fields = Enum.map_join(fields, ", ", &"`#{&1}`")

    values =
      Enum.map_join(values, ", ", fn
        nil ->
          "NULL"

        true ->
          "TRUE"

        false ->
          "FALSE"

        %DateTime{} = datetime ->
          "'#{NaiveDateTime.to_iso8601(datetime)}'"

        list when is_list(list) ->
          "'#{Jason.encode!(list)}'"

        %_{} = struct ->
          "'#{struct}'"

        map when is_map(map) ->
          "'#{Jason.encode!(map)}'"

        value when is_binary(value) ->
          "'#{value}'"

        value when is_bitstring(value) ->
          size = bit_size(value)
          <<value::size(size)>> = value
          "B'#{Integer.to_string(value, 2)}'"

        value ->
          "'#{value}'"
      end)

    %MyXQL.Result{last_insert_id: id} =
      query!(c, "INSERT INTO test_types (#{fields}) VALUES (#{values})")

    id
  end

  defp insert(%{protocol: :binary} = c, fields, values)
       when is_list(fields) and is_list(values) do
    fields = Enum.map_join(fields, ", ", &"`#{&1}`")
    placeholders = Enum.map_join(values, ", ", fn _ -> "?" end)
    statement = "INSERT INTO test_types (#{fields}) VALUES (#{placeholders})"
    %MyXQL.Result{last_insert_id: id} = query!(c, statement, values)
    id
  end

  defp insert(c, field, value) when is_binary(field) do
    insert(c, [field], [value])
  end

  defp get(c, fields, id) when is_list(fields) do
    fields = Enum.map_join(fields, ", ", &"`#{&1}`")
    statement = "SELECT #{fields} FROM test_types WHERE id = '#{id}'"
    %MyXQL.Result{rows: [values]} = query!(c, statement)
    values
  end

  defp get(c, field, id) do
    [value] = get(c, [field], id)
    value
  end

  defp query!(c, statement, params \\ [], opts \\ []) do
    opts = Keyword.put_new(opts, :query_type, c.protocol)
    MyXQL.query!(c.conn, statement, params, opts)
  end
end
