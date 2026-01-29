defmodule MyXQL.Protocol.GeometryCodec do
  @type no_srid() :: 0
  @type some_srid() :: pos_integer()

  @callback encode(struct()) :: {srid :: integer(), wkb :: binary()} | :unknown
  @callback decode(srid :: no_srid() | some_srid(), wkb :: binary()) :: struct() | :unknown

  import MyXQL.Protocol.Types

  @doc false
  def do_encode(struct) do
    with codec when not is_nil(codec) <- geometry_codec(),
         {srid, wkb} <- codec.encode(struct) do
      {
        :mysql_type_var_string,
        MyXQL.Protocol.Types.encode_string_lenenc(<<srid::uint4(), wkb::binary>>)
      }
    else
      _ -> :unknown
    end
  end

  @doc false
  def do_decode(srid, wkb) do
    codec = geometry_codec()

    if codec != nil do
      codec.decode(srid, wkb)
    else
      :unknown
    end
  end

  default_codec = if Code.ensure_loaded?(Geo), do: MyXQL.Protocol.GeometryCodec.Geo, else: nil

  defp geometry_codec do
    Application.get_env(:myxql, :geometry_codec, unquote(default_codec))
  end
end
