
if Code.ensure_loaded?(Geo) do
  defmodule GeoEncoderHelper do
    @moduledoc false
    import MyXQL.Protocol.Types, only: [uint4: 0]

    def encode_geometry(geo) do
      srid = geo.srid || 0
      binary = %{geo | srid: nil} |> Geo.WKB.encode_to_iodata(:ndr) |> IO.iodata_to_binary()

      {
        :mysql_type_var_string,
        MyXQL.Protocol.Types.encode_string_lenenc(<<srid::uint4(), binary::binary>>)
      }
    end
  end

  defimpl MyXQL.Protocol.Encoder, for: Geo.Point do
    def encode(geo), do: GeoEncoderHelper.encode_geometry(geo)
  end

  defimpl MyXQL.Protocol.Encoder, for: Geo.MultiPoint do
    def encode(geo), do: GeoEncoderHelper.encode_geometry(geo)
  end

  defimpl MyXQL.Protocol.Encoder, for: Geo.LineString do
    def encode(geo), do: GeoEncoderHelper.encode_geometry(geo)
  end

  defimpl MyXQL.Protocol.Encoder, for: Geo.MultiLineString do
    def encode(geo), do: GeoEncoderHelper.encode_geometry(geo)
  end

  defimpl MyXQL.Protocol.Encoder, for: Geo.Polygon do
    def encode(geo), do: GeoEncoderHelper.encode_geometry(geo)
  end

  defimpl MyXQL.Protocol.Encoder, for: Geo.MultiPolygon do
    def encode(geo), do: GeoEncoderHelper.encode_geometry(geo)
  end

  defimpl MyXQL.Protocol.Encoder, for: Geo.GeometryCollection do
    def encode(geo), do: GeoEncoderHelper.encode_geometry(geo)
  end
end
