if Code.ensure_loaded?(Geo) do
  defmodule MyXQL.Protocol.GeometryCodec.Geo do
    @behaviour MyXQL.Protocol.GeometryCodec

    supported_structs = [
      Geo.Point,
      Geo.GeometryCollection,
      Geo.LineString,
      Geo.MultiPoint,
      Geo.MultiLineString,
      Geo.MultiPolygon,
      Geo.Polygon
    ]

    def encode(%x{} = geo) when x in unquote(supported_structs) do
      srid = geo.srid || 0
      wkb = %{geo | srid: nil} |> Geo.WKB.encode_to_iodata(:ndr) |> IO.iodata_to_binary()
      {srid, wkb}
    end

    def encode(_), do: :unknown

    def decode(0, wkb), do: Geo.WKB.decode!(wkb)
    def decode(srid, wkb), do: Geo.WKB.decode!(wkb) |> Map.put(:srid, srid)
  end
end
