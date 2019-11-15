defmodule MyXQL.Geometry.Point do
  defstruct [:x, :y]
end

defmodule MyXQL.Geometry.Multipoint do
  defstruct [:points]
end
