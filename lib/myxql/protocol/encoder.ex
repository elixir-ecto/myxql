defprotocol MyXQL.Protocol.Encoder do
  @spec encode(struct()) :: {MyXQL.Protocol.Values.storage_type(), binary()}
  def encode(struct)
end

