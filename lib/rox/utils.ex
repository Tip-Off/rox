defmodule Rox.Utils do
  @moduledoc false

  @compile [
    inline: [{:decode, 1}, {:encode, 1}]
  ]

  def decode({:ok, <<"_$rx:", encoded::binary>>}), do: {:ok, :erlang.binary_to_term(encoded)}
  def decode(<<"_$rx:", encoded::binary>>), do: :erlang.binary_to_term(encoded)
  def decode(other), do: other

  def encode(val, opts \\ [])

  def encode(val, _) when is_binary(val), do: val

  def encode(val, opts) do
    val =
      case opts[:erl_compression] do
        nil -> :erlang.term_to_binary(val)
        level -> :erlang.term_to_binary(val, compressed: level)
      end

    "_$rx:" <> val
  end
end
