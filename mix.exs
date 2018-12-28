defmodule MyXQL.MixProject do
  use Mix.Project

  def project() do
    [
      app: :myxql,
      version: "0.1.0",
      elixir: "~> 1.4",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application() do
    [
      extra_applications: [:logger, :crypto, :ssl]
    ]
  end

  defp deps() do
    [
      {:db_connection, "~> 2.0"},
      {:decimal, "~> 1.6"},
      {:jason, "~> 1.0", optional: true},
      {:binpp, ">= 0.0.0", only: [:dev, :test]},
      {:dialyxir, "~> 1.0-rc", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end
end
