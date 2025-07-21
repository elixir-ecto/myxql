defmodule MyXQL.MixProject do
  use Mix.Project

  @version "0.8.0"
  @source_url "https://github.com/elixir-ecto/myxql"

  def project() do
    [
      app: :myxql,
      version: @version,
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      name: "MyXQL",
      description: "MySQL 5.5+ driver for Elixir",
      source_url: @source_url,
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application() do
    [
      extra_applications: [:ssl, :public_key],
      env: [
        json_library: Jason
      ]
    ]
  end

  defp package() do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs() do
    [
      source_ref: "v#{@version}",
      main: "readme",
      extras: ["README.md"]
    ]
  end

  defp deps() do
    [
      {:db_connection, "~> 2.4.1 or ~> 2.5", db_connection_opts()},
      {:decimal, "~> 1.6 or ~> 2.0"},
      {:jason, "~> 1.0", optional: true},
      {:geo, "~> 3.4 or ~> 4.0", optional: true},
      {:table, "~> 0.1.0", optional: true},
      {:binpp, ">= 0.0.0", only: [:dev, :test]},
      {:dialyxir, "~> 1.0", only: :dev, runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:benchee, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp db_connection_opts() do
    if path = System.get_env("DB_CONNECTION_PATH") do
      [path: path]
    else
      []
    end
  end
end
