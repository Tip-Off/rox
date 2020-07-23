defmodule Rox.Mixfile do
  use Mix.Project

  def project do
    [
      app: :rox,
      version: "2.3.1",
      elixir: "~> 1.7",
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      compilers: [:rustler] ++ Mix.compilers(),
      rustler_crates: rustler_crates(),
      package: package(),
      description: description(),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:rustler, "~> 0.21"},
      {:ex_doc, "~> 0.22", only: :dev},
      {:benchfella, "~> 0.3", only: :dev},
      {:faker, "~> 0.14", only: :dev},
      {:flow, "~> 1.0", only: :dev},
      {:gen_stage, "~> 1.0", only: :dev},
      {:dialyxir, "~> 1.0.0", only: :dev, runtime: false}
    ]
  end

  defp rustler_crates do
    [
      rox_nif: [
        path: "native/rox_nif",
        cargo: :system,
        default_features: false,
        features: [],
        mode: :release
      ]
    ]
  end

  defp description do
    """
    Rust powered NIF bindings to Facebook's RocksDB
    """
  end

  defp package do
    [
      name: :rox,
      files: [
        "lib",
        "native/rox_nif/Cargo.*",
        "native/rox_nif/src",
        "mix.exs",
        "README.md",
        "History.md",
        "LICENSE"
      ],
      maintainers: ["Griffin Smith"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/urbint/rox"}
    ]
  end
end
