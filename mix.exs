defmodule ChotoEcto.MixProject do
  use Mix.Project

  def project do
    [
      app: :choto_ecto,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 3.8"},
      {:choto_dbconnection, github: "ruslandoga/choto_dbconnection"}
    ]
  end
end
