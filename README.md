[`:ecto`](https://github.com/elixir-ecto/ecto/tree/master) adapter for [`:choto`](https://github.com/ruslandoga/choto)

```elixir
defmodule Repo do
  use Ecto.Repo, otp_app: :clickhouse_ne_tormozit, adapter: Ecto.Adapters.Choto
end

iex> {:ok, conn} = Repo.start_link()
iex> Repo.query!("select 1 + 1")
[data: [[{"plus(1, 1)", :u16}]], data: [[{"plus(1, 1)", :u16}, 2]], data: []]
```
