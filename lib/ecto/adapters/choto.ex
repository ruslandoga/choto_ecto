# adapted from https://github.com/elixir-sqlite/ecto_sqlite3/blob/main/lib/ecto/adapters/sqlite3.ex
# TODO also check out current clickhouse adapters and the postgres one

defmodule Ecto.Adapters.Choto do
  @moduledoc "TODO"

  use Ecto.Adapters.SQL, driver: :choto_dbconnection

  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Structure

  @impl Ecto.Adapter.Storage
  def storage_down(_opts) do
    raise "todo"
  end

  @impl Ecto.Adapter.Storage
  def storage_status(_opts) do
    :down
  end

  @impl Ecto.Adapter.Storage
  def storage_up(_opts) do
    raise "todo"
  end

  @impl Ecto.Adapter.Migration
  def supports_ddl_transaction?, do: false

  @impl Ecto.Adapter.Migration
  def lock_for_migrations(_meta, _options, fun) do
    fun.()
  end

  @impl Ecto.Adapter.Structure
  def structure_dump(_default, _config) do
    raise "todo"
  end

  @impl Ecto.Adapter.Structure
  def structure_load(_default, _config) do
    raise "todo"
  end

  @impl Ecto.Adapter.Schema
  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.generate()

  # TODO
  @impl Ecto.Adapter
  def loaders(_, type) do
    [type]
  end

  @impl Ecto.Adapter
  def dumpers(_primitive, type) do
    [type]
  end
end
