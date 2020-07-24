defmodule RoxTest do
  use ExUnit.Case, async: false
  alias Rox.Batch
  doctest Rox

  @cf "people"

  setup_all do
    path = Path.join(__DIR__, "test.rocksdb")

    {:ok, db} =
      Rox.open(path, [create_if_missing: true, auto_create_column_families: true], [@cf])

    on_exit(fn ->
      File.rm_rf(path)
      :ok
    end)

    {:ok, %{db: db}}
  end

  describe "Working with default column family" do
    test "simple put and get", %{db: db} do
      assert :not_found = Rox.get(db, "put_test")

      assert :ok = Rox.put(db, "put_test", "val")

      assert {:ok, "val"} = Rox.get(db, "put_test")
    end

    test "put with a binary key", %{db: db} do
      binary_key = <<131, 251, 222, 111>>

      assert :not_found = Rox.get(db, binary_key)
      assert :ok = Rox.put(db, binary_key, "test")
      assert {:ok, "test"} = Rox.get(db, binary_key)
    end

    test "delete", %{db: db} do
      assert :not_found = Rox.get(db, "delete_test")
      assert :ok = Rox.put(db, "delete_test", "some_val")
      assert {:ok, _val} = Rox.get(db, "delete_test")
      assert :ok = Rox.delete(db, "delete_test")

      assert :not_found = Rox.get(db, "delete_test")
    end
  end

  describe "Working with non-default column family" do
    test "simple put and get", %{db: db} do
      assert :not_found = Rox.get_cf(db, @cf, "put_test")

      assert :ok = Rox.put_cf(db, @cf, "put_test", "val")

      assert {:ok, "val"} = Rox.get_cf(db, @cf, "put_test")
    end

    test "delete", %{db: db} do
      assert :not_found = Rox.get_cf(db, @cf, "delete_test")
      assert :ok = Rox.put_cf(db, @cf, "delete_test", "some_val")
      assert {:ok, _val} = Rox.get_cf(db, @cf, "delete_test")
      assert :ok = Rox.delete_cf(db, @cf, "delete_test")

      assert :not_found = Rox.get_cf(db, @cf, "delete_test")
    end
  end

  describe "Batch Operations" do
    test "puts and deletes", %{db: db} do
      assert :not_found = Rox.get(db, "batch_put_test")
      assert :not_found = Rox.get_cf(db, @cf, "batch_put_test")

      assert :ok =
               Batch.new()
               |> Batch.put("batch_put_test", "works")
               |> Batch.put_cf(@cf, "batch_put_test", "works")
               |> Batch.write(db)

      assert {:ok, "works"} = Rox.get(db, "batch_put_test")
      assert {:ok, "works"} = Rox.get_cf(db, @cf, "batch_put_test")

      assert :ok =
               Batch.new()
               |> Batch.delete("batch_put_test")
               |> Batch.delete(@cf, "batch_put_test")
               |> Batch.write(db)

      assert :not_found = Rox.get(db, "batch_put_test")
      assert :not_found = Rox.get_cf(db, @cf, "batch_put_test")
    end
  end
end
