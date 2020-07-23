defmodule RoxBench do
  use Benchfella

  setup_all do
    :ok = Application.ensure_started(:faker)

    opts = [create_if_missing: true, auto_create_column_families: true]

    cfs = ["a", "b", "c"]

    {:ok, db} = Rox.open("./bench.rocksdb", opts, cfs)

    {:ok, %{db: db, cfs: cfs, default_cf: "a"}}
  end

  teardown_all _ do
    File.rm_rf!("./bench.rocksdb")
  end

  bench "random_writes" do
    Rox.put_cf(bench_context.db, bench_context.default_cf, random_key(), random_record())
  end

  defp random_key(), do: :crypto.rand_uniform(1, 10_000_000) |> Integer.to_string()

  defp random_record() do
    num_pets = :crypto.rand_uniform(0, 3)

    %{
      name: Faker.Person.name(),
      age: :crypto.rand_uniform(20, 50),
      favorite_color: Faker.Color.name(),
      description: Faker.Lorem.words(10),
      profile_url: Faker.Internet.image_url(),
      pets: Enum.map(0..num_pets, fn _ -> random_pet() end)
    }
  end

  defp random_pet() do
    %{
      name: Faker.Person.first_name(),
      age: :crypto.rand_uniform(1, 7)
    }
  end
end
