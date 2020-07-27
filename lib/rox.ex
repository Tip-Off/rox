defmodule Rox do
  @moduledoc """
  Elixir wrapper for RocksDB.

  """

  alias __MODULE__.{DB, ColumnFamily, Native, Utils}

  @type compaction_style :: :level | :universal | :fifo | :none
  @type compression_type :: :snappy | :zlib | :bzip2 | :lz4 | :lz4h | :none

  @type key :: String.t() | binary
  @type value :: any

  @type file_path :: String.t()

  @type block_based_table_options :: [
          {:no_block_cache, boolean}
          | {:block_size, pos_integer}
          | {:metadata_block_size, pos_integer}
          | {:cache_index_and_filter_blocks, boolean}
          | {:pin_l0_filter_and_index_blocks_in_cache, boolean}
        ]

  @type access_hint :: :normal | :sequential | :willneed | :none
  @type wal_recovery_mode ::
          :tolerate_corrupted_tail_records
          | :absolute_consistency
          | :point_in_time_recovery
          | :skip_any_corrupted_records

  @type db_options :: [
          {:block_based_options, block_based_table_options}
          | {:total_threads, pos_integer}
          | {:optimize_level_type_compaction_memtable_memory_budget, integer}
          | {:auto_create_column_families, boolean}
          | {:create_if_missing, boolean}
          | {:max_open_files, pos_integer}
          | {:compression_type, compression_type}
          | {:use_fsync, boolean}
          | {:bytes_per_sync, pos_integer}
          | {:table_cache_num_shard_bits, pos_integer}
          | {:min_write_buffer_number, pos_integer}
          | {:max_write_buffer_number, pos_integer}
          | {:write_buffer_size, pos_integer}
          | {:db_write_buffer_size, pos_integer}
          | {:max_bytes_for_level_base, pos_integer}
          | {:max_bytes_for_level_multiplier, pos_integer}
          | {:max_manifest_file_size, pos_integer}
          | {:target_file_size_base, pos_integer}
          | {:min_write_buffer_number_to_merge, pos_integer}
          | {:level_zero_file_num_compaction_trigger, non_neg_integer}
          | {:level_zero_slowdown_writes_trigger, non_neg_integer}
          | {:level_zero_stop_writes_trigger, non_neg_integer}
          # | {:compaction_style, compaction_style}
          | {:max_background_compactions, pos_integer}
          | {:max_background_flushes, pos_integer}
          | {:disable_auto_compactions, boolean}
          | {:report_bg_io_stats, boolean}
          | {:num_levels, pos_integer}
          | {:use_direct_reads, boolean}
          | {:use_direct_io_for_flush_and_compaction, boolean}
        ]

  @type write_options :: [
          {:sync, boolean} | {:disable_wal, boolean} | {:erl_compression, non_neg_integer}
        ]

  @doc """
  Open a RocksDB with the optional `db_opts` and `column_families`.

  If `cfs` are provided, a 3 element tuple will be returned with
  the second element being a map of column family names to `Rox.ColumnFamily` handles.
  The column families must have already been created via `create_cf` or the option
  `auto_create_column_families` can be set to `true`. If it is, the `db_opts` will be
  used to create the column families.


  The database will automatically be closed when the BEAM VM releases it for garbage collection.

  """
  @spec open(file_path, db_options, [ColumnFamily.t()]) :: {:ok, DB.t()} | {:error, any}
  def open(path, db_opts \\ [], cfs \\ [], cf_opts \\ [])
      when is_binary(path) and is_list(db_opts) and is_list(cfs) and is_list(cf_opts) do
    auto_create_cfs? = db_opts[:auto_create_column_families]

    db_opts = db_options_to_map(db_opts)

    case cfs do
      [] ->
        do_open_db_with_no_cf(path, db_opts)

      _ ->
        # First try opening with existing column families
        with {:ok, db} <- Native.open(path, db_opts, cfs, db_options_to_map(cf_opts)) do
          {:ok, DB.wrap_resource(db)}
        else
          {:error, <<"Invalid argument: Column family not found:", _rest::binary>>}
          when auto_create_cfs? ->
            do_open_db_and_create_cfs(path, db_opts, cfs, cf_opts)

          other ->
            other
        end
    end
  end

  defp do_open_db_with_no_cf(path, opts) do
    with {:ok, db} <- Native.open(path, opts, [], %{}) do
      {:ok, DB.wrap_resource(db)}
    end
  end

  defp do_open_db_and_create_cfs(path, opts, cfs, cf_opts) do
    with {:ok, db} <- do_open_db_with_no_cf(path, opts) do
      Enum.each(cfs, fn cf -> :ok = create_cf(db, cf, cf_opts) end)

      {:ok, db}
    end
  end

  @doc """
  Flushes database memtables to SST files on the disk using default options.

  """
  @spec flush(DB.t()) :: :ok | {:error, any}
  def flush(%DB{resource: raw_db}), do: Native.flush(raw_db)

  @doc """
  Create a column family in `db` with `name` and `opts`.

  """
  @spec create_cf(DB.t(), ColumnFamily.t(), db_options) :: :ok | {:error, any}
  def create_cf(%DB{resource: raw_db}, name, opts \\ []),
    do: Native.create_cf(raw_db, name, db_options_to_map(opts))

  @doc """
  Lists the existing column family names of the database at the given path.

  """
  @spec list_cf(file_path, db_options) :: {:ok, Enum.t()} | {:error, any}
  def list_cf(path, opts \\ []) when is_binary(path) and is_list(opts) do
    with {:ok, result} <- Native.list_cf(path, to_map(opts)) do
      {:ok, result}
    end
  end

  @doc """
  Put a key/value pair into the specified database.

  Optionally takes a list of `write_options`.

  Non-binary values will automatically be encoded using the `:erlang.term_to_binary/1` function.

  """

  @spec put(DB.t(), key, value, write_options) :: :ok | {:error, any}
  def put(%DB{resource: db}, key, value, write_opts \\ [])
      when is_binary(key) and is_list(write_opts) do
    {erl_compression, write_opts} = Keyword.pop_values(write_opts, :erl_compression)

    Native.put(db, key, Utils.encode(value, erl_compression), to_map(write_opts))
  end

  @doc """
  Put a key/value pair into the specified column family.

  Optionally takes a list of `write_options`.

  Non-binary values will automatically be encoded using the `:erlang.term_to_binary/1` function.

  """

  @spec put_cf(DB.t(), ColumnFamily.t(), key, value, write_options) :: :ok | {:error, any}
  def put_cf(%DB{resource: db}, cf, key, value, write_opts \\ []) when is_binary(key) do
    {erl_compression, write_opts} = Keyword.pop_values(write_opts, :erl_compression)

    Native.put_cf(db, cf, key, Utils.encode(value, erl_compression), to_map(write_opts))
  end

  @doc """
  Get a key/value pair in the given database with the specified `key`.

  For non-binary terms that were stored, they will be automatically decoded.

  """
  @spec get(DB.t(), key) :: {:ok, value} | :not_found | {:error, any}
  def get(%DB{resource: db}, key) when is_binary(key), do: db |> Native.get(key) |> Utils.decode()

  @doc """
  Get a key/value pair in the given column family with the specified `key`.

  For non-binary terms that were stored, they will be automatically decoded.

  """

  @spec get_cf(DB.t(), ColumnFamily.t(), key) ::
          {:ok, value} | :not_found | {:error, any}
  def get_cf(%DB{resource: db}, cf, key) when is_binary(key),
    do: db |> Native.get_cf(cf, key) |> Utils.decode()

  @doc """
  Return the approximate number of keys in the database.

  Implemented by calling GetIntProperty with `rocksdb.estimate-num-keys`

  """

  @spec count(DB.t()) :: non_neg_integer | {:error, any}
  def count(%DB{resource: db}), do: Native.count(db)

  @doc """
  Deletes the specified `key` from the provided database.

  Optionally takes a list of `write_opts`.

  """

  @spec delete(DB.t(), key, write_options) :: :ok | {:error, any}
  def delete(%DB{resource: db}, key, write_opts \\ []),
    do: Native.delete(db, key, to_map(write_opts))

  @doc """
  Deletes the specified `key` from the provided column family.

  Optionally takes a list of `write_opts`.

  """

  @spec delete_cf(DB.t(), ColumnFamily.t(), key, write_options) :: :ok | {:error, any}
  def delete_cf(%DB{resource: db}, cf, key, write_opts \\ []),
    do: Native.delete_cf(db, cf, key, to_map(write_opts))

  @doc "Retrieves a RocksDB property by name."
  def property(%DB{resource: db}, property), do: Native.property(db, property)

  @doc "Retrieves a RocksDB property by name for a specific column family."
  def property_cf(%DB{resource: db}, cf, property), do: Native.property_cf(db, cf, property)

  @doc "Retrieves a RocksDB property by name and casts it to an integer."
  def property_int(%DB{resource: db}, property), do: Native.property_int(db, property)

  @doc """
  Retrieves a RocksDB property by name and casts it to an integer for a specific
  column family.
  """
  def property_int_cf(%DB{resource: db}, cf, property),
    do: Native.property_int_cf(db, cf, property)

  defp db_options_to_map(db_options),
    do: db_options |> to_map() |> Map.update(:block_based_options, %{}, &to_map(&1))

  defp to_map(map) when is_map(map), do: map
  defp to_map([]), do: %{}
  defp to_map(enum), do: Enum.into(enum, %{})
end
