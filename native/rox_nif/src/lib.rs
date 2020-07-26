#[macro_use]
extern crate rustler;
extern crate rocksdb;

use std::io::Write;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, RwLock};

use rustler::resource::ResourceArc;

use rustler::{Decoder, Encoder, Env, Error, NifResult, Term};

use rocksdb::{DBCompressionType, IteratorMode, Options, WriteBatch, WriteOptions, BlockBasedOptions, DB};

use rustler::types::binary::{Binary, OwnedBinary};
use rustler::types::list::ListIterator;

mod atoms {
    rustler_atoms! {
        atom ok;
        atom error;
        atom not_found;

        // Batch Operation Atoms
        atom put;
        atom put_cf;
        atom delete;
        atom delete_cf;

        // Compression Type Atoms
        atom snappy;
        atom zlib;
        atom bzip2;
        atom lz4;
        atom lz4h;
        atom none;

        // Block Based Table Option atoms
        atom block_size;
        atom metadata_block_size;
        // atom partition_filters;
        // atom block_cache;
        // atom block_cache_compressed;
        atom no_block_cache;
        // atom bloom_filter;
        // atom cache_index_and_filter_blocks;
        // atom index_type;
        // atom pin_l0_filter_and_index_blocks_in_cache;
        // atom pin_top_level_index_and_filter;
        // atom format_version;
        // atom block_restart_interval;
        // atom index_block_restart_interval;
        // atom data_block_index_type;
        // atom data_block_hash_ratio;

        // CF Options Related atoms
        // atom block_cache_size_mb_for_point_lookup;
        // atom memtable_memory_budget;
        // atom write_buffer_size;
        // atom max_write_buffer_number;
        // atom min_write_buffer_number_to_merge;
        // atom compression;
        // atom num_levels;
        // atom level0_file_num_compaction_trigger;
        // atom level0_slowdown_writes_trigger;
        // atom level0_stop_writes_trigger;
        // atom max_mem_compaction_level;
        // atom target_file_size_base;
        // atom target_file_size_multiplier;
        // atom max_bytes_for_level_base;
        // atom max_bytes_for_level_multiplier;
        // atom expand_compaction_factor;
        // atom source_compaction_factor;
        // atom max_grandparent_overlap_factor;
        // atom soft_rate_limit;
        // atom hard_rate_limit;
        // atom arena_block_size;
        // atom disable_auto_compaction;
        // atom purge_redundant_kvs_while_flush;
        // atom compaction_style;
        // atom verify_checksums_in_compaction;
        // atom filter_deletes;
        // atom max_sequential_kip_in_iterations;
        // atom inplace_update_support;
        // atom inplace_update_num_locks;
        // atom table_factory_block_cache_size;
        // atom in_memory_mode;
        // atom block_based_table_options;

        // DB Options
        atom block_based_options;
        atom total_threads;
        atom optimize_level_type_compaction_memtable_memory_budget;
        atom auto_create_column_families;
        atom create_if_missing;
        atom max_open_files;
        atom compression_type;
        atom use_fsync;
        atom bytes_per_sync;
        atom table_cache_num_shard_bits;
        atom min_write_buffer_number;
        atom max_write_buffer_number;
        atom write_buffer_size;
        atom max_bytes_for_level_base;
        atom max_bytes_for_level_multiplier;
        atom max_manifest_file_size;
        atom target_file_size_base;
        atom min_write_buffer_number_to_merge;
        atom level_zero_file_num_compaction_trigger;
        atom level_zero_slowdown_writes_trigger;
        atom level_zero_stop_writes_trigger;
        // atom compaction_style;
        atom max_background_compactions;
        atom max_background_flushes;
        atom disable_auto_compactions;
        atom report_bg_io_stats;
        atom num_levels;
        // atom max_total_wal_size;
        // atom use_fsync;
        // atom db_paths;
        // atom db_log_dir;
        // atom wal_dir;
        // atom delete_obsolete_files_period_micros;
        // atom max_log_file_size;
        // atom log_file_time_to_roll;
        // atom keep_log_file_num;
        // atom max_manifest_file_size;
        // atom table_cache_numshardbits;
        // atom wal_ttl_seconds;
        // atom wal_size_limit_mb;
        // atom manifest_preallocation_size;
        // atom allow_mmap_reads;
        // atom allow_mmap_writes;
        // atom is_fd_close_on_exec;
        // atom skip_log_error_on_recovery;
        // atom stats_dump_period_sec;
        // atom advise_random_on_open;
        // atom access_hint;
        // atom compaction_readahead_size;
        // atom use_adaptive_mutex;
        // atom bytes_per_sync;
        // atom skip_stats_update_on_db_open;
        // atom wal_recovery_mode;
        atom use_direct_reads;
        atom use_direct_io_for_flush_and_compaction;

        // Write Options
        atom sync;
        atom disable_wal;
    }
}

struct DBHandle {
    pub db: Arc<RwLock<DB>>,
}

impl Deref for DBHandle {
    type Target = Arc<RwLock<DB>>;

    fn deref(&self) -> &Self::Target {
        &self.db
    }
}

struct CompressionType {
    pub raw: DBCompressionType,
}
impl<'a> Decoder<'a> for CompressionType {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if atoms::none() == term {
            Ok(CompressionType {
                raw: DBCompressionType::None,
            })
        } else if atoms::snappy() == term {
            Ok(CompressionType {
                raw: DBCompressionType::Snappy,
            })
        } else if atoms::zlib() == term {
            Ok(CompressionType {
                raw: DBCompressionType::Zlib,
            })
        } else if atoms::bzip2() == term {
            Ok(CompressionType {
                raw: DBCompressionType::Bz2,
            })
        } else if atoms::lz4() == term {
            Ok(CompressionType {
                raw: DBCompressionType::Lz4,
            })
        } else if atoms::lz4h() == term {
            Ok(CompressionType {
                raw: DBCompressionType::Lz4hc,
            })
        } else {
            Err(Error::BadArg)
        }
    }
}

impl Into<DBCompressionType> for CompressionType {
    fn into(self) -> DBCompressionType {
        self.raw
    }
}

enum BatchOperation<'a> {
    Put(&'a [u8], &'a [u8]),
    PutCf(String, &'a [u8], &'a [u8]),
    Delete(&'a [u8]),
    DeleteCf(String, &'a [u8]),
}

impl<'a> Decoder<'a> for BatchOperation<'a> {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        let (operation, details): (Term, Term) = term.decode()?;
        if atoms::put() == operation {
            let (key, val): (Binary, Binary) = details.decode()?;
            
            Ok(BatchOperation::Put(key.as_slice(), val.as_slice()))
        } else if atoms::put_cf() == operation {
            let (cf, key, val): (String, Binary, Binary) = details.decode()?;
            
            Ok(BatchOperation::PutCf(cf, key.as_slice(), val.as_slice()))
        } else if atoms::delete() == operation {
            let key: Binary = details.decode()?;
            Ok(BatchOperation::Delete(key.as_slice()))
        } else if atoms::delete_cf() == operation {
            let (cf, key): (String, Binary) = details.decode()?;
            
            Ok(BatchOperation::DeleteCf(cf, key.as_slice()))
        } else {
            Err(Error::BadArg)
        }
    }
}

macro_rules! handle_error {
    ($env:expr, $e:expr) => {
        match $e {
            Ok(inner) => inner,
            Err(err) => return Ok((atoms::error(), err.to_string().encode($env)).encode($env)),
        }
    };
}

fn decode_write_options<'a>(env: Env<'a>, arg: Term<'a>) -> NifResult<WriteOptions> {
    let mut opts = WriteOptions::new();

    if let Ok(sync) = arg.map_get(atoms::sync().to_term(env)) {
        opts.set_sync(sync.decode()?);
    }

    if let Ok(disable_wal) = arg.map_get(atoms::disable_wal().to_term(env)) {
        opts.disable_wal(disable_wal.decode()?);
    }

    Ok(opts)
}

fn decode_block_based_options<'a>(env: Env<'a>, arg: Term<'a>) -> NifResult<BlockBasedOptions> {
    let mut opts = BlockBasedOptions::default();

    if let Ok(block_size) = arg.map_get(atoms::block_size().to_term(env)) {
        let i_size: u64 = block_size.decode()?;
        opts.set_block_size(i_size as usize);
    }

    if let Ok(metadata_block_size) = arg.map_get(atoms::metadata_block_size().to_term(env)) {
        let i_size: u64 = metadata_block_size.decode()?;
        opts.set_metadata_block_size(i_size as usize);
    }

    if let Ok(no_block_cache) = arg.map_get(atoms::no_block_cache().to_term(env)) {
        let value: bool = no_block_cache.decode()?;

        if value {
            opts.disable_cache();
        }
    }

    Ok(opts)
}

fn decode_db_options<'a>(env: Env<'a>, arg: Term<'a>) -> NifResult<Options> {
    let mut opts = Options::default();

    if let Ok(block_based_opts) = arg.map_get(atoms::block_based_options().to_term(env)) {
        let block_based_opts = decode_block_based_options(env, block_based_opts)?;

        opts.set_block_based_table_factory(&block_based_opts);
    }

    if let Ok(count) = arg.map_get(atoms::total_threads().to_term(env)) {
        opts.increase_parallelism(count.decode()?);
    }

    if let Ok(memtable_budget) =
        arg.map_get(atoms::optimize_level_type_compaction_memtable_memory_budget().to_term(env))
    {
        let i_size: u64 = memtable_budget.decode()?;
        opts.optimize_level_style_compaction(i_size as usize);
    }

    if let Ok(create_if_missing) = arg.map_get(atoms::create_if_missing().to_term(env)) {
        opts.create_if_missing(create_if_missing.decode()?);
    }

    if let Ok(max_open_files) = arg.map_get(atoms::max_open_files().to_term(env)) {
        opts.set_max_open_files(max_open_files.decode()?);
    }

    if let Ok(compression_type_opt) = arg.map_get(atoms::compression_type().to_term(env)) {
        let compression_type: CompressionType = compression_type_opt.decode()?;
        opts.set_compression_type(compression_type.into());
    }

    // TODO: Set Compression Type Per Level

    if let Ok(use_fsync) = arg.map_get(atoms::use_fsync().to_term(env)) {
        opts.set_use_fsync(use_fsync.decode()?);
    }

    if let Ok(bytes_per_sync) = arg.map_get(atoms::bytes_per_sync().to_term(env)) {
        opts.set_bytes_per_sync(bytes_per_sync.decode()?);
    }

    if let Ok(nbits) = arg.map_get(atoms::table_cache_num_shard_bits().to_term(env)) {
        opts.set_table_cache_num_shard_bits(nbits.decode()?);
    }

    if let Ok(nbuf) = arg.map_get(atoms::min_write_buffer_number().to_term(env)) {
        opts.set_min_write_buffer_number(nbuf.decode()?);
    }

    if let Ok(nbuf) = arg.map_get(atoms::max_write_buffer_number().to_term(env)) {
        opts.set_max_write_buffer_number(nbuf.decode()?);
    }

    if let Ok(size) = arg.map_get(atoms::write_buffer_size().to_term(env)) {
        let i_size: u64 = size.decode()?;
        opts.set_write_buffer_size(i_size as usize);
    }

    if let Ok(max_bytes) = arg.map_get(atoms::max_bytes_for_level_base().to_term(env)) {
        opts.set_max_bytes_for_level_base(max_bytes.decode()?);
    }

    if let Ok(multiplier) = arg.map_get(atoms::max_bytes_for_level_multiplier().to_term(env)) {
        opts.set_max_bytes_for_level_multiplier(multiplier.decode()?);
    }

    if let Ok(max_size) = arg.map_get(atoms::max_manifest_file_size().to_term(env)) {
        let i_size: u64 = max_size.decode()?;
        opts.set_max_manifest_file_size(i_size as usize);
    }

    if let Ok(target_size) = arg.map_get(atoms::target_file_size_base().to_term(env)) {
        opts.set_target_file_size_base(target_size.decode()?);
    }

    if let Ok(to_merge) = arg.map_get(atoms::min_write_buffer_number_to_merge().to_term(env)) {
        opts.set_min_write_buffer_number_to_merge(to_merge.decode()?);
    }

    if let Ok(n) = arg.map_get(atoms::level_zero_file_num_compaction_trigger().to_term(env)) {
        opts.set_level_zero_file_num_compaction_trigger(n.decode()?);
    }

    if let Ok(n) = arg.map_get(atoms::level_zero_slowdown_writes_trigger().to_term(env)) {
        opts.set_level_zero_slowdown_writes_trigger(n.decode()?);
    }

    if let Ok(n) = arg.map_get(atoms::level_zero_stop_writes_trigger().to_term(env)) {
        opts.set_level_zero_stop_writes_trigger(n.decode()?);
    }

    // Todo set compaction style

    if let Ok(n) = arg.map_get(atoms::max_background_compactions().to_term(env)) {
        opts.set_max_background_compactions(n.decode()?);
    }

    if let Ok(n) = arg.map_get(atoms::max_background_flushes().to_term(env)) {
        opts.set_max_background_flushes(n.decode()?);
    }

    if let Ok(disable) = arg.map_get(atoms::disable_auto_compactions().to_term(env)) {
        opts.set_disable_auto_compactions(disable.decode()?);
    }

    if let Ok(bg_io_stats) = arg.map_get(atoms::report_bg_io_stats().to_term(env)) {
        opts.set_report_bg_io_stats(bg_io_stats.decode()?);
    }

    // Todo: set WAL Recovery Mode

    if let Ok(num_levels) = arg.map_get(atoms::num_levels().to_term(env)) {
        opts.set_num_levels(num_levels.decode()?);
    }

    if let Ok(enabled) = arg.map_get(atoms::use_direct_io_for_flush_and_compaction().to_term(env)) {
        opts.set_use_direct_io_for_flush_and_compaction(enabled.decode()?);
    }

    if let Ok(enabled) = arg.map_get(atoms::use_direct_reads().to_term(env)) {
        opts.set_use_direct_reads(enabled.decode()?);
    }

    Ok(opts)
}

fn open<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let path: &Path = Path::new(args[0].decode()?);

    let db_opts = if args[1].map_size()? > 0 {
        decode_db_options(env, args[1])?
    } else {
        Options::default()
    };

    let cf: Vec<&str> = if args[2].list_length()? == 0 {
        vec![]
    } else {
        let iter: ListIterator = args[2].decode()?;
        let result: Vec<&str> = iter.map(|x| x.decode()).collect::<NifResult<Vec<&str>>>()?;

        result
    };

    let db: DB = handle_error!(env, DB::open_cf(&db_opts, path, &cf));

    let resp = (
        atoms::ok(),
        ResourceArc::new(DBHandle {
            db: Arc::new(RwLock::new(db)),
        }),
    ).encode(env);

    Ok(resp)
}

fn count<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db_handle = db_arc.deref();

    let db_read = db_handle.db.read().unwrap();
    let iterator = db_read.iterator(IteratorMode::Start);

    let count = iterator.count();

    Ok((count as u64).encode(env))
}

fn create_cf<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let mut db = db_arc.db.write().unwrap();

    let name: &str = args[1].decode()?;
    let has_db_opts = args[2].map_size()? > 0;
    let opts = if has_db_opts {
        decode_db_options(env, args[2])?
    } else {
        Options::default()
    };

    let _cf = handle_error!(env, db.create_cf(name, &opts));

    Ok(atoms::ok().encode(env))
}

fn list_cf<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let path: &Path = Path::new(args[0].decode()?);

    let db_opts = if args[1].map_size()? > 0 {
        decode_db_options(env, args[1])?
    } else {
        Options::default()
    };

    let paths: Vec<String> = handle_error!(env, DB::list_cf(&db_opts, path));
    let resp = (atoms::ok(), paths).encode(env);

    Ok(resp)
}

fn put<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.write().unwrap();

    let key: Binary = args[1].decode()?;
    let val: Binary = args[2].decode()?;

    let resp = if args[3].map_size()? > 0 {
        let write_opts = decode_write_options(env, args[2])?;
        db.put_opt(key.as_slice(), val.as_slice(), &write_opts)
    } else {
        db.put(key.as_slice(), val.as_slice())
    };

    handle_error!(env, resp);

    Ok(atoms::ok().encode(env))
}

fn put_cf<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.write().unwrap();

    let cf: String = args[1].decode()?;
    let cf_handle = db.cf_handle(&cf.as_str()).unwrap();
    
    let key: Binary = args[2].decode()?;
    let val: Binary = args[3].decode()?;

    let resp = if args[4].map_size()? > 0 {
        let write_opts = decode_write_options(env, args[2])?;
        
        db.put_cf_opt(cf_handle, key.as_slice(), val.as_slice(), &write_opts)
    } else {
        db.put_cf(cf_handle, key.as_slice(), val.as_slice())
    };

    handle_error!(env, resp);

    Ok(atoms::ok().encode(env))
}

fn delete<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.write().unwrap();

    let key: Binary = args[1].decode()?;

    let resp = if args[2].map_size()? > 0 {
        let write_opts = decode_write_options(env, args[2])?;
        db.delete_opt(key.as_slice(), &write_opts)
    } else {
        db.delete(key.as_slice())
    };

    handle_error!(env, resp);

    Ok(atoms::ok().encode(env))
}

fn delete_cf<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.write().unwrap();

    let cf: String = args[1].decode()?;
    let cf_handle = db.cf_handle(&cf.as_str()).unwrap();

    let key: Binary = args[2].decode()?;

    let resp = if args[3].map_size()? > 0 {
        let write_opts = decode_write_options(env, args[3])?;
        db.delete_cf_opt(cf_handle, key.as_slice(), &write_opts)
    } else {
        db.delete_cf(cf_handle, key.as_slice())
    };

    handle_error!(env, resp);

    Ok(atoms::ok().encode(env))
}

fn get<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.read().unwrap();

    let key = args[1].decode::<Binary>()?.as_slice();

    let resp = db.get(key);

    let val_option = handle_error!(env, resp);

    match val_option {
        Some(val) => {
            let mut bin = OwnedBinary::new(val.len()).unwrap();
            bin.as_mut_slice().write(&val).unwrap();

            Ok((atoms::ok(), bin.release(env).encode(env)).encode(env))
        }
        None => Ok(atoms::not_found().encode(env)),
    }
}

fn get_cf<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.read().unwrap();
    
    let cf: String = args[1].decode()?;
    let cf_handle = db.cf_handle(&cf.as_str()).unwrap();
    
    let key = args[2].decode::<Binary>()?.as_slice();

    let resp = db.get_cf(cf_handle, key);

    let val_option = handle_error!(env, resp);

    match val_option {
        Some(val) => {
            let mut bin = OwnedBinary::new(val.len()).unwrap();
            bin.as_mut_slice().write(&val).unwrap();

            Ok((atoms::ok(), bin.release(env).encode(env)).encode(env))
        }
        None => Ok(atoms::not_found().encode(env)),
    }
}

fn batch_write<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let ops_iter: ListIterator = args[0].decode()?;
    let ops: Vec<BatchOperation> =
        ops_iter
            .map(|x| x.decode())
            .collect::<NifResult<Vec<BatchOperation>>>()?;

    let db_arc: ResourceArc<DBHandle> = args[1].decode()?;
    let db = db_arc.db.write().unwrap();

    let mut batch = WriteBatch::default();
    for op in ops {
        match op {
            BatchOperation::Put(key, val) => batch.put(key, val),
            BatchOperation::PutCf(cf, key, val) => {
                let cf_handle = db.cf_handle(&cf.as_str()).unwrap();
                
                batch.put_cf(cf_handle, key, val)
            },
            BatchOperation::Delete(key) => batch.delete(key),
            BatchOperation::DeleteCf(cf, key) => {
                let cf_handle = db.cf_handle(&cf.as_str()).unwrap();
                
                batch.delete_cf(cf_handle, key)
            },
        }
    }

    handle_error!(env, db.write(batch));
    Ok(atoms::ok().encode(env))
}

fn property<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.read().unwrap();

    let property: String = args[1].decode()?;

    let val_option = handle_error!(env, db.property_value(&property));

    match val_option {
        Some(val) => {
            Ok((atoms::ok(), (val as String).encode(env)).encode(env))
        }
        None => Ok(atoms::not_found().encode(env)),
    }
}

fn property_int<'a>(env: Env<'a>, args: &[Term<'a>]) -> NifResult<Term<'a>> {
    let db_arc: ResourceArc<DBHandle> = args[0].decode()?;
    let db = db_arc.deref().db.read().unwrap();

    let property: String = args[1].decode()?;

    let val_option = handle_error!(env, db.property_int_value(&property));

    match val_option {
        Some(val) => {
            Ok((atoms::ok(), (val as u64).encode(env)).encode(env))
        }
        None => Ok(atoms::not_found().encode(env)),
    }
}

rustler_export_nifs!(
    "Elixir.Rox.Native",
    [
        ("open", 3, open),
        ("create_cf", 3, create_cf),
        ("put", 4, put),
        ("put_cf", 5, put_cf),
        ("delete", 3, delete),
        ("delete_cf", 4, delete_cf),
        ("count", 1, count),
        ("list_cf", 2, list_cf),
        ("get", 2, get),
        ("get_cf", 3, get_cf),
        ("batch_write", 2, batch_write),
        ("property", 2, property),
        ("property_int", 2, property_int)
    ],
    Some(on_load)
);

fn on_load<'a>(env: Env<'a>, _load_info: Term<'a>) -> bool {
    resource_struct_init!(DBHandle, env);

    true
}
