# Parquet Byte Range Cache: Path To Option 2

## Current State

The first byte-range cache step is intentionally small. DataFusion parquet reads
flow through `QuickwitObjectStore`, which maps `ObjectStore::get_opts` calls to
Quickwit's `Storage::get_slice` and `Storage::get_all`. The option-1 cache wraps
the resolved `Storage` with the existing Quickwit read-through cache layer and
installs a dedicated DataFusion parquet memory-sized cache.

This gets warm-cache wins for repeated identical ranges without adding Foyer or
introducing a new disk cache. The cache instance is only installed on the
DataFusion object-store bridge, so it does not need suffix routing to stay away
from the native search caches. Cache keys are still scoped by storage URI before
they enter the cache, so two buckets with the same object path do not collide.

## Why Option 1 Is Not A Full Range Cache

`MemorySizedCache` is keyed by exact `(path, byte_range)`. It does not serve
subranges from larger cached ranges, and it does not assemble a request from
multiple cached ranges. `StorageWithCache` also has a binary cache API:

1. Ask the cache for the full requested range.
2. On miss, fetch the full requested range from storage.
3. Insert the returned full requested range.

That contract is enough for exact repeated ranges, but it cannot make a partial
hit fetch only the missing blocks. A real block range cache needs to own the read
plan, not only the value lookup.

This limitation may or may not matter for the metrics query shape. If DataFusion
usually reads whole selected column chunks for touched row groups, repeated
queries tend to produce identical ranges and option 1 should be enough for warm
hits. Block normalization is most useful when repeated queries fetch overlapping
but non-identical byte envelopes.

Examples where overlap can happen:

- Page pruning selects different but overlapping pages inside the same column
  chunk.
- Two projections touch a shared column plus different neighboring columns.
- object_store coalesces nearby ranges differently across two scans, for example
  `[col_a, col_b]` in one scan and `[col_b, col_c]` in another.
- A time window shifts enough to change the coalesced request boundaries while
  still touching some of the same row groups or pages.

Examples where block normalization buys little:

- Queries repeatedly hit the same row groups and same projected columns, yielding
  identical column-chunk ranges.
- Queries target disjoint time windows or disjoint row groups.
- The footer metadata cache already absorbs the dominant repeated reads.

An observed metrics query shape had 365 parquet files, 365 matched row groups out
of 1.61K, all 2.13K page-index pages matched, and roughly 508 MiB scanned out of
2.70 GiB. In DataFusion's parquet reader, `bytes_scanned` is the sum of requested
byte ranges, while `scan_efficiency_ratio` uses the partitioned file's object
size as the denominator. That ratio is not evidence of full-file reads. It points
more toward stable row-group and column-chunk range reads than overlapping
sub-page reads. For that shape, an exact bounded-range cache is a reasonable
first step; block normalization should wait for evidence that warm misses overlap
cached ranges.

## Foyer Versus Quickwit Cache

Option 2 should not force rich range semantics into Quickwit's existing
`StorageCache` trait. That trait is intentionally simple: exact-key lookup,
exact-key insert. It is a good fit for option 1, but it is the wrong API for
partial hits, block assembly, disk persistence, and fetch coalescing.

Foyer is a better substrate for the eventual full cache because it gives us:

- hybrid memory plus disk storage;
- byte-weighted eviction;
- async `get_or_fetch` style APIs;
- cache persistence and recovery controls;
- operational knobs that already match the dd-datafusion disk cache path.

Foyer does not by itself provide object-store range semantics. The dd-datafusion
`DiskCacheObjectStoreProvider` stores exact bounded ranges by key. A read of
`0..8MiB` does not automatically satisfy a later read of `4MiB..6MiB` unless the
wrapper normalizes those reads into shared keys. To get true overlap semantics,
Quickwit still needs a block-normalizing object-store or storage wrapper on top
of Foyer.

## Option 2 Target

The next step should be a Foyer-backed, block-normalized read-through cache:

1. Normalize each requested byte range into fixed-size blocks.
2. Look up all blocks by `(storage_scope, path, block_index)`.
3. Fetch only missing blocks from the underlying storage, coalescing adjacent
   missing blocks when useful.
4. Insert fetched blocks into the cache.
5. Assemble and return exactly the originally requested byte range.

This makes overlapping reads hit even when DataFusion changes the coalesced
request shape between queries.

## Proposed Shape

Prefer an object-store wrapper if we are taking the Foyer dependency:

```text
FoyerRangeCachedObjectStore {
  object_store: Arc<dyn ObjectStore>,
  cache: Arc<HybridCache<BlockKey, BlockValue>>,
  scope: String,
  block_size: usize,
  max_cacheable_request_bytes: usize,
}
```

The cache key should be normalized by block:

```text
BlockKey {
  cache_version: u16,
  store_scope: String,
  object_path: String,
  block_index: u64,
}
BlockValue {
  bytes: Bytes,
  object_size: Option<u64>,
  e_tag: Option<String>,
  version: Option<String>,
}
```

The cache value should be one fixed block except for the final object block,
which may be shorter. Keeping object metadata with the value lets the wrapper
rebuild a correct `GetResult` without inventing metadata on warm hits.

A storage-level wrapper is still possible, but once Foyer is in play the
object-store layer is cleaner. It can implement `get_opts` and, later,
`get_ranges` directly, matching how DataFusion and parquet perform vectored IO.

## Read Algorithm

For `get_slice(path, requested)`:

1. Return empty bytes for empty ranges.
2. Compute `first_block = requested.start / block_size`.
3. Compute `last_block = (requested.end - 1) / block_size`.
4. Look up every block in `HybridCache<BlockKey, BlockValue>`.
5. Group consecutive misses into storage fetch ranges aligned to block
   boundaries, clamped to object length if known.
6. Fetch missing groups with `storage.get_slice`.
7. Split fetched bytes back into block entries and insert each block.
8. Assemble the requested range from cached and fetched blocks.

For `get_all`, either bypass the cache or use this block path only when the file
is below a configured maximum. Whole-file parquet reads can be large enough to
pollute the range cache.

## DataFusion Interaction

DataFusion's parquet reader uses `ObjectStore::get_ranges` for vectored reads.
Because `QuickwitObjectStore` does not override `get_ranges`, object_store's
default implementation coalesces nearby ranges before calling `get_opts`.

DataFusion's parquet `bytes_scanned` metric is incremented from the requested
range lengths in `AsyncFileReader::get_bytes` and `get_byte_ranges`. It is not
the same as remote bytes after cache hits, and it is not the same as the ratio
denominator. The `scan_efficiency_ratio` denominator is set from
`PartitionedFile.object_meta.size`, so a ratio such as `508 MiB / 2.70 GiB`
means the parquet reader requested 508 MiB of ranges from files whose candidate
object sizes sum to 2.70 GiB.

An object-store-level Foyer cache can start by implementing only `get_opts`; it
will still see the default coalesced reads and normalize those into blocks. The
next improvement would be overriding `get_ranges` so the cache can inspect the
original vectored ranges before object_store coalesces them.

That suggests two Foyer phases:

1. Foyer-backed `get_opts` block cache.
2. Optional `get_ranges` override if measurements show
   default coalescing causes too much overfetch or hides useful smaller ranges.

## Correctness And Operational Concerns

- Cache keys must include cache version, storage scope, object path, and block
  index.
- Partial final blocks need careful EOF handling.
- Short reads before EOF should remain errors.
- Requests larger than a configured limit should bypass insertion.
- Concurrent identical misses should eventually use singleflight-style
  suppression to avoid duplicate downloads.
- Object-store preconditions and metadata must be preserved on cache hits.
- Cache metrics should distinguish logical requested bytes, hit bytes, miss
  bytes, fetched bytes, and inserted bytes.
- DataFusion `bytes_scanned` may remain unchanged; validation should use storage
  GET/download counters and cache hit/miss counters.

## Distance Estimate

A useful Foyer-backed object-store block cache is probably around 900-1,400
lines:

- 200-350 lines for Foyer config, initialization, and metrics.
- 350-550 lines for block planning, fetch, split, and assembly logic.
- 250-400 lines for edge-case tests around overlap, EOF, partial hits, metadata,
  oversized bypasses, and key scoping.
- 100-200 lines for Quickwit wiring and docs.

Adding singleflight and a custom `ObjectStore::get_ranges` implementation pushes
it closer to the upper end, with denser correctness concerns.

Before writing option 2, measure warm cache misses from option 1 and classify
them by overlap. If missed ranges often overlap cached ranges by a large byte
percentage, option 2 is justified. If misses are mostly exact repeats, option 1
has a bug or cache-key mismatch. If misses are mostly disjoint, block
normalization will add complexity without moving IO much.

## Recommendation

Treat option 2 as a follow-up PR and make it Foyer-backed if we decide to go past
option 1. The existing Quickwit cache is enough for exact repeated parquet ranges,
but a durable block range cache wants Foyer as the storage engine plus explicit
block normalization for overlap semantics. The decision should be data-driven:
run representative warm queries with option 1 first, then only build option 2 if
the remaining misses show substantial byte overlap.
