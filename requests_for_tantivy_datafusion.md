# Requests for tantivy-datafusion

Changes made during the quickwit integration, and upstream improvements needed
in tantivy and tantivy-datafusion to do this properly.

---

## Changes made directly to tantivy-datafusion

### 1. Async document retrieval — replaced sync StoreReader with Searcher::doc_async

**Files changed**: `src/unified/single_table_provider.rs`, `src/warmup.rs`

**What changed**: Removed sync document store reads from the `spawn_blocking`
batch generation path entirely. Document retrieval now happens asynchronously
after each batch exits `spawn_blocking`:

- `ChunkBuilder::build` no longer creates a `StoreReader` or calls
  `store_reader.get(doc_id)`. It produces batches with fast fields + scores
  only, excluding the `_document` column.
- When `_document` is projected, `_doc_id` is always included in the
  intermediate batch (even if the user didn't select it).
- A new `fill_document_column_async` function receives each intermediate batch,
  reads `_doc_id` values, calls `Searcher::doc_async()` for each document
  (which reads only the specific compressed block needed, asynchronously), and
  produces the final output batch with `_document` added.
- The two-channel pipeline: `spawn_blocking` → `raw_tx` → async doc fill → `tx`
  → output stream. DataFusion only sees complete batches.
- `warmup_document_store` is no longer called during warmup — `doc_async`
  handles its own I/O on demand.

**Why**: The original code opened a `StoreReader` inside `spawn_blocking` and
called `store_reader.get(doc_id)` synchronously. On quickwit's `StorageDirectory`
(which only supports async reads), this panics. The intermediate "fix" was to
pre-load the entire `.store` file into cache via `FileSlice::read_bytes_async`,
but this was wasteful — loading megabytes of compressed blocks for a query that
needs 5 documents.

**How it works now**: `Searcher::doc_async()` (which tantivy already provides
and quickwit already uses in `fetch_docs.rs`) reads individual store blocks via
async I/O. Each document fetch loads only the ~16KB compressed block containing
that document. No full-file preloading, no sync I/O for documents.

### 2. TopNComputer workaround (`src/util.rs`)

**What changed**: Replaced `Weight::for_each_pruning` + `TopNComputer::threshold`
access with a manual `Scorer` loop that iterates all matching docs.

**Why**: `TopNComputer::threshold` is `pub(crate)` in quickwit's tantivy fork. The
original code set and read it directly for Block-WAND pruning. Without access, we
fall back to scoring every matching doc — correct but O(n) instead of sub-linear
for top-K.

**Committed in**: `53cf993` (accidentally committed during this session alongside
other changes — needs review).

### 3. BucketEntries workaround (`src/unified/agg_exec.rs`)

**What changed**: Added a local `bucket_entries_iter()` function that
pattern-matches on `BucketEntries::Vec` and `BucketEntries::HashMap` variants.

**Why**: `BucketEntries::iter()` is private in quickwit's tantivy fork. The enum
itself is public. The workaround duplicates the iteration logic.

**Committed in**: `53cf993` (same accidental commit).

### 4. Let-chain to nested if (`src/warmup.rs`)

**What changed**: Converted `if let Expr::Column(column) = node && ...` to
nested `if let` / `if` blocks.

**Why**: The `let` chain syntax requires Rust edition 2024. The tantivy-datafusion
crate uses edition 2021.

**Committed in**: `53cf993` (same accidental commit).

### 5. Codec `build_single_table_scan_schema` panic fix (`src/codec.rs`)

**What changed**: Changed `index_of("_doc_id")?` (which used `?` in a non-Result
function) to `.expect()`.

**Why**: The function returns `ScanSchema` (not `Result`). The `?` was a compile
error. Changed to expect since `_doc_id` is guaranteed to be present in the
canonical schema.

**Committed in**: `53cf993` (same accidental commit).

### Note on commit `53cf993`

This commit ("clean up") was auto-committed during the quickwit integration session
and contains ~4000 lines of changes including the above workarounds mixed with
legitimate multi-split, codec, type coercion, and test additions. It needs to be
reviewed and potentially split into separate commits.

---

## What tantivy needs to expose

### 1. `TopNComputer::threshold` should be `pub`

Block-WAND pruning via `Weight::for_each_pruning` requires reading the current
threshold from `TopNComputer` after each `push()`. Without this, the DataFusion
top-K path degrades to a full scan with in-memory sorting.

**Upstream ask**: Make `TopNComputer::threshold` public, or add a
`pub fn threshold(&self) -> Option<Score>` getter.

### 2. `BucketEntries::iter()` should be `pub`

The aggregation result types (`BucketEntries<BucketEntry>`, `BucketEntries<RangeBucketEntry>`)
are public enums, but the only way to iterate their contents requires matching on the
variants manually. This is a trivial visibility fix.

**Upstream ask**: Change `fn iter` to `pub fn iter` on `BucketEntries<T>`.

### 3. Document store: FIXED — now uses Searcher::doc_async

Tantivy already has `StoreReader::get_async` and `Searcher::doc_async`. This was
fixed in tantivy-datafusion (see change #1 above). The `warmup_document_store`
function is no longer called. Document retrieval uses `doc_async` which reads
only the specific compressed block needed per document.

### 4. `SegmentComponent` and segment file path construction

Building the `.store` file path requires knowing tantivy's naming convention
(`{segment_uuid}.store`). This is fragile — if tantivy changes the convention,
the warmup breaks silently. (Note: with change #1 above, the `.store` warmup
path in `warmup.rs` is now dead code since `warmup_document_store` is no longer
called. But the function still exists and uses this fragile path construction.)

**Upstream ask**: Expose `Segment::open_read(SegmentComponent::Store)` or an equivalent
on `SegmentReader` that returns a `FileSlice` for the store file.

### 5. Schema introspection without sync I/O

`tantivy_schema_to_arrow_from_index` needs to detect multi-valued fields by inspecting
segment columnar data (`field_cardinality`). This triggers sync reads on the `.fast` files.
For quickwit's storage-backed directories, this causes `StorageDirectory` errors during
planning.

**Upstream ask**: Either:
- Store field cardinality (single vs. multi-valued) in the segment meta (no I/O needed)
- Expose an async `SegmentReader::field_cardinality_async` method
- Or allow `IndexMeta` to carry schema-level cardinality hints set at indexing time

### 6. `Directory` trait needs `open_read` on `ManagedDirectory` without trait import

`Index::directory()` returns `&ManagedDirectory`. Calling `open_read` requires importing
`tantivy::directory::Directory` explicitly because the method is on the trait, not
inherent. This is a minor ergonomic issue but causes confusion.

---

## What tantivy-datafusion should improve

### 1. `SingleTableProvider::new` should not compute partition stats eagerly

Creating a `SingleTableProvider` from an `Index` immediately reads fast field
min/max values from every segment for partition pruning statistics. This triggers
sync I/O on storage-backed directories. Stats should be lazy or opt-in.

### 2. The codec should carry the index storage URI

The `OpenerMetadata` struct only has `identifier`, `tantivy_schema`, `segment_sizes`,
`footer_start/end`, and `multi_valued_fields`. For distributed execution, the worker
needs to know *where* the index lives (the storage URI) to open splits. Currently the
quickwit integration encodes this in the `identifier` field as `{uri}\0{split_id}`,
which is a hack.

**Fix**: Add an `index_uri: String` field to `OpenerMetadata` and serialize it in the
codec.

### 3. Warmup should be configurable per-query

Currently warmup is all-or-nothing based on `IndexOpener::needs_warmup()`. A query
that only reads fast fields doesn't need document store warmup. A query that only
does aggregations doesn't need inverted index warmup. The warmup should be driven
by what the query actually needs (projected columns, filter types, document retrieval).

### 4. The DDL schema mapping gap

When a user declares a schema via `CREATE EXTERNAL TABLE ... STORED AS tantivy`,
the declared column positions don't match tantivy's internal schema (which has
`_doc_id`, `_segment_ord` as the first two columns). This causes wrong-column bugs.
The `SingleTableProvider` needs a projection mapping layer between external and
internal schemas, or DDL registration should validate/reorder columns to match.
