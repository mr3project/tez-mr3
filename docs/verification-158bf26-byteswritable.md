# Verification for commit 158bf26fb346c6d91a8401da6fd987ce385ce078

## Scope checked
- Commit files touched:
  - `tez-common/src/main/java/org/apache/hadoop/io/BytesWritable.java`
  - `tez-runtime-library/...` (runtime-library only for all remaining files)

## Property 1: Existing code outside `tez-runtime-library` keeps same byte-copy behavior (practically)

### Findings
1. The legacy mutating APIs that callers typically use (`set(byte[], offset, length)`, `readFields`, constructor overloads without explicit offset) still end up with logical bytes copied into the writable-owned backing region beginning at index 0.
   - `set(byte[], offset, length)` still performs `System.arraycopy(..., bytes, 0, size)`.
   - `readFields` still reads into `bytes` from index 0.
   - Constructors that existed before (`BytesWritable(byte[])`, `BytesWritable(byte[], int)`) still route through offset `0`.
2. Search across the repository shows the newly added offset-aware zero-copy API (`setDirect`) is only used from `tez-runtime-library`.
3. Therefore, code outside `tez-runtime-library` that uses the original/legacy interaction style continues to observe equivalent copy semantics in practice.

### Caveat (important)
- `BytesWritable` in `tez-common` is globally changed and now supports non-zero offsets.
- If any external caller newly starts constructing with non-zero offsets or receives a `BytesWritable` that was populated via `setDirect`, then reading with `getBytes()` from index 0 is no longer equivalent to old assumptions.
- This caveat does not invalidate property (1) for existing call paths that continue using the historical APIs and usage patterns.

## Property 2: `tez-runtime-library` avoids unnecessary intermediate byte-array copies for in-memory shuffle key/value slices

### Findings
1. `InMemoryReader` now uses `setDirect(data, pos, len)` for keys and values, replacing explicit `setSize + System.arraycopy` copy paths.
   - This is the key zero-copy change for in-memory shuffle slices.
2. Runtime serialization/writer/comparator paths were updated to honor `BytesWritable` offsets by using `getBytesRaw()` + `getOffset()`.
   - This preserves correctness when `BytesWritable` is backed by a larger array with a non-zero logical offset.
3. Net effect: in-memory shuffle read path avoids intermediate key/value byte-array copies, while downstream runtime-library components are made offset-aware so they consume those slices correctly.

## Why copy is still used in `ValuesIterator` / `SerializationContext`

- Those call paths read from `DataInputBuffer` instances that may reuse mutable backing arrays across records.
- `setDirect(...)` would alias that shared/reused storage and can make previously returned `BytesWritable` objects observe mutated bytes.
- `expandIfNecessary(...) + setDirect(...)` does not fix this, because `setDirect(...)` still points at the source array rather than copying into writable-owned memory.
- The runtime-library call sites now copy explicitly via `expandIfNecessary(...) + System.arraycopy(...)` (without calling `BytesWritable.set(...)`) to preserve safety while following module-local style preference.

## Conclusion
- Property (1): **Holds for existing non-runtime-library usage patterns** (legacy APIs and copy behavior remain practically equivalent).
- Property (2): **Holds** (runtime-library now uses direct backing slices for in-memory shuffle key/value records and updated consumers to avoid forcing intermediate copies).
