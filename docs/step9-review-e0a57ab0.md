# Step 9 audit for commit e0a57ab08c88b6f1436e01c8c45efd5afc7409c3

## Scope
Audit of the three requested properties for the non-RLE + tez-shuffle path.

## Conclusion
- Property 1: **Implemented for the targeted path** (`compositeFetch=true`, non-RLE).
- Property 2: **Implemented** (`assert !(tezOffsetRecord != null) || !isRleEnabled;` in `IFile.Reader`).
- Property 3: **Implicitly satisfied via fetch/header path, but not explicitly asserted in `IFile.Reader` constructor**.

## Evidence notes
1. `spillOffsetRecordMap`/`offsetRecordMap` is created when spill records are produced in non-RLE tez-shuffle flows in both `PipelinedSorter` and `UnorderedPartitionedKVWriter`, and is passed into cache-writing APIs.
2. `IFile.Reader` has the non-RLE assertion when `tezOffsetRecord` is present.
3. `TezOffsetRecord` is only deserialized in `ShuffleHeader.readFields()` when `compositeFetch` is true, then propagated through fetchers into `MapOutput`/`FetchedInput` and finally into `IFile.Reader`/`InMemoryReader`.
