# SwiftFMDB

SwiftFMDB is pure swift version of FMDB.

## Benchmarks

Unicode benchmark bodies are disabled in the default test suite. Run them
explicitly with the benchmark flag in Release configuration; comparing
against the SQLite ICU baseline from a Debug build is not fair because
Swift optimizations materially affect the FMUnicode timings:

```
swift test -c release -Xswiftc -DSWIFTFMDB_ENABLE_SQLITE_BENCHMARKS -Xswiftc -enable-testing --filter SQLiteBenchmark
```

## Unicode case-fold data

`Sources/FMUnicode/UnicodeFoldTable.swift` is generated from Unicode
[`CaseFolding.txt`](https://www.unicode.org/Public/UCD/latest/ucd/CaseFolding.txt).
The table lets the Swift LIKE matcher perform the same simple case
folding as ICU `u_foldCase()` with default flags, matching SQLite's ICU
LIKE behavior exactly.

### Regenerating

Run from the repository root with no arguments to download the latest
`CaseFolding.txt` and regenerate the bundled table:

```
python scripts/generate_fold_table.py
```

You can also specify the input file and output path directly:

```
python scripts/generate_fold_table.py --input path/to/CaseFolding.txt path/to/output.swift
```
