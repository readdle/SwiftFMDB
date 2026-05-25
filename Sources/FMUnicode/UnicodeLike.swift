//
//  UnicodeLike.swift
//
//  ICU-style Unicode-aware LIKE matcher used by SwiftFMDB on platforms
//  where the SQLite ICU extension is not linked. The control flow is
//  shaped after SQLite's ICU extension (icuLikeFunc / icuLikeCompare in
//  ext/icu/icu.c) and covers UTF-8 traversal, '%' / '_' wildcard
//  handling, escape validation, recursion on '%', and SQLite result /
//  error reporting. Simple Unicode case folding is performed via the
//  bundled UnicodeFoldTable (generated from Unicode's
//  CaseFolding.txt status C+S rows), so the hot path never enters
//  Foundation, CoreFoundation, or ICU.
//
//  Hot helpers operate on UnsafePointer<UInt8> cursors that come
//  straight from sqlite3_value_text, so the matcher never allocates a
//  Swift String or crosses the String API boundary while comparing a
//  row.
//

#if FMUNICODE_ENABLE

import SQLiteEE

/// Translation table mirroring SQLite ICU's `icuUtf8Trans1` (same bytes
/// as core `sqlite3Utf8Trans1` in utf8.c). Indexed by
/// `(leadingByte - 0xC0)`, it returns the value bits of the leading byte
/// of a multibyte UTF-8 sequence.
private let utf8Trans1: [UInt8] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
    0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
]

/// Read the next UTF-8 scalar from `cursor`, advancing past the bytes that
/// were consumed. Mirrors SQLite ICU's `SQLITE_ICU_READ_UTF8` macro.
@inline(__always)
private func readUTF8(_ cursor: inout UnsafePointer<UInt8>) -> UInt32 {
    let leading = cursor.pointee
    cursor = cursor.successor()
    if leading < 0xC0 {
        return UInt32(leading)
    }
    var scalar = UInt32(utf8Trans1[Int(leading) &- 0xC0])
    while (cursor.pointee & 0xC0) == 0x80 {
        scalar = (scalar << 6) &+ (UInt32(cursor.pointee) & 0x3F)
        cursor = cursor.successor()
    }
    return scalar
}

/// Skip the next UTF-8 scalar in `cursor`. Mirrors SQLite ICU's
/// `SQLITE_ICU_SKIP_UTF8` macro.
@inline(__always)
private func skipUTF8(_ cursor: inout UnsafePointer<UInt8>) {
    let leading = cursor.pointee
    cursor = cursor.successor()
    if leading >= 0xC0 {
        while (cursor.pointee & 0xC0) == 0x80 {
            cursor = cursor.successor()
        }
    }
}

/// Simple Unicode case folding equivalent to ICU's `u_foldCase()` with
/// default flags. Mirrors the C matcher's `swiftfmdb_unicode_simple_fold`.
@inline(__always)
private func simpleFold(_ codepoint: UInt32, _ foldTable: UnsafeBufferPointer<UnicodeFoldPair>) -> UInt32 {
    // ASCII upper-case letters map via a fixed delta and dominate real
    // LIKE workloads, so handle them inline before the table search.
    if codepoint < 0x80 {
        if codepoint >= 0x41 && codepoint <= 0x5A {
            return codepoint &+ 0x20
        }
        return codepoint
    }

    var low = 0
    var high = foldTable.count
    while low < high {
        let mid = low &+ ((high &- low) >> 1)
        let entry = foldTable[mid]
        if entry.source == codepoint {
            return entry.folded
        }
        if entry.source < codepoint {
            low = mid &+ 1
        }
        else {
            high = mid
        }
    }
    return codepoint
}

/// Pattern/string comparison loop, shaped after SQLite ICU's
/// `icuLikeCompare`. `uEsc` is the escape code point or 0 if no escape
/// was supplied.
private func unicodeLikeCompare(_ pattern: UnsafePointer<UInt8>,
                                _ string: UnsafePointer<UInt8>,
                                _ uEsc: UInt32,
                                _ foldTable: UnsafeBufferPointer<UnicodeFoldPair>) -> Bool {
    let swiftfmdbMatchOne: UInt32 = 0x5F // '_'
    let swiftfmdbMatchAll: UInt32 = 0x25 // '%'

    var patternCursor = pattern
    var stringCursor = string
    var prevEscape = false

    while true {
        let uPattern = readUTF8(&patternCursor)
        if uPattern == 0 { break }

        if uPattern == swiftfmdbMatchAll && !prevEscape && uPattern != uEsc {
            var headByte = patternCursor.pointee
            while headByte == swiftfmdbMatchAll || headByte == swiftfmdbMatchOne {
                if headByte == swiftfmdbMatchOne {
                    if stringCursor.pointee == 0 {
                        return false
                    }
                    skipUTF8(&stringCursor)
                }
                patternCursor = patternCursor.successor()
                headByte = patternCursor.pointee
            }

            if patternCursor.pointee == 0 {
                return true
            }

            while stringCursor.pointee != 0 {
                if unicodeLikeCompare(patternCursor, stringCursor, uEsc, foldTable) {
                    return true
                }
                skipUTF8(&stringCursor)
            }
            return false
        }
        else if uPattern == swiftfmdbMatchOne && !prevEscape && uPattern != uEsc {
            if stringCursor.pointee == 0 {
                return false
            }
            skipUTF8(&stringCursor)
        }
        else if uPattern == uEsc && !prevEscape {
            prevEscape = true
        }
        else {
            let uString = readUTF8(&stringCursor)
            if simpleFold(uPattern, foldTable) != simpleFold(uString, foldTable) {
                return false
            }
            prevEscape = false
        }
    }

    return stringCursor.pointee == 0
}

/// SQLite scalar function callback for the SwiftFMDB Unicode-aware
/// LIKE matcher. SQL maps `value LIKE pattern` to `like(pattern, value)`
/// and `value LIKE pattern ESCAPE escape` to
/// `like(pattern, value, escape)`, matching SQLite's argument-order
/// convention.
public func unicodeLikeCallback(_ context: OpaquePointer?,
                                _ argc: Int32,
                                _ argv: UnsafeMutablePointer<OpaquePointer?>?) {
    let swiftfmdbMaxLikePatternLength: Int32 = 50_000

    guard let argv = argv else {
        return
    }

    if sqlite3_value_bytes(argv[0]) > swiftfmdbMaxLikePatternLength {
        sqlite3_result_error(context, "LIKE or GLOB pattern too complex", -1)
        return
    }

    guard let patternBytes = sqlite3_value_text(argv[0]),
          let stringBytes = sqlite3_value_text(argv[1]) else {
        return
    }

    var uEsc: UInt32 = 0
    if argc == 3 {
        guard let escapeBytes = sqlite3_value_text(argv[2]) else {
            return
        }
        let escapeByteCount = sqlite3_value_bytes(argv[2])
        var escapeCursor = UnsafePointer<UInt8>(escapeBytes)
        let escapeStart = escapeCursor
        uEsc = readUTF8(&escapeCursor)
        let consumed = escapeCursor - escapeStart
        if consumed != Int(escapeByteCount) {
            sqlite3_result_error(context, "ESCAPE expression must be a single character", -1)
            return
        }
        // uEsc is intentionally NOT folded; mirrors the C matcher and
        // SQLite ICU's icuLikeCompare, which compares the raw pattern
        // code point against the raw escape code point before falling
        // through to the folded equality check.
    }

    let matched = unicodeFoldTable.withUnsafeBufferPointer { foldTable in
        unicodeLikeCompare(patternBytes, stringBytes, uEsc, foldTable)
    }
    sqlite3_result_int(context, matched ? 1 : 0)
}

#endif
