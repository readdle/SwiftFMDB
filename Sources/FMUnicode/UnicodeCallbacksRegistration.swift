//
//  UnicodeCallbacksRegistration.swift
//
//  SQLite callback registration for SwiftFMDB's Unicode replacement target.
//

#if FMUNICODE_ENABLE

import SQLiteEE

public func registerUnicodeCallbacks(in database: OpaquePointer?) -> Int32 {
    let extraFlags = SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS
    // Mirror SQLite ICU's iContext convention: a non-null user-data pointer
    // marks an `upper` registration; a null pointer marks a `lower`.
    let upperMapping = UnsafeMutableRawPointer(database)
    let caseFunctions: [(name: String, argc: Int32, encoding: Int32, userData: UnsafeMutableRawPointer?)] = [
        ("upper", 1, SQLITE_UTF16, upperMapping),
        ("lower", 1, SQLITE_UTF16, nil),
        ("upper", 2, SQLITE_UTF16, upperMapping),
        ("lower", 2, SQLITE_UTF16, nil),
        ("upper", 1, SQLITE_UTF8, upperMapping),
        ("lower", 1, SQLITE_UTF8, nil),
        ("upper", 2, SQLITE_UTF8, upperMapping),
        ("lower", 2, SQLITE_UTF8, nil),
    ]

    for function in caseFunctions {
        let flags = function.encoding | extraFlags
        let result = sqlite3_create_function(database, function.name, function.argc, flags, function.userData, unicodeCaseCallback, nil, nil)
        guard result == SQLITE_OK else {
            return result
        }
    }

    let likeFunctions: [(name: String, argc: Int32)] = [
        ("like", 2),
        ("like", 3),
    ]

    for function in likeFunctions {
        let result = sqlite3_create_function(database, function.name, function.argc, SQLITE_UTF8 | extraFlags, nil, unicodeLikeCallback, nil, nil)
        guard result == SQLITE_OK else {
            return result
        }
    }

    return SQLITE_OK
}

#endif
