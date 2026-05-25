//
//  UnicodeCase.swift
//
//  SQLite upper/lower callbacks for SwiftFMDB's Unicode replacement target.
//

#if FMUNICODE_ENABLE

import Foundation
import SQLiteEE

private let sqliteTransientDestructor = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public func unicodeCaseCallback(_ context: OpaquePointer?,
                                _ argc: Int32,
                                _ argv: UnsafeMutablePointer<OpaquePointer?>?) {
    guard let argv = argv else {
        sqlite3_result_null(context)
        return
    }

    guard let value = stringFromSQLiteTextValue(argv[0]) else {
        sqlite3_result_null(context)
        return
    }

    let locale = argc == 2 ? locale(fromSQLiteValue: argv[1]) : nil
    let result: String
    if sqlite3_user_data(context) != nil, let locale = locale {
        result = value.uppercased(with: locale)
    }
    else if sqlite3_user_data(context) != nil {
        result = value.uppercased()
    }
    else if let locale = locale {
        result = value.lowercased(with: locale)
    }
    else {
        result = value.lowercased()
    }

    let byteCount = Int32(result.utf8.count)
    result.withCString { resultText in
        sqlite3_result_text(context, resultText, byteCount, sqliteTransientDestructor)
    }
}

private func stringFromSQLiteTextValue(_ value: OpaquePointer?) -> String? {
    guard let value = value, let text = sqlite3_value_text(value) else {
        return nil
    }
    let bytes = UnsafeBufferPointer(start: text, count: Int(sqlite3_value_bytes(value)))
    return String(decoding: bytes, as: UTF8.self)
}

private func locale(fromSQLiteValue value: OpaquePointer?) -> Locale? {
    // ICU treats NULL and empty locale arguments as the general, non-locale-specific mapping.
    guard let localeIdentifier = stringFromSQLiteTextValue(value), localeIdentifier.isEmpty == false else {
        return nil
    }

    return Locale(identifier: localeIdentifier)
}

#endif
