//
//  SQLiteTests.swift
//  SmartMailCore
//
//  Created by Konstantyn Gominyuk on 15.01.2020.
//  Copyright © 2020 Readdle. All rights reserved.
//

import Foundation
#if SWIFT_PACKAGE
import SQLiteEE
#elseif os(Windows)
import sqlite
#else
import RDSQLite3
#endif
@testable import SwiftFMDB
import XCTest

class SQLiteTests: SCDBTempDBTests {
    
    func testEnabledFlags() {
        var shouldBeEnabledOptions = [
            "ENABLE_ATOMIC_WRITE",
            "ENABLE_UNLOCK_NOTIFY",
            "ENABLE_API_ARMOR",
            "ENABLE_COLUMN_METADATA",
            "ENABLE_FTS5",
            "ENABLE_RTREE",
        ]
#if !EnableUnicodeReplacement
        shouldBeEnabledOptions.append("ENABLE_ICU")
#endif
        
        for option in shouldBeEnabledOptions {
            XCTAssertEqual(sqlite3_compileoption_used(option), 1, "SQLite should be compiled with \(option)")
        }
    }
    
    func testEnabledUSleep() {
        // if disabled, sleep will take at least 1 second
        let date1 = Date()
        sqlite3_sleep(50)
        let date2 = Date()
        let timeInterval = date2.timeIntervalSince(date1)
        XCTAssertTrue(0.04 < timeInterval && timeInterval < 0.2, "timeInterval = \(timeInterval) should be around 50 milliseconds")
    }
    
    func testEnabledFTS5() {
        let result = db.executeUpdate(cached: false, "CREATE VIRTUAL TABLE email USING fts5(sender, title, body);")
        XCTAssertTrue(result)
    }
    
    func testEnabledRTree() {
        let result = db.executeUpdate(cached: false, "CREATE VIRTUAL TABLE demo_index USING rtree(id, minX, maxX, minY, maxY);")
        XCTAssertTrue(result)
    }
    
    func testEnabledColumnMetadata() {
        // SQLITE_ENABLE_COLUMN_METADATA
        
        XCTAssertTrue(db.executeUpdate(cached: false, "CREATE TABLE contacts (first_name TEXT);"))
        let insertRequest = "INSERT INTO contacts (first_name) VALUES(?);"
        XCTAssertTrue(db.executeUpdate(cached: false, insertRequest, "anton"))
        XCTAssertTrue(db.executeUpdate(cached: false, insertRequest, "boris"))
        
        let rs = db.executeQuery(cached: false, "SELECT * FROM contacts")

        let cname = sqlite3_column_table_name(rs!.statement.statement, 0)
        let name = String(cString: cname!)
        let expectedName = "contacts"
        XCTAssertEqual(name, expectedName, "Table name should be: \(expectedName). Actual: \(name)")
        rs?.close()
    }
    
    func testEnabledAPIArmor() {
        // When defined SQLITE_ENABLE_API_ARMOR, this C-preprocessor macro activates extra code
        // that attempts to detect misuse of the SQLite API, such as passing in NULL pointers
        // to required parameters or using objects after they have been destroyed.
        
        // this call will crash if api armor disabled
        let prepareResult = sqlite3_prepare_v2(db.db, "CREATE TABLE contacts (contact_id INTEGER PRIMARY KEY);", -1, nil, nil)
        XCTAssertEqual(prepareResult, SQLITE_MISUSE)
    }
    
    func testUnicodeFunctions() {
        // ICU and FMUnicode allow case-insensitive search in non-English text.
        
        XCTAssertTrue(db.executeUpdate(cached: false, "CREATE TABLE contacts (first_name TEXT);"))
        
        let insertRequest = "INSERT INTO contacts (first_name) VALUES(?);"
        XCTAssertTrue(db.executeUpdate(cached: false, insertRequest, "çoğunlukla"))
        XCTAssertTrue(db.executeUpdate(cached: false, insertRequest, "ÇOĞUNLUKLA"))
        
        let rs = db.executeQuery(cached: false, "SELECT * FROM contacts WHERE UPPER(first_name) = UPPER('ÇOğunlUKLA')")
        var rsCount = 0
        while rs!.next() {
            rsCount += 1
        }
        XCTAssertEqual(rsCount, 2, "It should select 2 rows. Actual count: \(rsCount)")
        rs?.close()

        XCTAssertEqual(stringValue(for: "SELECT UPPER('çoğunlukla')"), "ÇOĞUNLUKLA")
        XCTAssertEqual(stringValue(for: "SELECT LOWER('ÇOĞUNLUKLA')"), "çoğunlukla")
        XCTAssertNil(stringValue(for: "SELECT UPPER(NULL)"))
        XCTAssertEqual(stringValue(for: "SELECT UPPER('abc123')"), "ABC123")
        XCTAssertEqual(stringValue(for: "SELECT UPPER('i', 'tr_TR')"), "İ")
        XCTAssertEqual(stringValue(for: "SELECT LOWER('I', 'tr_TR')"), "ı")
        XCTAssertEqual(stringValue(for: "SELECT UPPER('i', 'en_US')"), "I")
        XCTAssertEqual(stringValue(for: "SELECT UPPER('i', NULL)"), "I")
        XCTAssertEqual(stringValue(for: "SELECT LOWER('I', '')"), "i")
        XCTAssertEqual(stringValue(for: "SELECT UPPER('i', 123)"), "I")
        XCTAssertEqual(stringValue(for: "SELECT UPPER(123, 'tr_TR')"), "123")
        XCTAssertEqual(stringValue(for: "SELECT hex(UPPER(char(97, 0, 98)))"), "410042")

        assertUnicodeCaseFunctionSurface(in: db)
    }

    func testUnicodeFunctionsInUTF16Database() {
        let utf16DB = makeUTF16Database()
        defer {
            XCTAssertTrue(utf16DB.close())
        }

        XCTAssertTrue(stringValue(in: utf16DB, for: "PRAGMA encoding")?.hasPrefix("UTF-16") ?? false)
        XCTAssertEqual(stringValue(in: utf16DB, for: "SELECT UPPER('çoğunlukla')"), "ÇOĞUNLUKLA")
        XCTAssertEqual(stringValue(in: utf16DB, for: "SELECT LOWER('ÇOĞUNLUKLA')"), "çoğunlukla")
        XCTAssertNil(stringValue(in: utf16DB, for: "SELECT UPPER(NULL)"))
        XCTAssertEqual(stringValue(in: utf16DB, for: "SELECT UPPER('i', 'tr_TR')"), "İ")
        XCTAssertEqual(stringValue(in: utf16DB, for: "SELECT LOWER('I', 'tr_TR')"), "ı")
        XCTAssertEqual(stringValue(in: utf16DB, for: "SELECT UPPER('i', NULL)"), "I")

        assertUnicodeCaseFunctionSurface(in: utf16DB)
        assertUnicodeLikeFunctionSurface(in: utf16DB)
    }

    func testUnicodeCaseFunctionsInExpressionIndex() {
        #if EnableUnicodeReplacement
        XCTAssertTrue(db.executeUpdate(cached: false, "CREATE TABLE indexed_contacts (first_name TEXT);"))
        XCTAssertTrue(db.executeUpdate(cached: false, "INSERT INTO indexed_contacts (first_name) VALUES ('çoğunlukla'), ('ÇOĞUNLUKLA'), ('different');"))
        XCTAssertTrue(db.executeUpdate(cached: false, "CREATE INDEX idx_indexed_contacts_upper_name ON indexed_contacts(UPPER(first_name));"))

        let plan = db.executeQuery(cached: false, "EXPLAIN QUERY PLAN SELECT * FROM indexed_contacts WHERE UPPER(first_name) = UPPER(?)", "çoğunlukla")
        XCTAssertNotNil(plan)
        defer { plan?.close() }

        var planDetails = [String]()
        while plan?.next() == true {
            planDetails.append(plan?.string(forColumnIndex: 3) ?? "")
        }
        XCTAssertTrue(planDetails.contains(where: { $0.contains("idx_indexed_contacts_upper_name") }), planDetails.joined(separator: "\n"))
        XCTAssertEqual(db.int(forQuery: "SELECT count(*) FROM indexed_contacts WHERE UPPER(first_name) = UPPER(?)", cached: false, "çoğunlukla"), 2)
        #endif
    }

    func testUnicodeLikeFunction() {
        XCTAssertEqual(db.bool(forQuery: "SELECT 'ÇOĞUNLUKLA' LIKE 'çoğun%'", cached: false), true)
        XCTAssertEqual(db.bool(forQuery: "SELECT 'ÇOĞUNLUKLA' LIKE 'çoğun_____'", cached: false), true)
        XCTAssertEqual(db.bool(forQuery: "SELECT '100% ÇOĞUNLUKLA' LIKE '100!% çoğun%' ESCAPE '!'", cached: false), true)
        XCTAssertEqual(db.bool(forQuery: "SELECT '100X ÇOĞUNLUKLA' LIKE '100!% çoğun%' ESCAPE '!'", cached: false), false)

        XCTAssertEqual(db.bool(forQuery: "SELECT like('çoğun%', 'ÇOĞUNLUKLA')", cached: false), true)
        XCTAssertEqual(db.bool(forQuery: "SELECT like('ÇOĞUNLUKLA', 'çoğun%')", cached: false), false)

        assertUnicodeLikeFunctionSurface(in: db)
    }

    private func assertUnicodeCaseFunctionSurface(in database: FMDatabase, file: StaticString = #file, line: UInt = #line) {
        XCTAssertEqual(stringValue(in: database, for: "SELECT UPPER('ß')", file: file, line: line), "SS", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT LOWER('İ')", file: file, line: line), "i\u{0307}", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT LOWER('İ', 'tr_TR')", file: file, line: line), "i", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT UPPER('ς')", file: file, line: line), "Σ", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT LOWER('Σ')", file: file, line: line), "σ", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT UPPER('i', 'az')", file: file, line: line), "İ", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT LOWER('I', 'az')", file: file, line: line), "ı", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT LOWER('Ì', 'lt')", file: file, line: line), "i\u{0307}\u{0300}", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT UPPER('ß', 'x_unknown_locale')", file: file, line: line), "SS", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT LOWER(UPPER('straße')) = 'straße'", file: file, line: line), "0", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT UPPER(UPPER('Straße')) = UPPER('Straße')", file: file, line: line), "1", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT UPPER('')", file: file, line: line), "", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT LOWER('A')", file: file, line: line), "a", file: file, line: line)
        XCTAssertEqual(stringValue(in: database, for: "SELECT UPPER(char(128512))", file: file, line: line), "😀", file: file, line: line)

        let longValue = String(repeating: "ß", count: 200_000)
        XCTAssertEqual(database.int(forQuery: "SELECT length(UPPER(?))", cached: false, longValue), 400_000, file: file, line: line)
        XCTAssertEqual(database.string(forQuery: "SELECT substr(UPPER(?), 1, 4)", cached: false, longValue), "SSSS", file: file, line: line)
        XCTAssertEqual(database.string(forQuery: "SELECT substr(UPPER(?), -4)", cached: false, longValue), "SSSS", file: file, line: line)
    }

    private func assertUnicodeLikeFunctionSurface(in database: FMDatabase, file: StaticString = #file, line: UInt = #line) {
        XCTAssertEqual(database.bool(forQuery: "SELECT '' LIKE ''", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT '' LIKE '%'", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT 'alpha' LIKE '%a'", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT 'alpha' LIKE 'a%'", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT 'alpha' LIKE 'a'", cached: false), false, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT 'Σ' LIKE 'ς'", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT '100% ÇOĞUNLUKLA' LIKE '100é% çoğun%' ESCAPE 'é'", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT 'foo!bar' LIKE 'foo!!bar' ESCAPE '!'", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT like('100é% çoğun%', '100% ÇOĞUNLUKLA', 'é')", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT like('foo!!bar', 'foo!bar', '!')", cached: false), true, file: file, line: line)
        XCTAssertEqual(database.bool(forQuery: "SELECT like(char(97, 0, 98), char(97, 0, 99))", cached: false), true, file: file, line: line)
    }

    private func makeUTF16Database(file: StaticString = #file, line: UInt = #line) -> FMDatabase {
        let utf16DB = FMDatabase(path: "")
        XCTAssertTrue(utf16DB.open(), file: file, line: line)
        XCTAssertTrue(utf16DB.executeUpdate(cached: false, "PRAGMA encoding = 'UTF-16';"), file: file, line: line)
        XCTAssertTrue(utf16DB.executeUpdate(cached: false, "CREATE TABLE utf16_encoding_probe (value TEXT);"), file: file, line: line)
        return utf16DB
    }

    public static var allTests = [
        ("testEnabledFlags", testEnabledFlags),
        ("testEnabledUSleep", testEnabledUSleep),
        ("testEnabledFTS5", testEnabledFTS5),
        ("testEnabledRTree", testEnabledRTree),
        ("testEnabledColumnMetadata", testEnabledColumnMetadata),
        ("testEnabledAPIArmor", testEnabledAPIArmor),
        ("testUnicodeFunctions", testUnicodeFunctions),
        ("testUnicodeFunctionsInUTF16Database", testUnicodeFunctionsInUTF16Database),
        ("testUnicodeCaseFunctionsInExpressionIndex", testUnicodeCaseFunctionsInExpressionIndex),
        ("testUnicodeLikeFunction", testUnicodeLikeFunction)
    ]
    
}
