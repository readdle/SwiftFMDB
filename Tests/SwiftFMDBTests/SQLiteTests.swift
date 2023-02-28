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
#else
import RDSQLite3
#endif
@testable import SwiftFMDB
import XCTest

class SQLiteTests: SCDBTempDBTests {
    
    func testEnabledFlags() {
        let shouldBeEnabledOptions = ["ENABLE_ATOMIC_WRITE", "ENABLE_UNLOCK_NOTIFY", "ENABLE_API_ARMOR", "ENABLE_COLUMN_METADATA", "ENABLE_FTS5", "ENABLE_ICU", "ENABLE_RTREE"]
        
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
    
    func testEnabledICU() {
        // ICU allows using case insensitive search in a non-english language
        
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
    }

    public static var allTests = [
        ("testEnabledUSleep", testEnabledUSleep),
        ("testEnabledFTS5", testEnabledFTS5),
        ("testEnabledRTree", testEnabledRTree),
        ("testEnabledColumnMetadata", testEnabledColumnMetadata),
        ("testEnabledAPIArmor", testEnabledAPIArmor),
        ("testEnabledICU", testEnabledICU)
    ]
    
}
