//
//  FMDatabaseAdditionsTests.swift
//  SmartMailCore
//
//  Created by Andrew on 3/27/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Foundation
@testable import SwiftFMDB
import XCTest

class SCDatabaseAdditionsTests: SCDBTempDBTests {
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testFunkyTableNames() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table '234 fds' (foo text)"))
        XCTAssertFalse(db.hadError(), "table creation should have succeeded")
        if let rs = db.getTableSchema("234 fds") {
            XCTAssertTrue(rs.next(), "Schema should have succeded")
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hadError(), "There shouldn't be any errors")
    }
    
    func testBoolForQuery() {
        var result = db.bool(forQuery: "SELECT ? not null", cached: false, "") ?? false
        XCTAssertTrue(result, "Empty strings should be considered true")
        result = db.bool(forQuery: "SELECT ? not null", cached: false, Data()) ?? false
        XCTAssertTrue(result, "Empty data should be considered true")
    }
    
    func testIntForQuery() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t1 (a integer)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into t1 values (?)", 5))
        XCTAssertEqual(db.changes(), 1, "There should only be one change")
        let ia = db.int(forQuery: "select a from t1", cached: false)
        XCTAssertEqual(ia, 5, "foo")
    }
    
    func testDateForQuery() {
        let date = Date()
        XCTAssertTrue(db.executeUpdate(cached: false, "create table datetest (a double, b double, c double)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into datetest (a, b, c) values (?, ?, 0)", nil, date))
        let foo: Date? = db.date(forQuery: "select b from datetest where c = 0", cached: false)
        XCTAssertEqual(foo!.timeIntervalSince(date), 0.0, accuracy: 1.0, "Dates should be the same to within a second")
    }
    
    func testTableExists() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t4 (a text, b text)"))
        XCTAssertTrue(db.tableExists("t4"))
        XCTAssertFalse(db.tableExists("thisdoesntexist"))
        if let rs = db.getSchema() {
            while rs.next() {
                XCTAssertEqual(rs.string(forColumn: "type"), "table")
            }
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
    }
    
    func testColumnExists() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table nulltest (a text, b text)"))
        XCTAssertTrue(db.columnExists("a", inTableWithName: "nulltest"))
        XCTAssertTrue(db.columnExists("b", inTableWithName: "nulltest"))
        XCTAssertFalse(db.columnExists("c", inTableWithName: "nulltest"))
    }
    
    func testUserVersion() {
        db.userVersion = 12
        XCTAssertTrue(db.userVersion == 12)
    }

    func testApplicationID() {
        let id = db.applicationID() + 1
        db.setApplicationID(id)
        XCTAssertEqual(id, db.applicationID())
    }

    public static var allTests = [
        ("testFunkyTableNames", testFunkyTableNames),
        ("testBoolForQuery", testBoolForQuery),
        ("testIntForQuery", testIntForQuery),
        ("testDateForQuery", testDateForQuery),
        ("testTableExists", testTableExists),
        ("testColumnExists", testColumnExists),
        ("testUserVersion", testUserVersion),
        ("testApplicationID", testApplicationID)
    ]
}
