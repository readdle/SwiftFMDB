//
//  FMDatabaseQueueTests.swift
//  SmartMailCore
//
//  Created by Andrew on 3/28/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Dispatch
import Foundation
@testable import SwiftFMDB
import XCTest
#if os(Android) || os(Windows)
import sqlite
#else
import RDSQLite3
#endif

class SCDatabaseQueueTests: SCDBTempDBTests {
    
    private var queue: FMDatabaseQueue!
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        
        queue = FMDatabaseQueue(withPath: databasePath)!
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        queue.close()
        
        super.tearDown()
    }
    
    public class override func populateDatabase(_ db: FMDatabase) {
        _ = db.executeUpdate(cached: false, "create table easy (a text)")
        _ = db.executeUpdate(cached: false, "create table qfoo (foo text)")
        _ = db.executeUpdate(cached: false, "insert into qfoo values ('hi')")
        _ = db.executeUpdate(cached: false, "insert into qfoo values ('hello')")
        _ = db.executeUpdate(cached: false, "insert into qfoo values ('not')")
    }
    
    func testQueueSelect() {
        queue.inDatabase({ adb in
            var count: Int = 0
            if let rsl = adb.executeQuery(cached: false, "select * from qfoo where foo like 'h%'") {
                while rsl.next() {
                    count += 1
                }
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
            XCTAssertEqual(count, 2)
            count = 0
            if let rsl = adb.executeQuery(cached: false, "select * from qfoo where foo like ?", "h%") {
                while rsl.next() {
                    count += 1
                }
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
            XCTAssertEqual(count, 2)
        })
    }
    
    func testReadOnlyQueue() {
        guard let queue2 = FMDatabaseQueue(withPath: databasePath, flags: SQLITE_OPEN_READONLY) else {
            return
        }
        queue2.inDatabase({ db2 in
            if let rs1 = db2.executeQuery(cached: false, "SELECT * FROM qfoo") {
                rs1.close()
                XCTAssertFalse((db2.executeUpdate(cached: false, "insert into easy values (?)", Int(3))), "Insert should fail because this is a read-only database")
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
        })
        queue2.close()
        // Check that when we re-open the database, it's still read-only
        queue2.inDatabase({ db2 in
            if let rs1 = db2.executeQuery(cached: false, "SELECT * FROM qfoo") {
                rs1.close()
                XCTAssertFalse((db2.executeUpdate(cached: false, "insert into easy values (?)", Int(3))), "Insert should fail because this is a read-only database")
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
        })
        queue2.close()
    }
    
    func testStressTest() {
        let ops: Int = 16
        DispatchQueue.concurrentPerform(iterations: ops, execute: { nby in
            // just mix things up a bit for demonstration purposes.
            if nby % 2 == 1 {
                Thread.sleep(forTimeInterval: 0.01)
                queue.inTransaction({ adb, rollback in
                    if let rsl = adb.executeQuery(cached: false, "select * from qfoo where foo like 'h%'") {
                        while rsl.next() {
                            // whatever.
                        }
                    }
                    else {
                        XCTFail("Should have a non-nil result set")
                    }
                })
            }
            if nby % 3 == 1 {
                Thread.sleep(forTimeInterval: 0.01)
            }
            queue.inTransaction({ adb, rollback in
                XCTAssertTrue(adb.executeUpdate(cached: false, "insert into qfoo values ('1')"))
                XCTAssertTrue(adb.executeUpdate(cached: false, "insert into qfoo values ('2')"))
                XCTAssertTrue(adb.executeUpdate(cached: false, "insert into qfoo values ('3')"))
            })
        })
        
        queue.close()
        queue.inDatabase({ adb in
            XCTAssertTrue(adb.executeUpdate(cached: false, "insert into qfoo values ('1')"))
        })
    }
    
    func testTransaction() {
        queue.inDatabase({ adb in
            XCTAssertTrue(adb.executeUpdate(cached: false, "create table transtest (a integer)"))
            XCTAssertTrue(adb.executeUpdate(cached: false, "insert into transtest values (1)"))
            XCTAssertTrue(adb.executeUpdate(cached: false, "insert into transtest values (2)"))
            var rowCount: Int = 0
            if let ars = adb.executeQuery(cached: false, "select * from transtest") {
                while ars.next() {
                    rowCount += 1
                }
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
            XCTAssertEqual(rowCount, 2)
        })
        queue.inTransaction({ adb, rollback in
            XCTAssertTrue(adb.executeUpdate(cached: false, "insert into transtest values (3)"))
            // uh oh!, something went wrong (not really, this is just a test
            rollback = true
            return
        })
        queue.inDatabase({ adb in
            var rowCount: Int = 0
            if let ars = adb.executeQuery(cached: false, "select * from transtest") {
                while ars.next() {
                    rowCount += 1
                }
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
            XCTAssertFalse(adb.hasOpenResultSets())
            XCTAssertEqual(rowCount, 2)
        })
    }

    public static var allTests = [
        ("testQueueSelect", testQueueSelect),
        ("testReadOnlyQueue", testReadOnlyQueue),
        ("testStressTest", testStressTest),
        ("testTransaction", testTransaction)
    ]
}
