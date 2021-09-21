//
//  FMDatabasePoolTests.swift
//  SmartMailCore
//
//  Created by Andrew on 3/28/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Dispatch
import Foundation
@testable import SwiftFMDB
import XCTest

class SCDatabasePoolTests: SCDBTempDBTests, FMDatabasePoolDelegate {
    
    private var pool: FMDatabasePool!
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        
        pool = FMDatabasePool(path: databasePath)
        pool.delegate = self
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        for db in (pool.databaseInPool + pool.databaseOutPool) {
            XCTAssertFalse(db.hasOpenResultSets(), "Database still has open result sets on tear down: \(db.databasePath ?? "<no path>")")
            _ = db.close()
        }
        
        pool.releaseAllDatabases()
        super.tearDown()
    }
    
    public class override func populateDatabase(_ db: FMDatabase) {
        _ = db.executeUpdate(cached: false, "create table easy (a text)")
        _ = db.executeUpdate(cached: false, "create table easy2 (a text)")
        _ = db.executeUpdate(cached: false, "insert into easy values (?)", 1001)
        _ = db.executeUpdate(cached: false, "insert into easy values (?)", 1002)
        _ = db.executeUpdate(cached: false, "insert into easy values (?)", 1003)
        _ = db.executeUpdate(cached: false, "create table likefoo (foo text)")
        _ = db.executeUpdate(cached: false, "insert into likefoo values ('hi')")
        _ = db.executeUpdate(cached: false, "insert into likefoo values ('hello')")
        _ = db.executeUpdate(cached: false, "insert into likefoo values ('not')")
    }
    
    func databasePool(_ pool: FMDatabasePool, shouldAddDatabaseToPool database: FMDatabase) -> Bool {
        database.maxBusyRetryTimeInterval = 10
        // [database setCrashOnErrors:YES];
        return true
    }
    
    func databasePool(_ pool: FMDatabasePool, didAdd database: FMDatabase) {
    
    }
    
    func testPoolIsInitiallyEmpty() {
        XCTAssertEqual(pool.countOfOpenDatabases(), 0, "Pool should be empty on creation")
    }
    
    func testDatabaseCreation() {
        var db1: FMDatabase?
        pool.inDatabase({ db in
            XCTAssertEqual(self.pool.countOfOpenDatabases(), 1, "Should only have one database at this point")
            db1 = db
        })
        pool.inDatabase({ db in
            XCTAssertTrue(db === db1)
            self.pool.inDatabase({ db2 in
                XCTAssertTrue(db2 !== db, "We should get a different database because the first was in use.")
            })
        })
        XCTAssertEqual(pool.countOfOpenDatabases(), 2)
        pool.releaseAllDatabases()
        XCTAssertEqual(pool.countOfOpenDatabases(), 0, "We should be back to zero databases again")
    }
    
    func testCheckedInCheckoutOutCount() {
        pool.inDatabase({ aDb in
            guard let aDb = aDb else {
                XCTFail("database == nil")
                return
            }
            XCTAssertEqual(self.pool.countOfCheckedInDatabases(), 0)
            XCTAssertEqual(self.pool.countOfCheckedOutDatabases(), 1)
            XCTAssertTrue(aDb.executeUpdate(cached: false, "insert into easy (a) values (?)", "hi"))
            
            // just for fun.
            if let rs = aDb.executeQuery(cached: false, "select * from easy") {
                XCTAssertTrue(rs.next())
                while rs.next() {
                }
                // whatevers.
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
            
            XCTAssertEqual(self.pool.countOfOpenDatabases(), 1)
            XCTAssertEqual(self.pool.countOfCheckedInDatabases(), 0)
            XCTAssertEqual(self.pool.countOfCheckedOutDatabases(), 1)
        })
        XCTAssertEqual(pool.countOfOpenDatabases(), 1)
    }
    
    func testMaximumDatabaseLimit() {
        pool.maximumNumberOfDatabasesToCreate = 2
        pool.inDatabase({ db in
            self.pool.inDatabase({ db2 in
                self.pool.inDatabase({ db3 in
                    XCTAssertEqual(self.pool.countOfOpenDatabases(), Int(2))
                    XCTAssertNil(db3, "The third database must be nil because we have a maximum of 2 databases in the pool")
                })
            })
        })
    }
    
    func testTransaction() {
        pool.inTransaction({ adb, rollback in
            guard let adb = adb else {
                XCTFail("adb == nil")
                return
            }
            XCTAssertTrue(adb.executeUpdate(cached: false, "insert into easy values (?)", 1001))
            XCTAssertTrue(adb.executeUpdate(cached: false, "insert into easy values (?)", 1002))
            XCTAssertTrue(adb.executeUpdate(cached: false, "insert into easy values (?)", 1003))
            XCTAssertEqual(self.pool.countOfOpenDatabases(), 1)
            XCTAssertEqual(self.pool.countOfCheckedInDatabases(), 0)
            XCTAssertEqual(self.pool.countOfCheckedOutDatabases(), 1)
        })
        XCTAssertEqual(pool.countOfOpenDatabases(), 1)
        XCTAssertEqual(pool.countOfCheckedInDatabases(), 1)
        XCTAssertEqual(pool.countOfCheckedOutDatabases(), 0)
    }
    
    func testSelect() {
        pool.inDatabase({ db in
            guard let db = db else {
                XCTFail("db == nil")
                return
            }
            if let rs = db.executeQuery(cached: false, "select * from easy where a = ?", 1001) {
                XCTAssertTrue(rs.next())
                XCTAssertFalse(rs.next())
            }
        })
    }
    
    func testTransactionRollback() {
        pool.inDeferredTransaction({ adb, rollback in
            guard let adb = adb else {
                XCTFail("adb == nil")
                return
            }
            XCTAssertTrue((adb.executeUpdate(cached: false, "insert into easy values (?)", Int(1004))))
            XCTAssertTrue((adb.executeUpdate(cached: false, "insert into easy values (?)", Int(1005))))

            if let rs = adb.executeQuery(cached: false, "select * from easy where a == '1004'") {
                XCTAssertTrue(rs.next(), "1004 should be in database")
                XCTAssertFalse(rs.next(), "Should be single record in result set")
                rs.close()
            }
            else {
                XCTFail("Failed to execute query")
            }

            rollback = true
        })
        pool.inDatabase({ db in
            guard let db = db else {
                XCTFail("db == nil")
                return
            }
            
            if let rs = db.executeQuery(cached: false, "select * from easy where a == '1004'") {
                XCTAssertFalse(rs.next(), "1004 should be in database")
                rs.close()
            }
            else {
                XCTFail("Failed to execute query")
            }
        })
        XCTAssertEqual(pool.countOfOpenDatabases(), Int(1))
        XCTAssertEqual(pool.countOfCheckedInDatabases(), Int(1))
        XCTAssertEqual(pool.countOfCheckedOutDatabases(), Int(0))
    }
    
    func testSavepoint() {
        let err: Error? = pool.inSavePoint({ db, rollback in
            guard let db = db else {
                XCTFail("db == nil")
                return
            }
            XCTAssertTrue(db.executeUpdate(cached: false, "insert into easy values (?)", 1006))
        })
        XCTAssertNil(err)
    }
    
    func testNestedSavepointRollback() {
        let err = pool.inSavePoint({ adb, rollback in
            guard let adb = adb else {
                XCTFail("adb == nil")
                return
            }
            XCTAssertFalse(adb.hadError())
            XCTAssertTrue((adb.executeUpdate(cached: false, "insert into easy values (?)", 1009)))
            XCTAssertNil(adb.inSavePoint({ arollback in
                XCTAssertTrue((adb.executeUpdate(cached: false, "insert into easy values (?)", 1010)))
                arollback = true
            }))
        })
        XCTAssertNil(err)
        pool.inDatabase({ db in
            guard let db = db else {
                XCTFail("db == nil")
                return
            }
            if let rs = db.executeQuery(cached: false, "select * from easy where a = ?", 1009) {
                XCTAssertTrue(rs.next())
                XCTAssertFalse(rs.next())
                // close it out.
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
            if let rs = db.executeQuery(cached: false, "select * from easy where a = ?", 1010) {
                XCTAssertFalse(rs.next())
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
        })
    }
    
    func testLikeStringQuery() {
        pool.inDatabase({ db in
            guard let db = db else {
                XCTFail("db == nil")
                return
            }
            var count: Int = 0
            if let rsl = db.executeQuery(cached: false, "select * from likefoo where foo like 'h%'") {
                while rsl.next() {
                    count += 1
                }
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
            XCTAssertEqual(count, 2)
            
            count = 0
            if let rsl = db.executeQuery(cached: false, "select * from likefoo where foo like ?", "h%") {
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
    
    func testStressTest() {
        let ops: Int = 128
        DispatchQueue.concurrentPerform(iterations: ops, execute: { nby in
            // just mix things up a bit for demonstration purposes.
            if nby % 2 == 1 {
                Thread.sleep(forTimeInterval: 0.001)
            }
            pool.inDatabase({ db in
                guard let db = db else {
                    XCTFail("db == nil")
                    return
                }
                var i: Int = 0
                if let rsl = db.executeQuery(cached: false, "select * from likefoo where foo like 'h%'") {
                    while rsl.next() {
                        i += 1
                        if nby % 3 == 1 {
                            Thread.sleep(forTimeInterval: 0.0005)
                        }
                    }
                }
                else {
                    XCTFail("Should have a non-nil result set")
                }
                XCTAssertEqual(i, 2)
            })
        })
        XCTAssert(pool.countOfOpenDatabases() < 64, "There should be significantly less than 64 databases after that stress test")
    }
    
    func testReadWriteStressTest() {
        let ops: Int = 16
        DispatchQueue.concurrentPerform(iterations: ops, execute: { nby in
            // just mix things up a bit for demonstration purposes.
            if nby % 2 == 1 {
                Thread.sleep(forTimeInterval: 0.01)
                pool.inTransaction({ db, rollback in
                    guard let db = db else {
                        XCTFail("db == nil")
                        return
                    }
                    if let rsl = db.executeQuery(cached: false, "select * from likefoo where foo like 'h%'") {
                        while rsl.next() {
                            // whatever.
                        }
                    }
                })
            }
            if nby % 3 == 1 {
                Thread.sleep(forTimeInterval: 0.01)
            }
            pool.inTransaction({ db, rollback in
                guard let db = db else {
                    XCTFail("db == nil")
                    return
                }
                XCTAssertTrue(db.executeUpdate(cached: false, "insert into likefoo values ('1')"))
                XCTAssertTrue(db.executeUpdate(cached: false, "insert into likefoo values ('2')"))
                XCTAssertTrue(db.executeUpdate(cached: false, "insert into likefoo values ('3')"))
            })
        })
        pool.releaseAllDatabases()
        pool.inDatabase({ db in
            guard let db = db else {
                XCTFail("db == nil")
                return
            }
            XCTAssertTrue(db.executeUpdate(cached: false, "insert into likefoo values ('1')"))
        })
    }

    public static var allTests = [
        ("testSelect", testSelect),
        ("testTransaction", testTransaction),
        ("testMaximumDatabaseLimit", testMaximumDatabaseLimit),
        ("testCheckedInCheckoutOutCount", testCheckedInCheckoutOutCount),
        ("testDatabaseCreation", testDatabaseCreation),
        ("testPoolIsInitiallyEmpty", testPoolIsInitiallyEmpty),
        ("testTransactionRollback", testTransactionRollback),
        ("testSavepoint", testSavepoint),
        ("testNestedSavepointRollback", testNestedSavepointRollback),
        ("testLikeStringQuery", testLikeStringQuery),
        ("testStressTest", testStressTest),
        ("testReadWriteStressTest", testReadWriteStressTest)
    ]

}
