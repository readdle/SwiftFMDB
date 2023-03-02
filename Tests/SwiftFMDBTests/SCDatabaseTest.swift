//
//  FMDatabaseTest.swift
//  SmartMailCore
//
//  Created by Andrew on 3/24/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Foundation
@testable import SwiftFMDB
import XCTest
#if SWIFT_PACKAGE
import SQLiteEE
#else
import RDSQLite3
#endif

#if os(Android)
let atachmeDbPath = "/data/local/tmp/attachme.db"
#elseif os(Windows)
let atachmeDbPath = FileManager.default.temporaryDirectory.appendingPathComponent("attachme.db").path
#else
let atachmeDbPath = "/tmp/attachme.db"
#endif

public class SCDatabaseTest: SCDBTempDBTests {
    
    public override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    public override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    public class override func populateDatabase(_ db: FMDatabase) {
        _ = db.executeUpdate(cached: false, "create table test (a text, b text, c integer, d double, e double)")
        _ = db.beginTransaction()
        for i in 1 ..< 21 {
            _ = db.executeUpdate(cached: false, "insert into test (a, b, c, d, e) values (?, ?, ?, ?, ?)", "hi'",     // look!  I put in a ', and I'm not escaping it!
                "number \(i)", Int(i), Date(), Int(2.2))
        }
        _ = db.commit()
        // do it again, just because
        _ = db.beginTransaction()
        for i in 1 ..< 21 {
            _ = db.executeUpdate(cached: false, "insert into test (a, b, c, d, e) values (?, ?, ?, ?, ?)", "hi again'",     // look!  I put in a ', and I'm not escaping it!
                "number \(i)", Int(i), Date(), Int(2.2))
        }
        _ = db.commit()
        _ = db.executeUpdate(cached: false, "create table t3 (a somevalue)")
        _ = db.beginTransaction()
        for i in 0 ..< 20 {
            _ = db.executeUpdate(cached: false, "insert into t3 (a) values (?)", Int(i))
        }
        _ = db.commit()
    }
    
    func testFailOnInvalidKey() {
        _ = db.close()
        _ = db.open()
        _ = db.setKey("Invalid key")
        XCTAssertNil(db.executeQuery(cached: false, "select * from table"), "Shouldn't get results from an empty table")
        XCTAssertTrue(db.hadError(), "Should have failed")
    }
    
    func testFailOnUnopenedDatabase() {
        _ = db.close()
        XCTAssertNil(db.executeQuery(cached: false, "select * from table"), "Shouldn't get results from an empty table")
        XCTAssertTrue(db.hadError(), "Should have failed")
    }
    
    func testFailOnBadStatement() {
        XCTAssertFalse(db.executeUpdate(cached: false, "blah blah blah"), "Invalid statement should fail")
        XCTAssertTrue(db.hadError(), "Should have failed")
    }
    
    func testFailOnBadStatementWithError() {
        var error: Error?
        XCTAssertFalse(db.executeUpdate(cached: false, "blah blah blah", withError: &error), "Invalid statement should fail")
        XCTAssertNotNil(error, "Should have a non-nil NSError")
        
        #if !swift(>=5) // Android Swift 5
        XCTAssertEqual((error as! NSError?)?.code ?? 0, Int(SQLITE_ERROR), "Error should be SQLITE_ERROR")
        #else
        XCTAssertEqual((error as NSError?)?.code ?? 0, Int(SQLITE_ERROR), "Error should be SQLITE_ERROR")
        #endif
    }
    
    func testRekey() {
        let newKey = "New key"
        _ = db.rekey(newKey)
        _ = db.close()
        _ = db.open()
        _ = db.setKey(newKey)
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t1 (a integer)"))
        XCTAssertFalse(db.hadError(), "Rekey should have succeeded")
    }
    
    func testDecrypt() {
        _ = db.rekey(withData: nil)
        _ = db.close()
        _ = db.open()
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t1 (a integer)"))
        XCTAssertFalse(db.hadError(), "Rekey should have succeeded")
    }
    
    func testPragmaJournalMode() {
        let ps = db.executeQuery(cached: false, "pragma journal_mode=delete")
        XCTAssertFalse(db.hadError(), "pragma should have succeeded")
        XCTAssertNotNil(ps, "Result set should be non-nil")
        XCTAssertTrue(ps!.next(), "Result set should have a next result")
        ps?.close()
    }
    
    func testPragmaPageSize() {
        _ = db.executeUpdate(cached: false, "PRAGMA page_size=2048")
        XCTAssertFalse(db.hadError(), "pragma should have succeeded")
    }
    
    func testVacuum() {
        _ = db.executeUpdate(cached: false, "VACUUM")
        XCTAssertFalse(db.hadError(), "VACUUM should have succeeded")
    }
    
    func testSelectULL() {
        // Unsigned long long
        _ = db.executeUpdate(cached: false, "create table ull (a integer)")
        _ = db.executeUpdate(cached: false, "insert into ull (a) values (?)", UInt64.max)
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
        if let rs = db.executeQuery(cached: false, "select a from ull") {
            while rs.next() {
                XCTAssertEqual(rs.uint64(forColumnIndex: 0), UInt64.max, "Result should be ULLONG_MAX")
                XCTAssertEqual(rs.uint64(forColumn: "a"), UInt64.max, "Result should be ULLONG_MAX")
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testSelectByColumnName() {
        if let rs = db.executeQuery(cached: false, "select rowid,* from test where a = ?", "hi") {
            while rs.next() {
                XCTAssertTrue(rs.int(forColumn: "c") > 0)
                XCTAssertNotNil(rs.string(forColumn: "b"), "Should have non-nil string for 'b'")
                XCTAssertNotNil(rs.string(forColumn: "a"), "Should have non-nil string for 'a'")
                XCTAssertNotNil(rs.string(forColumn: "rowid"), "Should have non-nil string for 'rowid'")
                XCTAssertNotNil(rs.date(forColumn: "d"), "Should have non-nil date for 'd'")
                XCTAssertTrue(rs.double(forColumn: "d") > 0)
                XCTAssertTrue(rs.double(forColumn: "e") > 0)
                XCTAssertEqual(rs.columnName(forIndex: 0), "rowid")
                XCTAssertEqual(rs.columnName(forIndex: 1), "a")
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testSelectWithIndexedAndKeyedSubscript() {
        if let rs = db.executeQuery(cached: false, "select rowid, a, b, c from test") {
            while rs.next() {
                XCTAssertEqual(rs[0] as? AnyHashable, rs["rowid"] as? AnyHashable)
                XCTAssertEqual(rs[1] as? AnyHashable, rs["a"] as? AnyHashable)
                XCTAssertEqual(rs[2] as? AnyHashable, rs["b"] as? AnyHashable)
                XCTAssertEqual(rs[3] as? AnyHashable, rs["c"] as? AnyHashable)
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testBusyRetryTimeout() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t1 (a integer)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into t1 values (?)", Int(5)))
        db.maxBusyRetryTimeInterval = 2
        let newDB = FMDatabase(path: databasePath)
        XCTAssertTrue(newDB.open())
        _ = newDB.setKey(encryptionKey)
        if let rs = newDB.executeQuery(cached: false, "select rowid,* from test where a = ?", "hi'") {
            _ = rs.next() // just grab one... which will keep the db locked
            XCTAssertFalse(db.executeUpdate(cached: false, "insert into t1 values (5)"), "Insert should fail because the db is locked by a read")
            XCTAssertEqual(db.lastErrorCode(), SQLITE_BUSY, "SQLITE_BUSY should be the last error")
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertTrue(newDB.close())
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into t1 values (5)"), "The database shouldn't be locked at this point")
    }
    
    func testCaseSensitiveResultDictionary() {
        // case sensitive result dictionary test
        XCTAssertTrue(db.executeUpdate(cached: false, "create table cs (aRowName integer, bRowName text)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into cs (aRowName, bRowName) values (?, ?)", Int(1), "hello"))
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
        if let rs = db.executeQuery(cached: false, "select * from cs") {
            while rs.next() {
                let d: [AnyHashable: Any]? = rs.resultDictionary()
                XCTAssertNotNil(d?["aRowName"], "aRowName should be non-nil")
                XCTAssertNil(d?["arowname"], "arowname should be nil")
                XCTAssertNotNil(d?["bRowName"], "bRowName should be non-nil")
                XCTAssertNil(d?["browname"], "browname should be nil")
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testBoolInsert() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table btest (aRowName integer)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into btest (aRowName) values (?)", true))
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
        if let rs = db.executeQuery(cached: false, "select * from btest") {
            while rs.next() {
                XCTAssertTrue(rs.bool(forColumnIndex: 0), "first column should be true.")
                XCTAssertTrue(rs.int(forColumnIndex: 0) == 1, "first column should be equal to 1 - it was \(rs.int(forColumnIndex: 0)).")
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testNamedParametersCount() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table namedparamcounttest (a text, b text, c integer, d double)"))
        var dictionaryArgs = [AnyHashable: Any]()
        dictionaryArgs["a"] = "Text1"
        dictionaryArgs["b"] = "Text2"
        dictionaryArgs["c"] = 1
        dictionaryArgs["d"] = 2.0
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into namedparamcounttest values (:a, :b, :c, :d)", withParameterDictionary: dictionaryArgs))
        if let rs = db.executeQuery(cached: false, "select * from namedparamcounttest") {
            _ = rs.next()
            XCTAssertEqual(rs.string(forColumn: "a"), "Text1")
            XCTAssertEqual(rs.string(forColumn: "b"), "Text2")
            XCTAssertEqual(rs.int(forColumn: "c"), 1)
            XCTAssertEqual(rs.double(forColumn: "d"), 2.0)
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        
        // note that at this point, dictionaryArgs has way more values than we need, but the query should still work since
        // a is in there, and that's all we need.
        if let rs = db.executeQuery(cached: false, "select * from namedparamcounttest where a = :a", withParameterDictionary: dictionaryArgs) {
            XCTAssertTrue(rs.next())
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        
        // ***** Please note the following codes *****
        dictionaryArgs = [AnyHashable: Any]()
        dictionaryArgs["a"] = "NewText1"
        dictionaryArgs["b"] = "NewText2"
        dictionaryArgs["OneMore"] = "OneMoreText"
        XCTAssertTrue(db.executeUpdate(cached: false, "update namedparamcounttest set a = :a, b = :b where b = 'Text2'", withParameterDictionary: dictionaryArgs))
    }
    
    func testBlobs() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table blobTable (a text, b blob)"))
        let bytes = (0 ..< 1000).map { _ in
            UInt8.random(in: 0...UInt8.max)
        }
        let safariCompass = Data(bytes)
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into blobTable (a, b) values (?, ?)", "safari's compass", safariCompass))
        if let rs = db.executeQuery(cached: false, "select b from blobTable where a = ?", "safari's compass") {
            XCTAssertTrue(rs.next())
            let readData = rs.data(forColumn: "b")
            XCTAssertEqual(readData, safariCompass)
            // ye shall read the header for this function, or suffer the consequences.
            let readDataNoCopy = rs.dataNoCopy(forColumn: "b")
            XCTAssertEqual(readDataNoCopy, safariCompass)
            rs.close()
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testNullValues() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t2 (a integer, b integer)"))
        let result = db.executeUpdate(cached: false, "insert into t2 values (?, ?)", nil, Int(5))
        XCTAssertTrue(result, "Failed to insert a nil value")
        if let rs = db.executeQuery(cached: false, "select * from t2") {
            while rs.next() {
                XCTAssertNil(rs.string(forColumnIndex: 0), "Wasn't able to retrieve a null string")
                XCTAssertEqual(rs.string(forColumnIndex: 1), "5")
            }
            rs.close()
        }
        else {
            XCTAssert(false, "Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testNestedResultSets() {
        if let rs = db.executeQuery(cached: false, "select * from t3") {
            while rs.next() {
                let foo = rs.int(forColumnIndex: 0)
                let newVal: Int = foo + 100
                XCTAssertTrue(db.executeUpdate(cached: false, "update t3 set a = ? where a = ?", Int(newVal), Int(foo)))
                if let rs2 = db.executeQuery(cached: false, "select a from t3 where a = ?", Int(newVal)) {
                    XCTAssertTrue(rs2.next())
                    XCTAssertEqual(rs2.int(forColumnIndex: 0), newVal)
                    rs2.close()
                }
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testNSNullInsertion() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table nulltest (a text, b text)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into nulltest (a, b) values (?, ?)", nil, "a"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into nulltest (a, b) values (?, ?)", nil, "b"))
        if let rs = db.executeQuery(cached: false, "select * from nulltest") {
            while rs.next() {
                XCTAssertNil(rs.string(forColumnIndex: 0))
                XCTAssertNotNil(rs.string(forColumnIndex: 1))
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testNullDates() {
        let date = Date()
        XCTAssertTrue(db.executeUpdate(cached: false, "create table datetest (a double, b double, c double)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into datetest (a, b, c) values (?, ?, 0)", nil, date))
        if let rs = db.executeQuery(cached: false, "select * from datetest") {
            while rs.next() {
                let b: Date? = rs.date(forColumnIndex: 1)
                let c: Date? = rs.date(forColumnIndex: 2)
                XCTAssertNil(rs.date(forColumnIndex: 0))
                XCTAssertNotNil(c, "zero date shouldn't be nil")
                XCTAssertEqual(b!.timeIntervalSince(date), 0.0, accuracy: 1.0, "Dates should be the same to within a second")
                XCTAssertEqual(c!.timeIntervalSince1970, 0.0, accuracy: 1.0, "Dates should be the same to within a second")
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testLotsOfNULLs() {
        guard let safariCompass = try? Data(contentsOf: URL(fileURLWithPath: "/Applications/Safari.app/Contents/Resources/compass.icns")) else {
            return
        }
        XCTAssertTrue(db.executeUpdate(cached: false, "create table nulltest2 (s text, d data, i integer, f double, b integer)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into nulltest2 (s, d, i, f, b) values (?, ?, ?, ?, ?)", "Hi", safariCompass, 12, 4.4, true))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into nulltest2 (s, d, i, f, b) values (?, ?, ?, ?, ?)", nil, nil, nil, nil, nil))
        if let rs = db.executeQuery(cached: false, "select * from nulltest2") {
            while rs.next() {
                let i: Int? = rs.int(forColumnIndex: 2)
                if i == 12 {
                    // it's the first row we inserted.
                    XCTAssertFalse(rs.columnIndexIsNull(0))
                    XCTAssertFalse(rs.columnIndexIsNull(1))
                    XCTAssertFalse(rs.columnIndexIsNull(2))
                    XCTAssertFalse(rs.columnIndexIsNull(3))
                    XCTAssertFalse(rs.columnIndexIsNull(4))
                    XCTAssertTrue(rs.columnIndexIsNull(5))
                    XCTAssertEqual(rs.data(forColumn: "d"), safariCompass)
                    XCTAssertNil(rs.data(forColumn: "notthere"))
                    XCTAssertNil(rs.string(forColumnIndex: -2), "Negative columns should return nil results")
                    XCTAssertTrue(rs.bool(forColumnIndex: 4))
                    XCTAssertTrue(rs.bool(forColumn: "b"))
                    XCTAssertEqual(4.4, rs.double(forColumn: "f"), accuracy: 0.000_000_1, "Saving a float and returning it as a double shouldn't change the result much")
                    XCTAssertEqual(rs.int(forColumn: "i"), 12)
                    XCTAssertEqual(rs.int(forColumnIndex: 2), 12)
                    XCTAssertEqual(rs.int(forColumnIndex: 12), 0, "Non-existent columns should return zero for ints")
                    XCTAssertEqual(rs.int(forColumn: "notthere"), 0, "Non-existent columns should return zero for ints")
                    XCTAssertEqual(rs.int(forColumn: "i"), 12)
                    XCTAssertEqual(rs.int64(forColumn: "i"), 12)
                }
                else {
                    // let's test various null things.
                    XCTAssertTrue(rs.columnIndexIsNull(0))
                    XCTAssertTrue(rs.columnIndexIsNull(1))
                    XCTAssertTrue(rs.columnIndexIsNull(2))
                    XCTAssertTrue(rs.columnIndexIsNull(3))
                    XCTAssertTrue(rs.columnIndexIsNull(4))
                    XCTAssertTrue(rs.columnIndexIsNull(5))
                    XCTAssertNil(rs.data(forColumn: "d"))
                }
            }
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testUTF8Strings() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table utest (a text)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into utest values (?)", "/übertest"))
        if let rs = db.executeQuery(cached: false, "select * from utest where a = ?", "/übertest") {
            XCTAssertTrue(rs.next())
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertFalse(db.hasOpenResultSets(), "Shouldn't have any open result sets")
        XCTAssertFalse(db.hadError(), "Shouldn't have any errors")
    }
    
    func testArgumentsInArray() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table testOneHundredTwelvePointTwo (a text, b integer)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into testOneHundredTwelvePointTwo values (?, ?)", withArgumentsInArray: ["one", Int(2)]))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into testOneHundredTwelvePointTwo values (?, ?)", withArgumentsInArray: ["one", Int(3)]))
        if let rs = db.executeQuery(cached: false, "select * from testOneHundredTwelvePointTwo where b > ?", withArgumentsInArray: [Int(1)]) {
            XCTAssertTrue(rs.next())
            XCTAssertTrue(rs.hasAnotherRow())
            XCTAssertFalse(db.hadError())
            XCTAssertEqual(rs.string(forColumnIndex: 0), "one")
            XCTAssertEqual(rs.int(forColumnIndex: 1), 2)
            XCTAssertTrue(rs.next())
            XCTAssertEqual(rs.int(forColumnIndex: 1), 3)
            XCTAssertFalse(rs.next())
            XCTAssertFalse(rs.hasAnotherRow())
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
    }
    
    func testColumnNamesContainingPeriods() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t4 (a text, b text)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into t4 (a, b) values (?, ?)", "one", "two"))
        if let rs = db.executeQuery(cached: false, "select t4.a as 't4.a', t4.b from t4;") {
            XCTAssertNotNil(rs)
            XCTAssertTrue(rs.next())
            XCTAssertEqual(rs.string(forColumn: "t4.a"), "one")
            XCTAssertEqual(rs.string(forColumn: "b"), "two")
            XCTAssertTrue(rs.string(forColumn: "b") == "two", "String comparison should return zero")
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        
        // let's try these again, with the withArgumentsInArray: variation
        XCTAssertTrue(db.executeUpdate(cached: false, "drop table t4;", withArgumentsInArray: [Any]()))
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t4 (a text, b text)", withArgumentsInArray: [Any]()))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into t4 (a, b) values (?, ?)", withArgumentsInArray: ["one", "two"]))
        if let rs = db.executeQuery(cached: false, "select t4.a as 't4.a', t4.b from t4;", withArgumentsInArray: [Any]()) {
            XCTAssertNotNil(rs)
            XCTAssertTrue(rs.next())
            XCTAssertEqual(rs.string(forColumn: "t4.a"), "one")
            XCTAssertEqual(rs.string(forColumn: "b"), "two")
            XCTAssertTrue(rs.string(forColumn: "b") == "two", "String comparison should return zero")
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
    }

//    func testFormatStringParsing() {
//        XCTAssertTrue(db.executeUpdate(cached: false, "create table t5 (a text, b int, c blob, d text, e text)"))
//        db.executeUpdate(withFormat: "insert into t5 values (%s, %d, %@, %c, %lld)", "text", 42, "BLOB", "d", 12345678901234)
//        if let rs = db.executeQuery(withFormat: "select * from t5 where a = %s and a = %@ and b = %d", "text", "text", 42) {
//            XCTAssertTrue(rs.next())
//            XCTAssertEqual(rs.string(forColumn: "a"), "text")
//            XCTAssertEqual(rs.int(forColumn: "b"), 42)
//            XCTAssertEqual(rs.string(forColumn: "c"), "BLOB")
//            XCTAssertEqual(rs.string(forColumn: "d"), "d")
//            XCTAssertEqual(rs.longLongInt(forColumn: "e"), 12345678901234)
//            rs?.close()
//        }
//    }
    
    func testUpdateWithErrorAndBindings() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table t5 (a text, b int, c blob, d text, e text)"))
        let result: Bool = db.executeUpdate(cached: false, "insert into t5 values (?, ?, ?, ?, ?)", "text", Int(42), "BLOB", "d", Int(0))
        XCTAssertTrue(result)
    }
    
    func testSelectWithEmptyArgumentsArray() {
        let rs = db.executeQuery(cached: false, "select * from test where a=?")
        XCTAssertNil(rs)
    }
    
    func testDatabaseAttach() {
        let fileManager = FileManager()
        try? fileManager.removeItem(atPath: atachmeDbPath)
        let dbB = FMDatabase(path: atachmeDbPath)
        XCTAssertTrue(dbB.open())
        XCTAssertTrue(dbB.executeUpdate(cached: false, "create table attached (a text)"))
        XCTAssertTrue((dbB.executeUpdate(cached: false, "insert into attached values (?)", "test")))
        XCTAssertTrue(dbB.close())
        XCTAssertTrue(db.executeUpdate(cached: false, "attach database '\(atachmeDbPath)' as attack key ''"))
        if let rs = db.executeQuery(cached: false, "select * from attack.attached") {
            XCTAssertTrue(rs.next())
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
    }
    
    func testNamedParameters() {
        // -------------------------------------------------------------------------------
        // Named parameters.
        XCTAssertTrue(db.executeUpdate(cached: false, "create table namedparamtest (a text, b text, c integer, d double)"))
        var dictionaryArgs = [AnyHashable: Any]()
        dictionaryArgs["a"] = "Text1"
        dictionaryArgs["b"] = "Text2"
        dictionaryArgs["c"] = Int(1)
        dictionaryArgs["d"] = Int(2.0)
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into namedparamtest values (:a, :b, :c, :d)", withParameterDictionary: dictionaryArgs))
        if let rs = db.executeQuery(cached: false, "select * from namedparamtest") {
            XCTAssertTrue(rs.next())
            XCTAssertEqual(rs.string(forColumn: "a"), "Text1")
            XCTAssertEqual(rs.string(forColumn: "b"), "Text2")
            XCTAssertEqual(rs.int(forColumn: "c"), 1)
            XCTAssertEqual(rs.double(forColumn: "d"), 2.0)
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        
        dictionaryArgs = [AnyHashable: Any]()
        dictionaryArgs["blah"] = "Text2"
        if let rs = db.executeQuery(cached: false, "select * from namedparamtest where b = :blah", withParameterDictionary: dictionaryArgs) {
            XCTAssertTrue(rs.next())
            XCTAssertEqual(rs.string(forColumn: "b"), "Text2")
            rs.close()
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
    }
    
    func testPragmaDatabaseList() {
        if let rs = db.executeQuery(cached: false, "pragma database_list") {
            var counter: Int = 0
            while rs.next() {
                counter += 1
                guard let file = rs.string(forColumn: "file") else {
                    XCTFail("\"file\" column should contain a value")
                    continue
                }
                XCTAssertEqual(URL(fileURLWithPath: file), URL(fileURLWithPath: databasePath))
            }
            XCTAssertEqual(counter, 1, "Only one database should be attached")
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
    }
    
    func testCachedStatementsInUse() {
        XCTAssertTrue(db.executeUpdate(cached: false, "CREATE TABLE testCacheStatements(key INTEGER PRIMARY KEY, value INTEGER)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "INSERT INTO testCacheStatements (key, value) VALUES (1, 2)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "INSERT INTO testCacheStatements (key, value) VALUES (2, 4)"))
        
        let rs = db.executeQuery(cached: false, "SELECT * FROM testCacheStatements WHERE key=1")
        XCTAssertTrue(rs?.next() ?? false)
        rs?.close()
            
        let rs2 = db.executeQuery(cached: false, "SELECT * FROM testCacheStatements WHERE key=1")
        XCTAssertTrue(rs2?.next() ?? false)
        rs2?.close()
    }
    
    func testStatementCachingWorks() {
        XCTAssertTrue(db.executeUpdate(cached: false, "CREATE TABLE testStatementCaching ( value INTEGER )"))
        XCTAssertTrue(db.executeUpdate(cached: false, "INSERT INTO testStatementCaching( value ) VALUES (1)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "INSERT INTO testStatementCaching( value ) VALUES (1)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "INSERT INTO testStatementCaching( value ) VALUES (2)"))
        
        // two iterations.
        //  the first time through no statements will be from the cache.
        //  the second time through all statements come from the cache.
        for i in 1...2 {
            if let rs1 = db.executeQuery(cached: true, "SELECT rowid, * FROM testStatementCaching WHERE value = ?", 1) { // results in 2 rows...
                XCTAssertTrue(rs1.next())
                
                // confirm that we're seeing the benefits of caching.
                XCTAssertEqual(rs1.statement.useCount, Int64(i))
                if let rs2 = db.executeQuery(cached: true, "SELECT rowid, * FROM testStatementCaching WHERE value = ?", 2) { // results in 1 row
                    XCTAssertTrue(rs2.next())
                    XCTAssertEqual(rs2.statement.useCount, Int64(i))
                    
                    // This is the primary check - with the old implementation of statement caching, rs2 would have rejiggered the (cached) statement used by rs1, making this test fail to return the 2nd row in rs1.
                    XCTAssertTrue(rs1.next())
                    rs2.close()
                }
                else {
                    XCTFail("Should have a non-nil result set")
                }
                rs1.close()
            }
            else {
                XCTFail("Should have a non-nil result set")
            }
        }
    }
    
    func testDateFormat() {
        let testOneDateFormat: (_: FMDatabase, _: Date) -> Void = { db, testDate in
            XCTAssertTrue(db.executeUpdate(cached: false, "DROP TABLE IF EXISTS test_format"))
            XCTAssertTrue(db.executeUpdate(cached: false, "CREATE TABLE test_format ( test TEXT )"))
            XCTAssertTrue(db.executeUpdate(cached: false, "INSERT INTO test_format(test) VALUES (?)", testDate))
            if let rs = db.executeQuery(cached: false, "SELECT test FROM test_format") {
                XCTAssertTrue(rs.next())
                XCTAssertEqual(rs.date(forColumnIndex: 0), testDate)
                rs.close()
            }
            else {
                XCTAssert(false, "Should have a non-nil result set")
            }
        }

        let fmt = FMDatabase.storeableDateFormat("yyyy-MM-dd HH:mm:ss")!
        let testDate: Date? = fmt!.date(from: "2013-02-20 12:00:00")
        
        // test timestamp dates (ensuring our change does not break those)
        testOneDateFormat(db, testDate!)
        
        // now test the string-based timestamp
        db.setDateFormat(fmt!)
        testOneDateFormat(db, testDate!)
    }
    
    func testColumnNameMap() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table colNameTest (a, b, c, d)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into colNameTest values (1, 2, 3, 4)"))
        if let ars = db.executeQuery(cached: false, "select * from colNameTest") {
            let d = ars.columnNameToIndexMap
            ars.close()
            
            XCTAssertEqual(d.count, Int(4))
            XCTAssertEqual(d["a"], 0)
            XCTAssertEqual(d["b"], 1)
            XCTAssertEqual(d["c"], 2)
            XCTAssertEqual(d["d"], 3)
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
    }
    
    func testCustomFunction() {
        XCTAssertTrue(db.executeUpdate(cached: false, "create table ftest (foo text)"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into ftest values ('hello')"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into ftest values ('hi')"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into ftest values ('not h!')"))
        XCTAssertTrue(db.executeUpdate(cached: false, "insert into ftest values ('definitely not h!')"))
        db.makeFunctionNamed("StringStartsWithH", maximumArguments: 1, withBlock: { context, aargc, aargv in
            if sqlite3_value_type(aargv?[0]) == SQLITE_TEXT {
                if let c = sqlite3_value_text(aargv?[0]) {
                    let s = String(cString: c)
                    sqlite3_result_int(context, s.hasPrefix("h") ? 1 : 0)
                }
                else {
                    XCTFail("c == nil")
                }
            }
            else {
                XCTFail("Unknown format for StringStartsWithH (\(sqlite3_value_type(aargv?[0])))")
                sqlite3_result_null(context)
            }
        })
        var rowCount: Int = 0
        if let ars = db.executeQuery(cached: false, "select * from ftest where StringStartsWithH(foo)") {
            while ars.next() {
                rowCount += 1
            }
        }
        else {
            XCTFail("Should have a non-nil result set")
        }
        XCTAssertEqual(rowCount, 2)
    }
    
    func testExecuteStatements() {
        var success: Bool
        var sql: String = "create table bulktest1 (id integer primary key autoincrement, x text);" +
            "create table bulktest2 (id integer primary key autoincrement, y text);" +
            "create table bulktest3 (id integer primary key autoincrement, z text);" +
            "insert into bulktest1 (x) values ('XXX');" +
            "insert into bulktest2 (y) values ('YYY');" +
            "insert into bulktest3 (z) values ('ZZZ');"
        
        success = db.executeStatements(sql)
        XCTAssertTrue(success, "bulk create")
        
        sql = "select count(*) as count from bulktest1;" +
            "select count(*) as count from bulktest2;" +
            "select count(*) as count from bulktest3;"
        
        success = db.executeStatements(sql, withResultBlock: { dictionary in
            if let countStr = dictionary["count"] {
                let count = Int(countStr!)
                XCTAssertEqual(count, 1, "expected one record for dictionary \(dictionary)")
            }
            return 0
        })
        XCTAssertTrue(success, "bulk select")
        sql = "drop table bulktest1;" +
            "drop table bulktest2;" +
        "drop table bulktest3;"
        success = db.executeStatements(sql)
        XCTAssertTrue(success, "bulk drop")
    }

    public static var allTests = [
        ("testFailOnInvalidKey", testFailOnInvalidKey),
        ("testFailOnUnopenedDatabase", testFailOnUnopenedDatabase),
        ("testFailOnBadStatement", testFailOnBadStatement),
        ("testFailOnBadStatementWithError", testFailOnBadStatementWithError),
        ("testRekey", testRekey),
        ("testDecrypt", testDecrypt),
        ("testPragmaJournalMode", testPragmaJournalMode),
        ("testPragmaPageSize", testPragmaPageSize),
        ("testVacuum", testVacuum),
        ("testSelectULL", testSelectULL),
        ("testSelectByColumnName", testSelectByColumnName),
        ("testSelectWithIndexedAndKeyedSubscript", testSelectWithIndexedAndKeyedSubscript),
        ("testBusyRetryTimeout", testBusyRetryTimeout),
        ("testCaseSensitiveResultDictionary", testCaseSensitiveResultDictionary),
        ("testBoolInsert", testBoolInsert),
        ("testNamedParametersCount", testNamedParametersCount),
        ("testBlobs", testBlobs),
        ("testNullValues", testNullValues),
        ("testNestedResultSets", testNestedResultSets),
        ("testNSNullInsertion", testNSNullInsertion),
        ("testNullDates", testNullDates),
        ("testArgumentsInArray", testArgumentsInArray),
        ("testLotsOfNULLs", testLotsOfNULLs),
        ("testUTF8Strings", testUTF8Strings),
        ("testArgumentsInArray", testArgumentsInArray),
        ("testColumnNamesContainingPeriods", testColumnNamesContainingPeriods),
        ("testUpdateWithErrorAndBindings", testUpdateWithErrorAndBindings),
        ("testSelectWithEmptyArgumentsArray", testSelectWithEmptyArgumentsArray),
        ("testDatabaseAttach", testDatabaseAttach),
        ("testNamedParameters", testNamedParameters),
        ("testPragmaDatabaseList", testDateFormat),
        ("testCachedStatementsInUse", testCachedStatementsInUse),
        ("testStatementCachingWorks", testStatementCachingWorks),
        ("testDateFormat", testDateFormat),
        ("testColumnNameMap", testColumnNameMap),
        ("testCustomFunction", testCustomFunction),
        ("testExecuteStatements", testExecuteStatements)
    ]

}
