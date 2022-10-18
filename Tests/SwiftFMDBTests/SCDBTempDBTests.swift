//
//  FMDBTempDBTests.swift
//  SmartMailCore
//
//  Created by Andrew on 3/24/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Foundation
@testable import SwiftFMDB
import XCTest

#if os(Android)
    private let testDatabasePath = "/data/local/tmp/tmp.db"
    private let populatedDatabasePath = "/data/local/tmp/tmp-populated.db"
#elseif os(Windows)
    private let testDatabasePath = FileManager.default.temporaryDirectory.appendingPathComponent("tmp.db").path
    private let populatedDatabasePath = FileManager.default.temporaryDirectory.appendingPathComponent("tmp-populated.db").path
#else
    private let testDatabasePath = "/private/tmp/tmp.db"
    private let populatedDatabasePath = "/tmp/tmp-populated.db"
#endif

// sourcery: disableTests
public class SCDBTempDBTests: XCTestCase {
    
    var db: FMDatabase!
    var databasePath: String {
        return testDatabasePath
    }
    
    public override class func setUp() {
        super.setUp()
        
        print("Sqlite version:  \(FMDatabase.sqliteLibVersion())")
        
        let fileManager = FileManager.default

        #if os(Windows)
        let tmpFolderPath = URL(fileURLWithPath: testDatabasePath).deletingLastPathComponent().path
        try? fileManager.createDirectory(atPath: tmpFolderPath, withIntermediateDirectories: true, attributes: nil)
        #endif

        // Delete old populated database
        try? fileManager.removeItem(atPath: populatedDatabasePath)
        let db = FMDatabase(path: populatedDatabasePath)
        _ = db.open()
        populateDatabase(db)
        _ = db.close()
    }
    
    public override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
        // Delete the old database
        let fileManager = FileManager.default
        if fileManager.fileExists(atPath: testDatabasePath) {
            try! fileManager.removeItem(atPath: testDatabasePath)
        }
        try! fileManager.copyItem(atPath: populatedDatabasePath, toPath: testDatabasePath)
        db = FMDatabase(path: testDatabasePath)
        XCTAssertTrue(db.open(), "Wasn't able to open database")
    }
    
    public override func tearDown() {
        XCTAssertFalse(db.hasOpenResultSets(), "Database still has open result sets on tear down: \(db.databasePath ?? "<no path>")")
        _ = db.close()
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    public class func populateDatabase(_ db: FMDatabase) {}
    
}
