//
//  FMDatabasePool.swift
//  SmartMailCore
//
//  Created by Andrew on 3/24/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Dispatch
import Foundation
import Logging

#if SWIFT_PACKAGE
import SQLite
#elseif os(Windows)
import sqlite
#else
import RDSQLite3
#endif

/** FMDatabasePool delegate category
 
 This is a category that defines the protocol for the FMDatabasePool delegate
 */
public protocol FMDatabasePoolDelegate {
    /** Asks the delegate whether database should be added to the pool.
     
     @param pool     The `FMDatabasePool` object.
     @param database The `FMDatabase` object.
     
     @return `YES` if it should add database to pool; `NO` if not.
     
     */
    func databasePool(_ pool: FMDatabasePool, shouldAddDatabaseToPool database: FMDatabase) -> Bool
    
    /** Tells the delegate that database was added to the pool.
     
     @param pool     The `FMDatabasePool` object.
     @param database The `FMDatabase` object.
     
     */
    func databasePool(_ pool: FMDatabasePool, didAdd database: FMDatabase)
}

public final class FMDatabasePool: NSObject {
    
    /** Database path */
    public var path: String = ""
    
    /** Delegate object */
    public var delegate: FMDatabasePoolDelegate?
    
    /** Maximum number of databases to create */
    public var maximumNumberOfDatabasesToCreate: Int = 0
    
    /** Open flags */
    public private(set) var openFlags: Int32
    
    var lockQueue: DispatchQueue
    var databaseInPool: [FMDatabase]
    var databaseOutPool: [FMDatabase]
    
    // MARK: Initialization
    
    /** Create pool using path.
     
     @param aPath The file path of the database.
     
     @return The `FMDatabasePool` object. `nil` on error.
     */
    public static func databasePool(withPath aPath: String) -> FMDatabasePool {
        return FMDatabasePool(path: aPath)
    }
    
    /** Create pool using path and specified flags
     
     @param aPath The file path of the database.
     @param openFlags Flags passed to the openWithFlags method of the database
     
     @return The `FMDatabasePool` object. `nil` on error.
     */
    public static func databasePool(withPath aPath: String, flags openFlags: Int32) -> FMDatabasePool {
        return FMDatabasePool(path: aPath, flags: openFlags)
    }
    
    /** Create pool using path.
     
     @param aPath The file path of the database.
     
     @return The `FMDatabasePool` object. `nil` on error.
     */
    public convenience init(path aPath: String) {
        // default flags for sqlite3_open
        self.init(path: aPath, flags: SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
    }
    
    /** Create pool using path and specified flags.
     
     @param aPath The file path of the database.
     @param openFlags Flags passed to the openWithFlags method of the database
     
     @return The `FMDatabasePool` object. `nil` on error.
     */
    public init(path aPath: String, flags openFlags: Int32) {
        self.path = aPath
        self.lockQueue = DispatchQueue(label: "fmdb.self")
        self.databaseInPool = [FMDatabase]()
        self.databaseOutPool = [FMDatabase]()
        self.openFlags = openFlags
    }
    
    deinit {
        delegate = nil
    }

    // MARK: Keeping track of checked in/out databases
    
    /** Number of checked-in databases in pool
     
     @returns Number of databases
     */
    public func countOfCheckedInDatabases() -> Int {
        return self.lockQueue.sync {
            self.databaseInPool.count
        }
    }
    
    /** Number of checked-out databases in pool
     
     @returns Number of databases
     */
    public func countOfCheckedOutDatabases() -> Int {
        return self.lockQueue.sync {
            self.databaseOutPool.count
        }
    }
    
    /** Total number of databases in pool
     
     @returns Number of databases
     */
    public func countOfOpenDatabases() -> Int {
        return self.lockQueue.sync {
            self.databaseInPool.count + self.databaseOutPool.count
        }
    }
    
    /** Release all databases in pool */
    public func releaseAllDatabases() {
        self.lockQueue.sync {
            self.databaseInPool.removeAll()
            self.databaseOutPool.removeAll()
        }
    }
    
    // MARK: Perform database operations in pool
    
    /** Synchronously perform database operations in pool.
     
     @param block The code to be run on the `FMDatabasePool` pool.
     */
    public func inDatabase(_ block: @escaping (_ db: FMDatabase?) -> Void) {
        let db = self.db()
        block(db)
        pushDatabaseBack(inPool: db)
    }
    
    /** Synchronously perform database operations in pool using transaction.
     
     @param block The code to be run on the `FMDatabasePool` pool.
     */
    public func inTransaction(_ block: @escaping (_ db: FMDatabase?, _ rollback: inout Bool) -> Void) {
        beginTransaction(false, withBlock: block)
    }
    
    /** Synchronously perform database operations in pool using deferred transaction.
     
     @param block The code to be run on the `FMDatabasePool` pool.
     */
    public func inDeferredTransaction(_ block: @escaping (_ db: FMDatabase?, _ rollback: inout Bool) -> Void) {
        beginTransaction(true, withBlock: block)
    }
    
    private static var savePointIdx: UInt = 0

    /** Synchronously perform database operations in pool using save point.
     
     @param block The code to be run on the `FMDatabasePool` pool.
     
     @return `Error` object if error; `nil` if successful.
     
     @warning You can not nest these, since calling it will pull another database out of the pool and you'll get a deadlock. If you need to nest, use `<[FMDatabase startSavePointWithName:error:]>` instead.
     */
    
    public func inSavePoint(_ block: @escaping (_ db: FMDatabase?, _ rollback: inout Bool) -> Void) -> Error? {
        guard let db = self.db() else {
            return FMDatabaseError.SQLITE_DB_NOTFOUND
        }
        FMDatabasePool.savePointIdx += 1
        let name: String = "savePoint\(FMDatabasePool.savePointIdx)"
        var shouldRollback = false
        var err: Error?
        if db.startSavePoint(withName: name, error: &err) == false {
            pushDatabaseBack(inPool: db)
            return err
        }
        block(db, &shouldRollback)
        if shouldRollback {
            // We need to rollback and release this savepoint to remove it
            _ = db.rollbackToSavePoint(withName: name, error: &err)
        }
        _ = db.releaseSavePoint(withName: name, error: &err)
        pushDatabaseBack(inPool: db)
        return err
    }
    
    // MARK: Private methods
    
    private func beginTransaction(_ useDeferred: Bool, withBlock block: @escaping (_ db: FMDatabase?, _ rollback: inout Bool) -> Void) {
        var shouldRollback = false
        let db = self.db()
        if useDeferred {
            _ = db?.beginDeferredTransaction()
        }
        else {
            _ = db?.beginTransaction()
        }
        block(db, &shouldRollback)
        if shouldRollback {
            _ = db?.rollback()
        }
        else {
            _ = db?.commit()
        }
        pushDatabaseBack(inPool: db)
    }
    
    private func pushDatabaseBack(inPool db: FMDatabase?) {
        guard let db = db else {
            // db can be null if we set an upper bound on the # of databases to create.
            return
        }
        self.lockQueue.sync {
            if self.databaseInPool.contains(where: { $0 === db }) {
                //NSException(name: "Database already in pool", reason: "The FMDatabase being put back into the pool is already present in the pool", userInfo: nil).raise()
            }
            self.databaseInPool.append(db)
            if let index = self.databaseOutPool.firstIndex(where: { $0 === db }) {
                self.databaseOutPool.remove(at: index)
            }
        }
    }
    
    private func db() -> FMDatabase? {
        var db: FMDatabase?
        self.lockQueue.sync {
            db = self.databaseInPool.last
            var shouldNotifyDelegate: Bool = false
            if db != nil {
                self.databaseOutPool.append(db!)
                self.databaseInPool.removeLast()
            }
            else {
                if self.maximumNumberOfDatabasesToCreate > 0 {
                    let currentCount: Int = databaseOutPool.count + databaseInPool.count
                    if currentCount >= maximumNumberOfDatabasesToCreate {
                        logger.error("Maximum number of databases (\(Int(currentCount))) has already been reached!")
                        return
                    }
                }
                db = FMDatabase(path: self.path)
                shouldNotifyDelegate = true
            }
            //This ensures that the db is opened before returning
            let success = db?.open(withFlags: self.openFlags) ?? false
            if success {
                if self.delegate?.databasePool(self, shouldAddDatabaseToPool: db!) ?? false == false {
                    _ = db?.close()
                    db = nil
                }
                else {
                    //It should not get added in the pool twice if lastObject was found
                    if databaseOutPool.contains(where: { $0 === db }) == false {
                        databaseOutPool.append(db!)
                        if shouldNotifyDelegate {
                            delegate?.databasePool(self, didAdd: db!)
                        }
                    }
                }
            }
            else {
                logger.error("Could not open up the database at path \(path)")
                db = nil
            }

        }
        return db
    }
}

// Legacy Objective-C methods
public extension FMDatabasePool {
    
    // legacy method for Objective C
    func legacyInTransaction(_ block: @escaping (_ db: FMDatabase?, _ rollback: UnsafeMutablePointer<Bool>) -> Void) {
        self.inTransaction({ (_ db: FMDatabase?, _ rollback: inout Bool) -> Void in
            var rollbackObj: Bool = false
            block(db, &rollbackObj)
            rollback = rollbackObj
        })
    }
    
    // legacy method for Objective C
    func legacyInDeferredTransaction(_ block: @escaping (_ db: FMDatabase?, _ rollback: UnsafeMutablePointer<Bool>) -> Void) {
        self.inDeferredTransaction({ (_ db: FMDatabase?, _ rollback: inout Bool) -> Void in
            var rollbackObj: Bool = false
            block(db, &rollbackObj)
            rollback = rollbackObj
        })
    }
    
    // legacy method for Objective C
    func legacyInSavePoint(_ block: @escaping (_ db: FMDatabase?, _ rollback: UnsafeMutablePointer<Bool>) -> Void) -> Error? {
        return self.inSavePoint({ (_ db: FMDatabase?, _ rollback: inout Bool) -> Void in
            var rollbackObj: Bool = false
            block(db, &rollbackObj)
            rollback = rollbackObj
        })
    }
    
}
