//
//  FMDatabaseQueue.swift
//  SmartMailCore
//
//  Created by Andrew on 3/23/17.
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

private let kDispatchQueueSpecificKey = DispatchSpecificKey<FMDatabaseQueue>()
private let logger = Logger(label: "FMDatabaseQueue")

@objc(FMCoreDatabaseQueue)
@objcMembers
open class FMDatabaseQueue: NSObject {
    
    public var timeLoggingEnabled: Bool = false
    public var throwAssertWhenExecutedOnMainThread: Bool = false
    
    private var inTran = false
    private var nestedInTranRollback = false
    private var readonly = false
    
    open var db: FMDatabase? {
        didSet {
            updateDBHandlers()
        }
    }
    
    public var longQueryHandler: ((_ query: String, _ time: TimeInterval) -> Void)? {
        didSet {
            updateDBHandlers()
        }
    }
    
    public var indexCorruptionRecoveryHandler: ((_ time: TimeInterval) -> Void)? {
        didSet {
            updateDBHandlers()
        }
    }
    
    public var indexCorruptionFailedRecoveryHandler: (() -> Void)? {
        didSet {
            updateDBHandlers()
        }
    }
    
    public var dbCorruptionHandler: ((_ query: String) -> Void)? {
        didSet {
            updateDBHandlers()
        }
    }
    
    private func updateDBHandlers() {
        guard db != nil else {
            return
        }
        inDatabase { db in
            db.longQueryHandler = self.longQueryHandler
            db.indexCorruptionRecoveryHandler = self.indexCorruptionRecoveryHandler
            db.indexCorruptionFailedRecoveryHandler = self.indexCorruptionFailedRecoveryHandler
            db.dbCorruptionHandler = self.dbCorruptionHandler 
        }
    }
    
    public let queue = DispatchQueue(label: "fmdb")
    
    /** Path of database */
    public var path: String
    
    /** Open flags */
    public var openFlags: Int32 = 0
    
    // MARK: Initialization, opening, and closing of queue
    
    /** Create queue using path.
     
     @param aPath The file path of the database.
     
     @return The `FMDatabaseQueue` object. `nil` on error.
     */
    
    public static func databaseQueue(withPath aPath: String) -> FMDatabaseQueue? {
        return FMDatabaseQueue(withPath: aPath)
    }
    
    /** Create queue using path and specified flags.
     
     @param aPath The file path of the database.
     @param openFlags Flags passed to the openWithFlags method of the database
     
     @return The `FMDatabaseQueue` object. `nil` on error.
     */
    public static func databaseQueue(withPath aPath: String, flags: Int32) -> FMDatabaseQueue? {
        return FMDatabaseQueue(withPath: aPath, flags: flags)
    }
    
    /** Create queue using path.
     
     @param aPath The file path of the database.
     
     @return The `FMDatabaseQueue` object. `nil` on error.
     */
    public convenience init?(withPath aPath: String) {
        // default flags for sqlite3_open
        self.init(withPath: aPath, flags: SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
    }
    
    /** Create queue using path and specified flags.
     
     @param aPath The file path of the database.
     @param openFlags Flags passed to the openWithFlags method of the database
     
     @return The `FMDatabaseQueue` object. `nil` on error.
     */
    public init?(withPath aPath: String, flags: Int32) {
        db = FMDatabase.database(withPath: aPath)
        let success = db?.open(withFlags: flags) ?? false
        if  success == false {
            FMDatabaseQueue.logger.error("Could not create database queue for path \(aPath)")
            return nil
        }
        
        self.path = aPath
        self.openFlags = flags
        
        super.init()
        self.readonly = (openFlags & SQLITE_OPEN_READONLY) != 0
        self.queue.setSpecific(key: kDispatchQueueSpecificKey, value: self)
    }
    
    /** Returns the Class of 'FMDatabase' subclass, that will be used to instantiate database object.
     
     Subclasses can override this method to return specified Class of 'FMDatabase' subclass.
     
     @return The Class of 'FMDatabase' subclass, that will be used to instantiate database object.
     */
    
    /** Close database used by queue. */
    public func close() {
        queue.sync {
            _ = self.db?.close()
            self.db = nil
        }
    }
    
    open func database() -> FMDatabase? {
        if db == nil {
            db = FMDatabase.database(withPath: self.path)
            if db?.open(withFlags: self.openFlags) ?? false == false {
                logger.error("FMDatabaseQueue could not reopen database for path \(self.path)")
                db = nil
            }
        }
        db?.logsErrors = true
        db?.maxBusyRetryTimeInterval = 5000
        return db
    }
    
    // MARK: Dispatching database operations to queue
    
    /** Synchronously perform database operations on queue.
     
     @param block The code to be run on the queue of `FMDatabaseQueue`
     */
    open func inDatabase(_ block: @escaping (FMDatabase) -> Void) {
        inDatabaseAsync(false, block: block)
    }
    
    func beginTransaction(_ useDeferred: Bool, withBlock block: @escaping (_ db: FMDatabase, _ rollback: inout Bool) -> Void) {
        queue.sync {
            var shouldRollback = false
            guard let db = self.database() else {
                return
            }
            if useDeferred {
                _ = db.beginDeferredTransaction()
            }
            else {
                _ = db.beginTransaction()
            }
            block(db, &shouldRollback)
            if shouldRollback {
                _ = db.rollback()
            }
            else {
                _ = db.commit()
            }
        }
    }
    
    /** Synchronously perform database operations on queue, using transactions.
     
     @param block The code to be run on the queue of `FMDatabaseQueue`
     */
    open func inTransaction(_ block: @escaping (_ db: FMDatabase, _ rolback: inout Bool) -> Void) {
        inTransactionAsync(false, block: block)
    }
    
    /** Synchronously perform database operations on queue, using deferred transactions.
     
     @param block The code to be run on the queue of `FMDatabaseQueue`
     */
    open func inDeferredTransaction(_ block: @escaping (_ db: FMDatabase, _ rolback: inout Bool) -> Void) {
        assert(false, "not supported")
    }

    // NOTE: you can not nest these, since calling it will pull another database out of the pool and you'll get a deadlock.
    // If you need to nest, use FMDatabase's startSavePointWithName:error: instead.
    open func inSavePoint(_ block: (_ db: FMDatabase, _ rolback: inout Bool) -> Void) -> Error? {
        assert(false, "not supported")
        return nil
    }
    
    public func waitUntilAllOperationsFinished() {
        queue.sync {}
    }
    
    private func getCallStackSymbols() -> String {
        #if DEBUG
        // FIXME: Already implemented, update of libFoundation.so required.
        // https://github.com/apple/swift-corelibs-foundation/commit/84ee193f9181838a366fe5f5e02d6345c7338c3a#diff-1423414a6b3777fe633fd614b1a43c67R272
        #if !os(Android)
            return "\(Thread.callStackSymbols)"
        #else
            return ""
        #endif
        #else
        return "release:no stack"
        #endif
    }
    
    func isNestedCall() -> Bool {
        let currentSyncQueue = DispatchQueue.getSpecific(key: kDispatchQueueSpecificKey)
        return currentSyncQueue === self
    }
    
    func checkOpenResultSets(_ db: FMDatabase) {
        if db.hasOpenResultSets() {
            assert(false, "Find and close resulte set")
            logger.info("Warning: there is at least one open result set around after performing [FMDatabaseQueue inDatabase:]")
            #if DEBUG
                let openSetCopy = db.openResultSets
                for rs in openSetCopy {
                    logger.info("query: '\(String(describing: rs.query))'")
                }
            #endif
        }
    }
    
    func executeBlock(inQueue block: @escaping (_ db: FMDatabase) -> Void, async: Bool) {
        if throwAssertWhenExecutedOnMainThread && async == false && Thread.isMainThread {
            assert(false, "Remove db requests from main thread")
        }
        if timeLoggingEnabled == false {
            queue.sync {
                if let database = self.database() {
                    block(database)
                }
                else {
                    //error
                }
            }
            return
        }
        var executionTime: Double = 0
        let wt1 = Date.timeIntervalSinceReferenceDate
        let blockWithExecutionTimt = { () -> Void in
            let et1 = Date.timeIntervalSinceReferenceDate
            if let database = self.database() {
                block(database)
            }
            else {
                //error
            }
            let et2 = Date.timeIntervalSinceReferenceDate
            executionTime = et2 - et1
        }
        if async {
            queue.async(execute: blockWithExecutionTimt)
        }
        else {
            queue.sync(execute: blockWithExecutionTimt)
        }
        let wt2 = Date.timeIntervalSinceReferenceDate
        let waitingTime = wt2 - wt1
        if executionTime > 0.1 {
            if Thread.isMainThread {
                logger.info("Db block is executed too long (main thread), time: \(Float(executionTime))sec stack:\n\(getCallStackSymbols())")
            }
            else if executionTime > 3 {
                logger.info("Db block is executed too long (back thread), time: \(Float(executionTime))sec stack:\n\(getCallStackSymbols())")
            }
        }
        else if waitingTime > 0.1 && Thread.isMainThread {
            logger.info("Db block was in wait too long (main thread), time: \(Float(executionTime))sec stack:\n\(getCallStackSymbols())")
        }
    }
    
    func executeNestedBlock(_ block: @escaping (_ db: FMDatabase) -> Void) {
        guard let db = db else {
            assert(false, "Must be open")
            return
        }
        
        block(db)
    }
    
    public func inDatabaseAsync(_ async: Bool, block: @escaping (_: FMDatabase) -> Void) {
        if isNestedCall() {
            executeNestedBlock(block)
        }
        else {
            executeBlock(inQueue: { db in
                block(db)
                self.checkOpenResultSets(db)
            }, async: async)
        }
    }
    
    public func inTransactionAsync(_ async: Bool, block: @escaping (_: FMDatabase, _: inout Bool) -> Void) {
        if isNestedCall() {
            executeNestedBlock({ db in
                self.execute(inTransaction: block, db: db)
            })
        }
        else {
            executeBlock(inQueue: { db in
                self.execute(inTransaction: block, db: db)
                self.checkOpenResultSets(db)
            }, async: async)
        }
    }
    
    func execute(inTransaction block: @escaping (_ db: FMDatabase, _ rollback: inout Bool) -> Void, db: FMDatabase) {
        var souldBeginTran: Bool = inTran == false
        if readonly {
            logger.error("readonly, transaction is ignored")
            souldBeginTran = false
        }
        if souldBeginTran {
            inTran = true
            _ = db.beginTransaction()
        }
        var shouldRollback = nestedInTranRollback
        if shouldRollback == false {
            block(db, &shouldRollback)
        }
        else {
            // do not call block if some nested inTransaction block was rollbacked, becase of we'll rollback whole transaction
        }
        if souldBeginTran {
            if shouldRollback {
                _ = db.rollback()
            }
            else {
                _ = db.commit()
            }
            inTran = false
            nestedInTranRollback = false
        }
        else {
            // if some netes inTransaction block was rollbacked, we rollback whole transaction
            nestedInTranRollback = nestedInTranRollback || shouldRollback
        }
    }
    
    
    
}

public extension FMDatabaseQueue {
    
    // Legacy Objective C method
    func legacyInTransaction(_ block: @escaping (_ db: FMDatabase, _ rollback: UnsafeMutablePointer<Bool>) -> Void) {
        self.beginTransaction(true, withBlock: { (_ db: FMDatabase, _ rollback: inout Bool) -> Void in
            var rollbackObj: Bool = false
            block(db, &rollbackObj)
            rollback = rollbackObj
        })
    }
}

#if os(Android) || os(Windows)
#else
    public extension FMDatabaseQueue {
        @objc(inTransaction:)
        func _inTransaction(_ block: @escaping (_ db: FMDatabase, _ rollback: UnsafeMutablePointer<Bool>?) -> Void) {
            self.inTransaction { db, rollback in
                block(db, &rollback)
            }
        }
    }
#endif

