//
//  FMDatabase.swift
//  SmartMailCore
//
//  Created by Andrew on 3/22/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Foundation
import Logging

#if SWIFT_PACKAGE
import SQLiteEE
#else
import RDSQLite3
#endif

public let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
public let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public typealias FMDBExecuteStatementsCallbackBlock = ([String: String?]) -> Int32
public typealias FMDBSQLiteCallbackBlock = (OpaquePointer?, Int32, UnsafeMutablePointer<OpaquePointer?>?) -> Void

@objc

// swiftlint:disable operator_usage_whitespace
public enum FMDatabaseError: Int, Error {
    case SQLITE_OK           = 0   /* Successful result */
    /* beginning-of-error-codes */
    case SQLITE_ERROR        = 1   /* SQL error or missing database */
    case SQLITE_INTERNAL     = 2   /* Internal logic error in SQLite */
    case SQLITE_PERM         = 3   /* Access permission denied */
    case SQLITE_ABORT        = 4   /* Callback routine requested an abort */
    case SQLITE_BUSY         = 5   /* The database file is locked */
    case SQLITE_LOCKED       = 6   /* A table in the database is locked */
    case SQLITE_NOMEM        = 7   /* A malloc() failed */
    case SQLITE_READONLY     = 8   /* Attempt to write a readonly database */
    case SQLITE_INTERRUPT    = 9   /* Operation terminated by sqlite3_interrupt()*/
    case SQLITE_IOERR        = 10   /* Some kind of disk I/O error occurred */
    case SQLITE_CORRUPT      = 11   /* The database disk image is malformed */
    case SQLITE_NOTFOUND     = 12   /* Unknown opcode in sqlite3_file_control() */
    case SQLITE_FULL         = 13   /* Insertion failed because database is full */
    case SQLITE_CANTOPEN     = 14   /* Unable to open the database file */
    case SQLITE_PROTOCOL     = 15   /* Database lock protocol error */
    case SQLITE_EMPTY        = 16   /* Database is empty */
    case SQLITE_SCHEMA       = 17   /* The database schema changed */
    case SQLITE_TOOBIG       = 18   /* String or BLOB exceeds size limit */
    case SQLITE_CONSTRAINT   = 19   /* Abort due to constraint violation */
    case SQLITE_MISMATCH     = 20   /* Data type mismatch */
    case SQLITE_MISUSE       = 21   /* Library used incorrectly */
    case SQLITE_NOLFS        = 22   /* Uses OS features not supported on host */
    case SQLITE_AUTH         = 23   /* Authorization denied */
    case SQLITE_FORMAT       = 24   /* Auxiliary database format error */
    case SQLITE_RANGE        = 25   /* 2nd parameter to sqlite3_bind out of range */
    case SQLITE_NOTADB       = 26   /* File opened that is not a database file */
    case SQLITE_NOTICE       = 27   /* Notifications from sqlite3_log() */
    case SQLITE_WARNING      = 28   /* Warnings from sqlite3_log() */
    case SQLITE_ROW          = 100  /* sqlite3_step() has another row ready */
    case SQLITE_DONE         = 101  /* sqlite3_step() has finished executing */
    /* custom error codes */
    case SQLITE_DB_NOTFOUND  = 1001
}

private enum FMDatabaseExtendedErrorCode: Int32 {
    case SQLITE_CORRUPT_INDEX   = 779
}
// swiftlint:enable operator_usage_whitespace

/** A SQLite ([http://sqlite.org/](http://sqlite.org/)) Swift wrapper.
 
 ### Usage
 The three main classes in FMDB are:
 
 - `FMDatabase` - Represents a single SQLite database.  Used for executing SQL statements.
 - `<FMResultSet>` - Represents the results of executing a query on an `FMDatabase`.
 - `<FMDatabaseQueue>` - If you want to perform queries and updates on multiple threads, you'll want to use this class.
 
 ### See also
 
 - `<FMDatabasePool>` - A pool of `FMDatabase` objects.
 - `<FMStatement>` - A wrapper for `sqlite_stmt`.
 
 ### External links
 
 - [SQLite web site](http://sqlite.org/)
 - [SQLite FAQ](http://www.sqlite.org/faq.html)
 
 @warning Do not instantiate a single `FMDatabase` object and use it across multiple threads. Instead, use `<FMDatabaseQueue>`.
 
 */
@objcMembers 
public final class FMDatabase: NSObject {
    
    fileprivate var isExecutingStatement: Bool = false
    fileprivate var startBusyRetryTime: TimeInterval?
    private var openFunctions: [FMDBSQLiteCallback<FMDBSQLiteCallbackBlock>]
    private var dateFormat: DateFormatter?
    
    internal var db: OpaquePointer?

    public var openResultSets: Set<FMResultSet>
    
    /** Whether should trace execution */
    public var traceExecution: Bool = false
    
    /** Whether checked out or not */
    public var checkedOut: Bool?
    
    /** Crash on errors */
    public var crashOnErrors: Bool
    
    /** Database corruption handler */
    public var longQueryHandler: ((_ query: String, _ time: TimeInterval) -> Void)?
    public var indexCorruptionRecoveryHandler: ((_ time: TimeInterval) -> Void)?
    public var indexCorruptionFailedRecoveryHandler: (() -> Void)?
    public var dbCorruptionHandler: ((_ query: String) -> Void)?
    
    /** Logs errors */
    public var logsErrors: Bool = false
    
    /** Dictionary of cached statements */
    private var cachedStatements = [String: Set<FMStatement>]()
    private var cachedStatementKeys = [String: Int]()
    
    public private(set) var databasePath: String?
    public private(set) var inTransaction: Bool?
    
    // MARK: Initialization
    
    /** Create a `FMDatabase` object.
     
     An `FMDatabase` is created with a path to a SQLite database file.  This path can be one of these three:
     
     1. A file system path.  The file does not have to exist on disk.  If it does not exist, it is created for you.
     2. An empty string (`""`).  An empty database is created at a temporary location.  This database is deleted with the `FMDatabase` connection is closed.
     3. `nil`.  An in-memory database is created.  This database will be destroyed with the `FMDatabase` connection is closed.
     
     (For more information on temporary and in-memory databases, read the sqlite documentation on the subject: [http://www.sqlite.org/inmemorydb.html](http://www.sqlite.org/inmemorydb.html))
     
     @param inPath Path of database file
     
     @return `FMDatabase` object if successful; `nil` if failure.
     
     */
    
    public static func database(withPath inPath: String) -> FMDatabase {
        return FMDatabase(path: inPath)
    }
    
    public init(path inPath: String) {
        assert(sqlite3_threadsafe() > 0) // whoa there big boy- gotta make sure sqlite it happy with what we're going to do.
        
        self.databasePath = inPath
        self.openResultSets = Set<FMResultSet>()
        self.openFunctions = [FMDBSQLiteCallback<FMDBSQLiteCallbackBlock>]()
        self.db = nil
        self.logsErrors = true
        self.crashOnErrors = false
        self.maxBusyRetryTimeInterval = 2
    }
    
    deinit {
        _ = self.close()
        self.openResultSets.removeAll()
        self.openFunctions.removeAll()
    }
    
    // MARK: Opening and closing database
    
    /** Opening a new database connection
     
     The database is opened for reading and writing, and is created if it does not already exist.
     
     @return `true` if successful, `false` on error.
     
     @see [sqlite3_open()](http://sqlite.org/c3ref/open.html)
     @see openWithFlags:
     @see close
     */
    
    @discardableResult
    public func open() -> Bool {
        if self.db != nil {
            return true
        }
        let err = sqlite3_open(self.sqlitePath(), &db)
        if err != SQLITE_OK {
            logger.error("error opening!: \(err)")
            return false
        }
        if self.maxBusyRetryTimeInterval > 0.0 {
            // set the handler (trick for Swift propery didSet)
            let maxBusyRetryTimeInterval = self.maxBusyRetryTimeInterval
            self.maxBusyRetryTimeInterval = maxBusyRetryTimeInterval
        }
        return true
    }
    
    /** Opening a new database connection with flags
     
     @param flags one of the following three values, optionally combined with the `SQLITE_OPEN_NOMUTEX`, `SQLITE_OPEN_FULLMUTEX`, `SQLITE_OPEN_SHAREDCACHE`, `SQLITE_OPEN_PRIVATECACHE`, and/or `SQLITE_OPEN_URI` flags:
     
     `SQLITE_OPEN_READONLY`
     
     The database is opened in read-only mode. If the database does not already exist, an error is returned.
     
     `SQLITE_OPEN_READWRITE`
     
     The database is opened for reading and writing if possible, or reading only if the file is write protected by the operating system. In either case the database must already exist, otherwise an error is returned.
     
     `SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE`
     
     The database is opened for reading and writing, and is created if it does not already exist. This is the behavior that is always used for `open` method.
     
     @return `true` if successful, `false` on error.
     
     @see [sqlite3_open_v2()](http://sqlite.org/c3ref/open.html)
     @see open
     @see close
     */
    
    @discardableResult
    public func open(withFlags flags: Int32) -> Bool {
        if self.db != nil {
            return true
        }
        let err = sqlite3_open_v2(self.sqlitePath(), &db, flags, nil)
        if err != SQLITE_OK {
            logger.error("error opening!: \(err)")
            return false
        }
        if self.maxBusyRetryTimeInterval > 0.0 {
            // set the handler (trick for Swift propery didSet)
            let maxBusyRetryTimeInterval = self.maxBusyRetryTimeInterval
            self.maxBusyRetryTimeInterval = maxBusyRetryTimeInterval
        }
        return true

    }
    
    /** Closing a database connection
     
     @return `true` if success, `false` on error.
     
     @see [sqlite3_close()](http://sqlite.org/c3ref/close.html)
     @see open
     @see openWithFlags:
     */
    
    @discardableResult
    public func close() -> Bool {
        self.clearCachedStatements()
        self.closeOpenResultSets()
        if db == nil {
            return true
        }
        var rc: Int32
        var retry: Bool
        var triedFinalizingOpenStatements: Bool = false
        repeat {
            retry = false
            rc = sqlite3_close(db)
            if SQLITE_BUSY == rc || SQLITE_LOCKED == rc {
                if !triedFinalizingOpenStatements {
                    triedFinalizingOpenStatements = true
                    while let pStmt = sqlite3_next_stmt(db, nil) {
                        logger.trace("Closing leaked statement")
                        sqlite3_finalize(pStmt)
                        retry = true
                    }
                }
            }
            else if SQLITE_OK != rc {
                logger.error("error closing!: \(rc)")
            }
        } while retry
        db = nil
        return true
    }
    
    /** Test to see if we have a good connection to the database.
     
     This will confirm whether:
     
     - is database open
     - if open, it will try a simple SELECT statement and confirm that it succeeds.
     
     @return `true` if everything succeeds, `false` on failure.
     */
    
    public func goodConnection() -> Bool {
        if db != nil {
            return false
        }
        let rs: FMResultSet? = self.executeQuery(cached: false, "select name from sqlite_master where type='table'")
        if rs != nil {
            rs?.close()
            return true
        }
        return false
    }
    
    // MARK: Perform updates
    
    internal func executeUpdate(_ sql: String, error outErr: inout Error?, withArgumentsInArray arrayArgs: [Any?]?, orDictionary dictionaryArgs: [AnyHashable: Any?]?, cached: Bool, cacheLimit: Int) -> Bool {
        let t1 = Date.timeIntervalSinceReferenceDate
        
        if !self.databaseExists() {
            return false
        }
        if self.isExecutingStatement {
            self.warnInUse()
            return false
        }
        self.isExecutingStatement = true
        var rc: Int32 = 0
        var pStmt: OpaquePointer?
        var cachedStmt: FMStatement?
        if self.traceExecution {
            logger.trace("\(self) executeUpdate: \(sql)")
        }
        if cached {
            cachedStmt = self.cachedStatement(forQuery: sql)
            pStmt = cachedStmt?.statement
            cachedStmt?.reset()
        }
        if pStmt == nil {
            rc = sqlite3_prepare_v2(db, sql, -1, &pStmt, nil)
            if SQLITE_OK != rc {
                if logsErrors {
                    logger.error("DB Error: \(String(describing: self.lastErrorCode())) \"\(String(describing: self.lastErrorMessage()))\" DB Query: \(sql) DB Path: \(String(describing: databasePath))")
                }
                if rc == SQLITE_NOTADB || rc == SQLITE_CORRUPT, let dbCorruptionHandler = dbCorruptionHandler {
                    dbCorruptionHandler(sql)
                }
                if crashOnErrors {
                    assert(false, "DB Error: \(String(describing: self.lastErrorCode())) \"\(String(describing: self.lastErrorMessage()))\"")
                    abort()
                }
                sqlite3_finalize(pStmt)
                outErr = self.lastError()
                isExecutingStatement = false
                return false
            }
        }
        var obj: Any?
        var idx: Int32 = 0
        let queryCount = sqlite3_bind_parameter_count(pStmt)
        // If dictionaryArgs is passed in, that means we are using sqlite's named parameter support
        if dictionaryArgs?.isEmpty ?? true == false {
            for (dictionaryKey, value) in dictionaryArgs ?? [:] {
                // Prefix the key with a colon.
                let parameterName: String = ":\(dictionaryKey)"
                // Get the index for the parameter name.
                let namedIdx = sqlite3_bind_parameter_index(pStmt, parameterName)
                if namedIdx > 0 {
                    // Standard binding from here.
                    self.bindObject(value, toColumn: namedIdx, inStatement: pStmt)
                    // increment the binding count, so our check below works out
                    idx += 1
                }
                else {
                    logger.info("Could not find index for \(dictionaryKey)")
                }
            }
        }
        else {
            while idx < queryCount {
                if arrayArgs != nil && idx < Int32(arrayArgs!.count) {
                    obj = arrayArgs![Int(idx)]
                }
                else {
                    //We ran out of arguments
                    break
                }
                if traceExecution {
                    if let objData = obj as? Data {
                        logger.trace("data: \(objData.count) bytes")
                    }
                    else {
                        logger.trace("obj: \(String(describing: obj))")
                    }
                }
                idx += 1
                self.bindObject(obj, toColumn: idx, inStatement: pStmt)
            }
        }
        if idx != queryCount {
            logger.error("Error: the bind count (\(idx)) is not correct for the # of variables in the query (\(queryCount)) (\(sql)) (executeUpdate)")
            if let cachedStmt = cachedStmt {
                cachedStmt.reset()
            }
            else {
                sqlite3_finalize(pStmt)
            }
            isExecutingStatement = false
            return false
        }
        /* Call sqlite3_step() to run the virtual machine. Since the SQL being
         ** executed is not a SELECT statement, we assume no data will be returned.
         */

        var attemptedToRecoverIndexes = false
        var shouldStep = true

        while shouldStep {
            shouldStep = false

            rc = sqlite3_step(pStmt)
            if SQLITE_DONE == rc {
                // all is well, let's return.
            }
            else if SQLITE_ERROR == rc {
                if logsErrors {
                    let swiftError = String(cString: sqlite3_errmsg(db))
                    logger.error("Error calling sqlite3_step (\(rc): \(swiftError)) SQLITE_ERROR, DB Query : \(sql)")
                }
            }
            else if SQLITE_MISUSE == rc {
                // uh oh.
                if logsErrors {
                    let swiftError = String(cString: sqlite3_errmsg(db))
                    logger.error("Error calling sqlite3_step (\(rc): \(swiftError)) SQLITE_MISUSE, DB Query: \(sql)")
                }
            }
            else {
                let extendedCode = sqlite3_extended_errcode(db)
                let isIndexCorruption = SQLITE_CORRUPT == rc && extendedCode == FMDatabaseExtendedErrorCode.SQLITE_CORRUPT_INDEX.rawValue
                if isIndexCorruption {
                    if logsErrors {
                        let swiftError = String(cString: sqlite3_errmsg(db))
                        logger.error("Index corrupted calling sqlite3_step (\(rc): \(swiftError)) eu, Extended Code = \(extendedCode), DB Query: \(sql)")
                    }

                    if attemptedToRecoverIndexes == false {
                        attemptedToRecoverIndexes = true

                        if attemptRecoverIndexes() {
                            sqlite3_reset(pStmt)
                            shouldStep = true
                            continue
                        }
                    }
                    else {
                        logger.info("Attempt to recover indexes failed. Skipping another attempt to avoid infinite loop.")
                        indexCorruptionFailedRecoveryHandler?()
                    }
                }

                // wtf?
                if logsErrors {
                    let swiftError = String(cString: sqlite3_errmsg(db))
                    logger.error("Unknown error calling sqlite3_step (\(rc): \(swiftError)) eu, Extended Code = \(extendedCode), DB Query: \(sql)")
                }
                if rc == SQLITE_NOTADB || rc == SQLITE_CORRUPT, let dbCorruptionHandler = dbCorruptionHandler {
                    dbCorruptionHandler(sql)
                }
                if crashOnErrors {
                    assert(false, "DB Error: \(String(describing: self.lastErrorCode())) \"\(String(describing: self.lastErrorMessage()))\"")
                    abort()
                }

                if let cachedStmt = cachedStmt {
                    cachedStmt.reset()
                }
                else {
                    sqlite3_finalize(pStmt)
                }

                outErr = self.lastError()
                isExecutingStatement = false
                return false
            }
            if rc == SQLITE_ROW {
                assert(false, "A executeUpdate is being called with a query string '\(sql)'")
            }
        }

        if cached && cachedStmt == nil {
            cachedStmt = FMStatement()
            cachedStmt?.statement = pStmt
            self.setCachedStatement(cachedStmt!, forQuery: sql, cacheLimit: cacheLimit)
        }
        var closeErrorCode: Int32
        if cachedStmt != nil {
            cachedStmt?.useCount = cachedStmt!.useCount + 1
            closeErrorCode = sqlite3_reset(pStmt)
        }
        else {
            /* Finalize the virtual machine. This releases all memory and other
             ** resources allocated by the sqlite3_prepare() call above.
             */
            closeErrorCode = sqlite3_finalize(pStmt)
        }
        if closeErrorCode != SQLITE_OK {
            if logsErrors {
                let swiftError = sqlite3_errmsg(db)
                logger.error("Unknown error finalizing or resetting statement (\(closeErrorCode): \(swiftError?.debugDescription ?? "nil" )), DB Query: \(sql)")
            }
        }
        isExecutingStatement = false
        
        let t2 = Date.timeIntervalSinceReferenceDate
        let diff = t2 - t1
        if diff > 0.1 && !attemptedToRecoverIndexes {
            longQueryHandler?(sql, diff)
         
            if Thread.isMainThread {
                logger.info("Query is executed too long (main thread), time: \(diff) sec query:\n\(sql)")
            }
            else if diff > 1 {
                logger.info("Query is executed too long (back thread), time: \(diff) sec query:\n\(sql)")
            }
        }
        
        return (rc == SQLITE_DONE || rc == SQLITE_OK)
    }

    private func attemptRecoverIndexes() -> Bool {
        var pStmt: OpaquePointer?
        defer { sqlite3_finalize(pStmt) }

        let start = Date.timeIntervalSinceReferenceDate
        let prepareRC = sqlite3_prepare_v2(db, "REINDEX", -1, &pStmt, nil)
        guard SQLITE_OK == prepareRC else {
            if logsErrors {
                let swiftError = String(cString: sqlite3_errmsg(db))
                let extendedCode = sqlite3_extended_errcode(db)
                logger.error("Failed to prepare reindex statement (\(prepareRC): \(swiftError)) eu, Extended Code = \(extendedCode)")
            }

            indexCorruptionFailedRecoveryHandler?()
            return false
        }

        let stepRC = sqlite3_step(pStmt)
        guard SQLITE_DONE == stepRC else {
            if logsErrors {
                let swiftError = String(cString: sqlite3_errmsg(db))
                let extendedCode = sqlite3_extended_errcode(db)
                logger.error("Failed to execute reindex statement (\(stepRC): \(swiftError)) eu, Extended Code = \(extendedCode)")
            }

            indexCorruptionFailedRecoveryHandler?()
            return false
        }

        let end = Date.timeIntervalSinceReferenceDate
        let duration = end - start
        logger.info("Corrupted index recovery took: \(duration) seconds")
        indexCorruptionRecoveryHandler?(duration)
        return true
    }
    
    /** Execute single update statement
     
     This method executes a single SQL update statement (i.e. any SQL that does not return results, such as `UPDATE`, `INSERT`, or `DELETE`. This method employs [`sqlite3_prepare_v2`](http://sqlite.org/c3ref/prepare.html), [`sqlite3_bind`](http://sqlite.org/c3ref/bind_blob.html) to bind values to `?` placeholders in the SQL with the optional list of parameters, and [`sqlite_step`](http://sqlite.org/c3ref/step.html) to perform the update.
     
     @param sql The SQL to be performed, with optional `?` placeholders.
     
     @param outErr A reference to the `Error` pointer to be updated with an auto released `Error` object if an error if an error occurs. If `nil`, no `Error` object will be returned.
     
     @param ... Optional parameters to bind to `?` placeholders in the SQL statement. 
     
     @return `true` upon success; `false` upon failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see lastError
     @see lastErrorCode
     @see lastErrorMessage
     @see [`sqlite3_bind`](http://sqlite.org/c3ref/bind_blob.html)
     */
    
    @discardableResult public func executeUpdate(cached: Bool, _ sql: String, _ args: Any? ...) -> Bool {
        var err: Error?
        return executeUpdate(sql, error: &err, withArgumentsInArray: args, orDictionary: nil, cached: cached, cacheLimit: 1)
    }

    @discardableResult public func executeUpdate(cached: Bool, _ sql: String, withError: inout Error?, _ args: Any? ...) -> Bool {
        return executeUpdate(sql, error: &withError, withArgumentsInArray: args, orDictionary: nil, cached: cached, cacheLimit: 1)
    }
    
    @discardableResult public func executeUpdate(cached: Bool, _ sql: String, withArgumentsInArray arguments: [Any]) -> Bool {
        var error: Error?
        return self.executeUpdate(sql, error: &error, withArgumentsInArray: arguments, orDictionary: nil, cached: cached, cacheLimit: 1)
    }
    
    /** Execute single update statement
     
     This method executes a single SQL update statement (i.e. any SQL that does not return results, such as `UPDATE`, `INSERT`, or `DELETE`. This method employs [`sqlite3_prepare_v2`](http://sqlite.org/c3ref/prepare.html) and [`sqlite_step`](http://sqlite.org/c3ref/step.html) to perform the update. Unlike the other `executeUpdate` methods, this uses printf-style formatters (e.g. `%s`, `%d`, etc.) to build the SQL.
     
     The optional values provided to this method should be objects (e.g. `NSString`, `NSNumber`, `NSNull`, `NSDate`, and `NSData` objects), not fundamental data types (e.g. `int`, `long`, `NSInteger`, etc.). This method automatically handles the aforementioned object types, and all other object types will be interpreted as text values using the object's `description` method.
     
     @param sql The SQL to be performed, with optional `?` placeholders.
     
     @param arguments A `Dictionary` of objects keyed by column names that will be used when binding values to the `?` placeholders in the SQL statement.
     
     @return `true` upon success; `dalse` upon failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see lastError
     @see lastErrorCode
     @see lastErrorMessage
     */
    @discardableResult
    public func executeUpdate(cached: Bool = false, _ sql: String, withParameterDictionary arguments: [AnyHashable: Any]) -> Bool {
        var error: Error?
        return self.executeUpdate(sql, error: &error, withArgumentsInArray: nil, orDictionary: arguments, cached: cached, cacheLimit: 1)
    }
    
    /** Execute multiple SQL statements
     
     This executes a series of SQL statements that are combined in a single string (e.g. the SQL generated by the `sqlite3` command line `.dump` command). This accepts no value parameters, but rather simply expects a single string with multiple SQL statements, each terminated with a semicolon. This uses `sqlite3_exec`.
     
     @param  sql  The SQL to be performed
     
     @return `true` upon success; `false` upon failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see executeStatements:withResultBlock:
     @see [sqlite3_exec()](http://sqlite.org/c3ref/exec.html)
     
     */
    
    @discardableResult
    public func executeStatements(_ sql: String) -> Bool {
        return self.executeStatements(sql, withResultBlock: nil)
    }
    
    /** Execute multiple SQL statements with callback handler
     
     This executes a series of SQL statements that are combined in a single string (e.g. the SQL generated by the `sqlite3` command line `.dump` command). This accepts no value parameters, but rather simply expects a single string with multiple SQL statements, each terminated with a semicolon. This uses `sqlite3_exec`.
     
     @param sql       The SQL to be performed.
     @param block     A block that will be called for any result sets returned by any SQL statements.
     Note, if you supply this block, it must return integer value, zero upon success (this would be a good opportunity to use SQLITE_OK),
     non-zero value upon failure (which will stop the bulk execution of the SQL).  If a statement returns values, the block will be called with the results from the query in NSDictionary *resultsDictionary.
     This may be `nil` if you don't care to receive any results.
     
     @return          `true` upon success; `false` upon failure. If failed, you can call `<lastError>`,
     `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see executeStatements:
     @see [sqlite3_exec()](http://sqlite.org/c3ref/exec.html)
     
     */
    @discardableResult
    public func executeStatements(_ sql: String, withResultBlock block: FMDBExecuteStatementsCallbackBlock?) -> Bool {
        var rc: Int32
        var errmsg: UnsafeMutablePointer<Int8>?
        let unmanagedCallback = Unmanaged.passRetained(FMDBSQLiteCallback<FMDBExecuteStatementsCallbackBlock>(block: block))
        rc = sqlite3_exec(db, sql, block != nil ? sqliteExecCallback : nil, unmanagedCallback.toOpaque(), &errmsg)
        unmanagedCallback.release()
        if errmsg != nil && self.logsErrors {
            logger.error("Error inserting batch: \(String(describing: errmsg))")
            sqlite3_free(errmsg)
        }
        return (rc == SQLITE_OK)
    }
    
    /** Last insert rowid
     
     Each entry in an SQLite table has a unique 64-bit signed integer key called the "rowid". The rowid is always available as an undeclared column named `ROWID`, `OID`, or `_ROWID_` as long as those names are not also used by explicitly declared columns. If the table has a column of type `INTEGER PRIMARY KEY` then that column is another alias for the rowid.
     
     This routine returns the rowid of the most recent successful `INSERT` into the database from the database connection in the first argument. As of SQLite version 3.7.7, this routines records the last insert rowid of both ordinary tables and virtual tables. If no successful `INSERT`s have ever occurred on that database connection, zero is returned.
     
     @return The rowid of the last inserted row.
     
     @see [sqlite3_last_insert_rowid()](http://sqlite.org/c3ref/last_insert_rowid.html)
     
     */
    
    public func lastInsertRowId() -> Int64 {
        if isExecutingStatement {
            self.warnInUse()
            return 0
        }
        isExecutingStatement = true
        let ret = sqlite3_last_insert_rowid(db)
        isExecutingStatement = false
        return ret
    }
    
    /** The number of rows changed by prior SQL statement.
     
     This function returns the number of database rows that were changed or inserted or deleted by the most recently completed SQL statement on the database connection specified by the first parameter. Only changes that are directly specified by the INSERT, UPDATE, or DELETE statement are counted.
     
     @return The number of rows changed by prior SQL statement.
     
     @see [sqlite3_changes()](http://sqlite.org/c3ref/changes.html)
     
     */
    
    public func changes() -> Int32 {
        if isExecutingStatement {
            self.warnInUse()
            return 0
        }
        isExecutingStatement = true
        let ret = sqlite3_changes(db)
        isExecutingStatement = false
        return ret
    }
    
    // MARK: Retrieving results
    
    internal func executeQuery(_ sql: String, withArgumentsInArray arrayArgs: [Any?]?, orDictionary dictionaryArgs: [AnyHashable: Any?]?, cached: Bool, cacheLimit: Int) -> FMResultSet? {
        if self.databaseExists() == false {
            return nil
        }
        if isExecutingStatement {
            self.warnInUse()
            return nil
        }
        isExecutingStatement = true
        var rc: Int32 = 0x00
        var pStmt: OpaquePointer?
        var statement: FMStatement?
        var rs: FMResultSet
        if traceExecution {
            logger.error("\(self) executeQuery: \(sql)")
        }
        if cached {
            statement = self.cachedStatement(forQuery: sql)
            pStmt = statement?.statement
            statement?.reset()
        }
        if pStmt == nil {
            rc = sqlite3_prepare_v2(db, sql, -1, &pStmt, nil)
            if SQLITE_OK != rc {
                if logsErrors {
                    logger.error("DB Error: \(String(describing: self.lastErrorCode())) \"\(String(describing: self.lastErrorMessage()))\" DB Query: \(sql) DB Path: \(String(describing: databasePath))")
                }
                if rc == SQLITE_NOTADB || rc == SQLITE_CORRUPT, let dbCorruptionHandler = dbCorruptionHandler {
                    dbCorruptionHandler(sql)
                }
                if crashOnErrors {
                    assert(false, "DB Error: \(String(describing: self.lastErrorCode())) \"\(String(describing: self.lastErrorMessage()))\"")
                    abort()
                }
                sqlite3_finalize(pStmt)
                isExecutingStatement = false
                return nil
            }
        }
        var obj: Any?
        var idx: Int32 = 0
        let queryCount = sqlite3_bind_parameter_count(pStmt)
        // pointed out by Dominic Yu (thanks!)
        // If dictionaryArgs is passed in, that means we are using sqlite's named parameter support
        if dictionaryArgs?.isEmpty ?? true == false {
            for (dictionaryKey, value) in dictionaryArgs ?? [:] {
                // Prefix the key with a colon.
                let parameterName: String = ":\(dictionaryKey)"
                // Get the index for the parameter name.
                let namedIdx = sqlite3_bind_parameter_index(pStmt, parameterName)
                if namedIdx > 0 {
                    // Standard binding from here.
                    self.bindObject(value, toColumn: namedIdx, inStatement: pStmt)
                    // increment the binding count, so our check below works out
                    idx += 1
                }
                else {
                    logger.trace("Could not find index for \(dictionaryKey)")
                }
            }
        }
        else {
            while idx < queryCount {
                if arrayArgs != nil && idx < Int32(arrayArgs!.count) {
                    obj = arrayArgs![Int(idx)]
                }
                else {
                    //We ran out of arguments
                    break
                }
                if traceExecution {
                    if let objData = obj as? Data {
                        logger.trace("data: \(objData.count) bytes")
                    }
                    else {
                        logger.trace("obj: \(String(describing: obj))")
                    }
                }
                idx += 1
                self.bindObject(obj, toColumn: idx, inStatement: pStmt)
            }
        }
        if idx != queryCount {
            logger.error("Error: the bind count is not correct for the # of variables (executeQuery)")
            
            if let statement = statement {
                statement.reset()
            }
            else {
                sqlite3_finalize(pStmt)
            }
            isExecutingStatement = false
            return nil
        }
        if statement == nil {
            statement = FMStatement()
            statement?.statement = pStmt
            if cached {
                self.setCachedStatement(statement!, forQuery: sql, cacheLimit: cacheLimit)
            }
        }
        // the statement gets closed in rs's dealloc or [rs close];
        rs = FMResultSet(statement: statement!, usingParentDatabase: self)
        rs.query = sql
        _ = openResultSets.insert(rs)
        statement?.useCount += 1
        isExecutingStatement = false
        return rs
    }
    
    /** Execute select statement
     
     Executing queries returns an `<FMResultSet>` object if successful, and `nil` upon failure.  Like executing updates, there is a variant that accepts an `Error` parameter.  Otherwise you should use the `<lastErrorMessage>` and `<lastErrorMessage>` methods to determine why a query failed.
     
     In order to iterate through the results of your query, you use a `while()` loop.  You also need to "step" (via `<[FMResultSet next]>`) from one record to the other.
     
     This method employs [`sqlite3_bind`](http://sqlite.org/c3ref/bind_blob.html) for any optional value parameters. This  properly escapes any characters that need escape sequences (e.g. quotation marks), which eliminates simple SQL errors as well as protects against SQL injection attacks.
     
     @param sql The SELECT statement to be performed, with optional `?` placeholders.
     
     @param ... Optional parameters to bind to `?` placeholders in the SQL statement. These should be Objective-C objects (e.g. `NSString`, `NSNumber`, etc.), not fundamental C data types (e.g. `int`, `char *`, etc.).
     
     @return A `<FMResultSet>` for the result set upon success; `nil` upon failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see FMResultSet
     @see [`FMResultSet next`](<[FMResultSet next]>)
     @see [`sqlite3_bind`](http://sqlite.org/c3ref/bind_blob.html)
     */
    
    public func executeQuery(cached: Bool, _ sql: String, _ args: Any? ...) -> FMResultSet? {
        return executeQuery(sql, withArgumentsInArray: args, orDictionary: nil, cached: cached, cacheLimit: 1)
    }
    
    public func executeQuery(cached: Bool, _ sql: String, arg1: String) -> FMResultSet? {
        return executeQuery(sql, withArgumentsInArray: [arg1], orDictionary: nil, cached: cached, cacheLimit: 1)
    }
    
    public func executeQuery(cached: Bool, _ sql: String, withArgumentsInArray arguments: [Any?]) -> FMResultSet? {
        return executeQuery(sql, withArgumentsInArray: arguments, orDictionary: nil, cached: cached, cacheLimit: 1)
    }
    
    /** Execute select statement
     
     Executing queries returns an `<FMResultSet>` object if successful, and `nil` upon failure.  Like executing updates, there is a variant that accepts an `NSError **` parameter.  Otherwise you should use the `<lastErrorMessage>` and `<lastErrorMessage>` methods to determine why a query failed.
     
     In order to iterate through the results of your query, you use a `while()` loop.  You also need to "step" (via `<[FMResultSet next]>`) from one record to the other.
     
     @param sql The SELECT statement to be performed, with optional `?` placeholders.
     
     @param arguments A `Dictionary` of objects keyed by column names that will be used when binding values to the `?` placeholders in the SQL statement.
     
     @return A `<FMResultSet>` for the result set upon success; `nil` upon failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see FMResultSet
     @see [`FMResultSet next`](<[FMResultSet next]>)
     */
    
    public func executeQuery(cached: Bool = false, _ sql: String, withParameterDictionary arguments: [AnyHashable: Any]) -> FMResultSet? {
        return executeQuery(sql, withArgumentsInArray: nil, orDictionary: arguments, cached: cached, cacheLimit: 1)
    }
    
    // MARK: Transactions
    
    /** Begin a transaction
     
     @return `true` on success; `false` on failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see commit
     @see rollback
     @see beginDeferredTransaction
     @see inTransaction
     */
    @discardableResult
    public func beginTransaction() -> Bool {
        let b = self.executeUpdate(cached: true, "begin exclusive transaction")
        if b {
            inTransaction = true
        }
        return b
    }
    
    /** Begin a deferred transaction
     
     @return `true` on success; `false` on failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see commit
     @see rollback
     @see beginTransaction
     @see inTransaction
     */
    @discardableResult
    public func beginDeferredTransaction() -> Bool {
        let b = self.executeUpdate(cached: true, "begin deferred transaction")
        if b {
            inTransaction = true
        }
        return b
    }
    
    /** Commit a transaction
     
     Commit a transaction that was initiated with either `<beginTransaction>` or with `<beginDeferredTransaction>`.
     
     @return `true` on success; `false` on failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see beginTransaction
     @see beginDeferredTransaction
     @see rollback
     @see inTransaction
     */
    @discardableResult
    public func commit() -> Bool {
        let b = self.executeUpdate(cached: true, "commit transaction")
        if b {
            inTransaction = false
        }
        return b
    }
    
    /** Rollback a transaction
     
     Rollback a transaction that was initiated with either `<beginTransaction>` or with `<beginDeferredTransaction>`.
     
     @return `true` on success; `false` on failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see beginTransaction
     @see beginDeferredTransaction
     @see commit
     @see inTransaction
     */
    @discardableResult
    public func rollback() -> Bool {
        let b = self.executeUpdate(cached: true, "rollback transaction")
        if b {
            inTransaction = false
        }
        return b
    }
    
    // MARK: Cached statements and result sets
    
    /** Clear cached statements */
    
    public func clearCachedStatements() {
        for (_, statements) in cachedStatements {
            for statement in statements {
                statement.close()
            }
        }
        cachedStatements.removeAll()
        cachedStatementKeys.removeAll()
    }
    
    /** Close all open result sets */
    
    public func closeOpenResultSets() {
        //Copy the set so we don't get mutation errors
        for rs in openResultSets {
            openResultSets.remove(rs)
            rs.close()
        }
    }
    
    /** Whether database has any open result sets
     
     @return `true` if there are open result sets; `false` if not.
     */
    
    public func hasOpenResultSets() -> Bool {
        return openResultSets.count > 0
    }
    
    internal func resultSetDidClose(_ resultSet: FMResultSet) {
        openResultSets.remove(resultSet)
    }
    
    // MARK: Encryption methods
    
    /** Set encryption key.
     
     @param key The key to be used.
     
     @return `true` if success, `false` on error.
     
     @see http://www.sqlite-encrypt.com/develop-guide.htm
     
     @warning You need to have purchased the sqlite encryption extensions for this method to work.
     */
    
    public func setKey(_ key: String) -> Bool {
        guard let keyData = key.data(using: .utf8) else {
            return false
        }
        return self.setKey(withData: keyData)
    }
    
    /** Reset encryption key
     
     @param key The key to be used.
     
     @return `true` if success, `false` on error.
     
     @see http://www.sqlite-encrypt.com/develop-guide.htm
     
     @warning You need to have purchased the sqlite encryption extensions for this method to work.
     */
    
    public func rekey(_ key: String) -> Bool {
        guard let keyData = key.data(using: .utf8) else {
            return false
        }
        return self.rekey(withData: keyData)
    }
    
    /** Set encryption key using `keyData`.
     
     @param keyData The `NSData` to be used.
     
     @return `true` if success, `false` on error.
     
     @see http://www.sqlite-encrypt.com/develop-guide.htm
     
     @warning You need to have purchased the sqlite encryption extensions for this method to work.
     */
    
    public func setKey(withData keyData: Data?) -> Bool {
        #if SQLITE_HAS_CODEC
            guard let keyData = keyData else {
                return false
            }
            var rc = sqlite3_key(db, keyData.bytes, Int32(keyData.count))
            return (rc == SQLITE_OK)
        #else
            return false
        #endif
    }
    
    /** Reset encryption key using `keyData`.
     
     @param keyData The `NSData` to be used.
     
     @return `true` if success, `false` on error.
     
     @see http://www.sqlite-encrypt.com/develop-guide.htm
     
     @warning You need to have purchased the sqlite encryption extensions for this method to work.
     */
    
    public func rekey(withData keyData: Data?) -> Bool {
        #if SQLITE_HAS_CODEC
            guard let keyData = keyData else {
                return false
            }
            var rc = sqlite3_rekey(db, keyData.bytes, Int32(keyData.count))
            if rc != SQLITE_OK {
                logger.error("error on rekey: \(rc), error message = \(self.lastErrorMessage())")
            }
            return (rc == SQLITE_OK)
        #else
            return false
        #endif
    }
    
    // MARK: Retrieving error codes
    
    /** Last error message
     
     Returns the English-language text that describes the most recent failed SQLite API call associated with a database connection. If a prior API call failed but the most recent API call succeeded, this return value is undefined.
     
     @return `String` of the last error message.
     
     @see [sqlite3_errmsg()](http://sqlite.org/c3ref/errcode.html)
     @see lastErrorCode
     @see lastError
     
     */
    
    public func lastErrorMessage() -> String? {
        return String(utf8String: sqlite3_errmsg(db))
    }
    
    /** Last error code
     
     Returns the numeric result code or extended result code for the most recent failed SQLite API call associated with a database connection. If a prior API call failed but the most recent API call succeeded, this return value is undefined.
     
     @return Integer value of the last error code.
     
     @see [sqlite3_errcode()](http://sqlite.org/c3ref/errcode.html)
     @see lastErrorMessage
     @see lastError
     
     */
    
    public func lastErrorCode() -> Int32 {
        return sqlite3_errcode(db)
    }
    
    /** Had error
     
     @return `true` if there was an error, `false` if no error.
     
     @see lastError
     @see lastErrorCode
     @see lastErrorMessage
     
     */
    
    public func hadError() -> Bool {
        let lastErrCode = self.lastErrorCode()
        return lastErrCode > SQLITE_OK && lastErrCode < SQLITE_ROW
    }
    
    /** Last error
     
     @return `Error` representing the last error.
     
     @see lastErrorCode
     @see lastErrorMessage
     
     */
    public func lastError() -> NSError? {
        let errorMessage = [NSLocalizedDescriptionKey: lastErrorMessage() ?? ""]
        return NSError(domain: "FMDatabase", code: Int(sqlite3_errcode(db)), userInfo: errorMessage)
    }
    
    public var maxBusyRetryTimeInterval: TimeInterval {
        didSet {
            if db == nil {
                return
            }
            if maxBusyRetryTimeInterval > 0 {
                sqlite3_busy_handler(db, databaseBusyHandlerCallback, Unmanaged.passUnretained(self).toOpaque())
            }
            else {
                // turn it off otherwise
                sqlite3_busy_handler(db, nil, nil)
            }
        }
    }
    
    // MARK: Save points
    
    fileprivate func FMDBEscapeSavePointName(savepointName: String) -> String {
        return savepointName.replacingOccurrences(of: "'", with: "''")
    }
    
    /** Start save point
     
     @param name Name of save point.
     
     @param outErr A `NSError` object to receive any error object (if any).
     
     @return `true` on success; `false` on failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see releaseSavePointWithName:error:
     @see rollbackToSavePointWithName:error:
     */
    
    public func startSavePoint(withName name: String, error outErr: inout Error?) -> Bool {
        let sql: String = "savepoint '\(FMDBEscapeSavePointName(savepointName: name))';"
        if self.executeUpdate(cached: false, sql) == false {
            outErr = self.lastError()
            return false
        }
        return true
    }
    
    /** Release save point
     
     @param name Name of save point.
     
     @param outErr A `NSError` object to receive any error object (if any).
     
     @return `true` on success; `false` on failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see startSavePointWithName:error:
     @see rollbackToSavePointWithName:error:
     
     */
    
    public func releaseSavePoint(withName name: String, error outErr: inout Error?) -> Bool {
        let sql: String = "release savepoint '\(FMDBEscapeSavePointName(savepointName: name))';"
        let worked = self.executeUpdate(cached: false, sql)
        if worked == false {
            outErr = self.lastError()
        }
        return worked
    }
    
    /** Roll back to save point
     
     @param name Name of save point.
     @param outErr A `NSError` object to receive any error object (if any).
     
     @return `true` on success; `false` on failure. If failed, you can call `<lastError>`, `<lastErrorCode>`, or `<lastErrorMessage>` for diagnostic information regarding the failure.
     
     @see startSavePointWithName:error:
     @see releaseSavePointWithName:error:
     
     */
    
    public func rollbackToSavePoint(withName name: String, error outErr: inout Error?) -> Bool {
        let sql = "rollback transaction to savepoint '\(FMDBEscapeSavePointName(savepointName: name))';"
        let worked = self.executeUpdate(cached: false, sql)
        if worked == false {
            outErr = self.lastError()
        }
        return worked
    }
    
    private static var savePointIdx: UInt = 0
    
    /** Start save point
     
     @param block Block of code to perform from within save point.
     
     @return The NSError corresponding to the error, if any. If no error, returns `nil`.
     
     @see startSavePointWithName:error:
     @see releaseSavePointWithName:error:
     @see rollbackToSavePointWithName:error:
     
     */
    
    public func inSavePoint(_ block: (_ rollback: inout Bool) -> Void) -> Error? {
        FMDatabase.savePointIdx += 1
        let name: String = "dbSavePoint\(FMDatabase.savePointIdx)"
        var shouldRollback: Bool = false
        var err: Error?
        if self.startSavePoint(withName: name, error: &err) == false {
            return err
        }
        block(&shouldRollback)
        if shouldRollback {
            // We need to rollback and release this savepoint to remove it
            _ = self.rollbackToSavePoint(withName: name, error: &err)
        }
        _ = self.releaseSavePoint(withName: name, error: &err)
        return err
    }
    
    // MARK: Date formatter
    
    /** Generate an `DateFormatter` that won't be broken by permutations of timezones or locales.
     
     Use this method to generate values to set the dateFormat property.
     
     @param format A valid NSDateFormatter format string.
     
     @return A `NSDateFormatter` that can be used for converting dates to strings and vice versa.
     
     @see hasDateFormatter
     @see setDateFormat:
     @see dateFromString:
     @see stringFromDate:
     @see storeableDateFormat:
     
     @warning Note that `DateFormatter` is not thread-safe, so the formatter generated by this method should be assigned to only one FMDB instance and should not be used for other purposes.
     
     */
    
    public static func storeableDateFormat(_ format: String) -> DateFormatter?? {
        let result = DateFormatter()
        result.locale = Locale(identifier: "en_US")
        result.dateFormat = format
        result.timeZone = TimeZone(secondsFromGMT: 0)
        return result
    }
    
    /** Test whether the database has a date formatter assigned.
     
     @return `true` if there is a date formatter; `false` if not.
     
     @see hasDateFormatter
     @see setDateFormat:
     @see dateFromString:
     @see stringFromDate:
     @see storeableDateFormat:
     */
    
    public func hasDateFormatter() -> Bool {
        return dateFormat != nil
    }
    
    /** Set to a date formatter to use string dates with sqlite instead of the default UNIX timestamps.
     
     @param format Set to nil to use UNIX timestamps. Defaults to nil. Should be set using a formatter generated using FMDatabase.storeableDateFormat.
     
     @see hasDateFormatter
     @see setDateFormat:
     @see dateFromString:
     @see stringFromDate:
     @see storeableDateFormat:
     
     @warning Note there is no direct getter for the `DateFormatter`, and you should not use the formatter you pass to FMDB for other purposes, as `DateFormatter` is not thread-safe.
     */
    
    public func setDateFormat(_ format: DateFormatter) {
        self.dateFormat = format
    }
    
    /** Convert the supplied String to Date, using the current database formatter.
     
     @param s `String` to convert to `Date`.
     
     @return The `Date` object; or `nil` if no formatter is set.
     
     @see hasDateFormatter
     @see setDateFormat:
     @see dateFromString:
     @see stringFromDate:
     @see storeableDateFormat:
     */
    
    public func dateFromString(_ s: String?) -> Date? {
        guard let s = s else {
            return nil
        }
        return dateFormat?.date(from: s)
    }
    
    /** Convert the supplied Date to String, using the current database formatter.
     
     @param date `Date` of date to convert to `String`.
     
     @return The `String` representation of the date; `nil` if no formatter is set.
     
     @see hasDateFormatter
     @see setDateFormat:
     @see dateFromString:
     @see stringFromDate:
     @see storeableDateFormat:
     */
    
    public func stringFromDate(_ date: Date) -> String? {
        return dateFormat?.string(from: date)
    }
    
    // MARK: SQLite information
    
    /** Test to see if the library is threadsafe
     
     @return `false` if and only if SQLite was compiled with mutexing code omitted due to the SQLITE_THREADSAFE compile-time option being set to 0.
     
     @see [sqlite3_threadsafe()](http://sqlite.org/c3ref/threadsafe.html)
     */
    
    public static func isSQLiteThreadSafe() -> Bool {
        // make sure to read the sqlite headers on this guy!
        return sqlite3_threadsafe() != 0
        
    }
    
    /** Run-time library version numbers
     
     @return The sqlite library version string.
     
     @see [sqlite3_libversion()](http://sqlite.org/c3ref/libversion.html)
     */
    
    public static func sqliteLibVersion() -> String {
        return String(cString: sqlite3_libversion())
    }
    
    fileprivate func sqlitePath() -> String {
        if let databasePath = databasePath {
            if databasePath.count == 0 {
                return "" // this creates a temporary database (it's an sqlite thing).
            }
            return databasePath
        }
        return ":memory:"
    }
    
    fileprivate func databaseExists() -> Bool {
        if db == nil {
            logger.info("The FMDatabase \(self) is not open.")
            #if !NS_BLOCK_ASSERTIONS
                if self.crashOnErrors {
                    assert(false, "The FMDatabase \(self) is not open.")
                    abort()
                }
            #endif
            return false
        }
        return true
    }
    
    fileprivate func warnInUse() {
        logger.warning("The FMDatabase \(self) is currently in use.")
        #if !NS_BLOCK_ASSERTIONS
            if self.crashOnErrors {
                assert(false, "The FMDatabase \(self) is currently in use.")
                abort()
            }
        #endif
    }
    
    public func printExplainQueryPlan(_ format: String, args: Any ...) {
        let query = "EXPLAIN QUERY PLAN \(format)"
        if let rs = self.executeQuery(cached: false, query, args) {
            while rs.next() {
                let selectid = rs.int(forColumnIndex: 0)
                let order = rs.int(forColumnIndex: 1)
                let from = rs.int(forColumnIndex: 2)
                let detail = rs.string(forColumnIndex: 3)
                logger.trace("\(selectid) \(order) \(from) \(String(describing: detail))\n")
            }
            rs.close()
        }
    }
    
    // MARK: Cached statements
    private func setCachedStatement(_ statement: FMStatement, forQuery query: String, cacheLimit: Int) {
        statement.query = query
        var statements = self.cachedStatements[query] ?? Set<FMStatement>()
        #if !os(Android) && DEBUG
        guard let key = keyFor(pStmt: statement.statement) else {
            assert(false, "key is nil")
            return
        }
        let cachedCount = cachedStatementKeys[key, default: 0] + 1
        if statements.isEmpty, cachedCount > cacheLimit {
            assert(false, "Found similar statement with different arguments list. Should be executed as not cached.")
            return
        }
        cachedStatementKeys[key] = cachedCount
        #endif
        statements.insert(statement)
        self.cachedStatements[query] = statements
    }
    
    private func cachedStatement(forQuery query: String) -> FMStatement? {
        let statements = self.cachedStatements[query]
        return statements?.first(where: { $0.inUse == false })
    }
    
    public func makeFunctionNamed(_ name: String, maximumArguments count: Int32, withBlock block: @escaping FMDBSQLiteCallbackBlock) {
        let callback = FMDBSQLiteCallback<FMDBSQLiteCallbackBlock>(block: block)
        self.openFunctions.append(callback)
        /* I tried adding custom functions to release the block when the connection is destroyed- but they seemed to never be called, so we use _openFunctions to store the values instead. */
        sqlite3_create_function(self.db, name, count, SQLITE_UTF8, Unmanaged.passUnretained(callback).toOpaque(), sqliteCreateFunctionCallback, nil, nil)
    }
    
    // MARK: SQL manipulation
    fileprivate func bindObject(_ obj: Any?, toColumn idx: Int32, inStatement pStmt: OpaquePointer?) {
        if let objString = obj as? String {
            sqlite3_bind_text(pStmt, idx, objString, -1, SQLITE_TRANSIENT)
        }
        else if let nsNumber = obj as? NSNumber {
            let objCType = nsNumber.objCType.pointee
            switch objCType {
            case 0x0063/*"c"*/, 0x0043 /*"C"*/, 0x0073 /*"s"*/, 0x0053 /*"S"*/, 0x0069 /*"i"*/:
                sqlite3_bind_int(pStmt, idx, nsNumber.int32Value)
            case 0x0049/*"I"*/, 0x006C /*"l"*/, 0x004C /*"L"*/, 0x0071 /*"q"*/, 0x0051 /*"Q"*/:
                sqlite3_bind_int64(pStmt, idx, nsNumber.int64Value)
            case 0x0066/*"f"*/, 0x0064 /*"d"*/:
                sqlite3_bind_double(pStmt, idx, nsNumber.doubleValue)
            case 0x0042/*"B"*/:
                sqlite3_bind_int(pStmt, idx, nsNumber.boolValue ? 1 : 0)
            default:
                sqlite3_bind_text(pStmt, idx, nsNumber.description, -1, SQLITE_TRANSIENT)
            }
        }
        else if obj == nil || obj is NSNull {
            sqlite3_bind_null(pStmt, idx)
        }
        else if let objDate = obj as? Date {
            if self.hasDateFormatter() {
                sqlite3_bind_text(pStmt, idx, self.stringFromDate(objDate), -1, SQLITE_TRANSIENT)
            }
            else {
                sqlite3_bind_double(pStmt, idx, objDate.timeIntervalSince1970)
            }
        }
        else if let objData = obj as? Data {
            if objData.count > 0 {
                objData.withUnsafeBytes({ buf -> Void in
                    let bytes = buf.bindMemory(to: Int8.self).baseAddress
                    sqlite3_bind_blob(pStmt, idx, bytes, Int32(objData.count), SQLITE_TRANSIENT)
                })
            }
            else {
                sqlite3_bind_blob(pStmt, idx, "", 0, SQLITE_TRANSIENT)
            }
        }
        else if let objBool = obj as? Bool {
            sqlite3_bind_int(pStmt, idx, objBool ? Int32(1) : Int32(0))
        }
        else if let objInt = obj as? Int {
            sqlite3_bind_int64(pStmt, idx, Int64(objInt))
        }
        else if let objUInt = obj as? UInt {
            sqlite3_bind_int64(pStmt, idx, Int64(bitPattern: UInt64(objUInt)))
        }
        else if let objInt8 = obj as? Int8 {
            sqlite3_bind_int(pStmt, idx, Int32(objInt8))
        }
        else if let objUInt8 = obj as? UInt8 {
            sqlite3_bind_int(pStmt, idx, Int32(objUInt8))
        }
        else if let objInt16 = obj as? Int16 {
            sqlite3_bind_int(pStmt, idx, Int32(objInt16))
        }
        else if let objUInt16 = obj as? UInt16 {
            sqlite3_bind_int(pStmt, idx, Int32(objUInt16))
        }
        else if let objInt32 = obj as? Int32 {
            sqlite3_bind_int(pStmt, idx, objInt32)
        }
        else if let objUInt32 = obj as? UInt32 {
            sqlite3_bind_int(pStmt, idx, Int32(bitPattern: objUInt32))
        }
        else if let objInt64 = obj as? Int64 {
            sqlite3_bind_int64(pStmt, idx, objInt64)
        }
        else if let objUInt64 = obj as? UInt64 {
            sqlite3_bind_int64(pStmt, idx, Int64(bitPattern: objUInt64))
        }
        else if let objFloat = obj as? Float {
            sqlite3_bind_double(pStmt, idx, Double(objFloat))
        }
        else if let objDouble = obj as? Double {
            sqlite3_bind_double(pStmt, idx, objDouble)
        }
        else if let objString = obj as? CustomStringConvertible {
            assert(false, "Unknown type")
            sqlite3_bind_text(pStmt, idx, objString.description, -1, SQLITE_TRANSIENT)
        }
        else {
            assert(false, "Unknown type")
            sqlite3_bind_text(pStmt, idx, obj.debugDescription, -1, SQLITE_TRANSIENT)
        }
    }
    
}

// Wrapper to save block in ARC model
fileprivate final class FMDBSQLiteCallback<T> {
    fileprivate var block: T?
    
    fileprivate init(block: T?) {
        self.block = block
    }
    
    fileprivate func clear() {
        self.block = nil
    }
}

private let sqliteExecCallback: sqlite3_callback = { theBlockAsVoid, columns, values, names in
    guard let execCallbackBlock = Unmanaged<FMDBSQLiteCallback<FMDBExecuteStatementsCallbackBlock>>.fromOpaque(theBlockAsVoid!).takeUnretainedValue().block else {
        return SQLITE_OK
    }
    var dictionary = [String: String?](minimumCapacity: Int(columns))
    for i in 0 ..< Int(columns) {
        var key = String(utf8String: names![i]!)
        var value = values?[i] != nil ? String(utf8String: values![i]!) : nil
        dictionary[key!] = value
    }
    return execCallbackBlock(dictionary)
}

private func databaseBusyHandlerCallback(_ f: UnsafeMutableRawPointer?, _ count: Int32) -> Int32 {
    let fmDatabase = Unmanaged<FMDatabase>.fromOpaque(f!).takeUnretainedValue()
    if count == 0 {
        fmDatabase.startBusyRetryTime = Date.timeIntervalSinceReferenceDate
        return 1
    }
    let delta = Date.timeIntervalSinceReferenceDate - fmDatabase.startBusyRetryTime!
    if delta < fmDatabase.maxBusyRetryTimeInterval {
        sqlite3_sleep(50)
        // milliseconds
        return 1
    }
    return 0
}

private func sqliteCreateFunctionCallback(_ context: OpaquePointer?, _ aarc: Int32, _ aarg: UnsafeMutablePointer<OpaquePointer?>?) {
    let userData = sqlite3_user_data(context)
    if let block = Unmanaged<FMDBSQLiteCallback<FMDBSQLiteCallbackBlock>>.fromOpaque(userData!).takeUnretainedValue().block {
        block(context, aarc, aarg)
    }
}

/** Swift wrapper for `sqlite3_stmt`
 
 This is a wrapper for a SQLite `sqlite3_stmt`. Generally when using FMDB you will not need to interact directly with `FMStatement`, but rather with `<FMDatabase>` and `<FMResultSet>` only.
 
 ### See also
 
 - `<FMDatabase>`
 - `<FMResultSet>`
 - [`sqlite3_stmt`](http://www.sqlite.org/c3ref/stmt.html)
 */
@objc(RSMFMStatement)
public final class FMStatement: NSObject {

    /** Usage count */
    public var useCount: Int64
    
    /** SQL statement */
    public var query: String
    
    /** SQLite sqlite3_stmt
     
     @see [`sqlite3_stmt`](http://www.sqlite.org/c3ref/stmt.html)
     */
    public var statement: OpaquePointer?
    
    /** Indication of whether the statement is in use */
    
    public var inUse: Bool
    
    fileprivate override init() {
        self.useCount = 0
        self.query = ""
        self.inUse = false
    }
    
    deinit {
        self.close()
    }
    
    /** Close statement */
    
    public func close() {
        if statement != nil {
            sqlite3_finalize(statement!)
            statement = nil
        }
        inUse = false
    }
    
    /** Reset statement */
    
    public func reset() {
        if statement != nil {
            sqlite3_reset(statement!)
        }
        inUse = false
    }
    
    /// A textual representation of this instance.
    ///
    /// Instead of accessing this property directly, convert an instance of any
    /// type to a string by using the `String(describing:)` initializer.
    public override var description: String {
        return "\(self.useCount) hit(s) for query \(self.query)"
    }
    
    public static func == (lhs: FMStatement, rhs: FMStatement) -> Bool {
        return lhs.isEqual(rhs)
    }
    
    /// The hash value.
    ///
    /// Hash values are not guaranteed to be equal across different executions of
    /// your program. Do not save hash values to use during a future execution.
    public override var hash: Int {
        return self.statement?.hashValue ?? 0
    }
}

// Legacy Objective C methods
private extension FMDatabase {
    
    func legacyCountOfArgs(_ sql: String, cached: Bool) -> Int32 {
        if !self.databaseExists() {
            return -1
        }
        if self.isExecutingStatement {
            self.warnInUse()
            return -1
        }
        self.isExecutingStatement = true
        var rc: Int32 = 0
        var pStmt: OpaquePointer?
        var cachedStmt: FMStatement?
        if self.traceExecution {
            logger.trace("\(self) executeUpdate: \(sql)")
        }
        if cached {
            cachedStmt = self.cachedStatement(forQuery: sql)
            pStmt = cachedStmt?.statement
            cachedStmt?.reset()
        }
        if pStmt == nil {
            rc = sqlite3_prepare_v2(db, sql, -1, &pStmt, nil)
            if SQLITE_OK != rc {
                if logsErrors {
                    logger.error("DB Error: \(String(describing: self.lastErrorCode())) \"\(String(describing: self.lastErrorMessage()))\" DB Query: \(sql) DB Path: \(String(describing: databasePath))")
                }
                if rc == SQLITE_NOTADB || rc == SQLITE_CORRUPT, let dbCorruptionHandler = dbCorruptionHandler {
                    dbCorruptionHandler(sql)
                }
                if crashOnErrors {
                    assert(false, "DB Error: \(String(describing: self.lastErrorCode())) \"\(String(describing: self.lastErrorMessage()))\"")
                    abort()
                }
                sqlite3_finalize(pStmt)
                //outErr = self.lastError
                isExecutingStatement = false
                return -1
            }
        }
        let count = sqlite3_bind_parameter_count(pStmt)
        isExecutingStatement = false
        return count
    }
}

#if !os(Android) && !os(Windows)
// MARK: - Legacy Obj interface

public extension FMDatabase {
    @objc(executeCached:query:)
    @discardableResult
    func legacyExecuteQuery(cached: Bool, _ sql: String) -> FMResultSet? {
        return executeQuery(sql, withArgumentsInArray: nil, orDictionary: nil, cached: cached, cacheLimit: 1)
    }

    @objc(executeCached:update:)
    @discardableResult
    func legacyExecuteUpdate(cached: Bool, _ sql: String) -> Bool {
        var err: Error?
        return executeUpdate(sql, error: &err, withArgumentsInArray: nil, orDictionary: nil, cached: cached, cacheLimit: 1)
    }

    @objc(executeCached:query:withArgumentsInArray:)
    @discardableResult
    func legacyExecuteQuery(cached: Bool, _ sql: String, withArgumentsInArray arguments: [Any]) -> FMResultSet? {
        return executeQuery(sql, withArgumentsInArray: arguments, orDictionary: nil, cached: cached, cacheLimit: 1)
    }

    @objc(executeCached:update:withArgumentsInArray:)
    @discardableResult
    func legacyExecuteUpdate(cached: Bool, _ sql: String, withArgumentsInArray arguments: [Any]) -> Bool {
        var err: Error?
        return executeUpdate(sql, error: &err, withArgumentsInArray: arguments, orDictionary: nil, cached: cached, cacheLimit: 1)
    }
    
    func executeUpdate(cached: Bool, _ sql: String, withError: UnsafeMutablePointer<Error?>, withArgumentsInArray args: [Any]) -> Bool {
        var error: Error?
        let result = executeUpdate(sql, error: &error, withArgumentsInArray: args, orDictionary: nil, cached: cached, cacheLimit: 1)
        withError.pointee = error
        return result
    }
    
    func legacyInSavePoint(_ block: (_ rollback: UnsafeMutablePointer<Bool>) -> Void) -> Error? {
        return self.inSavePoint({ (_ rollback: inout Bool) -> Void in
            var rollbackObj: Bool = false
            block(&rollbackObj)
            rollback = rollbackObj
        })
    }
}
#endif

// MARK: - Cache

internal extension FMDatabase {

    /** Key for prepared SQL Statement

     @param pStmt prepared sql statement was created by sqlite3_prepare_v2(), sqlite3_prepare_v3(), sqlite3_prepare16_v2(), or sqlite3_prepare16_v3().

     @return key if success, `nil` on error.

     @see [sqlite3_normalized_sql()](https://sqlite.org/c3ref/expanded_sql.html)
     */
    func keyFor(pStmt: OpaquePointer?) -> String? {
        #if !os(Android) && DEBUG
        guard let utf8String = sqlite3_normalized_sql(pStmt) else {
            return nil
        }
        guard let key = String(utf8String: utf8String) else {
            return nil
        }
        return key
            .replacingOccurrences(of: "(?,?,?)", with: "()") // 'IN (a, b, ...)' -> '()'
            .replacingOccurrences(of: "\\b([^=]+)\\=\\?", with: "$1 ", options: .regularExpression) // 'a = 1' -> 'a'
            .replacingOccurrences(of: "\\b(\\w\\sOR)(?:\\s\\1)+", with: "$1", options: .regularExpression) // 'a OR a OR ' -> 'a OR '
            .replacingOccurrences(of: "\\b(\\w\\sAND)(?:\\s\\1)+", with: "$1", options: .regularExpression) // 'a AND a AND ' -> 'a AND '
            .lowercased()
        #else
        return nil
        #endif
    }

}

// MARK: - Query/Update parameters

public extension FMDatabase {
    @discardableResult func executeUpdate(_ parameters: FMParameters) -> Bool {
        var error: Error?
        return executeUpdate(parameters.sql,
                             error: &error,
                             withArgumentsInArray: parameters.arguments,
                             orDictionary: parameters.parameters,
                             cached: parameters.cached,
                             cacheLimit: parameters.cacheLimit)
    }

    func executeQuery(_ parameters: FMParameters) -> FMResultSet? {
        return executeQuery(parameters.sql,
                            withArgumentsInArray: parameters.arguments,
                            orDictionary: parameters.parameters,
                            cached: parameters.cached,
                            cacheLimit: parameters.cacheLimit)
    }
}
