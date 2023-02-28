//
//  FMResultSet.swift
//  SmartMailCore
//
//  Created by Andrew on 3/23/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Foundation
import Logging

#if SWIFT_PACKAGE
import SQLiteEE
#else
import RDSQLite3
#endif

public final class FMResultSet: NSObject {
    
    public internal(set) var parentDB: FMDatabase
    public var query: String?
    public var statement: FMStatement!
    private var _columnNameToIndexMap: [String: Int32]?
    
    // MARK: Creating and closing database
    
    /** Create result set from `<FMStatement>`
     
     @param statement A `<FMStatement>` to be performed
     
     @param aDB A `<FMDatabase>` to be used
     
     @return A `FMResultSet` on success; `nil` on failure
     */
    
    public static func resultSet(withStatement statement: FMStatement, usingParentDatabase db: FMDatabase) -> FMResultSet {
        return FMResultSet(statement: statement, usingParentDatabase: db)
    }
    
    init(statement: FMStatement, usingParentDatabase: FMDatabase) {
        self.statement = statement
        self.parentDB = usingParentDatabase
        assert(statement.inUse == false)
        statement.inUse = true
    }
    
    deinit {
        self.close()
    }
    
    public func close() {
        if self.statement != nil {
            self.parentDB.resultSetDidClose(self)
            self.statement?.reset()
            self.statement = nil
            // TODO: should be done only once
        }
    }
    
    // MARK: Iterating through the result set
    
    /** Retrieve next row for result set.
     
     You must always invoke `next` before attempting to access the values returned in a query, even if you're only expecting one.
     
     @return `true` if row successfully retrieved; `false` if end of result set reached
     
     @see hasAnotherRow
     */
    
    @discardableResult
    public func next() -> Bool {
        let t1 = Date.timeIntervalSinceReferenceDate
        
        let rc = sqlite3_step(self.statement.statement)
        if SQLITE_BUSY == rc || SQLITE_LOCKED == rc {
            logger.error("\(#function):\(#line) Database busy (\(parentDB.databasePath ?? "---"))")
            NSLog("Database busy")
        }
        else if SQLITE_DONE == rc || SQLITE_ROW == rc {
            // all is well, let's return.
        }
        else if SQLITE_ERROR == rc {
            logger.error("Error calling sqlite3_step (\(rc): \(parentDB.lastErrorMessage() ?? "---")) rs")
        }
        else if SQLITE_MISUSE == rc {
            // uh oh.
            logger.error("Error calling sqlite3_step (\(rc): \(parentDB.lastErrorMessage() ?? "---")) rs")
        }
        else {
            // wtf?
            logger.error("Unknown error calling sqlite3_step (\(rc): \(parentDB.lastErrorMessage() ?? "---"))  rs")
        }
        if rc != SQLITE_ROW {
            self.close()
        }
        
        let t2 = Date.timeIntervalSinceReferenceDate
        let diff = t2 - t1
        if diff > 0.1 {
            if Thread.isMainThread {
                logger.info("Query is executed too long (main thread), time: \(diff) sec query:\n\(query ?? "---"))")
            }
            else if diff > 1 {
                logger.info("Query is executed too long (back thread), time: \(diff) sec query:\n\(query ?? "---"))")
            }
        }
        
        if rc == SQLITE_NOTADB || rc == SQLITE_CORRUPT, let dbCorruptionHandler = parentDB.dbCorruptionHandler {
            dbCorruptionHandler(query ?? "no query")
        }
        
        return (rc == SQLITE_ROW)
    }
    
    /** Did the last call to `<next>` succeed in retrieving another row?
     
     @return `true` if the last call to `<next>` succeeded in retrieving another record; `false` if not.
     
     @see next
     
     @warning The `hasAnotherRow` method must follow a call to `<next>`. If the previous database interaction was something other than a call to `next`, then this method may return `false`, whether there is another row of data or not.
     */
    
    public func hasAnotherRow() -> Bool {
        return parentDB.lastErrorCode() == SQLITE_ROW
    }
    
    // MARK: Retrieving information from result set
    
    /** How many columns in result set
     
     @return Integer value of the number of columns.
     */
    
    public func columnCount() -> Int32 {
        return sqlite3_column_count(statement.statement)
    }
    
    public var columnNameToIndexMap: [String: Int32] {
        if _columnNameToIndexMap == nil {
            let columnCount = sqlite3_column_count(statement.statement)
            _columnNameToIndexMap = [String: Int32](minimumCapacity: Int(columnCount))
            for columnIdx in 0 ..< columnCount {
                _columnNameToIndexMap![String(utf8String: sqlite3_column_name(statement.statement, columnIdx))!.lowercased()] = columnIdx
            }
        }
        return _columnNameToIndexMap!
    }
    
    /** Column index for column name
     
     @param columnName `String` value of the name of the column.
     
     @return Zero-based index for column.
     */
    
    public func columnIndex(forName columnName: String) -> Int32 {
        let column = columnName.lowercased()
        if let n = self.columnNameToIndexMap[column] {
            return n
        }
        logger.warning("Warning: I could not find the column named '\(columnName)'.")
        return -1
    }
    
    /** Column name for column index
     
     @param columnIdx Zero-based index for column.
     
     @return columnName `String` value of the name of the column.
     */
    
    public func columnName(forIndex columnIdx: Int32) -> String? {
        return String(utf8String: sqlite3_column_name(statement.statement, columnIdx))
    }
    
    /** Result set Int32 value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `Int32` value of the result set's column.
     */
    
    public func int32(forColumn columnName: String) -> Int32 {
        return sqlite3_column_int(statement.statement, self.columnIndex(forName: columnName))
    }
    
    /** Result set Int32 value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `Int32` value of the result set's column.
     */
    
    public func int32(forColumnIndex columnIdx: Int32) -> Int32 {
        return sqlite3_column_int(statement.statement, columnIdx)
    }

    /** Result set Int value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `Int` value of the result set's column.
     */
    
    public func int(forColumn columnName: String) -> Int {
        return Int(sqlite3_column_int64(statement.statement, self.columnIndex(forName: columnName)))
    }
    
    /** Result set Int value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `Int` value of the result set's column.
     */
    
    public func int(forColumnIndex columnIdx: Int32) -> Int {
        return Int(sqlite3_column_int64(statement.statement, columnIdx))
    }
    
    /** Result set `Int64` value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `Int64` value of the result set's column.
     */
    
    public func int64(forColumn columnName: String) -> Int64 {
        return sqlite3_column_int64(statement.statement, self.columnIndex(forName: columnName))
    }
    
    /** Result set `Int64` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `Int64` value of the result set's column.
     */
    
    public func int64(forColumnIndex columnIdx: Int32) -> Int64 {
        return sqlite3_column_int64(statement.statement, columnIdx)
    }

    /** Result set Unt32 value for column.

     @param columnName `String` value of the name of the column.

     @return `Unt32` value of the result set's column.
    */

    public func uint32(forColumn columnName: String) -> UInt32 {
        return UInt32(bitPattern: sqlite3_column_int(statement.statement, self.columnIndex(forName: columnName)))
    }

    /** Result set Unt32 value for column.

     @param columnIdx Zero-based index for column.

     @return `Unt32` value of the result set's column.
     */

    public func uint32(forColumnIndex columnIdx: Int32) -> UInt32 {
        return UInt32(bitPattern: sqlite3_column_int(statement.statement, columnIdx))
    }

    /** Result set `UInt` value for column.

     @param columnName `String` value of the name of the column.

     @return `UInt` value of the result set's column.
     */

    public func uint(forColumn columnName: String) -> UInt {
        return UInt(bitPattern: Int(sqlite3_column_int64(statement.statement, self.columnIndex(forName: columnName))))
    }

    /** Result set `UInt` value for column.

     @param columnIdx Zero-based index for column.

     @return `UInt` value of the result set's column.
     */

    public func uint(forColumnIndex columnIdx: Int32) -> UInt {
        return UInt(bitPattern: Int(sqlite3_column_int64(statement.statement, columnIdx)))
    }

    /** Result set `UInt64` value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `UInt64` value of the result set's column.
     */
    
    public func uint64(forColumn columnName: String) -> UInt64 {
        return UInt64(bitPattern: sqlite3_column_int64(statement.statement, columnIndex(forName: columnName)))
    }
    
    /** Result set `UInt64` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `UInt64` value of the result set's column.
     */
    
    public func uint64(forColumnIndex columnIdx: Int32) -> UInt64 {
        return UInt64(bitPattern: sqlite3_column_int64(statement.statement, columnIdx))
    }
    
    /** Result set `BOOL` value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `BOOL` value of the result set's column.
     */
    
    public func bool(forColumn columnName: String) -> Bool {
        return int32(forColumnIndex: self.columnIndex(forName: columnName)) == 1
    }
    
    /** Result set `BOOL` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `BOOL` value of the result set's column.
     */
    
    public func bool(forColumnIndex columnIdx: Int32) -> Bool {
        return int32(forColumnIndex: columnIdx) == 1
    }
    
    /** Result set `double` value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `double` value of the result set's column.
     
     */
    
    public func double(forColumn columnName: String) -> Double {
        return sqlite3_column_double(statement.statement, self.columnIndex(forName: columnName))
    }
    
    /** Result set `double` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `double` value of the result set's column.
     
     */
    
    public func double(forColumnIndex columnIdx: Int32) -> Double {
        return sqlite3_column_double(statement.statement, columnIdx)
    }
    
    /** Result set `String` value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `String` value of the result set's column.
     
     */
    
    public func string(forColumn columnName: String) -> String? {
        return self.string(forColumnIndex: self.columnIndex(forName: columnName))
    }
    
    /** Result set `String` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `String` value of the result set's column.
     */
    
    public func string(forColumnIndex columnIdx: Int32) -> String? {
        if sqlite3_column_type(statement.statement, columnIdx) == SQLITE_NULL || (columnIdx < 0) {
            return nil
        }
        guard let c = sqlite3_column_text(statement.statement, columnIdx) else {
            // null row.
            return nil
        }
        return String(cString: c)
    }
    
    /** Result set `Date` value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `Date` value of the result set's column.
     */
    
    public func date(forColumn columnName: String) -> Date? {
        return self.date(forColumnIndex: self.columnIndex(forName: columnName))
    }
    
    /** Result set `Date` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `Date` value of the result set's column.
     
     */
    
    public func date(forColumnIndex columnIdx: Int32) -> Date? {
        if sqlite3_column_type(statement.statement, columnIdx) == SQLITE_NULL || (columnIdx < 0) {
            return nil
        }
        return parentDB.hasDateFormatter() ? parentDB.dateFromString(self.string(forColumnIndex: columnIdx)) : Date(timeIntervalSince1970: self.double(forColumnIndex: columnIdx))
    }
    
    /** Result set `Data` value for column.
     
     This is useful when storing binary data in table (such as image or the like).
     
     @param columnName `String` value of the name of the column.
     
     @return `Data` value of the result set's column.
     
     */
    
    public func data(forColumn columnName: String) -> Data? {
        return self.data(forColumnIndex: self.columnIndex(forName: columnName))
    }
    
    /** Result set `Data` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `Data` value of the result set's column.
     */
    
    public func data(forColumnIndex columnIdx: Int32) -> Data? {
        if sqlite3_column_type(statement.statement, columnIdx) == SQLITE_NULL || (columnIdx < 0) {
            return nil
        }
        
        let dataSize = sqlite3_column_bytes(statement.statement, columnIdx)
        if dataSize == 0 { // The return value from sqlite3_column_blob() for a zero-length BLOB is a NULL pointer. https://www.sqlite.org/c3ref/column_blob.html
            return Data()
        }
        
        return Data(bytes: sqlite3_column_blob(statement.statement, columnIdx), count: Int(dataSize))
    }
    
    /** Result set object for column.
     
     @param columnName `NSString` value of the name of the column.
     
     @return Either `NSNumber`, `NSString`, `NSData`, or `NSNull`. If the column was `NULL`, this returns `[NSNull null]` object.
     
     @see objectForKeyedSubscript:
     */
    
    public func object(forColumn columnName: String) -> Any? {
        return self.object(forColumnIndex: self.columnIndex(forName: columnName))
    }
    
    /** Result set object for column.
     
     @param columnIdx Zero-based index for column.
     
     @return Either `NSNumber`, `NSString`, `NSData`, or `NSNull`. If the column was `NULL`, this returns `[NSNull null]` object.
     
     @see objectAtIndexedSubscript:
     */
    
    public func object(forColumnIndex columnIdx: Int32) -> Any? {
        let columnType = sqlite3_column_type(statement.statement, columnIdx)
        var returnValue: Any? 
        if columnType == SQLITE_INTEGER {
            returnValue = Int(self.int64(forColumnIndex: columnIdx))
        }
        else if columnType == SQLITE_FLOAT {
            returnValue = Int(self.double(forColumnIndex: columnIdx))
        }
        else if columnType == SQLITE_BLOB {
            returnValue = self.data(forColumnIndex: columnIdx)
        }
        else {
            //default to a string for everything else
            returnValue = self.string(forColumnIndex: columnIdx)
        }
        return returnValue
    }
    
    /** Result set `Data` value for column.
     
     @param columnName `String` value of the name of the column.
     
     @return `Data` value of the result set's column.
     
     @warning If you are going to use this data after you iterate over the next row, or after you close the
     result set, make sure to make a copy of the data first (or just use `<dataForColumn:>`/`<dataForColumnIndex:>`)
     If you don't, you're going to be in a world of hurt when you try and use the data.
     
     */
    
    public func dataNoCopy(forColumn columnName: String) -> Data? {
        return dataNoCopy(forColumnIndex: self.columnIndex(forName: columnName))
    }
    
    /** Result set `Data` value for column.
     
     @param columnIdx Zero-based index for column.
     
     @return `Data` value of the result set's column.
     
     @warning If you are going to use this data after you iterate over the next row, or after you close the
     result set, make sure to make a copy of the data first (or just use `<dataForColumn:>`/`<dataForColumnIndex:>`)
     If you don't, you're going to be in a world of hurt when you try and use the data.
     
     */
    
    public func dataNoCopy(forColumnIndex columnIdx: Int32) -> Data? {
        if sqlite3_column_type(statement.statement, columnIdx) == SQLITE_NULL || (columnIdx < 0) {
            return nil
        }
        let dataSize = sqlite3_column_bytes(statement.statement, columnIdx)
        return Data(bytesNoCopy: UnsafeMutableRawPointer(mutating: sqlite3_column_blob(statement.statement, columnIdx)), count: Int(dataSize), deallocator: Data.Deallocator.none)
    }
    
    /** Is the column `NULL`?
     
     @param columnIdx Zero-based index for column.
     
     @return `true` if column is `NULL`; `false` if not `NULL`.
     */
    
    public func columnIndexIsNull(_ columnIdx: Int32) -> Bool {
        return sqlite3_column_type(statement.statement, columnIdx) == SQLITE_NULL
    }
    
    /** Is the column `NULL`?
     
     @param columnName `NSString` value of the name of the column.
     
     @return `true` if column is `NULL`; `false` if not `NULL`.
     */
    
    public func columnIsNull(_ columnName: String) -> Bool {
        return self.columnIndexIsNull(self.columnIndex(forName: columnName))
    }
    
    /** Returns a dictionary of the row results mapped to case sensitive keys of the column names. 
     
     @returns `Dictionary` of the row results.
     
     @warning The keys to the dictionary are case sensitive of the column names.
     */
    
    public func resultDictionary() -> [AnyHashable: Any]? {
        let num_cols = Int(sqlite3_data_count(statement.statement))
        if num_cols > 0 {
            var dict = [AnyHashable: Any](minimumCapacity: num_cols)
            let columnCount = sqlite3_column_count(statement.statement)
            for columnIdx in 0..<columnCount {
                if let columnName = String(utf8String: sqlite3_column_name(statement.statement, columnIdx)) {
                    let objectValue = self.object(forColumnIndex: columnIdx)
                    dict[columnName] = objectValue
                }
            }
            return dict
        }
        else {
            logger.warning("Warning: There seem to be no columns in this set.")
        }
        return nil
    }
    
    // subscript for column index
    public subscript(index: Int) -> Any? {
        return self.object(forColumnIndex: Int32(index))
    }
    
    // subscript for column name
    public subscript(index: String) -> Any? {
        return self.object(forColumn: index)
    }
    
    /// The hash value.
    ///
    /// Hash values are not guaranteed to be equal across different executions of
    /// your program. Do not save hash values to use during a future execution.
    public override var hash: Int {
        return self.statement.statement?.hashValue ?? 0
    }
    
    public static func == (lhs: FMResultSet, rhs: FMResultSet) -> Bool {
        return lhs.isEqual(rhs)
    }
}

// Legacy extension for Objective-C
public extension FMResultSet {

    func longLongInt(forColumnIndex columnIdx: Int32) -> UInt64 {
        return uint64(forColumnIndex: columnIdx)
    }
}
