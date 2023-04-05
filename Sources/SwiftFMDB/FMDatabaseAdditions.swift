//
//  FMDatabaseAdditions.swift
//  SmartMailCore
//
//  Created by Andrew on 3/24/17.
//  Copyright © 2017 Readdle. All rights reserved.
//

import Foundation
import Logging

#if SWIFT_PACKAGE
import SQLite
#elseif os(Windows)
import sqlite
#else
import RDSQLite3
#endif

/** Category of additions for `<FMDatabase>` class.
 
 ### See also
 
 - `<FMDatabase>`
 */

public extension FMDatabase {
    
    private func result<T>(forQuery query: String, cached: Bool, withArgs args: [Any?]?, withBlock block: (_: FMResultSet) -> T?) -> T? {
        guard let resultSet = self.executeQuery(query, withArgumentsInArray: args, orDictionary: nil, cached: cached, cacheLimit: 1), resultSet.next() else {
            return nil
        }
        let result = block(resultSet)
        resultSet.close()
        return result
    }
    
    // MARK: Schema related operations
    
    /** Does table exist in database?
     
     @param tableName The name of the table being looked for.
     
     @return `YES` if table found; `NO` if not found.
     */
    func tableExists(_ tableName: String) -> Bool {
        let tableName = tableName.lowercased()
        let rs = executeQuery(cached: true, "select [sql] from sqlite_master where [type] = 'table' and lower(name) = ?", withArgumentsInArray: [tableName])
        //if at least one next exists, table exists
        let returnBool = rs?.next()
        //close and free object
        rs?.close()
        return returnBool ?? false
    }
    /** The schema of the database.
     
     This will be the schema for the entire database. For each entity, each row of the result set will include the following fields:
     
     - `type` - The type of entity (e.g. table, index, view, or trigger)
     - `name` - The name of the object
     - `tbl_name` - The name of the table to which the object references
     - `rootpage` - The page number of the root b-tree page for tables and indices
     - `sql` - The SQL that created the entity
     
     @return `FMResultSet` of schema; `nil` on error.
     
     @see [SQLite File Format](http://www.sqlite.org/fileformat.html)
     */
    func getSchema() -> FMResultSet? {
        //result colums: type[STRING], name[STRING],tbl_name[STRING],rootpage[INTEGER],sql[STRING]
        let rs = executeQuery(cached: true, "SELECT type, name, tbl_name, rootpage, sql FROM (SELECT * FROM sqlite_master UNION ALL SELECT * FROM sqlite_temp_master) WHERE type != 'meta' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, type DESC, name")
        return rs
    }
    
    /** The schema of the database.
     
     This will be the schema for a particular table as report by SQLite `PRAGMA`, for example:
     
     PRAGMA table_info('employees')
     
     This will report:
     
     - `cid` - The column ID number
     - `name` - The name of the column
     - `type` - The data type specified for the column
     - `notnull` - whether the field is defined as NOT NULL (i.e. values required)
     - `dflt_value` - The default value for the column
     - `pk` - Whether the field is part of the primary key of the table
     
     @param tableName The name of the table for whom the schema will be returned.
     
     @return `FMResultSet` of schema; `nil` on error.
     
     @see [table_info](http://www.sqlite.org/pragma.html#pragma_table_info)
     */
    func getTableSchema(_ tableName: String) -> FMResultSet? {
        //result colums: cid[INTEGER], name,type [STRING], notnull[INTEGER], dflt_value[],pk[INTEGER]
        let rs = executeQuery(cached: false, "pragma table_info('\(tableName)')")
        return rs
    }
    
    /** Test to see if particular column exists for particular table in database
     
     @param columnName The name of the column.
     
     @param tableName The name of the table.
     
     @return `YES` if column exists in table in question; `NO` otherwise.
     */
    func columnExists(_ columnName: String, inTableWithName tableName: String) -> Bool {
        var returnBool: Bool = false
        let tableName = tableName.lowercased()
        let columnName = columnName.lowercased()
        if let rs = getTableSchema(tableName) {
            //check if column is present in table schema
            while rs.next() {
                if rs.string(forColumn: "name")?.lowercased() == columnName {
                    returnBool = true
                    break
                }
            }
            //If this is not done FMDatabase instance stays out of pool
            rs.close()
        }
        return returnBool
    }
    
    func applicationID() -> UInt64 {
        var r: UInt64 = 0
        if let rs = executeQuery(cached: true, "pragma application_id"), rs.next() {
            r = rs.uint64(forColumnIndex: 0)
            rs.close()
        }
        return r
    }
    
    func setApplicationID(_ appID: UInt64) {
        let query: String = "pragma application_id=\(appID)"
        let rs = executeQuery(cached: false, query)
        _ = rs?.next()
        rs?.close()
    }
    
    var userVersion: UInt64 {
        get {
            var r: UInt64 = 0
            if let rs = executeQuery(cached: true, "pragma user_version"), rs.next() {
                r = rs.uint64(forColumnIndex: 0)
                rs.close()
            }
            return r
        }
        set {
            let query: String = "pragma user_version = \(newValue)"
            let rs = executeQuery(cached: false, query)
            _ = rs?.next()
            rs?.close()
        }
    }
    
    //clang diagnostic pop
    func validateSQL(_ sql: String, error: inout Error?) -> Bool {
        var pStmt: OpaquePointer?
        var validationSucceeded: Bool = true
        let rc = sqlite3_prepare_v2(db, sql, -1, &pStmt, nil)
        if rc != SQLITE_OK {
            validationSucceeded = false
            error = lastError()
        }
        sqlite3_finalize(pStmt)
        return validationSucceeded
    }
}

/** Caching API extension.

*/

public extension FMDatabase {
    func bool(forQuery query: String, cached: Bool, _ args: Any? ...) -> Bool? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.bool(forColumnIndex: 0) })
    }

    func string(forQuery query: String, cached: Bool, _ args: Any? ...) -> String? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.string(forColumnIndex: 0) })
    }

    func int(forQuery query: String, cached: Bool, _ args: Any? ...) -> Int? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.int(forColumnIndex: 0) })
    }

    func double(forQuery query: String, cached: Bool, _ args: Any? ...) -> Double? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.double(forColumnIndex: 0) })
    }

    func data(forQuery query: String, cached: Bool, _ args: Any? ...) -> Data? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.data(forColumnIndex: 0) })
    }

    func date(forQuery query: String, cached: Bool, _ args: Any? ...) -> Date? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.date(forColumnIndex: 0) })
    }

    func indexSet(forQuery query: String, cached: Bool, _ args: Any? ...) -> IndexSet? {
        return indexSet(forQuery: query, cached: cached, args)
    }

    func indexSet(forQuery query: String, cached: Bool, _ args: [Any?]?) -> IndexSet? {
        guard let rs = self.executeQuery(query, withArgumentsInArray: args, orDictionary: nil, cached: cached, cacheLimit: 1) else {
            assert(self.lastError()?.code == FMDatabaseError.SQLITE_OK.rawValue)
            return nil
        }

        var r = IndexSet()

        while rs.next() {
            let value = rs.int(forColumnIndex: 0)
            r.insert(value)
        }
        rs.close()

        assert(self.lastError()?.code == FMDatabaseError.SQLITE_OK.rawValue)

        return r
    }
}

public extension FMDatabase {
    func bool(forQuery query: String, cached: Bool, withArgs args: [Any]) -> Bool {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.bool(forColumnIndex: 0) }) ?? false
    }

    func string(forQuery query: String, cached: Bool, withArgs args: [Any]) -> String? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.string(forColumnIndex: 0) })
    }

    func int(forQuery query: String, cached: Bool, withArgs args: [Any]) -> Int {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.int(forColumnIndex: 0) }) ?? 0
    }

    func double(forQuery query: String, cached: Bool, withArgs args: [Any]) -> Double {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.double(forColumnIndex: 0) }) ?? 0.0
    }

    func data(forQuery query: String, cached: Bool, withArgs args: [Any]) -> Data? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.data(forColumnIndex: 0) })
    }

    func date(forQuery query: String, cached: Bool, withArgs args: [Any]) -> Date? {
        return result(forQuery: query, cached: cached, withArgs: args, withBlock: { $0.date(forColumnIndex: 0) })
    }
}

extension NSObject {
    
    internal var logger: Logger {
        return type(of: self).logger
    }
    
    internal static var logger: Logger {
        return Logger(label: String(describing: self))
    }

}
