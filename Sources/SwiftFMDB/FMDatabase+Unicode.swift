#if FMUNICODE_ENABLE
import FMUnicode
#endif

#if SWIFT_PACKAGE
import SQLiteEE
#elseif os(Windows)
import sqlite
#else
import RDSQLite3
#endif

extension FMDatabase {

    internal func registerUnicodeFunctions() -> Int32 {
        #if FMUNICODE_ENABLE
        return registerUnicodeCallbacks(in: self.db)
        #else
        return SQLITE_OK
        #endif
    }

}
