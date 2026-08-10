#if EnableUnicodeReplacement && EnableSQLiteICU
#error("EnableUnicodeReplacement and EnableSQLiteICU are mutually exclusive production configurations.")
#endif

#if EnableUnicodeReplacement
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
        #if EnableUnicodeReplacement
        return registerUnicodeCallbacks(in: self.db)
        #else
        return SQLITE_OK
        #endif
    }

}
