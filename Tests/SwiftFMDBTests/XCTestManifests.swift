import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(SCDatabaseTest.allTests),
        testCase(SCDatabaseQueueTests.allTests),
        testCase(SCDatabasePoolTests.allTests),
        testCase(SCDatabaseAdditionsTests.allTests),
        testCase(SCResultSetCostTests.allTests),
        testCase(SQLiteTests.allTests),
        testCase(SQLiteBenchmark.allTests),
    ]
}
#endif
