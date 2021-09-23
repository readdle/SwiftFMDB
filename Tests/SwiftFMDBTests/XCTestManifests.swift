import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(SCDatabaseTest.allTests),
        testCase(SCDatabaseQueueTests.allTests),
        testCase(SCDatabasePoolTests.allTests),
        testCase(SCDatabaseAdditionsTests.allTests),
        testCase(SQLiteTests.allTests),
    ]
}
#endif
