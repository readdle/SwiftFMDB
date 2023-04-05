import XCTest

let allTests = [
    testCase(SCDatabaseTest.allTests),
    testCase(SCDatabaseQueueTests.allTests),
    testCase(SCDatabasePoolTests.allTests),
    testCase(SCDatabaseAdditionsTests.allTests),
    testCase(SQLiteTests.allTests),
]

XCTMain(allTests)
