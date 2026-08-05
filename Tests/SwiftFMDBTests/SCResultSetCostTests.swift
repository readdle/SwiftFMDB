//
//  SCResultSetCostTests.swift
//  SwiftFMDB
//
//  Copyright © 2026 Readdle. All rights reserved.
//

import Foundation
@testable import SwiftFMDB
import XCTest

public class SCResultSetCostTests: SCDBTempDBTests {

    public override func setUp() {
        super.setUp()
        _ = db.executeUpdate(cached: false, "create table cost (a integer, b text)")
    }

    private func insertRows(_ count: Int) {
        _ = db.beginTransaction()
        for i in 0..<count {
            _ = db.executeUpdate(cached: true, "insert into cost (a, b) values (?, ?)", i, "row \(i)")
        }
        _ = db.commit()
    }

    // MARK: - Whole-query accounting

    /// The old instrumentation timed a single `sqlite3_step`, so a query costing seconds across many
    /// *fast* steps reported nothing. The result set now carries the totals for the whole query.
    func testResultSetAccumulatesEveryStep() {
        let rows = 500
        insertRows(rows)

        guard let rs = db.executeQuery(cached: true, "select a, b from cost") else {
            return XCTFail("no result set")
        }

        var seen = 0
        while rs.next() {
            seen += 1
        }

        XCTAssertEqual(seen, rows)
        // One step per row plus the final SQLITE_DONE step.
        XCTAssertEqual(rs.stepCount, rows + 1)
        // next() closed it on the SQLITE_DONE step, so the totals are already final.
        XCTAssertNil(rs.statement)
    }

    /// The cost has to span the whole iteration, not one step: a query is slow for its caller even when
    /// every single step was fast.
    func testTotalTimeCoversWorkBetweenSteps() {
        insertRows(20)

        guard let rs = db.executeQuery(cached: true, "select a, b from cost") else {
            return XCTFail("no result set")
        }

        let callerWorkPerRow = 0.01
        var rowsRead = 0
        while rs.next() {
            rowsRead += 1
            Thread.sleep(forTimeInterval: callerWorkPerRow)
        }

        XCTAssertEqual(rowsRead, 20)

        let expectedWork = Double(rowsRead) * callerWorkPerRow
        XCTAssertGreaterThan(rs.totalTime, expectedWork * 0.5)
    }

    /// The clock starts at the first step, so a result set nobody iterated has nothing to report and
    /// cannot drag statement preparation or a cold start into the number.
    func testUnsteppedResultSetHasNoCost() {
        insertRows(5)

        guard let rs = db.executeQuery(cached: true, "select a, b from cost") else {
            return XCTFail("no result set")
        }

        Thread.sleep(forTimeInterval: 0.15)
        XCTAssertEqual(rs.totalTime, 0)
        XCTAssertEqual(rs.stepCount, 0)

        XCTAssertTrue(rs.next())
        XCTAssertGreaterThan(rs.totalTime, 0)
        rs.close()
    }

    /// Without a stop at close every reported duration would keep growing until the object died,
    /// inflating each one by however long the caller held on to it.
    func testTotalTimeStopsAtClose() {
        insertRows(5)

        guard let rs = db.executeQuery(cached: true, "select a, b from cost") else {
            return XCTFail("no result set")
        }

        XCTAssertTrue(rs.next())
        rs.close()
        let atClose = rs.totalTime

        Thread.sleep(forTimeInterval: 0.05)

        XCTAssertEqual(rs.totalTime, atClose)
        XCTAssertNil(rs.statement)
        XCTAssertFalse(db.hasOpenResultSets())
    }

    public static var allTests = [
        ("testResultSetAccumulatesEveryStep", testResultSetAccumulatesEveryStep),
        ("testTotalTimeCoversWorkBetweenSteps", testTotalTimeCoversWorkBetweenSteps),
        ("testUnsteppedResultSetHasNoCost", testUnsteppedResultSetHasNoCost),
        ("testTotalTimeStopsAtClose", testTotalTimeStopsAtClose)
    ]

}
