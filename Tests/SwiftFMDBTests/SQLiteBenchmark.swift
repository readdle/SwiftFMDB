//
//  SQLiteBenchmark.swift
//  SmartMailCore
//
//  Created by Cursor on 23.05.2026.
//  Copyright © 2026 Readdle. All rights reserved.
//

import Foundation
@testable import SwiftFMDB
import XCTest

class SQLiteBenchmark: SCDBTempDBTests {

    func testUnicodeLikePerformanceEvaluation() {
        #if SWIFTFMDB_ENABLE_SQLITE_BENCHMARKS
#if EnableUnicodeReplacement
        let implementationLabel = "SwiftFMDB replacement"
#else
        let implementationLabel = "SQLite ICU"
#endif
        let rowCount = 500_000
        let functionCallCount = rowCount
        let measuredPassCount = 6

        XCTAssertTrue(db.executeUpdate(cached: false, "DROP TABLE IF EXISTS unicode_like_performance_input;"))
        XCTAssertTrue(db.executeUpdate(cached: false, """
            CREATE TEMP TABLE unicode_like_performance_input (
                value TEXT NOT NULL,
                prefix_pattern TEXT NOT NULL,
                one_pattern TEXT NOT NULL,
                escaped_value TEXT NOT NULL,
                escape_pattern TEXT NOT NULL
            );
            """))
        XCTAssertTrue(db.executeUpdate(cached: false, """
            WITH RECURSIVE sequence(n) AS (
                VALUES(1)
                UNION ALL
                SELECT n + 1 FROM sequence WHERE n < \(rowCount)
            )
            INSERT INTO unicode_like_performance_input (value, prefix_pattern, one_pattern, escaped_value, escape_pattern)
            SELECT
                CASE n % 5
                    WHEN 0 THEN 'çoğunlukla'
                    WHEN 1 THEN 'mañana'
                    WHEN 2 THEN 'caféteria'
                    WHEN 3 THEN 'добро'
                    ELSE 'æther'
                END,
                CASE n % 5
                    WHEN 0 THEN 'ÇOĞUN%'
                    WHEN 1 THEN 'MAÑ%'
                    WHEN 2 THEN 'CAFÉ%'
                    WHEN 3 THEN 'ДОБ%'
                    ELSE 'ÆTH%'
                END,
                CASE n % 5
                    WHEN 0 THEN 'ÇOĞUN_____'
                    WHEN 1 THEN 'MAÑ___'
                    WHEN 2 THEN 'CAFÉ_____'
                    WHEN 3 THEN 'ДОБ__'
                    ELSE 'ÆTH__'
                END,
                CASE n % 5
                    WHEN 0 THEN '100% çoğunlukla'
                    WHEN 1 THEN '50_ mañana'
                    WHEN 2 THEN '80% caféteria'
                    WHEN 3 THEN '7% добро'
                    ELSE '2_ æther'
                END,
                CASE n % 5
                    WHEN 0 THEN '100!% ÇOĞUN%'
                    WHEN 1 THEN '50!_ MAÑ%'
                    WHEN 2 THEN '80!% CAFÉ%'
                    WHEN 3 THEN '7!% ДОБ%'
                    ELSE '2!_ ÆTH%'
                END
            FROM sequence;
            """))

        let queries = [
            (
                label: "LIKE prefix wildcard",
                sql: """
                SELECT SUM(value LIKE prefix_pattern)
                FROM unicode_like_performance_input;
                """
            ),
            (
                label: "LIKE single-character wildcard",
                sql: """
                SELECT SUM(value LIKE one_pattern)
                FROM unicode_like_performance_input;
                """
            ),
            (
                label: "LIKE escaped wildcard",
                sql: """
                SELECT SUM(escaped_value LIKE escape_pattern ESCAPE '!')
                FROM unicode_like_performance_input;
                """
            )
        ]

        for query in queries {
            let warmupResult = timedIntegerValue(in: db, label: query.label, sql: query.sql)
            XCTAssertEqual(warmupResult.value, rowCount)
        }

        var timings: [String: [TimeInterval]] = [:]
        for pass in 0..<measuredPassCount {
            let orderedQueries = pass.isMultiple(of: 2) ? queries : queries.reversed()
            for query in orderedQueries {
                let measuredResult = timedIntegerValue(in: db, label: query.label, sql: query.sql)
                XCTAssertEqual(measuredResult.value, rowCount)
                timings[query.label, default: []].append(measuredResult.elapsed)
                print("Unicode LIKE performance pass \(pass + 1) [\(implementationLabel)] [\(query.label)]: rows=\(rowCount), functionCalls=\(functionCallCount), elapsedSeconds=\(Self.formatSeconds(measuredResult.elapsed))")
            }
        }

        for query in queries {
            guard let queryTimings = timings[query.label] else {
                XCTFail("Missing timings for \(query.label)")
                continue
            }

            print("Unicode LIKE performance summary [\(implementationLabel)] [\(query.label)]: rows=\(rowCount), functionCalls=\(functionCallCount), passes=\(queryTimings.count), \(Self.formattedTimingSummary(queryTimings))")
        }
        #endif
    }

    func testUnicodeFunctionsPerformanceEvaluation() {
        #if SWIFTFMDB_ENABLE_SQLITE_BENCHMARKS
        runUnicodeCasePerformanceEvaluation(in: db, databaseEncodingLabel: "default UTF-8 database")

        let utf16DB = makeUTF16Database()
        defer {
            XCTAssertTrue(utf16DB.close())
        }

        runUnicodeCasePerformanceEvaluation(in: utf16DB, databaseEncodingLabel: "UTF-16 database")
        #endif
    }

    private func runUnicodeCasePerformanceEvaluation(in database: FMDatabase, databaseEncodingLabel: String) {
        let rowCount = 500_000
        let functionCallCount = rowCount * 2
        let measuredPassCount = 6

        XCTAssertTrue(database.executeUpdate(cached: false, "DROP TABLE IF EXISTS unicode_case_performance_input;"))
        XCTAssertTrue(database.executeUpdate(cached: false, "CREATE TEMP TABLE unicode_case_performance_input (value TEXT NOT NULL, locale TEXT NOT NULL);"))
        XCTAssertTrue(database.executeUpdate(cached: false, """
            WITH RECURSIVE sequence(n) AS (
                VALUES(1)
                UNION ALL
                SELECT n + 1 FROM sequence WHERE n < \(rowCount)
            )
            INSERT INTO unicode_case_performance_input (value, locale)
            SELECT
                CASE n % 6
                    WHEN 0 THEN 'çoğunlukla'
                    WHEN 1 THEN 'istanbul'
                    WHEN 2 THEN 'IĞDIR'
                    WHEN 3 THEN 'straße'
                    WHEN 4 THEN 'mañana'
                    ELSE 'SwiftFMDB123'
                END,
                CASE n % 3
                    WHEN 0 THEN 'tr_TR'
                    ELSE 'en_US'
                END
            FROM sequence;
            """))

        let queries = [
            (
                label: "UPPER/LOWER one-argument",
                sql: """
                SELECT SUM(LENGTH(UPPER(value))) + SUM(LENGTH(LOWER(value)))
                FROM unicode_case_performance_input;
                """
            ),
            (
                label: "UPPER/LOWER locale-aware",
                sql: """
                SELECT SUM(LENGTH(UPPER(value, locale))) + SUM(LENGTH(LOWER(value, locale)))
                FROM unicode_case_performance_input;
                """
            )
        ]

        for query in queries {
            let warmupResult = timedIntegerValue(in: database, label: query.label, sql: query.sql)
            XCTAssertGreaterThan(warmupResult.value, 0)
        }

        var timings: [String: [TimeInterval]] = [:]
        for pass in 0..<measuredPassCount {
            let orderedQueries = pass.isMultiple(of: 2) ? queries : queries.reversed()
            for query in orderedQueries {
                let measuredResult = timedIntegerValue(in: database, label: query.label, sql: query.sql)
                XCTAssertGreaterThan(measuredResult.value, 0)
                timings[query.label, default: []].append(measuredResult.elapsed)
                print("Unicode case performance pass \(pass + 1) [\(databaseEncodingLabel)] [\(query.label)]: rows=\(rowCount), functionCalls=\(functionCallCount), elapsedSeconds=\(Self.formatSeconds(measuredResult.elapsed))")
            }
        }

        for query in queries {
            guard let queryTimings = timings[query.label] else {
                XCTFail("Missing timings for \(query.label)")
                continue
            }

            print("Unicode case performance summary [\(databaseEncodingLabel)] [\(query.label)]: rows=\(rowCount), functionCalls=\(functionCallCount), passes=\(queryTimings.count), \(Self.formattedTimingSummary(queryTimings))")
        }
    }

    private func timedIntegerValue(in database: FMDatabase, label: String, sql: String, file: StaticString = #file, line: UInt = #line) -> (value: Int, elapsed: TimeInterval) {
        let startedAt = Date()
        let rs = database.executeQuery(cached: false, sql)

        XCTAssertNotNil(rs, file: file, line: line)
        defer { rs?.close() }

        XCTAssertEqual(rs?.next(), true, file: file, line: line)
        let result = rs?.int(forColumnIndex: 0) ?? 0
        let elapsed = Date().timeIntervalSince(startedAt)
        return (result, elapsed)
    }

    private static func formattedTimingSummary(_ timings: [TimeInterval]) -> String {
        let sortedTimings = timings.sorted()
        let min = sortedTimings.first ?? 0
        let max = sortedTimings.last ?? 0
        let average = timings.reduce(0, +) / Double(timings.count)
        let median: TimeInterval

        if sortedTimings.isEmpty {
            median = 0
        }
        else if sortedTimings.count.isMultiple(of: 2) {
            let upperMiddleIndex = sortedTimings.count / 2
            median = (sortedTimings[upperMiddleIndex - 1] + sortedTimings[upperMiddleIndex]) / 2
        }
        else {
            median = sortedTimings[sortedTimings.count / 2]
        }

        return "minSeconds=\(formatSeconds(min)), medianSeconds=\(formatSeconds(median)), averageSeconds=\(formatSeconds(average)), maxSeconds=\(formatSeconds(max))"
    }

    private static func formatSeconds(_ seconds: TimeInterval) -> String {
        return String(format: "%.6f", seconds)
    }

    private func makeUTF16Database(file: StaticString = #file, line: UInt = #line) -> FMDatabase {
        let utf16DB = FMDatabase(path: "")
        XCTAssertTrue(utf16DB.open(), file: file, line: line)
        XCTAssertTrue(utf16DB.executeUpdate(cached: false, "PRAGMA encoding = 'UTF-16';"), file: file, line: line)
        XCTAssertTrue(utf16DB.executeUpdate(cached: false, "CREATE TABLE utf16_encoding_probe (value TEXT);"), file: file, line: line)
        return utf16DB
    }

    public static var allTests = [
        ("testUnicodeLikePerformanceEvaluation", testUnicodeLikePerformanceEvaluation),
        ("testUnicodeFunctionsPerformanceEvaluation", testUnicodeFunctionsPerformanceEvaluation)
    ]

}
