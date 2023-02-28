// swift-tools-version:5.4
// The swift-tools-version declares the minimum version of Swift required to build this package.

import Foundation
import PackageDescription

let package = Package(
    name: "SwiftFMDB",
    products: [
        .library(name: "SwiftFMDB", targets: ["SwiftFMDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", .upToNextMinor(from: "1.4.4")),
        .package(name: "SQLiteEE", url: "git@github.com:readdle/swift-sqlite-ee.git", .upToNextMinor(from: "3.39.4"))
    ],
    targets: [
        .target(name: "SwiftFMDB",
                dependencies: [
                    "SQLiteEE",
                    .product(name: "Logging", package: "swift-log")
                ],
                cSettings: [
                    .define("SQLITE_ENABLE_NORMALIZE", to: "1")
                ]),
        .testTarget(name: "SwiftFMDBTests", dependencies: ["SwiftFMDB"]),
    ]
)
