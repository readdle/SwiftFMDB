// swift-tools-version:5.4
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let isFMUnicodeEnabled = true

let fmUnicodeSwiftSettings: [SwiftSetting] = isFMUnicodeEnabled ? [
    .define("FMUNICODE_ENABLE", .when(platforms: [.windows]))
] : []

let package = Package(
    name: "SwiftFMDB",
    products: [
        .library(name: "SwiftFMDB", targets: ["SwiftFMDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", .upToNextMinor(from: "1.6.2")),
        .package(name: "SQLiteEE", url: "git@github.com:readdle/swift-sqlite-ee.git", .branch("feature/AB-102203-win-package"))
    ],
    targets: [
        .target(name: "FMUnicode",
                dependencies: [
                    "SQLiteEE"
                ],
                swiftSettings: fmUnicodeSwiftSettings),
        .target(name: "SwiftFMDB",
                dependencies: [
                    "FMUnicode",
                    "SQLiteEE",
                    .product(name: "Logging", package: "swift-log")
                ],
                cSettings: [
                    .define("SQLITE_HAS_CODEC", to: "1"),
                    .define("SQLITE_ENABLE_NORMALIZE", to: "1"),
                ],
                swiftSettings: fmUnicodeSwiftSettings),
        .testTarget(name: "SwiftFMDBTests",
                dependencies: ["SwiftFMDB"],
                exclude: ["main.swift"],
                swiftSettings: fmUnicodeSwiftSettings),
    ]
)
