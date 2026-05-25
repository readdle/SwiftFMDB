// swift-tools-version:6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

private extension String {
    static let icu = "EnableSQLiteICU"
    static let unicode = "EnableUnicodeReplacement"
}

let package = Package(
    name: "SwiftFMDB",
    products: [
        .library(name: "SwiftFMDB", targets: ["SwiftFMDB"]),
    ],
    traits: [
        .default(enabledTraits: [.unicode]),
        .trait(name: .icu, description: "Forward SQLite ICU support to the SQLiteEE dependency."),
        .trait(name: .unicode, description: "Enable SwiftFMDB's Unicode upper/lower/like replacement."),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", .upToNextMinor(from: "1.6.2")),
        .package(
            url: "git@github.com:readdle/swift-sqlite-ee.git",
            branch: "feature/AB-102203-win-package",
            traits: [
                .trait(name: .icu, condition: .when(traits: [.icu])),
            ]
        ),
    ],
    targets: [
        .target(name: "FMUnicode",
                dependencies: [
                    .product(name: "SQLiteEE", package: "swift-sqlite-ee")
                ]),
        .target(name: "SwiftFMDB",
                dependencies: [
                    "FMUnicode",
                    .product(name: "SQLiteEE", package: "swift-sqlite-ee"),
                    .product(name: "Logging", package: "swift-log")
                ],
                cSettings: [
                    .define("SQLITE_HAS_CODEC", to: "1"),
                    .define("SQLITE_ENABLE_NORMALIZE", to: "1"),
                ]),
        .testTarget(name: "SwiftFMDBTests",
                dependencies: ["SwiftFMDB"],
                exclude: ["main.swift"]),
    ],
    swiftLanguageModes: [.v5]
)
