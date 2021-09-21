//
//  FMRequest.swift
//  SparkDatabase
//
//  Created by nik on 3/14/20.
//  Copyright © 2020 Dmitry Protserov. All rights reserved.
//

import Foundation

public struct FMParameters {
    let sql: String
    let arguments: [Any?]?
    let parameters: [AnyHashable: Any?]?
    let cached: Bool
    let cacheLimit: Int

    public init(cached: Bool, cacheLimit: Int, sql: String, _ args: [Any?]?) {
        self.cached = cached
        self.cacheLimit = cacheLimit
        self.sql = sql
        self.arguments = args
        self.parameters = nil
    }

    public init(cached: Bool, cacheLimit: Int, sql: String, _ args: [AnyHashable: Any]?) {
        self.cached = cached
        self.cacheLimit = cacheLimit
        self.sql = sql
        self.arguments = nil
        self.parameters = args
    }
}
