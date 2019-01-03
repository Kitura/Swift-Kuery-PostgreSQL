/**
 * Copyright IBM Corporation 2016, 2017
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

#if os(Linux)
    import Glibc
#elseif os(OSX)
    import Darwin
#endif

import XCTest
import Foundation
import Dispatch

import SwiftKuery
import SwiftKueryPostgreSQL

func read(fileName: String) -> String {
    // Read in a configuration file into an NSData
    do {
        var pathToTests = #file
        if pathToTests.hasSuffix("CommonUtils.swift") {
            pathToTests = pathToTests.replacingOccurrences(of: "CommonUtils.swift", with: "")
        }
        let fileData = try Data(contentsOf: URL(fileURLWithPath: "\(pathToTests)\(fileName)"))
        XCTAssertNotNil(fileData, "Failed to read in the \(fileName) file")
        
        let resultString = String(data: fileData, encoding: String.Encoding.utf8)
        
        guard
            let resultLiteral = resultString
            else {
                XCTFail("Error in \(fileName).")
                exit(1)
        }
        return resultLiteral.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
    } catch {
        XCTFail("Error in \(fileName).")
        exit(1)
    }
}

func executeQuery(query: Query, connection: Connection, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeQueryWithParameters(query: Query, connection: Connection, parameters: Any?..., callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeQueryWithNamedParameters(query: Query, connection: Connection, parameters: [String:Any?], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}


func executeRawQueryWithParameters(_ raw: String, connection: Connection, parameters: Any?..., callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeRawQuery(_ raw: String, connection: Connection, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func cleanUp(table: String, connection: Connection, callback: @escaping (QueryResult)->()) {
    connection.execute("DROP TABLE \"" + table + "\"") { result in
        callback(result)
    }
}

private func printResultAndGetRowsAsArray(_ result: QueryResult, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    var rows: [[Any?]] = [[Any?]]()
    if let resultSet = result.asResultSet {
        resultSet.getColumnTitles() { titles, error in
            guard let titles = titles else {
                return callback(result, nil)
            }
            let length = titles.count > 6 ? 18 : 36
            for title in titles {
                print(title.padding(toLength: length, withPad: " ", startingAt: 0), terminator: "")
            }
            print()
            resultSet.forEach() { row, error in
                guard let row = row else {
                    // No more rows
                    return callback(result, rows)
                }
                for value in row {
                    var valueToPrint = ""
                    if let value = value {
                        valueToPrint = String(describing: value)
                    }
                    print(valueToPrint.padding(toLength: length, withPad: " ", startingAt: 0), terminator: "")
                }
                print()
                rows.append(row)
            }
        }
    } else if let value = result.asValue  {
        print("Result: ", value)
        callback(result, nil)
    } else if result.success  {
        print("Success")
        callback(result, nil)
    } else if let queryError = result.asError {
        print("Error in query: ", queryError)
        callback(result, nil)
    }
}

func createConnection() -> PostgreSQLConnection {
    // We do not specify a database name for our tests so postgreSQL defaults to using the username as the database name.
    let host = read(fileName: "host.txt")
    let port = Int32(read(fileName: "port.txt"))!
    let username = read(fileName: "username.txt")
    let password = read(fileName: "password.txt")
    
    // Create connection with URL
    //return PostgreSQLConnection(url: URL(string: "Postgres://\(username):\(password)@\(host):\(port)")!)
    
    return PostgreSQLConnection(host: host, port: port, options: [.userName(username), .password(password)])
}

func extractTime(from date: Date, withTimeZone: Bool = false) -> String {
    let dateFormatter = DateFormatter()
    if withTimeZone {
        dateFormatter.dateFormat = "hh:mm:ss Z"
    }
    else {
        dateFormatter.dateFormat = "hh:mm:ss"
    }
    dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
    return dateFormatter.string(from: date)
}

func extractDate(from date: Date) -> String {
    let dateFormatter = DateFormatter()
    dateFormatter.dateFormat = "yyyy-MM-dd"
    dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
    return dateFormatter.string(from: date)
}



class CommonUtils {
    private var pool: ConnectionPool?
    static let sharedInstance = CommonUtils()
    private init() {}

    func getConnectionPool() -> ConnectionPool {
        if let pool = pool {
            return pool
        }
        let host = read(fileName: "host.txt")
        let port = Int32(read(fileName: "port.txt"))!
        let username = read(fileName: "username.txt")
        let password = read(fileName: "password.txt")
        
        pool = PostgreSQLConnection.createPool(host: host, port: port, options: [.userName(username), .password(password)], poolOptions: ConnectionPoolOptions(initialCapacity: 0, maxCapacity: 1))
        return pool!
    }
    
    func getNewConnectionPool(resultsInBinaryFormat: Bool = true) -> ConnectionPool {
        let host = read(fileName: "host.txt")
        let port = Int32(read(fileName: "port.txt"))!
        let username = read(fileName: "username.txt")
        let password = read(fileName: "password.txt")
        
        pool = PostgreSQLConnection.createPool(host: host, port: port, options: [.userName(username), .password(password)], poolOptions: ConnectionPoolOptions(initialCapacity: 0, maxCapacity: 1))
        return pool!
    }

}
