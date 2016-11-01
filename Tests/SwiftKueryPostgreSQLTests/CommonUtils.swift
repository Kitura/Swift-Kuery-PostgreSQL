/**
 * Copyright IBM Corporation 2016
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

func executeQuery(query: Query, connection: Connection, callback: @escaping (QueryResult)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query) { result in
        printResult(result)
        callback(result)
    }
}

func executeQueryWithParameters(query: Query, connection: Connection, parameters: Any..., callback: @escaping (QueryResult)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        printResult(result)
        callback(result)
    }
}

func executeRawQuery(_ raw: String, connection: Connection, callback: @escaping (QueryResult)->()) {
    print("=======\(raw)=======")
    connection.execute(raw) { result in
        printResult(result)
        callback(result)
    }
}

func cleanUp(table: String, connection: Connection, callback: @escaping (QueryResult)->()) {
    connection.execute("DROP TABLE " + table) { result in
        callback(result)
    }
}

func printResult(_ result: QueryResult) {
    if let (titles, rows) = result.asRows {
        for title in titles {
            print(title.padding(toLength: 11, withPad: " ", startingAt: 0), terminator: "")
        }
        print()
        for row in rows {
            for value in row {
                var valueToPrint = ""
                if value != nil {
                    valueToPrint = value as! String
                }
                print(valueToPrint.padding(toLength: 11, withPad: " ", startingAt: 0), terminator: "")
            }
            print()
        }
    }
    else if result.success  {
        print("Success")
    }
    else if let queryError = result.asError {
        print("Error in query: ", queryError)
    }
}

func createConnection() -> PostgreSQLConnection {
    let host = read(fileName: "host.txt")
    let port = Int32(read(fileName: "port.txt"))!
    let username = read(fileName: "username.txt")
    let password = read(fileName: "password.txt")
    
    return PostgreSQLConnection(host: host, port: port, options: [.userName(username), .password(password)])
}

// Dummy class for test framework
class CommonUtils { }
