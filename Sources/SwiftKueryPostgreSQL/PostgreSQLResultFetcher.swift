/**
 Copyright IBM Corporation 2016
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import SwiftKuery
import CLibpq

import Foundation

// MARK: PostgreSQLResultFetcher

/// An implementation of query result fetcher.
public class PostgreSQLResultFetcher: ResultFetcher {
    
    private let titles: [String]
    private let rows: [[Any?]]
    private var index = -1
    
    init(queryResult: OpaquePointer) {
        var result = [[Any?]]()
        let rows = PQntuples(queryResult)
        let columns = PQnfields(queryResult)
        var columnNames = [String]()
        
        for column in 0 ..< columns {
            columnNames.append(String(validatingUTF8: PQfname(queryResult, column))!)
        }
        
        for rowIndex in 0 ..< rows {
            var row = [Any?]()
            
            for column in 0 ..< columns {
                if PQgetisnull(queryResult, rowIndex, column) == 1 {
                    row.append(nil)
                }
                else {
                    row.append(PostgreSQLResultFetcher.convert(queryResult, row: rowIndex, column: column))
                }
            }
            result.append(row)
        }
        
        titles = columnNames
        self.rows = result
    }
    
    /// Fetch the next row of the query result. This function is blocking.
    ///
    /// - Returns: An array of values of type Any? representing the next row from the query result.
    public func fetchNext() -> [Any?]? {
        index += 1
        if index < rows.count {
            return (rows[index])
        }
        else {
            return nil
        }
    }
    
    /// Fetch the next row of the query result. This function is non-blocking.
    ///
    /// - Parameter callback: A callback to call when the next row of the query result is ready.
    public func fetchNext(callback: ([Any?]?) ->()) {
        index += 1
        if index < rows.count {
            callback(rows[index])
        }
        else {
            callback(nil)
        }
    }
    
    /// Fetch the titles of the query result. This function is blocking.
    ///
    /// - Returns: An array of column titles of type String.
    public func fetchTitles() -> [String] {
        return titles
    }
    
    private static func convert(_ queryResult: OpaquePointer, row: Int32, column: Int32) -> Any {
        let data = Data(bytes: PQgetvalue(queryResult, row, column),
                        count: Int(PQgetlength(queryResult, row, column)))
        
        if PQfformat(queryResult, column) == 0 {
            return String(data: data, encoding: String.Encoding.utf8) as Any
        }
        else {
            return data
        }
    }
}
