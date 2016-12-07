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
    private var row: [Any?]?
    private var connection: OpaquePointer?
    private var hasMoreRows = true
    
    init(queryResult: OpaquePointer, connection: OpaquePointer?) {
        self.connection = connection
        
        let columns = PQnfields(queryResult)
        var columnNames = [String]()
        for column in 0 ..< columns {
            columnNames.append(String(validatingUTF8: PQfname(queryResult, column))!)
        }
        titles = columnNames
        row = buildRow(queryResult: queryResult)
    }
    
    /// Fetch the next row of the query result. This function is blocking.
    ///
    /// - Returns: An array of values of type Any? representing the next row from the query result.
    public func fetchNext() -> [Any?]? {
        if let row = row {
            self.row = nil
            return row
        }
        
        if !hasMoreRows {
            return nil
        }
        
        guard let queryResult = PQgetResult(connection) else {
            // We are not supposed to get here, because we clear the result if we get PGRES_TUPLES_OK.
            hasMoreRows = false
            return nil
        }
        
        let status = PQresultStatus(queryResult)
        if status == PGRES_TUPLES_OK {
            // The last row.
            clearResult(connection: connection)
            hasMoreRows = false
            return nil
        }
        if status != PGRES_SINGLE_TUPLE {
            clearResult(connection: connection)
            hasMoreRows = false
            return nil
        }
        
        hasMoreRows = true
        return buildRow(queryResult: queryResult)
    }
    
    private func buildRow(queryResult: OpaquePointer) -> [Any?] {
        let columns = PQnfields(queryResult)
        var row = [Any?]()
        for column in 0 ..< columns {
            if PQgetisnull(queryResult, 0, column) == 1 {
                row.append(nil)
            }
            else {
                row.append(PostgreSQLResultFetcher.convert(queryResult, row: 0, column: column))
            }
        }
        PQclear(queryResult)
        return row
    }
    
    /// Fetch the next row of the query result. This function is non-blocking.
    ///
    /// - Parameter callback: A callback to call when the next row of the query result is ready.
    public func fetchNext(callback: ([Any?]?) ->()) {
        // For now
        callback(fetchNext())
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
