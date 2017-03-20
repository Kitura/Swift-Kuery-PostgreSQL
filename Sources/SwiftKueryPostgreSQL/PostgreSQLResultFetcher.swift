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
        let value = PQgetvalue(queryResult, row, column)
        let count = Int(PQgetlength(queryResult, row, column))
        let data = Data(bytes: value!, count: count)
        
        let type = PostgreSQLType(rawValue: PQftype(queryResult, column))
        
        if PQfformat(queryResult, column) == 0 {
            let valueAsText = String(data: data, encoding: String.Encoding.utf8)!
            
            guard let type = type else {
                return valueAsText
            }
    
//            let dateFormatter = DateFormatter()
//            dateFormatter.timeZone = TimeZone(abbreviation: "UTC")

            switch type {
            case .int2:
                return Int16(valueAsText) ?? valueAsText
            case .int4:
                return Int32(valueAsText) ?? valueAsText
            case .int8:
                return Int64(valueAsText) ?? valueAsText
            case .float4:
                return Float(valueAsText) ?? valueAsText
            case .float8:
                return Double(valueAsText) ?? valueAsText
            case .numeric:
                return Double(valueAsText) ?? valueAsText // Is Double enough here?
                
            case .bool:
                let boolAsText = valueAsText == "t" ? "true" : (valueAsText == "f" ? "false" : valueAsText)
                return Bool(boolAsText) ?? valueAsText
                
//            case .date:
//                dateFormatter.dateFormat = "yyyy-MM-dd"
//                return dateFormatter.date(from: valueAsText) ?? valueAsText
//
//            case .time:
//                dateFormatter.dateFormat = "hh:mm:ss"
//                return dateFormatter.date(from: valueAsText) ?? valueAsText
//
//            case .timetz:
//                dateFormatter.dateFormat = "hh:mm:ssZ"
//                return dateFormatter.date(from: valueAsText) ?? valueAsText
//
//            case .timestamp:
//                dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss"
//                return dateFormatter.date(from: valueAsText) ?? valueAsText
//
//            case .timestamptz:
//                dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ssZ"
//                return dateFormatter.date(from: valueAsText) ?? valueAsText
                
            default:
                return valueAsText
            }
        }
        else {
            guard let type = type, let value = value else {
                return data
            }
            
            switch type {
            case .varchar:
                fallthrough
            case .char:
                fallthrough
            case .name:
                fallthrough
            case .text:
                fallthrough
            case .bpchar:
                return String(cString: value)
                
            case .int2:
                return Int16(bigEndian: value.withMemoryRebound(to: Int16.self, capacity: 1) { $0.pointee })
            
            case .int4:
                return Int32(bigEndian: value.withMemoryRebound(to: Int32.self, capacity: 1) { $0.pointee })

            case .int8:
                return Int64(bigEndian: value.withMemoryRebound(to: Int64.self, capacity: 1) { $0.pointee })
               
            case .float4:
                return Float32(bitPattern: UInt32(bigEndian: data.withUnsafeBytes { $0.pointee } ))

            case .float8:
                return Float64(bitPattern: UInt64(bigEndian: data.withUnsafeBytes { $0.pointee } ))
                
//            case .numeric:
                
            case .date:
                let days = Int32(bigEndian: value.withMemoryRebound(to: Int32.self, capacity: 1) { $0.pointee })
                let timeInterval = TimeInterval(days * secondsInDay)
                return Date(timeIntervalSince1970: timeInterval + timeIntervalBetween1970AndPostgresReferenceDate)
                
            case .time:
                fallthrough
            case .timetz:
                fallthrough
            case .timestamp:
                fallthrough
            case .timestamptz:
                let microseconds = Int64(bigEndian: value.withMemoryRebound(to: Int64.self, capacity: 1) { $0.pointee })
                let timeInterval = TimeInterval(microseconds / 1000000)
                return Date(timeIntervalSince1970: timeInterval + timeIntervalBetween1970AndPostgresReferenceDate)

            case .bool:
                return Bool(value.withMemoryRebound(to: Bool.self, capacity: 1) { $0.pointee })
            
            default:
                return data
            }
        }
    }
    
    private static let secondsInDay: Int32 = 24 * 60 * 60
    // Reference date in Postgres is 2000-01-01, while in Swift it is 2001-01-01. There were 366 days in the year 2000. 
    private static let timeIntervalBetween1970AndPostgresReferenceDate = Date.timeIntervalBetween1970AndReferenceDate - TimeInterval(366 * secondsInDay)
}
