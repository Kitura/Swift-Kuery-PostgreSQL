/**
 Copyright IBM Corporation 2016, 2017
 
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
    private var connection: PostgreSQLConnection
    var hasMoreRows = true
    
    init(queryResult: OpaquePointer, connection: PostgreSQLConnection) {
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
        
        guard let queryResult = PQgetResult(connection.connection) else {
            // We are not supposed to get here, because we clear the result if we get PGRES_TUPLES_OK.
            hasMoreRows = false
            return nil
        }
        
        let status = PQresultStatus(queryResult)
        if status == PGRES_TUPLES_OK {
            // The end of the query results.
            clearResult(queryResult, connection: connection)
            hasMoreRows = false
            return nil
        }
        if status != PGRES_SINGLE_TUPLE {
            clearResult(queryResult, connection: connection)
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
            return String(data: data, encoding: String.Encoding.utf8) ?? ""
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
                fallthrough
            case .json:
                fallthrough
            case .xml:
                return String(cString: value)
                
            case .int2:
                return PostgreSQLResultFetcher.int16NetworkToHost(from: value)
                
            case .int4:
                return Int32(bigEndian: value.withMemoryRebound(to: Int32.self, capacity: 1) { $0.pointee })
                
            case .int8:
                return Int64(bigEndian: value.withMemoryRebound(to: Int64.self, capacity: 1) { $0.pointee })
                
            case .float4:
                return Float32(bitPattern: UInt32(bigEndian: data.withUnsafeBytes { $0.pointee } ))
                
            case .float8:
                return Float64(bitPattern: UInt64(bigEndian: data.withUnsafeBytes { $0.pointee } ))
                
            case .numeric:
                // Numeric is a sequence of Int16's: number of digits, weight, sign, display scale, numeric digits.
                // The numeric digits are stored in the form of a series of 16 bit base-10000 numbers each representing
                // four decimal digits of the original number.
                // For example, for -12345.12 the numeric value received from PostgreSQL will be
                // 00030001 40000002 00010929 04b0
                // The number of digits is 3, the digits are 0001 0929 04b0 (1 2345 12 decimal).
                // The weight is 1, meaning there are two digits before the decimal point.
                // The sign is 0x4000, meaning this is a negative number.
                // The display scale is 2, meaning there are 2 "decimal" digits after the decimal point.
                // https://www.postgresql.org/message-id/491DC5F3D279CD4EB4B157DDD62237F404E27FE9@zipwire.esri.com
                let sign = PostgreSQLResultFetcher.int16NetworkToHost(from: value.advanced(by: 4))
                if sign == -16384 { // 0xC000
                    return "NaN"
                }
                
                let numberOfDigits = PostgreSQLResultFetcher.int16NetworkToHost(from: value)
                if numberOfDigits <= 0  {
                    return "0"
                }
                
                var result: String = ""
                let weight = PostgreSQLResultFetcher.int16NetworkToHost(from: value.advanced(by: 2))
                var currentDigitData = value.advanced(by: 8)
                var currentDigitNumber: Int16 = 0
                
                if weight >= 0 {
                    for i in 0 ... weight {
                        if currentDigitNumber < numberOfDigits {
                            let digitsAsInt16 = PostgreSQLResultFetcher.int16NetworkToHost(from: currentDigitData)
                            if i == 0 {
                                result += String(digitsAsInt16)
                            }
                            else {
                                result +=  String(format: "%04d", digitsAsInt16)
                            }
                            currentDigitData = currentDigitData.advanced(by: 2)
                            currentDigitNumber = i + 1
                        }
                        else {
                            result += "0000"
                        }
                    }
                }
                
                let displayScale = Int(PostgreSQLResultFetcher.int16NetworkToHost(from: value.advanced(by: 6)))
                if displayScale > 0 {
                    var fraction = ""
                    
                    for i in currentDigitNumber ..< numberOfDigits {
                        let digitsAsInt16 = PostgreSQLResultFetcher.int16NetworkToHost(from: currentDigitData)
                        let digitsAsString = String(format: "%04d", digitsAsInt16)
                        fraction += digitsAsString
                        currentDigitData = currentDigitData.advanced(by: 2)
                        currentDigitNumber = i
                    }

                    if weight < 0 && fraction.characters.count < displayScale { // 0.x number, add zeroes
                        fraction = fraction.leftPadding(toLength: displayScale)
                    }
                    
                    while fraction.hasSuffix("0") {
                        fraction = String(fraction.characters.dropLast())
                    }

                    if result.isEmpty {
                        result = "0"
                    }
                    
                    if !fraction.isEmpty {
                        result += "." + fraction
                    }
                }
                
                if sign == 0x4000 {
                    result = "-" + result
                }
                
                return result
                
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
                
            case .uuid:
                let uuid = UUID(uuid: uuid_t(value.withMemoryRebound(to: uuid_t.self, capacity: 1) { $0.pointee }))
                return uuid.uuidString
            }
        }
    }
    
    private static func int16NetworkToHost(from pointer: UnsafeMutablePointer<Int8>) -> Int16 {
        return Int16(bigEndian: pointer.withMemoryRebound(to: Int16.self, capacity: 1) { $0.pointee })
    }
    
    private static let secondsInDay: Int32 = 24 * 60 * 60
    // Reference date in Postgres is 2000-01-01, while in Swift it is 2001-01-01. There were 366 days in the year 2000.
    private static let timeIntervalBetween1970AndPostgresReferenceDate = Date.timeIntervalBetween1970AndReferenceDate - TimeInterval(366 * secondsInDay)
}

extension String {
    func leftPadding(toLength: Int, withPad: String = "0") -> String {
        guard toLength > self.characters.count else { return self }
        let padding = String(repeating: withPad, count: toLength - self.characters.count)
        return padding + self
    }
}

