/**
 Copyright IBM Corporation 2017
 
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

import XCTest
import SwiftKuery

import Foundation

@testable import SwiftKueryPostgreSQL

#if os(Linux)
let tableNumeric = "tableNumericLinux"
let tableBool = "tableBoolLinux"
let tableDate = "tableDateLinux"
let tableString = "tableStringLinux"
let tableTextResultNumeric = "tableTextResultNumericLinux"
let tableTextResultBool = "tableTextResultBoolLinux"
let tableTextResultDate = "tableTextResultDateLinux"
let tableTextResultString = "tableTextResultStringLinux"
#else
let tableNumeric = "tableNumericOSX"
let tableBool = "tableBoolOSX"
let tableDate = "tableDateOSX"
let tableString = "tableStringOSX"
let tableTextResultNumeric = "tableTextResultNumericOSX"
let tableTextResultBool = "tableTextResultBoolOSX"
let tableTextResultDate = "tableTextResultDateOSX"
let tableTextResultString = "tableTextResultStringOSX"
#endif

class TestTypes: XCTestCase {
    
    static var allTests: [(String, (TestTypes) -> () throws -> Void)] {
        return [
            ("testBoolTypes", testBoolTypes),
            ("testDateTypes", testDateTypes),
            ("testNumericTypes", testNumericTypes),
            ("testStringTypes", testStringTypes),
            ("testTextResultBoolTypes", testTextResultBoolTypes),
            ("testTextResultDateTypes", testTextResultDateTypes),
            ("testTextResultNumericTypes", testTextResultNumericTypes),
            ("testTextResultStringTypes", testTextResultStringTypes),
        ]
    }
    
    class NumericTable: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        let d = Column("d")
        let e = Column("e")
        let f = Column("f")
        let g = Column("g")
        let h = Column("h")
        let i = Column("i")
        let j = Column("j")
        let k = Column("k")
        
        let tableName = tableNumeric
    }
    
    func testNumericTypes() {
        let t = NumericTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b smallint, c integer, d bigint, e decimal(7,2), f numeric(8,3), g real, h double precision, i smallserial, j serial, k bigserial)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let i1 = Insert(into: t, values: "apple", 10, -2147483648, -21474836480, "nan", 123.6789, 12345.1234567, 12345.1234567)
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! Int16, 10, "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! Int32, -2147483648, "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as! Int64, -21474836480, "Wrong value in row 0 column 3")
//                            XCTAssertTrue((rows![0][4]! as! Double).isNaN, "Wrong value in row 0 column 4")
//                            XCTAssertEqual(rows![0][5]! as! Double, Double(123.6789), "Wrong value in row 0 column 5")
                            XCTAssertEqual(rows![0][6]! as! Float, Float(12345.1234567), "Wrong value in row 0 column 6")
                            XCTAssertEqual(rows![0][7]! as! Double, 12345.1234567, "Wrong value in row 0 column 7")
                            XCTAssertEqual(rows![0][8]! as! Int16, 1, "Wrong value in row 0 column 8")
                            XCTAssertEqual(rows![0][9]! as! Int32, 1, "Wrong value in row 0 column 9")
                            XCTAssertEqual(rows![0][10]! as! Int64, 1, "Wrong value in row 0 column 10")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    class BooleanTable: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        
        let tableName = tableBool
    }
    
    func testBoolTypes() {
        let t = BooleanTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b boolean, c boolean)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let i1 = Insert(into: t, values: "apple", "1", "false")
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! Bool, true, "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! Bool, false, "Wrong value in row 0 column 2")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    class DateTable: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        let d = Column("d")
        let e = Column("e")
        let f = Column("f")
        
        let tableName = tableDate
    }
    
    private func extractTime(from date: Date, withTimeZone: Bool = false) -> String {
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
    
    private func extractDate(from date: Date) -> String {
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = "yyyy-MM-dd"
        dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
        return dateFormatter.string(from: date)
    }
    
    func testDateTypes() {
        let t = DateTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b date, c time, d time with time zone, e timestamp, f timestamp with time zone)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let now = Date()
                    
                    let thenString = "2017-03-19 11:15:15 +0800"
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss Z"
                    dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
                    let then: Date = dateFormatter.date(from: thenString)!
                    
                    dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss"
                    let thenWithoutTimeZone: Date = dateFormatter.date(from: "2017-03-19 11:15:15")!
                    
                   var i = Insert(into: t, values: "now", now, now, now, now, now)
                    executeQuery(query: i, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        
                        i = Insert(into: t, values: "then", thenString, thenString, thenString, thenString, thenString)
                        executeQuery(query: i, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                
                                XCTAssertEqual(rows![0][0]! as! String, "now", "Wrong value in row 0 column 0")
                                
                                var date = self.extractDate(from: rows![0][1]! as! Date)
                                let nowDate = self.extractDate(from: now)
                                XCTAssertEqual(date, nowDate, "Wrong value in row 0 column 1")
                                
                                var time = self.extractTime(from: rows![0][2]! as! Date)
                                let nowTime = self.extractTime(from: now)
                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 2")
                                time = self.extractTime(from: rows![0][3]! as! Date)
                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 3")
                                
                                var timestamp = rows![0][4]! as! Date
                                let nowTimeInterval = Int(now.timeIntervalSince1970)
                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), nowTimeInterval, "Wrong value in row 0 column 4")
                                timestamp = rows![0][5]! as! Date
                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), nowTimeInterval, "Wrong value in row 0 column 5")
                                
                                
                                XCTAssertEqual(rows![1][0]! as! String, "then", "Wrong value in row 1 column 0")
                                
                                date = self.extractDate(from: rows![1][1]! as! Date)
                                let thenDate = self.extractDate(from: then)
                                XCTAssertEqual(date, thenDate, "Wrong value in row 1 column 1")
                                
                                time = self.extractTime(from: rows![1][2]! as! Date)
                                let thenTime = self.extractTime(from: thenWithoutTimeZone)
                                XCTAssertEqual(time, thenTime, "Wrong value in row 1 column 2")
                                time = self.extractTime(from: rows![1][3]! as! Date)
                                XCTAssertEqual(time, thenTime, "Wrong value in row 1 column 3")
                                
                                timestamp = rows![1][4]! as! Date
                                let thenWithoutTimeZoneTimeInterval = Int(thenWithoutTimeZone.timeIntervalSince1970)
                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), thenWithoutTimeZoneTimeInterval, "Wrong value in row 1 column 4")
                                timestamp = rows![1][5]! as! Date
                                let thenTimeInterval = Int(then.timeIntervalSince1970)
                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), thenTimeInterval, "Wrong value in row 1 column 5")
                                
                                let drop = Raw(query: "DROP TABLE", table: t)
                                executeQuery(query: drop, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                }
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    class StringTable: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        let d = Column("d")
        let e = Column("e")
        let f = Column("f")
        let g = Column("g")
        
        let tableName = tableString
    }
    
    func testStringTypes() {
        let t = StringTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(4), b character varying(4), c character(5), d char(5), e text, f \"char\", g name)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let i1 = Insert(into: t, values: "qiwi", "qiwi", "apple", "apple", "apple and banana", "a", "mandarin")
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows![0][0]! as! String, "qiwi", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! String, "qiwi", "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! String, "apple", "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as! String, "apple", "Wrong value in row 0 column 3")
                            XCTAssertEqual(rows![0][4]! as! String, "apple and banana", "Wrong value in row 0 column 4")
                            XCTAssertEqual(rows![0][5]! as! String, "a", "Wrong value in row 0 column 5")
                            XCTAssertEqual(rows![0][6]! as! String, "mandarin", "Wrong value in row 0 column 6")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    class NumericTableTextResult: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        let d = Column("d")
        let e = Column("e")
        let f = Column("f")
        let g = Column("g")
        let h = Column("h")
        let i = Column("i")
        let j = Column("j")
        let k = Column("k")
        
        let tableName = tableTextResultNumeric
    }
    
    func testTextResultNumericTypes() {
        let t = NumericTableTextResult()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool(resultsInBinaryFormat: false)
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b smallint, c integer, d bigint, e decimal(7,2), f numeric(8,3), g real, h double precision, i smallserial, j serial, k bigserial)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let i1 = Insert(into: t, values: "apple", 10, -2147483648, -21474836480, "nan", 123.6789, 12345.1234567, 12345.1234567)
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! Int16, 10, "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! Int32, -2147483648, "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as! Int64, -21474836480, "Wrong value in row 0 column 3")
                            XCTAssertTrue((rows![0][4]! as! Double).isNaN, "Wrong value in row 0 column 4")
                            XCTAssertEqual(rows![0][5]! as! Double, 123.679, "Wrong value in row 0 column 5")
                            XCTAssertEqual(rows![0][6]! as! Float, 12345.1, "Wrong value in row 0 column 6")
                            XCTAssertEqual(rows![0][7]! as! Double, 12345.1234567, "Wrong value in row 0 column 7")
                            XCTAssertEqual(rows![0][8]! as! Int16, 1, "Wrong value in row 0 column 8")
                            XCTAssertEqual(rows![0][9]! as! Int32, 1, "Wrong value in row 0 column 9")
                            XCTAssertEqual(rows![0][10]! as! Int64, 1, "Wrong value in row 0 column 10")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    class BooleanTableTextResult: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        
        let tableName = tableTextResultBool
    }
    
    func testTextResultBoolTypes() {
        let t = BooleanTableTextResult()
        
        let pool = CommonUtils.sharedInstance.getNewConnectionPool(resultsInBinaryFormat: false)
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b boolean, c boolean)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let i1 = Insert(into: t, values: "apple", "1", "false")
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! Bool, true, "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! Bool, false, "Wrong value in row 0 column 2")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    class DateTableTextResult: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        let d = Column("d")
        let e = Column("e")
        let f = Column("f")
        
        let tableName = tableTextResultDate
    }
    
    
    func testTextResultDateTypes() {
        let t = DateTableTextResult()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool(resultsInBinaryFormat: false)
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b date, c time, d time with time zone, e timestamp, f timestamp with time zone)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let now = Date()
                    
                    let thenString = "2017-03-19 11:15:15 +0800"
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss Z"
                    dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
                    let then: Date = dateFormatter.date(from: thenString)!
                    
                    let thenStringWithoutTimeZone = "2017-03-19 11:15:15"
                    dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss"
                    let thenWithoutTimeZone: Date = dateFormatter.date(from: thenStringWithoutTimeZone)!
                    
                    var i = Insert(into: t, values: "now", now, now, now, now, now)
                    executeQuery(query: i, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        
                        i = Insert(into: t, values: "then", thenString, thenString, thenString, thenString, thenString)
                        executeQuery(query: i, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                
                                XCTAssertEqual(rows![0][0]! as! String, "now", "Wrong value in row 0 column 0")
                                
                                var date = rows![0][1]! as! String
                                let nowDate = self.extractDate(from: now)
                                XCTAssertEqual(date, nowDate, "Wrong value in row 0 column 1")
                                
                                var time = rows![0][2]! as! String
                                var nowTime = self.extractTime(from: now)
                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 2")
                                
                                nowTime = self.extractTime(from: now, withTimeZone: true)
                                time = rows![0][3]! as! String + "00" // Postgres represents time zone with 2 digits
                                nowTime = nowTime.replacingOccurrences(of: " ", with: "") // Postgres time doesn't have spaces
                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 3")
                                
                                var timestamp = rows![0][4]! as! String
                                let nowWithoutTimeZone = String(now.description.characters.dropLast(6))
                                XCTAssertEqual(timestamp, nowWithoutTimeZone, "Wrong value in row 0 column 4")
                                
                                timestamp = rows![0][5]! as! String
                                dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ssZ"
                                var timestampDate: Date = dateFormatter.date(from: timestamp)!

                                XCTAssertEqual(timestampDate.description, now.description, "Wrong value in row 0 column 5")
                                
                                
                                XCTAssertEqual(rows![1][0]! as! String, "then", "Wrong value in row 1 column 0")
                                
                                date = rows![1][1]! as! String
                                let thenDate = self.extractDate(from: then)
                                XCTAssertEqual(date, thenDate, "Wrong value in row 1 column 1")
                                
                                time = rows![1][2]! as! String
                                var thenTime = self.extractTime(from: thenWithoutTimeZone)
                                XCTAssertEqual(time, thenTime, "Wrong value in row 1 column 2")
                                
                                time = rows![1][3]! as! String
                                dateFormatter.dateFormat = "hh:mm:ssZ"
                                let timeWithZone: Date = dateFormatter.date(from: time)!
                                time = self.extractTime(from: timeWithZone, withTimeZone: true)
                                thenTime = self.extractTime(from: then, withTimeZone: true)
                                XCTAssertEqual(time, thenTime, "Wrong value in row 1 column 3")
                                
                                timestamp = rows![1][4]! as! String
                                XCTAssertEqual(timestamp, thenStringWithoutTimeZone, "Wrong value in row 1 column 4")
                                
                                timestamp = rows![1][5]! as! String
                                dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ssZ"
                                timestampDate = dateFormatter.date(from: timestamp)!
                                XCTAssertEqual(timestampDate.description, then.description, "Wrong value in row 1 column 5")
                                
                                let drop = Raw(query: "DROP TABLE", table: t)
                                executeQuery(query: drop, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                }
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
//    func testTextResultDateTypes() {
//        let t = DateTableTextResult()
//        
//        let pool = CommonUtils.sharedInstance.getConnectionPool(resultsInBinaryFormat: false)
//        performTest(asyncTasks: { expectation in
//            
//            guard let connection = pool.getConnection() else {
//                XCTFail("Failed to get connection")
//                return
//            }
//            
//            cleanUp(table: t.tableName, connection: connection) { result in
//                
//                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b date, c time, d time with time zone, e timestamp, f timestamp with time zone)", connection: connection) { result, rows in
//                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
//                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
//                    
//                    let now = Date()
//                    
//                    let thenString = "2017-03-19 11:15:15 +0800"
//                    let dateFormatter = DateFormatter()
//                    dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss Z"
//                    dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
//                    let then: Date = dateFormatter.date(from: thenString)!
//                    
//                    dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss"
//                    let thenWithoutTimeZone: Date = dateFormatter.date(from: "2017-03-19 11:15:15")!
//                    
//                    var i = Insert(into: t, values: "now", now, now, now, now, now)
//                    executeQuery(query: i, connection: connection) { result, rows in
//                        XCTAssertEqual(result.success, true, "INSERT failed")
//                        
//                        i = Insert(into: t, values: "then", thenString, thenString, thenString, thenString, thenString)
//                        executeQuery(query: i, connection: connection) { result, rows in
//                            XCTAssertEqual(result.success, true, "INSERT failed")
//                            
//                            let s1 = Select(from: t)
//                            executeQuery(query: s1, connection: connection) { result, rows in
//                                XCTAssertEqual(result.success, true, "SELECT failed")
//                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
//                                XCTAssertNotNil(rows, "SELECT returned no rows")
//                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
//                                
//                                XCTAssertEqual(rows![0][0]! as! String, "now", "Wrong value in row 0 column 0")
//                                
//                                var date = self.extractDate(from: rows![0][1]! as! Date)
//                                let nowDate = self.extractDate(from: now)
//                                XCTAssertEqual(date, nowDate, "Wrong value in row 0 column 1")
//                                
//                                var time = self.extractTime(from: rows![0][2]! as! Date)
//                                let nowTime = self.extractTime(from: now)
//                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 2")
//                                time = self.extractTime(from: rows![0][3]! as! Date)
//                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 3")
//                                
//                                var timestamp = rows![0][4]! as! Date
//                                let nowTimeInterval = Int(now.timeIntervalSince1970)
//                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), nowTimeInterval, "Wrong value in row 0 column 4")
//                                timestamp = rows![0][5]! as! Date
//                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), nowTimeInterval, "Wrong value in row 0 column 5")
//                                
//                                
//                                XCTAssertEqual(rows![1][0]! as! String, "then", "Wrong value in row 1 column 0")
//                                
//                                date = self.extractDate(from: rows![1][1]! as! Date)
//                                let thenDate = self.extractDate(from: then)
//                                XCTAssertEqual(date, thenDate, "Wrong value in row 1 column 1")
//                                
//                                time = self.extractTime(from: rows![1][2]! as! Date)
//                                var thenTime = self.extractTime(from: thenWithoutTimeZone)
//                                XCTAssertEqual(time, thenTime, "Wrong value in row 1 column 2")
//                                time = self.extractTime(from: rows![1][3]! as! Date)
//                                thenTime = self.extractTime(from: then)
//                                XCTAssertEqual(time, thenTime, "Wrong value in row 1 column 3")
//                                
//                                timestamp = rows![1][4]! as! Date
//                                let thenWithoutTimeZoneTimeInterval = Int(thenWithoutTimeZone.timeIntervalSince1970)
//                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), thenWithoutTimeZoneTimeInterval, "Wrong value in row 1 column 4")
//                                timestamp = rows![1][5]! as! Date
//                                let thenTimeInterval = Int(then.timeIntervalSince1970)
//                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), thenTimeInterval, "Wrong value in row 1 column 5")
//                                
//                                let drop = Raw(query: "DROP TABLE", table: t)
//                                executeQuery(query: drop, connection: connection) { result, rows in
//                                    XCTAssertEqual(result.success, true, "DROP TABLE failed")
//                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//            expectation.fulfill()
//        })
//    }
    
    class StringTableTextResult: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        let d = Column("d")
        let e = Column("e")
        let f = Column("f")
        let g = Column("g")
        
        let tableName = tableTextResultString
    }
    
    func testTextResultStringTypes() {
        let t = StringTableTextResult()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool(resultsInBinaryFormat: false)
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(4), b character varying(4), c character(5), d char(5), e text, f \"char\", g name)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let i1 = Insert(into: t, values: "qiwi", "qiwi", "apple", "apple", "apple and banana", "a", "mandarin")
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows![0][0]! as! String, "qiwi", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! String, "qiwi", "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! String, "apple", "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as! String, "apple", "Wrong value in row 0 column 3")
                            XCTAssertEqual(rows![0][4]! as! String, "apple and banana", "Wrong value in row 0 column 4")
                            XCTAssertEqual(rows![0][5]! as! String, "a", "Wrong value in row 0 column 5")
                            XCTAssertEqual(rows![0][6]! as! String, "mandarin", "Wrong value in row 0 column 6")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    

}
