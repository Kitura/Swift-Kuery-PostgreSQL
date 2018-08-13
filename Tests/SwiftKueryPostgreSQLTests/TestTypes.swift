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
let tableNumberTypes = "tableNumberTypesLinux"
let tableBool = "tableBoolLinux"
let tableDate = "tableDateLinux"
let tableString = "tableStringLinux"
let tableUUID = "tableUUIDLinux"
#else
let tableNumeric = "tableNumericOSX"
let tableNumberTypes = "tableNumberTypesOSX"
let tableBool = "tableBoolOSX"
let tableDate = "tableDateOSX"
let tableString = "tableStringOSX"
let tableUUID = "tableUUIDOSX"
#endif

class TestTypes: XCTestCase {
    
    static var allTests: [(String, (TestTypes) -> () throws -> Void)] {
        return [
            ("testBoolTypes", testBoolTypes),
            ("testDateTypes", testDateTypes),
            ("testNumberTypes", testNumberTypes),
            ("testNumeric", testNumeric),
            ("testStringTypes", testStringTypes),
            ("testUUID", testUUID),            
        ]
    }
    
    class NumberTypesTable: Table {
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
        
        let tableName = tableNumberTypes
    }
    
    func testNumberTypes() {
        let t = NumberTypesTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b smallint, c integer, d bigint, e decimal(7,2), f numeric, g real, h double precision, i smallserial, j serial, k bigserial)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    var i = Insert(into: t, values: "apple", 10, -2147483648, -21474836480, "nan", "4.000678", 12345.1234567, 12345.1234567)
                    executeQuery(query: i, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s = Select(from: t).order(by: .ASC(t.a))
                        executeQuery(query: s, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                            XCTAssertEqual(rows![0].count, 11, "SELECT returned wrong number of columns")
                            
                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! Int16, 10, "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! Int32, -2147483648, "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as! Int64, -21474836480, "Wrong value in row 0 column 3")
                            XCTAssertEqual(rows![0][4]! as! String, "NaN", "Wrong value in row 0 column 4")
                            XCTAssertEqual(rows![0][5]! as! String, "4.000678", "Wrong value in row 0 column 5")
                            XCTAssertEqual(rows![0][6]! as! Float, Float(12345.1234567), "Wrong value in row 0 column 6")
                            XCTAssertEqual(rows![0][7]! as! Double, 12345.1234567, "Wrong value in row 0 column 7")
                            XCTAssertEqual(rows![0][8]! as! Int16, 1, "Wrong value in row 0 column 8")
                            XCTAssertEqual(rows![0][9]! as! Int32, 1, "Wrong value in row 0 column 9")
                            XCTAssertEqual(rows![0][10]! as! Int64, 1, "Wrong value in row 0 column 10")
                            
                            i = Insert(into: t, values: "banana", -10000, 2147483647, 21474836480, "-0.029", "0111111114.000678", -12345.1234567, -0.123456789)
                            executeQuery(query: i, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                
                                executeQuery(query: s, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                    XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows")
                                    XCTAssertEqual(rows![1].count, 11, "SELECT returned wrong number of columns")
                                    
                                    XCTAssertEqual(rows![1][0]! as! String, "banana", "Wrong value in row 1 column 0")
                                    XCTAssertEqual(rows![1][1]! as! Int16, -10000, "Wrong value in row 1 column 1")
                                    XCTAssertEqual(rows![1][2]! as! Int32, 2147483647, "Wrong value in row 1 column 2")
                                    XCTAssertEqual(rows![1][3]! as! Int64, 21474836480, "Wrong value in row 1 column 3")
                                    XCTAssertEqual(rows![1][4]! as! String, "-0.03", "Wrong value in row 1 column 4")
                                    XCTAssertEqual(rows![1][5]! as! String, "111111114.000678", "Wrong value in row 1 column 5")
                                    XCTAssertEqual(rows![1][6]! as! Float, Float(-12345.1234567), "Wrong value in row 1 column 6")
                                    XCTAssertEqual(rows![1][7]! as! Double, -0.123456789, "Wrong value in row 1 column 7")
                                    XCTAssertEqual(rows![1][8]! as! Int16, 2, "Wrong value in row 1 column 8")
                                    XCTAssertEqual(rows![1][9]! as! Int32, 2, "Wrong value in row 1 column 9")
                                    XCTAssertEqual(rows![1][10]! as! Int64, 2, "Wrong value in row 1 column 10")

                                    let drop = Raw(query: "DROP TABLE", table: t)
                                    executeQuery(query: drop, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                        semaphore.signal()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            semaphore.wait()
            expectation.fulfill()
        })
    }
    
    class NumericTable: Table {
        let a = Column("a")
        let b = Column("b")
        let c = Column("c")
        let d = Column("d")
        let e = Column("e")
        
        let tableName = tableNumeric
    }
    
    func testNumeric() {
        let t = NumericTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b decimal(7,2), c numeric, d decimal, e numeric(12,4))", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    var i = Insert(into: t, values: "apple", "12345.1234567", "12345.1234567", "0.001", "0.00001")
                    executeQuery(query: i, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s = Select(from: t).order(by: .ASC(t.a))
                        executeQuery(query: s, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                            XCTAssertEqual(rows![0].count, 5, "SELECT returned wrong number of columns")

                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! String, "12345.12", "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! String, "12345.1234567", "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as! String, "0.001", "Wrong value in row 0 column 3")
                            XCTAssertEqual(rows![0][4]! as! String, "0", "Wrong value in row 0 column 4")
                            
                            i = Insert(into: t, values: "banana", "12345", "123451234567", "0.01", "0.01")
                            executeQuery(query: i, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                
                                executeQuery(query: s, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                    XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows")
                                    XCTAssertEqual(rows![1].count, 5, "SELECT returned wrong number of columns")
                                    
                                   XCTAssertEqual(rows![1][0]! as! String, "banana", "Wrong value in row 1 column 0")
                                    XCTAssertEqual(rows![1][1]! as! String, "12345", "Wrong value in row 1 column 1")
                                    XCTAssertEqual(rows![1][2]! as! String, "123451234567", "Wrong value in row 1 column 2")
                                    XCTAssertEqual(rows![1][3]! as! String, "0.01", "Wrong value in row 1 column 3")
                                    XCTAssertEqual(rows![1][4]! as! String, "0.01", "Wrong value in row 1 column 4")
                                    
                                    i = Insert(into: t, values: "clementine", "123", "12", "1", "4561")
                                    executeQuery(query: i, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                        
                                        executeQuery(query: s, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows")
                                            XCTAssertEqual(rows![2].count, 5, "SELECT returned wrong number of columns")
                                            
                                            XCTAssertEqual(rows![2][0]! as! String, "clementine", "Wrong value in row 2 column 0")
                                            XCTAssertEqual(rows![2][1]! as! String, "123", "Wrong value in row 2 column 1")
                                            XCTAssertEqual(rows![2][2]! as! String, "12", "Wrong value in row 2 column 2")
                                            XCTAssertEqual(rows![2][3]! as! String, "1", "Wrong value in row 2 column 3")
                                            XCTAssertEqual(rows![2][4]! as! String, "4561", "Wrong value in row 2 column 4")

                                            i = Insert(into: t, values: "date", "-123", "-41.010", "-0.21", "-1234560")
                                            executeQuery(query: i, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "INSERT failed")
                                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                
                                                executeQuery(query: s, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                    XCTAssertEqual(rows!.count, 4, "SELECT returned wrong number of rows")
                                                    XCTAssertEqual(rows![3].count, 5, "SELECT returned wrong number of columns")
                                                    
                                                    XCTAssertEqual(rows![3][0]! as! String, "date", "Wrong value in row 3 column 0")
                                                    XCTAssertEqual(rows![3][1]! as! String, "-123", "Wrong value in row 3 column 1")
                                                    XCTAssertEqual(rows![3][2]! as! String, "-41.01", "Wrong value in row 3 column 2")
                                                    XCTAssertEqual(rows![3][3]! as! String, "-0.21", "Wrong value in row 3 column 3")
                                                    XCTAssertEqual(rows![3][4]! as! String, "-1234560", "Wrong value in row 3 column 4")

                                                    let longNumber = "12345678901234567890123456789012345678901234567890123456789012345678901234567890.123456789012345678901234567890123456789012345678901234567890123456789012345678901"
                                                    let negativeLongNumber = "-" + longNumber
                                                    i = Insert(into: t, values: "fig", "-123.003000", longNumber, negativeLongNumber, "0000")
                                                    executeQuery(query: i, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                        
                                                        executeQuery(query: s, connection: connection) { result, rows in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                                            XCTAssertEqual(rows!.count, 5, "SELECT returned wrong number of rows")
                                                            XCTAssertEqual(rows![4].count, 5, "SELECT returned wrong number of columns")
                                                            
                                                            XCTAssertEqual(rows![4][0]! as! String, "fig", "Wrong value in row 4 column 0")
                                                            XCTAssertEqual(rows![4][1]! as! String, "-123", "Wrong value in row 4 column 1")
                                                            XCTAssertEqual(rows![4][2]! as! String, longNumber, "Wrong value in row 4 column 2")
                                                            XCTAssertEqual(rows![4][3]! as! String, negativeLongNumber, "Wrong value in row 4 column 3")
                                                            XCTAssertEqual(rows![4][4]! as! String, "0", "Wrong value in row 4 column 4")
                                                            
                                                            i = Insert(into: t, values: "grape", "90000.0", "-400000000", "20000.000000000000000", "0.000000000000")
                                                            executeQuery(query: i, connection: connection) { result, rows in
                                                                XCTAssertEqual(result.success, true, "INSERT failed")
                                                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                                
                                                                executeQuery(query: s, connection: connection) { result, rows in
                                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                    XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows")
                                                                    XCTAssertEqual(rows![5].count, 5, "SELECT returned wrong number of columns")
                                                                    
                                                                    XCTAssertEqual(rows![5][0]! as! String, "grape")
                                                                    XCTAssertEqual(rows![5][1]! as! String, "90000")
                                                                    XCTAssertEqual(rows![5][2]! as! String, "-400000000")
                                                                    XCTAssertEqual(rows![5][3]! as! String, "20000")
                                                                    XCTAssertEqual(rows![5][4]! as! String, "0")
                                                                    
                                                                    let drop = Raw(query: "DROP TABLE", table: t)
                                                                    executeQuery(query: drop, connection: connection) { result, rows in
                                                                        XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                                                        semaphore.signal()
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            semaphore.wait()
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

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b boolean, c boolean)", connection: connection) { result, rows in
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
                            XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                            XCTAssertEqual(rows![0].count, 3, "SELECT returned wrong number of columns")
                            
                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! Bool, true, "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! Bool, false, "Wrong value in row 0 column 2")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                semaphore.signal()
                            }
                        }
                    }
                }
            }
            semaphore.wait()
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
    
    func testDateTypes() {
        let t = DateTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b date, c time, d time with time zone, e timestamp, f timestamp with time zone)", connection: connection) { result, rows in
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
                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows")
                                XCTAssertEqual(rows![0].count, 6, "SELECT returned wrong number of columns")
                                
                                XCTAssertEqual(rows![0][0]! as! String, "now", "Wrong value in row 0 column 0")
                                
                                var date = extractDate(from: rows![0][1]! as! Date)
                                let nowDate = extractDate(from: now)
                                XCTAssertEqual(date, nowDate, "Wrong value in row 0 column 1")
                                
                                var time = extractTime(from: rows![0][2]! as! Date)
                                let nowTime = extractTime(from: now)
                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 2")
                                time = extractTime(from: rows![0][3]! as! Date)
                                XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 3")
                                
                                var timestamp = rows![0][4]! as! Date
                                let nowTimeInterval = Int(now.timeIntervalSince1970)
                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), nowTimeInterval, "Wrong value in row 0 column 4")
                                timestamp = rows![0][5]! as! Date
                                XCTAssertEqual(Int(timestamp.timeIntervalSince1970), nowTimeInterval, "Wrong value in row 0 column 5")
                                
                                
                                XCTAssertEqual(rows![1][0]! as! String, "then", "Wrong value in row 1 column 0")
                                
                                date = extractDate(from: rows![1][1]! as! Date)
                                let thenDate = extractDate(from: then)
                                XCTAssertEqual(date, thenDate, "Wrong value in row 1 column 1")
                                
                                time = extractTime(from: rows![1][2]! as! Date)
                                let thenTime = extractTime(from: thenWithoutTimeZone)
                                XCTAssertEqual(time, thenTime, "Wrong value in row 1 column 2")
                                time = extractTime(from: rows![1][3]! as! Date)
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
                                    semaphore.signal()
                                }
                            }
                        }
                    }
                }
            }
            semaphore.wait()
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
        let h = Column("h")
        let i = Column("i")
        
        let tableName = tableString
    }
    
    func testStringTypes() {
        let t = StringTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(4), b character varying(4), c character(5), d char(5), e text, f \"char\", g name, h json, i xml)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let json = "{\"id\": 1, \"name\": \"A green door\", \"price\": 12.50, \"tags\": [\"home\", \"green\"]}"
                    let xml = "<card xmlns=\"http://businesscard.org\"> <name>John Doe</name> <title>CEO, Widget Inc.</title> <email>john.doe@widget.com</email> <phone>(202) 456-1414</phone> <logo url=\"widget.gif\"/> </card>"
                    
                    let i1 = Insert(into: t, values: "qiwi", "qiwi", "apple", "apple", "apple and banana", "a", "mandarin", json, xml)
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                            XCTAssertEqual(rows![0].count, 9, "SELECT returned wrong number of columns")
                            
                            XCTAssertEqual(rows![0][0]! as! String, "qiwi", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as! String, "qiwi", "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as! String, "apple", "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as! String, "apple", "Wrong value in row 0 column 3")
                            XCTAssertEqual(rows![0][4]! as! String, "apple and banana", "Wrong value in row 0 column 4")
                            XCTAssertEqual(rows![0][5]! as! String, "a", "Wrong value in row 0 column 5")
                            XCTAssertEqual(rows![0][6]! as! String, "mandarin", "Wrong value in row 0 column 6")
                            XCTAssertEqual(rows![0][7]! as! String, json, "Wrong value in row 0 column 7")
                            XCTAssertEqual(rows![0][8]! as! String, xml, "Wrong value in row 0 column 8")
                            
                            let drop = Raw(query: "DROP TABLE", table: t)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                semaphore.signal()
                            }
                        }
                    }
                }
            }
            semaphore.wait()
            expectation.fulfill()
        })
    }
    
    class UUIDTable: Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableUUID
    }
    
    func testUUID() {
        let t = UUIDTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b uuid)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let uuid1 = UUID().uuidString
                    let uuid2 = UUID().uuidString
                    
                    let i1 = Insert(into: t, values: "apple", uuid1)
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let i2 = Insert(into: t, values: "banana", Parameter())
                        executeQueryWithParameters(query: i2, connection: connection, parameters: uuid2) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 2, "INSERT returned wrong number of rows: \(rows!.count) instead of 2")
                                
                                XCTAssertEqual(rows![0][1]! as! String, uuid1, "Wrong value in row 0 column 1")
                                XCTAssertEqual(rows![1][1]! as! String, uuid2, "Wrong value in row 1 column 1")
                                semaphore.signal()
                            }
                        }
                    }
                }
            }
            semaphore.wait()
            expectation.fulfill()
        })
    }
}
