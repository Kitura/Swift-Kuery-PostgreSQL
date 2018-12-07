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

import XCTest
import SwiftKuery

import Foundation

@testable import SwiftKueryPostgreSQL

#if os(Linux)
let tableSelect = "tableSelectLinux"
let tableSelect2 = "tableSelect2Linux"
let tableSelect3 = "tableSelect3Linux"
let tableSelectDate = "tableSelectDateLinux"
let tableConnectionState = "tableConnectionStateLinux"
#else
let tableSelect = "tableSelectOSX"
let tableSelect2 = "tableSelect2OSX"
let tableSelect3 = "tableSelect3OSX"
let tableSelectDate = "tableSelectDateOSX"
let tableConnectionState = "tableConnectionStateOSX"
#endif

class TestSelect: XCTestCase {
    
    static var allTests: [(String, (TestSelect) -> () throws -> Void)] {
        return [
            ("testSelect", testSelect),
            ("testSelectDate", testSelectDate),
            ("testSelectFromMany", testSelectFromMany),
            ("testConnectionState", testConnectionState),
        ]
    }
    
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableSelect
    }
    class MyTable2 : Table {
        let c = Column("c")
        let b = Column("b")
        
        let tableName = tableSelect2
    }
    class MyTable3 : Table {
        let d = Column("d")
        let b = Column("b")
        
        let tableName = tableSelect3
    }
    
    func testSelect() {
        let t = MyTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection() { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { result in
                    
                    executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: i1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            
                            let s1 = Select(from: t)
                                .limit(to: 3)
                                .offset(2)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                                
                                let sd1 = Select.distinct(t.a, from: t)
                                    .where(t.a.like("b%") && t.b.isNotNull())
                                executeQuery(query: sd1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                    XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows: \(rows!.count) instead of 1")
                                    
                                    let s3 = Select(t.b, t.a, from: t)
                                        .where(((t.a == "banana") || (ucase(t.a) == "APPLE")) && (t.b == 27 || t.b == -7 || t.b == 17))
                                        .order(by: .ASC(t.b), .DESC(t.a))
                                    executeQuery(query: s3, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                        let resultSet = result.asResultSet!
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        XCTAssertEqual(rows!.count, 4, "SELECT returned wrong number of rows: \(rows!.count) instead of 4")
                                        resultSet.getColumnTitles() { titles, error in
                                            guard let titles = titles else {
                                                XCTFail("No titles in result set")
                                                return
                                            }
                                            XCTAssertEqual(titles[0], "b", "Wrong column name: \(titles[0]) instead of b")
                                            XCTAssertEqual(titles[1], "a", "Wrong column name: \(titles[1]) instead of a")

                                            let s4 = Select(t.a, from: t)
                                                .where(t.b >= 0)
                                                .group(by: t.a)
                                                .order(by: .DESC(t.a))
                                                .having(sum(t.b) > 3)
                                            executeQuery(query: s4, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                                let resultSet = result.asResultSet!
                                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                                resultSet.getColumnTitles() { titles, error in
                                                    guard let titles = titles else {
                                                        XCTFail("No titles in result set")
                                                        return
                                                    }
                                                    XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of a")
                                                    XCTAssertEqual(rows![0][0]! as! String, "banana", "Wrong value in row 0 column 0")
                                                    XCTAssertEqual(rows![1][0]! as! String, "apple", "Wrong value in row 1 column 0")

                                                    let s4Raw = Select(RawField("left(a, 2) as raw"), from: t)
                                                        .where("b >= 0")
                                                        .group(by: t.a)
                                                        .order(by: .DESC(t.a))
                                                        .having("sum(b) > 3")
                                                    executeQuery(query: s4Raw, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                                        let resultSet = result.asResultSet!
                                                        XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                                        resultSet.getColumnTitles() { titles, error in
                                                            guard let titles = titles else {
                                                                XCTFail("No titles in result set")
                                                                return
                                                            }
                                                            XCTAssertEqual(titles[0], "raw", "Wrong column name: \(titles[0]) instead of raw")
                                                            XCTAssertEqual(rows![0][0]! as! String, "ba", "Wrong value in row 0 column 0")
                                                            XCTAssertEqual(rows![1][0]! as! String, "ap", "Wrong value in row 1 column 0")

                                                            let s5 = Select(t.a, t.b, from: t)
                                                                .limit(to: 2)
                                                                .order(by: .DESC(t.a))
                                                            executeQuery(query: s5, connection: connection) { result, rows in
                                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                                                XCTAssertEqual(rows![0][0]! as! String, "banana", "Wrong value in row 0 column 0")
                                                                XCTAssertEqual(rows![1][0]! as! String, "banana", "Wrong value in row 1 column 0")

                                                                let s6 = Select(ucase(t.a).as("upper case"), t.b, from: t)
                                                                    .where(t.a.between("apra", and: "aprt"))
                                                                executeQuery(query: s6, connection: connection) { result, rows in
                                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                    XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows: \(rows!.count) instead of 1")
                                                                    let resultSet = result.asResultSet!
                                                                    resultSet.getColumnTitles() { titles, error in
                                                                        guard let titles = titles else {
                                                                            XCTFail("No titles in result set")
                                                                            return
                                                                        }
                                                                        XCTAssertEqual(titles[0], "upper case", "Wrong column name: \(titles[0]) instead of 'upper case'")
                                                                        XCTAssertEqual(rows![0][0]! as! String, "APRICOT", "Wrong value in row 0 column 0")

                                                                        let s61 = Select(ucase(t.a).as("upper case"), t.b, from: t)
                                                                            .where(t.a.between("apra", and: "aprt").isNotNull())
                                                                        executeQuery(query: s61, connection: connection) { result, rows in
                                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                            XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")

                                                                            let s7 = Select(from: t)
                                                                                .where(t.a.in("apple", "lalala"))
                                                                            executeQuery(query: s7, connection: connection) { result, rows in
                                                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                                                                XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")

                                                                                let s8 = Select(from: t)
                                                                                    .where("a IN ('apple', 'lalala')")
                                                                                executeQuery(query: s8, connection: connection) { result, rows in
                                                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                                    XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                                                                    XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")

                                                                                    let s9 = "Select * from \"\(t.tableName)\" where a IN ('apple', 'lalala')"
                                                                                    executeRawQuery(s9, connection: connection) { result, rows in
                                                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                                        XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                                                                        XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")

                                                                                        let drop = Raw(query: "DROP TABLE", table: t)
                                                                                        executeQuery(query: drop, connection: connection) { result, rows in
                                                                                            XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                                                                            XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                                                                            expectation.fulfill()
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
                            }
                        }
                    }
                }
            }
        })
    }
    
    func testSelectFromMany() {
        let t1 = MyTable()
        let t2 = MyTable2()
        let t3 = MyTable3()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection() { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t1.tableName, connection: connection) { result in
                    cleanUp(table: t2.tableName, connection: connection) { result in
                        cleanUp(table: t3.tableName, connection: connection) { result in

                            executeRawQuery("CREATE TABLE \"" +  t1.tableName + "\" (a varchar(40), b integer)", connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                                executeRawQuery("CREATE TABLE \"" +  t2.tableName + "\" (c varchar(40), b integer)", connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                                    executeRawQuery("CREATE TABLE \"" +  t3.tableName + "\" (d varchar(40), b integer)", connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                                        let i1 = Insert(into: t1, rows: [["apple", 10], ["apricot", 3], ["banana", 17]])
                                        executeQuery(query: i1, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "INSERT failed")
                                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                            let i2 = Insert(into: t2, rows: [["apple", 17], ["banana", -7], ["banana", 10]])
                                            executeQuery(query: i2, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "INSERT failed")
                                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                                let i3 = Insert(into: t3, rows: [["banana", 10], ["apricot", -3], ["apple", 17]])
                                                executeQuery(query: i3, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                                    let s1 = Select(from: [t1, t2, t3])
                                                        .where(t1.b == t2.b && t3.b == t1.b)
                                                    executeQuery(query: s1, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                                        let resultSet = result.asResultSet!
                                                        XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                                        resultSet.getColumnTitles() { titles, error in
                                                            guard let titles = titles else {
                                                                XCTFail("No titles in result set")
                                                                return
                                                            }
                                                            XCTAssertEqual(titles.count, 6, "SELECT returned wrong number of columns: \(titles.count) instead of 6")
                                                            expectation.fulfill()
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
        })
    }
    
    class DateTable: Table {
        let a = Column("a")
        let date = Column("date")
        let timestamp = Column("timestamp")
        let time = Column("time")
        
        let tableName = tableSelectDate
    }
    
    private func extractTime(from date: Date) -> String {
        let hour = Calendar.current.component(.hour, from: date)
        let minutes = Calendar.current.component(.minute, from: date)
        let seconds = Calendar.current.component(.second, from: date)
        return "\(hour):\(minutes):\(seconds)"
    }
    
    func testSelectDate() {
        let t = DateTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection() { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { result in

                    executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), date date, timestamp timestamp with time zone, time time with time zone)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let i1 = Insert(into: t, values: "now", Date(), Date(), Date())
                        executeQuery(query: i1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")

                            let s1 = Select(from: t)
                                .where(t.date >= Date(timeIntervalSince1970: 50000)
                                    && t.time.between(self.extractTime(from: Date(timeIntervalSince1970: 0)),
                                                      and: self.extractTime(from: Date(timeIntervalSinceNow: 1000))))
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows: \(rows!.count) instead of 1")

                                let i2 = Insert(into: t, values: "then", Date(timeIntervalSince1970: 0), Date(timeIntervalSince1970: 0), Date(timeIntervalSince1970: 0))
                                executeQuery(query: i2, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "INSERT failed")

                                    let s2 = Select(from: t)
                                        .where(t.date.in(Date(timeIntervalSince1970: 0))
                                            && t.timestamp != Date(timeIntervalSince1970: 100))
                                    executeQuery(query: s2, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows: \(rows!.count) instead of 1")

                                        let s3 = Select(from: t)
                                            .where(now() > t.date)
                                        executeQuery(query: s3, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")
                                            expectation.fulfill()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
    
    
    class ConnectionStateTable: Table {
        let a = Column("a", Varchar.self, length: 30)
        let b = Column("b", Int32.self)
        
        let tableName = tableConnectionState
    }
    
    func testConnectionState() {
        let t = ConnectionStateTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection() { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { result in

                    t.create(connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let i = Insert(into: t, rows: [["apple", 1], ["apricot", 2], ["banana", 3], ["qiwi", -1], ["plum", -2], ["peach", -3]])
                        executeQuery(query: i, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")

                            let s1 = Select(from: t).where(t.b > 0)
                            s1.execute(connection) { result1 in
                                let s2 = Select(from: t).where(t.b < 0)
                                s2.execute(connection) { result2 in

                                    XCTAssertEqual(result1.success, true, "SELECT 1 failed")
                                    XCTAssertEqual(result2.success, true, "SELECT 2 failed")

                                    if let resultSet = result2.asResultSet {
                                        resultSet.forEach() { row, error in
                                            guard let row = row else {
                                                // A nil row indicates the end of the operation
                                                return expectation.fulfill()
                                            }
                                            if let b = row[1] as? Int32 {
                                                XCTAssertTrue(b < 0, "Bad result for SELECT")
                                            } else {
                                                XCTFail("Wrong type in SELECT")
                                            }

                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
