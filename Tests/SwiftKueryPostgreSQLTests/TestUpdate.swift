/**
 Copyright IBM Corporation 2016, 2017, 2018, 2019
 
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

@testable import SwiftKueryPostgreSQL

#if os(Linux)
let tableUpdate = "tableUpdateLinux"
#else
let tableUpdate = "tableUpdateOSX"
#endif

class TestUpdate: XCTestCase {
    
    static var allTests: [(String, (TestUpdate) -> () throws -> Void)] {
        return [
            ("testUpdateAndDelete", testUpdateAndDelete),
            ("testUpdateNilValue", testUpdateNilValue),
            ("testUpdateTimestamp", testUpdateTimestamp),
        ]
    }
    
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableUpdate
    }

    class MyTimestampTable: Table {
        let a = Column("a", Int32.self, primaryKey: true)
        let b = Column("b", String.self)

        let tableName = "MyTimestampTable" + tableNameSuffix
    }

    func testUpdateTimestamp() {
        let t = MyTimestampTable(lastUpdated: true)

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

                        let insert = Insert(into: t, rows: [[1, "Original"],[2,"Original"]])
                        executeQuery(query: insert, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            let select = Select(from: t)
                            executeQuery(query: select, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")

                                let insertTime: Date = rows![1][2] as! Date
                                // We need to ensure the update timestamp is later than the default value assigned on insert.
                                usleep(1000000)

                                let update = Update(t, set: [(t.b, "Updated")], where: t.a == 2)
                                executeQuery(query: update, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                    let select = Select(from: t)
                                    executeQuery(query: select, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows: \(rows!.count) instead of 2")

                                        XCTAssertNotEqual(insertTime.timeIntervalSinceReferenceDate, (rows![1][2] as! Date).timeIntervalSinceReferenceDate, "Update trigger failed.")
                                        expectation.fulfill()
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
    
    func testUpdateAndDelete () {
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
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")

                                let u1 = Update(t, set: [(t.a, "peach"), (t.b, 2)])
                                    .where(t.a == "banana")
                                executeQuery(query: u1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                    let u2 = Update(t, set: [(t.a, "peach"), (t.b, 2)])
                                        .where(t.a == "apple")
                                        .suffix("RETURNING b")
                                    executeQuery(query: u2, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "UPDATE failed")
                                        XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")
                                        XCTAssertNotNil(result.asResultSet, "UPDATE returned no rows")
                                        XCTAssertNotNil(rows, "UPDATE returned no rows")
                                        let resultSet = result.asResultSet!
                                        XCTAssertEqual(rows!.count, 2, "UPDATE returned wrong number of rows: \(rows!.count) instead of 2")
                                        resultSet.getColumnTitles() { titles, error in
                                            guard let titles = titles else {
                                                XCTFail("No titles in result set")
                                                return
                                            }
                                            XCTAssertEqual(titles[0], "b", "Wrong column name: \(titles[0]) instead of b")

                                            XCTAssertEqual(rows![0][0]! as! Int32, 2, "Wrong value in row 0 column 0")
                                            XCTAssertEqual(rows![1][0]! as! Int32, 2, "Wrong value in row 1 column 0")

                                            let s2 = Select(t.a, t.b, from: t)
                                                .where(t.a == "banana")
                                            executeQuery(query: s2, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNil(result.asResultSet, "SELECT should not return any rows")

                                                let d1 = Delete(from: t)
                                                    .where(t.b == "2")
                                                    .suffix("RETURNING b")
                                                executeQuery(query: d1, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "DELETE failed")
                                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                                    XCTAssertNotNil(result.asResultSet, "DELETE returned no rows")
                                                    XCTAssertNotNil(rows, "DELETE returned no rows")
                                                    XCTAssertEqual(rows!.count, 5, "DELETE returned wrong number of rows: \(rows!.count) instead of 5")

                                                    executeQuery(query: s1, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                                        XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows: \(rows!.count) instead of 1")

                                                        let d2 = Delete(from: t)
                                                        executeQuery(query: d2, connection: connection) { result, rows in
                                                            XCTAssertEqual(result.success, true, "DELETE failed")
                                                            XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")

                                                            executeQuery(query: s1, connection: connection) { result, rows in
                                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                                XCTAssertNil(result.asResultSet, "SELECT should not return any rows")
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
        })
    }

    func testUpdateNilValue() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { _ in

                    executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let insert = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: insert, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            let nilString: String? = nil
                            let update = Update(t, set:[(t.a, nilString as Any)], where: t.a == "apple")
                            executeQuery(query: update, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "UPDATE failed")
                                XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                let select = Select(from: t)
                                executeQuery(query: select, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                    XCTAssertNotNil(rows, "Expected rows but none returned")
                                    for row in rows! {
                                        XCTAssertNotEqual(row[0] as? String, "apple", "Row returned with \"apple\" instead of expected value \"nil\"")
                                    }
                                    expectation.fulfill()
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
