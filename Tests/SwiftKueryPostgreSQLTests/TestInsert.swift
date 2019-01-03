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

@testable import SwiftKueryPostgreSQL

#if os(Linux)
let tableInsert = "tableInsertLinux"
let tableInsert2 = "tableInsert2Linux"
let tableInsert3 = "tableInsert3Linux"
#else
let tableInsert = "tableInsertOSX"
let tableInsert2 = "tableInsert2OSX"
let tableInsert3 = "tableInsert3OSX"
#endif

class TestInsert: XCTestCase {

    static var allTests: [(String, (TestInsert) -> () throws -> Void)] {
        return [
            ("testInsert", testInsert),
            ("testInsertID", testInsertID)
        ]
    }

    class MyTable : Table {
        let a = Column("a", autoIncrement: true, primaryKey: true)
        let b = Column("b")

        let tableName = tableInsert
    }

    class MyTable2 : Table {
        let a = Column("a")
        let b = Column("b")

        let tableName = tableInsert2
    }

    class MyTable3 : Table {
        let a = Column("a", autoIncrement: true, primaryKey: true)
        let b = Column("b")

        let tableName = tableInsert3
    }
    func testInsert() {
        let t = MyTable()
        let t2 = MyTable2()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection() { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { result in
                    cleanUp(table: t2.tableName, connection: connection) { result in

                        executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b integer)", connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                            XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                            executeRawQuery("CREATE TABLE \"" +  t2.tableName + "\" (a varchar(40), b integer)", connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                                let i1 = Insert(into: t, values: "apple", 10)
                                executeQuery(query: i1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                    let i2 = Insert(into: t, valueTuples: (t.a, "apricot"), (t.b, "3"))
                                        .suffix("RETURNING *")
                                    executeQuery(query: i2, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                        XCTAssertNotNil(result.asResultSet, "INSERT returned no rows")
                                        XCTAssertNotNil(rows, "INSERT returned no rows")
                                        let resultSet = result.asResultSet!
                                        XCTAssertEqual(rows!.count, 1, "INSERT returned wrong number of rows: \(rows!.count) instead of 1")
                                        resultSet.getColumnTitles() { titles, error in
                                            guard let titles = titles else {
                                                XCTFail("No titles in result set")
                                                return
                                            }
                                            XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of a")
                                            XCTAssertEqual(titles[1], "b", "Wrong column name: \(titles[1]) instead of b")
                                            XCTAssertEqual(rows![0][0]! as! String, "apricot", "Wrong value in row 0 column 0")
                                            XCTAssertEqual(rows![0][1]! as! Int32, 3, "Wrong value in row 1 column 0")

                                            let i3 = Insert(into: t, columns: [t.a, t.b], values: ["banana", 17])
                                                .suffix("RETURNING b")
                                            executeQuery(query: i3, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "INSERT failed")
                                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                XCTAssertNotNil(result.asResultSet, "INSERT returned no rows")
                                                XCTAssertNotNil(rows, "INSERT returned no rows")
                                                let resultSet = result.asResultSet!
                                                XCTAssertEqual(rows!.count, 1, "INSERT returned wrong number of rows: \(rows!.count) instead of 1")
                                                resultSet.getColumnTitles() { titles, error in
                                                    guard let titles = titles else {
                                                    XCTFail("No titles in result set")
                                                    return
                                                    }
                                                    XCTAssertEqual(titles[0], "b", "Wrong column name: \(titles[0]) instead of b")
                                                    XCTAssertEqual(titles.count, 1, "Wrong number of columns: \(titles.count) instead of 1")
                                                    XCTAssertEqual(rows![0][0]! as! Int32, 17, "Wrong value in row 0 column 0")

                                                    let i4 = Insert(into: t, rows: [["apple", 17], ["banana", -7], ["banana", 27]])
                                                        .suffix("RETURNING b")
                                                    executeQuery(query: i4, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                        XCTAssertNotNil(result.asResultSet, "INSERT returned no rows")
                                                        XCTAssertNotNil(rows, "INSERT returned no rows")
                                                        XCTAssertEqual(rows!.count, 3, "INSERT returned wrong number of rows: \(rows!.count) instead of 3")

                                                        let i5 = Insert(into: t, rows: [["apple", 5], ["banana", 10], ["banana", 3]])
                                                            .suffix("RETURNING b, a")
                                                        executeQuery(query: i5, connection: connection) { result, rows in
                                                            XCTAssertEqual(result.success, true, "INSERT failed")
                                                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                            XCTAssertNotNil(result.asResultSet, "INSERT returned no rows")
                                                            XCTAssertNotNil(rows, "INSERT returned no rows")
                                                            let resultSet = result.asResultSet!
                                                            XCTAssertEqual(rows!.count, 3, "INSERT returned wrong number of rows: \(rows!.count) instead of 3")
                                                            resultSet.getColumnTitles() { titles, error in
                                                                guard let titles = titles else {
                                                                    XCTFail("No titles in result set")
                                                                    return
                                                                }
                                                                XCTAssertEqual(titles.count, 2, "Wrong number of columns: \(titles.count) instead of 2")

                                                                let i6 = Insert(into: t2, Select(from: t).where(t.a == "apple"))
                                                                    .suffix("RETURNING *")
                                                                executeQuery(query: i6, connection: connection) { result, rows in
                                                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                                    XCTAssertNotNil(rows, "INSERT returned no rows")
                                                                    let resultSet = result.asResultSet!
                                                                    XCTAssertEqual(rows!.count, 3, "INSERT returned wrong number of rows: \(rows!.count) instead of 3")
                                                                    resultSet.getColumnTitles() { titles, error in
                                                                        guard let titles = titles else {
                                                                            XCTFail("No titles in result set")
                                                                            return
                                                                        }
                                                                        XCTAssertEqual(titles.count, 2, "Wrong number of columns: \(titles.count) instead of 2")

                                                                        let s1 = Select(from: t)
                                                                        executeQuery(query: s1, connection: connection) { result, rows in
                                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                                                            XCTAssertEqual(rows!.count, 9, "INSERT returned wrong number of rows: \(rows!.count) instead of 9")

                                                                            let dropT = Raw(query: "DROP TABLE", table: t)
                                                                            executeQuery(query: dropT, connection: connection) { result, rows in
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
        })
    }

    func testInsertID() {
        let t3 = MyTable3()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection() { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t3.tableName, connection: connection) { result in
                    executeRawQuery("CREATE TABLE \"" +  t3.tableName + "\" (a SERIAL PRIMARY KEY, b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        let i7 = Insert(into: t3, valueTuples: [(t3.b, 5)], returnID: true)
                        executeQuery(query: i7, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            XCTAssertNotNil(result.asResultSet, "INSERT returned no rows")
                            XCTAssertNotNil(rows, "INSERT returned no rows")
                            let resultSet = result.asResultSet!
                            XCTAssertEqual(rows!.count, 1, "INSERT returned wrong number of rows: \(rows!.count) instead of 1")
                            resultSet.getColumnTitles() { titles, error in
                                guard let titles = titles else {
                                    XCTFail("No titles in result set")
                                    return
                                }
                                XCTAssertEqual(titles.count, 1, "Wrong number of columns: \(titles.count) instead of 1")

                                let dropT3 = Raw(query: "DROP TABLE", table: t3)
                                executeQuery(query: dropT3, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
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
