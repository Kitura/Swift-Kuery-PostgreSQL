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
let tableAlias = "tableAliasLinux"
#else
let tableAlias = "tableAliasOSX"
#endif

class TestAlias: XCTestCase {
    
    static var allTests: [(String, (TestAlias) -> () throws -> Void)] {
        return [
            ("testAlias", testAlias),
        ]
    }
    
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableAlias
    }
    
    func testAlias() {
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

                            let s1 = Select(t.a.as("\"fruit name\""), t.b.as("number"), from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                let resultSet = result.asResultSet!
                                XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")
                                resultSet.getColumnTitles() { titles, error in
                                    guard let titles = titles else {
                                    XCTFail("No titles in result set")
                                    return
                                    }
                                    XCTAssertEqual(titles[1], "number", "Wrong column name: \(titles[1]) instead of 'number'")
                                    XCTAssertEqual(titles[0], "fruit name", "Wrong column name: \(titles[0]) instead of 'fruit name'")

                                    let s2 = Select(from: t.as("new"))
                                    executeQuery(query: s2, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        let resultSet = result.asResultSet!
                                        XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")
                                        resultSet.getColumnTitles() { titles, error in
                                            guard let titles = titles else {
                                                XCTFail("No titles in result set")
                                                return
                                            }
                                            XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
                                            XCTAssertEqual(titles[1], "b", "Wrong column name: \(titles[1]) instead of 'b'")
                                            let t2 = t.as("\"t 2\"")
                                            let s3 = Select(t2.a, from: t2)
                                            executeQuery(query: s3, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                                let resultSet = result.asResultSet!
                                                XCTAssertEqual(rows!.count, 6, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")
                                                resultSet.getColumnTitles() { titles, error in
                                                    guard let titles = titles else {
                                                        XCTFail("No titles in result set")
                                                        return
                                                    }
                                                    XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
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
        })
    }
}
