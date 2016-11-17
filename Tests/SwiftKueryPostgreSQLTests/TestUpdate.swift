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
        ]
    }
    
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableUpdate
    }
    
    func testUpdateAndDelete () {
        let t = MyTable()
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: t.tableName, connection: connection) { result in
                    
                    executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b integer)", connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: i1, connection: connection) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                let (_, rows) = result.asRows!
                                XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                
                                let u1 = Update(t, set: [(t.a, "peach"), (t.b, 2)])
                                    .where(t.a == "banana")
                                executeQuery(query: u1, connection: connection) { result in
                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")
                                    
                                    let u2 = Update(t, set: [(t.a, "peach"), (t.b, 2)])
                                        .where(t.a == "apple")
                                        .returning(t.b)
                                    executeQuery(query: u2, connection: connection) { result in
                                        XCTAssertEqual(result.success, true, "UPDATE failed")
                                        XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")
                                        let (titles, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 2, "UPDATE returned wrong number of rows: \(rows.count) instead of 2")
                                        XCTAssertEqual(titles[0], "b", "Wrong column name: \(titles[0]) instead of b")
                                        XCTAssertEqual(rows[0][0]! as! String, "2", "Wrong value in row 0 column 0: \(rows[0][0]) instead of 2")
                                        XCTAssertEqual(rows[1][0]! as! String, "2", "Wrong value in row 1 column 0: \(rows[1][0]) instead of 2")
                                        
                                        let s2 = Select(t.a, t.b, from: t)
                                            .where(t.a == "banana")
                                        executeQuery(query: s2, connection: connection) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (_, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 0, "Result not empty")
                                            
                                            let d1 = Delete(from: t)
                                                .where(t.b == "2")
                                            executeQuery(query: d1, connection: connection) { result in
                                                XCTAssertEqual(result.success, true, "DELETE failed")
                                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                                
                                                executeQuery(query: s1, connection: connection) { result in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                    let (_, rows) = result.asRows!
                                                    XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                                    
                                                    let d2 = Delete(from: t)
                                                    executeQuery(query: d2, connection: connection) { result in
                                                        XCTAssertEqual(result.success, true, "DELETE failed")
                                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                                        
                                                        executeQuery(query: s1, connection: connection) { result in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                            let (_, rows) = result.asRows!
                                                            XCTAssertEqual(rows.count, 0, "Table not empty after DELETE all")
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
            expectation.fulfill()
        })
    }
}
