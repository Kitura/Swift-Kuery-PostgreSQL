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
let tableInsert = "tableInsertLinux"
#else
let tableInsert = "tableInsertOSX"
#endif

class TestInsert: XCTestCase {
    
    static var allTests: [(String, (TestInsert) -> () throws -> Void)] {
        return [
            ("testInsert", testInsert),
        ]
    }
        
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let name = tableInsert
    }
    
    func testInsert() {
        let t = MyTable()
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: t.name, connection: connection) { result in
                    
                    executeRawQuery("CREATE TABLE " +  t.name + " (a varchar(40), b integer)", connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                        
                        let i1 = Insert(into: t, values: "apple", 10)
                        executeQuery(query: i1, connection: connection) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            let i2 = Insert(into: t, valueTuples: (t.a, "apricot"), (t.b, "3"))
                            executeQuery(query: i2, connection: connection) { result in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                
                                let i3 = Insert(into: t, columns: [t.a, t.b], values: ["banana", 17])
                                executeQuery(query: i3, connection: connection) { result in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                    
                                    let i4 = Insert(into: t, rows: [["apple", 17], ["banana", -7], ["banana", 27]])
                                    executeQuery(query: i4, connection: connection) { result in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                        
                                        let s1 = Select(from: t)
                                        executeQuery(query: s1, connection: connection) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (_, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                            
                                            let drop = Raw(query: "DROP TABLE", table: t)
                                            executeQuery(query: drop, connection: connection) { result in
                                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
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
