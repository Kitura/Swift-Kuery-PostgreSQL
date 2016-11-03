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
        
        let name = tableAlias
    }
    
    func testAlias() {
        let t = MyTable()
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: t.name, connection: connection) { result in
                    
                    executeRawQuery("CREATE TABLE " +  t.name + " (a varchar(40), b integer)", connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: i1, connection: connection) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            
                            let s1 = Select(t.a.as("\"fruit name\""), t.b.as("number"), from: t)
                            executeQuery(query: s1, connection: connection) { result in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                let (titles, rows) = result.asRows!
                                XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                XCTAssertEqual(titles[0], "fruit name", "Wrong column name: \(titles[0]) instead of 'fruit name'")
                                XCTAssertEqual(titles[1], "number", "Wrong column name: \(titles[1]) instead of 'number'")
                                
                                let s2 = Select(from: t.as("new"))
                                executeQuery(query: s2, connection: connection) { result in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                    let (titles, rows) = result.asRows!
                                    XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                    XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
                                    XCTAssertEqual(titles[1], "b", "Wrong column name: \(titles[1]) instead of 'b'")
                                    
                                    let t2 = t.as("\"t 2\"")
                                    let s3 = Select(t2.a, from: t2)
                                    executeQuery(query: s3, connection: connection) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (titles, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                        XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
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
