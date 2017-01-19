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

@testable import SwiftKueryPostgreSQL

#if os(Linux)
let tableWith = "tableWithLinux"
let tableAuxiliary = "tableAuxiliaryLinux"
let tableSelectWith = "tableSelectWithLinux"
#else
let tableWith = "tableWithOSX"
let tableAuxiliary = "tableAuxiliaryOSX"
let tableSelectWith = "tableSelectWithOSX"
#endif

class TestWith: XCTestCase {
    
    static var allTests: [(String, (TestWith) -> () throws -> Void)] {
        return [
            ("testWith", testWith),
        ]
    }
    
    class MyTable1: Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableWith
    }
    
    class MyTable2: Table {
        let c = Column("c")
        let b = Column("b")
        
        let tableName = tableSelectWith
    }
    
    func testWith() {
        let t1 = MyTable1()
        let t2 = MyTable2()
        
        class WithTable: AuxiliaryTable {
            let tableName = tableAuxiliary
            
            let d = Column("d")
            let f = Column("f")
        }
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: t1.tableName, connection: connection) { result in
                    
                    cleanUp(table: t2.tableName, connection: connection) { result in
                        
                        executeRawQuery("CREATE TABLE " +  t1.tableName + " (a varchar(40), b integer)", connection: connection) { result, rows in
                            
                            XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                            XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                            
                            executeRawQuery("CREATE TABLE " +  t2.tableName + " (c varchar(40), b integer)", connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                
                                let i1 = Insert(into: t1, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                                executeQuery(query: i1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                    
                                    let i2 = Insert(into: t2, rows: [["apple", 11], ["apricot", 3], ["banana", 17], ["apple", 1], ["peach", -7]])
                                    executeQuery(query: i2, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                        
                                        
                                        let withTable = WithTable(as: Select(t2.c.as("d"), t2.b.as("f"), from: t2))
                                        let s = with(withTable, Select(withTable.d, t1.a, from: [t1, withTable]))
                                        executeQuery(query: s, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            XCTAssertEqual(rows!.count, 30, "SELECT returned wrong number of rows: \(rows!.count) instead of 6")
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
