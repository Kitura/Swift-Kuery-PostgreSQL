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
let tableAuxiliary2 = "tableAuxiliary2Linux"
let tableSelectWith = "tableSelectWithLinux"
let tableSelectWith2 = "tableSelectWith2Linux"
#else
let tableWith = "tableWithOSX"
let tableAuxiliary = "tableAuxiliaryOSX"
let tableAuxiliary2 = "tableAuxiliary2OSX"
let tableSelectWith = "tableSelectWithOSX"
let tableSelectWith2 = "tableSelectWith2OSX"
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
    
    class MyTable3: Table {
        let x = Column("x")
        let y = Column("y")
        
        let tableName = tableSelectWith2
    }
    
    func testWith() {
        let t1 = MyTable1()
        let t2 = MyTable2()
        let t3 = MyTable3()
        
        class WithTable: AuxiliaryTable {
            let tableName = tableAuxiliary
            
            let d = Column("d")
            let f = Column("f")
        }
        
        class WithTable2: AuxiliaryTable {
            let tableName = tableAuxiliary2
            
            let x = Column("x")
            let y = Column("y")
        }
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t1.tableName, connection: connection) { result in
                
                cleanUp(table: t2.tableName, connection: connection) { result in
                    
                    executeRawQuery("CREATE TABLE \"" +  t1.tableName + "\" (a varchar(40), b integer)", connection: connection) { result, rows in
                        
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        executeRawQuery("CREATE TABLE \"" +  t2.tableName + "\" (c varchar(40), b integer)", connection: connection) { result, rows in
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
                                    var s = with(withTable, Select(withTable.d, t1.a, from: [t1, withTable]))
                                    executeQuery(query: s, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        XCTAssertEqual(rows!.count, 30, "SELECT returned wrong number of rows: \(rows!.count) instead of 30")
                                        
                                        s = with(withTable, Select(withTable.d, t1.a, from: t1)
                                            .join(withTable)
                                            .on(t1.a == withTable.d))
                                        executeQuery(query: s, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            XCTAssertEqual(rows!.count, 8, "SELECT returned wrong number of rows: \(rows!.count) instead of 8")
                                            
                                            cleanUp(table: t3.tableName, connection: connection) { result in
                                                
                                                executeRawQuery("CREATE TABLE \"" +  t3.tableName + "\" (x varchar(40), y integer)", connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                                    
                                                    let i3 = Insert(into: t3, rows: [["apple", 10], ["apricot", 3], ["banana", 2]])
                                                    executeQuery(query: i3, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                        
                                                        let withTable2 = WithTable2(as: Select(t3.x, t3.y, from: t3))
                                                        s = with([withTable, withTable2],
                                                                 Select(t1.a, from: t1)
                                                                    .join(withTable).on(withTable.d == t1.a)
                                                                    .join(withTable2).on(withTable2.y == t1.b))
                                                        executeQuery(query: s, connection: connection) { result, rows in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                                            XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                                                            
                                                            let insertSelect = Select(withTable.d, from: withTable)
                                                            let i = with(withTable,
                                                                         Insert(into: t1, columns: [t1.a], insertSelect))
                                                            executeQuery(query: i, connection: connection) { result, rows in
                                                                XCTAssertEqual(result.success, true, "INSERT failed")
                                                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                                
                                                                let u = with(withTable,
                                                                             Update(t1, set: [(t1.a, "peach"), (t1.b, 2)])
                                                                                .where(t1.a.in(Select(withTable.d, from: withTable))))
                                                                    .suffix("RETURNING *")
                                                                executeQuery(query: u, connection: connection) { result, rows in
                                                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")
                                                                    XCTAssertNotNil(result.asResultSet, "UPDATE returned no rows")
                                                                    XCTAssertNotNil(rows, "UPDATE returned no rows")
                                                                    XCTAssertEqual(rows!.count, 11, "UPDATE returned wrong number of rows: \(rows!.count) instead of 11")
                                                                    XCTAssertEqual(rows![0][0]! as! String, "peach", "Wrong value in row 0 column 0")
                                                                    
                                                                    var d = with(withTable,
                                                                                 Delete(from: t1)
                                                                                    .where(t1.a.in(Select(withTable.d, from: withTable))))
                                                                        .suffix("RETURNING *")
                                                                    executeQuery(query: d, connection: connection) { result, rows in
                                                                        XCTAssertEqual(result.success, true, "DELETE failed")
                                                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                                                        XCTAssertNotNil(result.asResultSet, "DELETE returned no rows")
                                                                        XCTAssertNotNil(rows, "DELETE returned no rows")
                                                                        XCTAssertEqual(rows!.count, 11, "DELETE returned wrong number of rows: \(rows!.count) instead of 11")
                                                                        
                                                                        d = with(withTable,
                                                                                 Delete(from: t1)
                                                                                    .where(t1.a != withTable.d))
                                                                            .suffix("RETURNING *")
                                                                        executeQuery(query: d, connection: connection) { result, rows in
                                                                            XCTAssertEqual(result.success, true, "DELETE failed")
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
            }
            semaphore.wait()
            expectation.fulfill()
        })
    }
}
