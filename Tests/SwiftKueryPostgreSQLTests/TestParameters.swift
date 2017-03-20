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
let tableParameters = "tableParametersLinux"
#else
let tableParameters = "tableParametersOSX"
#endif

class TestParameters: XCTestCase {
    
    static var allTests: [(String, (TestParameters) -> () throws -> Void)] {
        return [
            ("testParameters", testParameters),
        ]
    }
    
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableParameters
    }
    
    func testParameters() {
        let t = MyTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                executeRawQuery("CREATE TABLE " +  t.tableName + " (a varchar(40), b integer)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let i1 = Insert(into: t, rows: [[Parameter(), 10], ["apricot", Parameter()], [Parameter(), Parameter()]])
                    executeQueryWithParameters(query: i1, connection: connection, parameters: "apple", 3, "banana", -8) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                            XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows![0][0]) instead of 'apple'")
                            XCTAssertEqual(rows![1][0]! as! String, "apricot", "Wrong value in row 0 column 0: \(rows![1][0]) instead of 'apricot'")
                            XCTAssertEqual(rows![2][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows![2][0]) instead of 'banana'")
                            XCTAssertEqual(rows![0][1]! as! Int32, 10, "Wrong value in row 0 column 0: \(rows![0][1]) instead of 10")
                            XCTAssertEqual(rows![1][1]! as! Int32, 3, "Wrong value in row 0 column 0: \(rows![1][1]) instead of 3")
                            XCTAssertEqual(rows![2][1]! as! Int32, -8, "Wrong value in row 0 column 0: \(rows![2][1]) instead of -8")
                            
                            let u1 = Update(t, set: [(t.a, Parameter()), (t.b, Parameter())], where: t.a == "banana")
                            executeQueryWithParameters(query: u1, connection: connection, parameters: "peach", 2) { result, rows in
                                XCTAssertEqual(result.success, true, "UPDATE failed")
                                XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")
                                
                                executeQuery(query: s1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                    XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                                    XCTAssertEqual(rows![2][0]! as! String, "peach", "Wrong value in row 0 column 0: \(rows![2][0]) instead of 'peach'")
                                    XCTAssertEqual(rows![2][1]! as! Int32, 2, "Wrong value in row 0 column 0: \(rows![2][1]) instead of 2")
                                    
                                    let raw = "UPDATE " + t.tableName + " SET a = 'banana', b = $1 WHERE a = $2"
                                    executeRawQueryWithParameters(raw, connection: connection, parameters: 4, "peach") { result, rows in
                                        XCTAssertEqual(result.success, true, "UPDATE failed")
                                        XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")
                                        
                                        executeQuery(query: s1, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                                            XCTAssertEqual(rows![2][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows![2][0]) instead of 'peach'")
                                            XCTAssertEqual(rows![2][1]! as! Int32, 4, "Wrong value in row 0 column 0: \(rows![2][1]) instead of 4")
                                            
                                            let s2 = Select(from: t).where(t.a != Parameter())
                                            executeQueryWithParameters(query: s2, connection: connection, parameters: nil) { result, rows in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
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
