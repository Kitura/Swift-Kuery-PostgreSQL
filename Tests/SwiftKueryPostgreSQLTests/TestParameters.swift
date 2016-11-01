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
        
        let name = tableParameters
    }
    
    func testParameters() {
        let t = MyTable()
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: t.name, connection: connection) { result in
                    
                    executeRawQuery("CREATE TABLE " +  t.name + " (a varchar(40), b integer)", connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                        
                        let i1 = Insert(into: t, rows: [[Parameter(), 10], ["apricot", Parameter()], [Parameter(), Parameter()]])
                        executeQueryWithParameters(query: i1, connection: connection, parameters: "apple", 3, "banana", -8) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                let (_, rows) = result.asRows!
                                XCTAssertEqual(rows.count, 3, "SELECT returned wrong number of rows: \(rows.count) instead of 3")
                                XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of 'apple'")
                                XCTAssertEqual(rows[1][0]! as! String, "apricot", "Wrong value in row 0 column 0: \(rows[1][0]) instead of 'apricot'")
                                XCTAssertEqual(rows[2][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[2][0]) instead of 'banana'")
                                XCTAssertEqual(rows[0][1]! as! String, "10", "Wrong value in row 0 column 0: \(rows[0][1]) instead of 10")
                                XCTAssertEqual(rows[1][1]! as! String, "3", "Wrong value in row 0 column 0: \(rows[1][1]) instead of 3")
                                XCTAssertEqual(rows[2][1]! as! String, "-8", "Wrong value in row 0 column 0: \(rows[2][1]) instead of -8")
                                
                                let u1 = Update(t, set: [(t.a, Parameter()), (t.b, Parameter())], where: t.a == "banana")
                                executeQueryWithParameters(query: u1, connection: connection, parameters: "peach", 2) { result in
                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError)")
                                    
                                    executeQuery(query: s1, connection: connection) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (_, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 3, "SELECT returned wrong number of rows: \(rows.count) instead of 3")
                                        XCTAssertEqual(rows[2][0]! as! String, "peach", "Wrong value in row 0 column 0: \(rows[2][0]) instead of 'peach'")
                                        XCTAssertEqual(rows[2][1]! as! String, "2", "Wrong value in row 0 column 0: \(rows[2][1]) instead of 2")
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
