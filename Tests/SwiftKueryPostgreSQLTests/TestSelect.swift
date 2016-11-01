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
let tableSelect = "tableSelectLinux"
#else
let tableSelect = "tableSelectOSX"
#endif

class TestSelect: XCTestCase {
    
    static var allTests: [(String, (TestSelect) -> () throws -> Void)] {
        return [
            ("testSelect", testSelect),
        ]
    }
    
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let name = tableSelect
    }
    
    func testSelect() {
        let t = MyTable()
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: t.name, connection: connection) { result in
                    
                    executeRawQuery("CREATE TABLE " +  t.name + " (a varchar(40), b integer)", connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                        
                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: i1, connection: connection) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                let (_, rows) = result.asRows!
                                XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                
                                let sd1 = Select.distinct(t.a, from: t)
                                    .where(t.a.like("b%"))
                                executeQuery(query: sd1, connection: connection) { result in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                    let (_, rows) = result.asRows!
                                    XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                    
                                    let s3 = Select(t.b, t.a, from: t)
                                        .where(((t.a == "banana") || (ucase(t.a) == "APPLE")) && (t.b == 27 || t.b == -7 || t.b == 17))
                                        .order(by: .ASC(t.b), .DESC(t.a))
                                    executeQuery(query: s3, connection: connection) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (titles, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 4, "SELECT returned wrong number of rows: \(rows.count) instead of 4")
                                        XCTAssertEqual(titles[0], "b", "Wrong column name: \(titles[0]) instead of b")
                                        XCTAssertEqual(titles[1], "a", "Wrong column name: \(titles[1]) instead of a")
                                        
                                        let s4 = Select(t.a, from: t)
                                            .where(t.b >= 0)
                                            .group(by: t.a)
                                            .order(by: .DESC(t.a))
                                            .having(sum(t.b) > 3)
                                        executeQuery(query: s4, connection: connection) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (titles, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                            XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of a")
                                            XCTAssertEqual(rows[0][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[0][0]) instead of banana")
                                            XCTAssertEqual(rows[1][0]! as! String, "apple", "Wrong value in row 1 column 0: \(rows[1][0]) instead of apple")
                                            
                                            let s4Raw = Select(RawField("left(a, 2) as raw"), from: t)
                                                .where("b >= 0")
                                                .group(by: t.a)
                                                .order(by: .DESC(t.a))
                                                .having("sum(b) > 3")
                                            executeQuery(query: s4Raw, connection: connection) { result in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                let (titles, rows) = result.asRows!
                                                XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                XCTAssertEqual(titles[0], "raw", "Wrong column name: \(titles[0]) instead of raw")
                                                XCTAssertEqual(rows[0][0]! as! String, "ba", "Wrong value in row 0 column 0: \(rows[0][0]) instead of ba")
                                                XCTAssertEqual(rows[1][0]! as! String, "ap", "Wrong value in row 1 column 0: \(rows[1][0]) instead of ap")
                                                
                                                let s5 = Select(t.a, t.b, from: t)
                                                    .limit(to: 2)
                                                    .order(by: .DESC(t.a))
                                                executeQuery(query: s5, connection: connection) { result in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                    let (_, rows) = result.asRows!
                                                    XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                    XCTAssertEqual(rows[0][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[0][0]) instead of banana")
                                                    XCTAssertEqual(rows[1][0]! as! String, "banana", "Wrong value in row 1 column 0: \(rows[1][0]) instead of banana")
                                                    
                                                    let s6 = Select(ucase(t.a).as("case"), t.b, from: t)
                                                        .where(t.a.between("apra", and: "aprt"))
                                                    executeQuery(query: s6, connection: connection) { result in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                        let (titles, rows) = result.asRows!
                                                        XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                                        XCTAssertEqual(titles[0], "case", "Wrong column name: \(titles[0]) instead of 'case'")
                                                        XCTAssertEqual(rows[0][0]! as! String, "APRICOT", "Wrong value in row 0 column 0: \(rows[0][0]) instead of APRICOT")
                                                        
                                                        let s7 = Select(from: t)
                                                            .where(t.a.in("apple", "lalala"))
                                                        executeQuery(query: s7, connection: connection) { result in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                            let (_, rows) = result.asRows!
                                                            XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                            XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                            
                                                            let s8 = Select(from: t)
                                                                .where("a IN ('apple', 'lalala')")
                                                            executeQuery(query: s8, connection: connection) { result in
                                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                let (_, rows) = result.asRows!
                                                                XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                                XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                                
                                                                let s9 = "Select * from \(t.name) where a IN ('apple', 'lalala')"
                                                                executeRawQuery(s9, connection: connection) { result in
                                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                    let (_, rows) = result.asRows!
                                                                    XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                                    XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                                    
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
