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
let table1Join = "table1JoinLinux"
let table2Join = "table2JoinLinux"
let tableUnion = "tableUnionLinux"
#else
let table1Join = "table1JoinOSX"
let table2Join = "table2JoinOSX"
let tableUnion = "tableUnionOSX"
#endif

class TestJoin: XCTestCase {
    
    static var allTests: [(String, (TestJoin) -> () throws -> Void)] {
        return [
            ("testJoin", testJoin),
            ("testUnion", testUnion),
        ]
    }
    
    class MyTable1: Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = table1Join
    }
    
    class MyTable2: Table {
        let c = Column("c")
        let b = Column("b")
        
        let tableName = table2Join
    }
    
    class MyTable3: Table {
        let d = Column("d")
        let b = Column("b")
        
        let tableName = tableUnion
    }
    
    func testJoin() {
        let myTable1 = MyTable1()
        let myTable2 = MyTable2()
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: myTable1.tableName, connection: connection) { result in
                    
                    cleanUp(table: myTable2.tableName, connection: connection) { result in
                        
                        executeRawQuery("CREATE TABLE " +  myTable1.tableName + " (a varchar(40), b integer)", connection: connection) { result, rows in
                            
                            XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                            XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                            
                            executeRawQuery("CREATE TABLE " +  myTable2.tableName + " (c varchar(40), b integer)", connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                
                                let i1 = Insert(into: myTable1, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                                executeQuery(query: i1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                    
                                    let i2 = Insert(into: myTable2, rows: [["apple", 11], ["apricot", 3], ["banana", 17], ["apple", 1], ["peach", -7]])
                                    executeQuery(query: i2, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                        
                                        let s1 = Select(from: myTable1)
                                            .join(myTable2)
                                            .on(myTable1.b == myTable2.b)
                                        executeQuery(query: s1, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            let resultSet = result.asResultSet!
                                            XCTAssertEqual(rows!.count, 4, "SELECT returned wrong number of rows: \(rows!.count) instead of 4")
                                            let titles = resultSet.titles
                                            XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
                                            XCTAssertEqual(titles[1], "b", "Wrong column name: \(titles[1]) instead of 'b'")
                                            XCTAssertEqual(titles[2], "c", "Wrong column name: \(titles[2]) instead of 'c'")
                                            XCTAssertEqual(titles[3], "b", "Wrong column name: \(titles[3]) instead of 'b'")
                                            XCTAssertEqual(rows![0][0]! as! String, "apricot", "Wrong value in row 0 column 0: \(rows![0][0]) instead of 'apricot'")
                                            XCTAssertEqual(rows![1][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows![1][0]) instead of 'banana'")
                                            XCTAssertEqual(rows![2][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows![2][0]) instead of apple")
                                            XCTAssertEqual(rows![3][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows![3][0]) instead of 'banana'")
                                            
                                            let t1 = myTable1.as("t1")
                                            let t2 = myTable2.as("t2")
                                            let s2 = Select(from: t1)
                                                .join(t2)
                                                .on(t1.b == t2.b)
                                            executeQuery(query: s2, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                XCTAssertEqual(rows!.count, 4, "SELECT returned wrong number of rows: \(rows!.count) instead of 4")
                                                XCTAssertEqual(rows![0][0]! as! String, "apricot", "Wrong value in row 0 column 0: \(rows![0][0]) instead of 'apricot'")
                                                XCTAssertEqual(rows![1][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows![1][0]) instead of 'banana'")
                                                XCTAssertEqual(rows![2][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows![2][0]) instead of 'apple'")
                                                XCTAssertEqual(rows![3][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows![3][0]) instead of 'banana'")
                                                
                                                let s3 = Select(from: myTable1)
                                                    .leftJoin(myTable2)
                                                    .on(myTable1.a == myTable2.c)
                                                executeQuery(query: s3, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                    XCTAssertEqual(rows!.count, 8, "SELECT returned wrong number of rows: \(rows!.count) instead of 8")
                                                    
                                                    let s4 = Select(from: t1)
                                                        .fullJoin(t2)
                                                        .using(t1.b)
                                                    executeQuery(query: s4, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        let resultSet = result.asResultSet!
                                                        XCTAssertEqual(rows!.count, 8, "SELECT returned wrong number of rows: \(rows!.count) instead of 8")
                                                        XCTAssertEqual(resultSet.titles.count, 3, "SELECT returned wrong number of columns: \(resultSet.titles.count) instead of 3")
                                                        
                                                        let s5 = Select(from: t1)
                                                            .naturalJoin(t2)
                                                        executeQuery(query: s5, connection: connection) { result, rows in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                            let resultSet = result.asResultSet!
                                                            XCTAssertEqual(rows!.count, 4, "SELECT returned wrong number of rows: \(rows!.count) instead of 4")
                                                            XCTAssertEqual(resultSet.titles.count, 3, "SELECT returned wrong number of columns: \(resultSet.titles.count) instead of 3")
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
    
    func testUnion() {
        let myTable1 = MyTable1()
        let myTable2 = MyTable2()
        let myTable3 = MyTable3()
        
        let connection = createConnection()
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                cleanUp(table: myTable1.tableName, connection: connection) { result in
                    
                    cleanUp(table: myTable2.tableName, connection: connection) { result in
                        
                        cleanUp(table: myTable3.tableName, connection: connection) { result in
                            
                            executeRawQuery("CREATE TABLE " +  myTable1.tableName + " (a varchar(40), b integer)", connection: connection) { result, rows in
                                
                                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                
                                executeRawQuery("CREATE TABLE " +  myTable2.tableName + " (c varchar(40), b integer)", connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                    
                                    executeRawQuery("CREATE TABLE " +  myTable3.tableName + " (d varchar(40), b integer)", connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                        
                                        let i1 = Insert(into: myTable1, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                                        executeQuery(query: i1, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "INSERT failed")
                                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                            
                                            let i2 = Insert(into: myTable2, rows: [["apple", 11], ["apricot", 3]])
                                            executeQuery(query: i2, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "INSERT failed")
                                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                
                                                let i3 = Insert(into: myTable3, rows: [["banana", 17], ["apple", 1], ["peach", -7]])
                                                executeQuery(query: i3, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                                    
                                                    let s = Select(myTable1.a, from: myTable1)
                                                        .union(Select(myTable2.c, from: myTable2))
                                                        .unionAll(Select(myTable3.d, from: myTable3)
                                                            .where(myTable3.b > 0))
                                                    executeQuery(query: s, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        let resultSet = result.asResultSet!
                                                        XCTAssertEqual(rows!.count, 5, "SELECT returned wrong number of rows: \(rows!.count) instead of 5")
                                                        XCTAssertEqual(resultSet.titles.count, 1, "SELECT returned wrong number of columns: \(resultSet.titles.count) instead of 1")
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
