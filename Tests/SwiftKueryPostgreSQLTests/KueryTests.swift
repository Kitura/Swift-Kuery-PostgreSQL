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
let tableNameSelect = "tableSelectLinux"
let tableNameInsert = "tableInsertLinux"
let tableNameUpdate = "tableUpdateLinux"
let tableNameAlias = "tableAliasLinux"
let table1NameJoin = "table1JoinLinux"
let table2NameJoin = "table2JoinLinux"
#else
let tableNameSelect = "tableSelectOSX"
let tableNameInsert = "tableInsertOSX"
let tableNameUpdate = "tableUpdateOSX"
let tableNameAlias = "tableAliasOSX"
let table1NameJoin = "table1JoinOSX"
let table2NameJoin = "table2JoinOSX"
#endif

class KueryTests: XCTestCase {
    
    static var allTests: [(String, (KueryTests) -> () throws -> Void)] {
        return [
            ("testInsert", testInsert),
            ("testSelect", testSelect),
            ("testUpdateAndDelete", testUpdateAndDelete),
            ("testAlias", testAlias),
            ("testJoin", testJoin),
        ]
    }
    
    let host = read(fileName: "host.txt")
    let port = Int32(read(fileName: "port.txt"))!
    let username = read(fileName: "username.txt")
    let password = read(fileName: "password.txt")
    
    
    public class MyTableInsert : Table {
        let a = Column("a")
        let b = Column("b")
        
        let name = tableNameInsert
    }
    
    func testInsert() {
        let t = MyTableInsert()
        
        let connection = PostgreSQLConnection(host: host, port: port, options: [.userName(username), .password(password)])
        performTest(asyncTasks: { expectation in
            
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                connection.execute("DROP TABLE " + tableNameInsert) { result in
                    
                    print("=======CREATE TABLE \(tableNameInsert) (a varchar(40), b integer)=======")
                    connection.execute("CREATE TABLE \(tableNameInsert) (a varchar(40), b integer)") { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                        
                        KueryTests.printSuccess(result: result)
                        
                        let i1 = Insert(into: t, values: "apple", 10)
                        print("=======\(connection.descriptionOf(query: i1))=======")
                        connection.execute(query: i1) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            KueryTests.printSuccess(result: result)
                            
                            let i2 = Insert(into: t, valueTuples: (t.a, "apricot"), (t.b, "3"))
                            print("=======\(connection.descriptionOf(query: i2))=======")
                            connection.execute(query: i2) { result in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                
                                KueryTests.printSuccess(result: result)
                                
                                let i3 = Insert(into: t, columns: [t.a, t.b], values: ["banana", 17])
                                print("=======\(connection.descriptionOf(query: i3))=======")
                                connection.execute(query: i3) { result in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                    
                                    KueryTests.printSuccess(result: result)
                                    
                                    let i4 = Insert(into: t, rows: [["apple", 17], ["banana", -7], ["banana", 27]])
                                    print("=======\(connection.descriptionOf(query: i4))=======")
                                    connection.execute(query: i4) { result in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                        
                                        KueryTests.printSuccess(result: result)
                                        
                                        let s1 = Select(from: t)
                                        print("=======\(connection.descriptionOf(query: s1))=======")
                                        connection.execute(query: s1) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (_, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                            
                                            KueryTests.printResultAsRows(result: result)
                                            
                                            let drop = Raw(query: "DROP TABLE", table: t)
                                            print("=======\(connection.descriptionOf(query: drop))=======")
                                            drop.execute(connection) { result in
                                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                
                                                KueryTests.printSuccess(result: result)
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
    
    public class MyTableSelect : Table {
        let a = Column("a")
        let b = Column("b")
        
        let name = tableNameSelect
    }
    
    func testSelect() {
        let t = MyTableSelect()
        
        let connection = PostgreSQLConnection(host: host, port: port, options: [.userName(username), .password(password)])
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                connection.execute("DROP TABLE " + tableNameSelect) { result in
                    
                    print("=======CREATE TABLE \(tableNameSelect) (a varchar(40), b integer)=======")
                    connection.execute("CREATE TABLE \(tableNameSelect) (a varchar(40), b integer)") { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                        
                        KueryTests.printSuccess(result: result)
                        
                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        print("=======\(connection.descriptionOf(query: i1))=======")
                        connection.execute(query: i1) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            KueryTests.printSuccess(result: result)
                            
                            let s1 = Select(from: t)
                            print("=======\(connection.descriptionOf(query: s1))=======")
                            connection.execute(query: s1) { result in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                let (_, rows) = result.asRows!
                                XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                
                                KueryTests.printResultAsRows(result: result)
                                
                                let sd1 = Select.distinct(t.a, from: t)
                                    .where(t.a.like("b%"))
                                print("=======\(connection.descriptionOf(query: sd1))=======")
                                connection.execute(query: sd1) { result in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                    let (_, rows) = result.asRows!
                                    XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                    
                                    KueryTests.printResultAsRows(result: result)
                                    
                                    let s3 = Select(t.b, t.a, from: t)
                                        .where(((t.a == "banana") || (ucase(t.a) == "APPLE")) && (t.b == 27 || t.b == -7 || t.b == 17))
                                        .order(by: .ASC(t.b), .DESC(t.a))
                                    print("=======\(connection.descriptionOf(query: s3))=======")
                                    connection.execute(query: s3) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (titles, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 4, "SELECT returned wrong number of rows: \(rows.count) instead of 4")
                                        XCTAssertEqual(titles[0], "b", "Wrong column name: \(titles[0]) instead of b")
                                        XCTAssertEqual(titles[1], "a", "Wrong column name: \(titles[1]) instead of a")
                                        
                                        KueryTests.printResultAsRows(result: result)
                                        
                                        let s4 = Select(t.a, from: t)
                                            .where(t.b >= 0)
                                            .group(by: t.a)
                                            .order(by: .DESC(t.a))
                                            .having(sum(t.b) > 3)
                                        print("=======\(connection.descriptionOf(query: s4))=======")
                                        connection.execute(query: s4) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (titles, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                            XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of a")
                                            XCTAssertEqual(rows[0][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[0][0]) instead of banana")
                                            XCTAssertEqual(rows[1][0]! as! String, "apple", "Wrong value in row 1 column 0: \(rows[1][0]) instead of apple")
                                            
                                            KueryTests.printResultAsRows(result: result)
                                            
                                            let s4Raw = Select(RawField("left(a, 2) as raw"), from: t)
                                                .where("b >= 0")
                                                .group(by: t.a)
                                                .order(by: .DESC(t.a))
                                                .having("sum(b) > 3")
                                            print("=======\(connection.descriptionOf(query: s4Raw))=======")
                                            connection.execute(query: s4Raw) { result in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                let (titles, rows) = result.asRows!
                                                XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                XCTAssertEqual(titles[0], "raw", "Wrong column name: \(titles[0]) instead of raw")
                                                XCTAssertEqual(rows[0][0]! as! String, "ba", "Wrong value in row 0 column 0: \(rows[0][0]) instead of ba")
                                                XCTAssertEqual(rows[1][0]! as! String, "ap", "Wrong value in row 1 column 0: \(rows[1][0]) instead of ap")
                                                
                                                KueryTests.printResultAsRows(result: result)
                                                
                                                let s5 = Select(t.a, t.b, from: t)
                                                    .limit(to: 2)
                                                    .order(by: .DESC(t.a))
                                                print("=======\(connection.descriptionOf(query: s5))=======")
                                                connection.execute(query: s5) { result in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                    let (_, rows) = result.asRows!
                                                    XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                    XCTAssertEqual(rows[0][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[0][0]) instead of banana")
                                                    XCTAssertEqual(rows[1][0]! as! String, "banana", "Wrong value in row 1 column 0: \(rows[1][0]) instead of banana")
                                                    KueryTests.printResultAsRows(result: result)
                                                    
                                                    let s6 = Select(ucase(t.a).as("case"), t.b, from: t)
                                                        .where(t.a.between("apra", and: "aprt"))
                                                    print("=======\(connection.descriptionOf(query: s6))=======")
                                                    connection.execute(query: s6) { result in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                        let (titles, rows) = result.asRows!
                                                        XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                                        XCTAssertEqual(titles[0], "case", "Wrong column name: \(titles[0]) instead of 'case'")
                                                        XCTAssertEqual(rows[0][0]! as! String, "APRICOT", "Wrong value in row 0 column 0: \(rows[0][0]) instead of APRICOT")
                                                        
                                                        KueryTests.printResultAsRows(result: result)
                                                        
                                                        let s7 = Select(from: t)
                                                            .where(t.a.in("apple", "lalala"))
                                                        print("=======\(connection.descriptionOf(query: s7))=======")
                                                        connection.execute(query: s7) { result in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                            let (_, rows) = result.asRows!
                                                            XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                            XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                            
                                                            KueryTests.printResultAsRows(result: result)
                                                            
                                                            let s8 = Select(from: t)
                                                                .where("a IN ('apple', 'lalala')")
                                                            print("=======\(connection.descriptionOf(query: s8))=======")
                                                            connection.execute(query: s8) { result in
                                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                let (_, rows) = result.asRows!
                                                                XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                                XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                                
                                                                KueryTests.printResultAsRows(result: result)
                                                                
                                                                let s9 = "Select * from \(tableNameSelect) where a IN ('apple', 'lalala')"
                                                                print("=======\(s9)=======")
                                                                connection.execute(s9) { result in
                                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                    let (_, rows) = result.asRows!
                                                                    XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                                    XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                                    
                                                                    KueryTests.printResultAsRows(result: result)
                                                                    
                                                                    let drop = Raw(query: "DROP TABLE", table: t)
                                                                    print("=======\(connection.descriptionOf(query: drop))=======")
                                                                    drop.execute(connection) { result in
                                                                        XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                                        
                                                                        KueryTests.printSuccess(result: result)
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
    
    public class MyTableUpdate : Table {
        let a = Column("a")
        let b = Column("b")
        
        let name = tableNameUpdate
    }
    
    func testUpdateAndDelete () {
        let t = MyTableUpdate()
        
        let connection = PostgreSQLConnection(host: host, port: port, options: [.userName(username), .password(password)])
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                connection.execute("DROP TABLE " + tableNameUpdate) { result in
                    
                    print("=======CREATE TABLE \(tableNameUpdate) (a varchar(40), b integer)=======")
                    connection.execute("CREATE TABLE \(tableNameUpdate) (a varchar(40), b integer)") { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                        
                        KueryTests.printSuccess(result: result)
                        
                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        print("=======\(connection.descriptionOf(query: i1))=======")
                        connection.execute(query: i1) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            KueryTests.printSuccess(result: result)
                            
                            let s1 = Select(from: t)
                            print("=======\(connection.descriptionOf(query: s1))=======")
                            connection.execute(query: s1) { result in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                let (_, rows) = result.asRows!
                                XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                
                                KueryTests.printResultAsRows(result: result)
                                
                                let u1 = Update(t, set: [(t.a, "peach"), (t.b, 2)])
                                    .where(t.a == "banana")
                                print("=======\(connection.descriptionOf(query: u1))=======")
                                connection.execute(query: u1) { result in
                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError)")
                                    
                                    KueryTests.printSuccess(result: result)
                                    
                                    let s2 = Select(t.a, t.b, from: t)
                                        .where(t.a == "banana")
                                    print("=======\(connection.descriptionOf(query: s2))=======")
                                    connection.execute(query: s2) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (_, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 0, "Result not empty")
                                        
                                        let d1 = Delete(from: t)
                                            .where(t.b == "2")
                                        print("=======\(connection.descriptionOf(query: d1))=======")
                                        connection.execute(query: d1) { result in
                                            XCTAssertEqual(result.success, true, "DELETE failed")
                                            XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                            
                                            KueryTests.printSuccess(result: result)
                                            
                                            print("=======\(connection.descriptionOf(query: s1))=======")
                                            connection.execute(query: s1) { result in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                let (_, rows) = result.asRows!
                                                XCTAssertEqual(rows.count, 3, "SELECT returned wrong number of rows: \(rows.count) instead of 3")
                                                
                                                KueryTests.printResultAsRows(result: result)
                                                
                                                let d2 = Delete(from: t)
                                                print("=======\(connection.descriptionOf(query: d2))=======")
                                                connection.execute(query: d2) { result in
                                                    XCTAssertEqual(result.success, true, "DELETE failed")
                                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                    
                                                    KueryTests.printSuccess(result: result)
                                                    
                                                    let s1 = Select(from: t)
                                                    print("=======\(connection.descriptionOf(query: s1))=======")
                                                    s1.execute(connection) { result in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                        let (_, rows) = result.asRows!
                                                        XCTAssertEqual(rows.count, 0, "Table not empty after DELETE all")
                                                        
                                                        KueryTests.printResultAsRows(result: result)
                                                        
                                                        let drop = Raw(query: "DROP TABLE", table: t)
                                                        print("=======\(connection.descriptionOf(query: drop))=======")
                                                        drop.execute(connection) { result in
                                                            XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                                            XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                            
                                                            KueryTests.printSuccess(result: result)
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
    
    public class MyTableAlias : Table {
        let a = Column("a")
        let b = Column("b")
        
        let name = tableNameAlias
    }
    
    func testAlias() {
        let t = MyTableAlias()
        
        let connection = PostgreSQLConnection(host: host, port: port, options: [.userName(username), .password(password)])
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                connection.execute("DROP TABLE " + tableNameAlias) { result in
                    
                    print("=======CREATE TABLE \(tableNameAlias) (a varchar(40), b integer)=======")
                    connection.execute("CREATE TABLE \(tableNameAlias) (a varchar(40), b integer)") { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                        
                        KueryTests.printSuccess(result: result)
                        
                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        print("=======\(connection.descriptionOf(query: i1))=======")
                        connection.execute(query: i1) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            KueryTests.printSuccess(result: result)
                            
                            let s1 = Select(t.a.as("fruit"), t.b.as("number"), from: t)
                            print("=======\(connection.descriptionOf(query: s1))=======")
                            connection.execute(query: s1) { result in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                let (titles, rows) = result.asRows!
                                XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                XCTAssertEqual(titles[0], "fruit", "Wrong column name: \(titles[0]) instead of 'fruit'")
                                XCTAssertEqual(titles[1], "number", "Wrong column name: \(titles[1]) instead of 'number'")
                                
                                KueryTests.printResultAsRows(result: result)
                                
                                let s2 = Select(from: t.as("new"))
                                print("=======\(connection.descriptionOf(query: s2))=======")
                                connection.execute(query: s2) { result in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                    let (titles, rows) = result.asRows!
                                    XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                    XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
                                    XCTAssertEqual(titles[1], "b", "Wrong column name: \(titles[1]) instead of 'b'")
                                    
                                    KueryTests.printResultAsRows(result: result)
                                    
                                    let t2 = t.as("t2")
                                    let s3 = Select(t2.a, from: t2)
                                    print("=======\(connection.descriptionOf(query: s3))=======")
                                    connection.execute(query: s3) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (titles, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                        XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
                                        
                                        KueryTests.printResultAsRows(result: result)
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
    
    
    public class MyTable1Join : Table {
        let a = Column("a")
        let b = Column("b")
        
        let name = table1NameJoin
    }
    
    public class MyTable2Join : Table {
        let c = Column("c")
        let b = Column("b")
        
        let name = table2NameJoin
    }
    
    
    func testJoin() {
        let myTable1 = MyTable1Join()
        let myTable2 = MyTable2Join()
        
        let connection = PostgreSQLConnection(host: host, port: port, options: [.userName(username), .password(password)])
        performTest(asyncTasks: { expectation in
            connection.connect() { error in
                XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
                
                connection.execute("DROP TABLE " + table1NameJoin) { result in
                    
                    connection.execute("DROP TABLE " + table2NameJoin) { result in
                        
                        print("=======CREATE TABLE \(table1NameJoin) (a varchar(40), b integer)=======")
                        connection.execute("CREATE TABLE \(table1NameJoin) (a varchar(40), b integer)") { result in
                            XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                            XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                            
                            KueryTests.printSuccess(result: result)
                            
                            print("=======CREATE TABLE \(table2NameJoin) (c varchar(40), b integer)=======")
                            connection.execute("CREATE TABLE \(table2NameJoin) (c varchar(40), b integer)") { result in
                                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                                
                                KueryTests.printSuccess(result: result)
                                
                                let i1 = Insert(into: myTable1, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                                print("=======\(connection.descriptionOf(query: i1))=======")
                                connection.execute(query: i1) { result in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                    
                                    KueryTests.printSuccess(result: result)
                                    
                                    let i2 = Insert(into: myTable2, rows: [["apple", 11], ["apricot", 3], ["banana", 17], ["apple", 1], ["peach", -7]])
                                    print("=======\(connection.descriptionOf(query: i2))=======")
                                    connection.execute(query: i2) { result in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                        
                                        KueryTests.printSuccess(result: result)
                                        
                                        let s1 = Select(from: myTable1)
                                            .join(myTable2)
                                            .on(myTable1.b == myTable2.b)
                                        print("=======\(connection.descriptionOf(query: s1))=======")
                                        connection.execute(query: s1) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (titles, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 4, "SELECT returned wrong number of rows: \(rows.count) instead of 4")
                                            XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of 'a'")
                                            XCTAssertEqual(titles[1], "b", "Wrong column name: \(titles[1]) instead of 'b'")
                                            XCTAssertEqual(titles[2], "c", "Wrong column name: \(titles[2]) instead of 'c'")
                                            XCTAssertEqual(titles[3], "b", "Wrong column name: \(titles[3]) instead of 'b'")
                                            XCTAssertEqual(rows[0][0]! as! String, "apricot", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apricot")
                                            XCTAssertEqual(rows[1][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[1][0]) instead of banana")
                                            XCTAssertEqual(rows[2][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[2][0]) instead of apple")
                                            XCTAssertEqual(rows[3][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[3][0]) instead of banana")

                                            KueryTests.printResultAsRows(result: result)
                                            
                                            let t1 = myTable1.as("t1")
                                            let t2 = myTable2.as("t2")
                                            let s2 = Select(from: t1)
                                                .join(t2)
                                                .on(t1.b == t2.b)
                                            print("=======\(connection.descriptionOf(query: s2))=======")
                                            connection.execute(query: s2) { result in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                let (_, rows) = result.asRows!
                                                XCTAssertEqual(rows.count, 4, "SELECT returned wrong number of rows: \(rows.count) instead of 4")
                                                XCTAssertEqual(rows[0][0]! as! String, "apricot", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apricot")
                                                XCTAssertEqual(rows[1][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[1][0]) instead of banana")
                                                XCTAssertEqual(rows[2][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[2][0]) instead of apple")
                                                XCTAssertEqual(rows[3][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[3][0]) instead of banana")
                                                
                                                KueryTests.printResultAsRows(result: result)
                                                
                                                let s3 = Select(from: myTable1)
                                                    .leftJoin(myTable2)
                                                    .on(myTable1.a == myTable2.c)
                                                print("=======\(connection.descriptionOf(query: s3))=======")
                                                connection.execute(query: s3) { result in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                    let (_, rows) = result.asRows!
                                                    XCTAssertEqual(rows.count, 8, "SELECT returned wrong number of rows: \(rows.count) instead of 8")
                                                    
                                                    KueryTests.printResultAsRows(result: result)
                                                    
                                                    let s4 = Select(from: t1)
                                                        .fullJoin(t2)
                                                        .using(t1.b)
                                                    print("=======\(connection.descriptionOf(query: s4))=======")
                                                    connection.execute(query: s4) { result in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                        let (titles, rows) = result.asRows!
                                                        XCTAssertEqual(rows.count, 8, "SELECT returned wrong number of rows: \(rows.count) instead of 8")
                                                        XCTAssertEqual(titles.count, 3, "SELECT returned wrong number of columns: \(titles.count) instead of 3")
                                                        
                                                        KueryTests.printResultAsRows(result: result)
                                                        
                                                        let s5 = Select(from: t1)
                                                            .naturalJoin(t2)
                                                        print("=======\(connection.descriptionOf(query: s5))=======")
                                                        connection.execute(query: s5) { result in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                            let (titles, rows) = result.asRows!
                                                            XCTAssertEqual(rows.count, 4, "SELECT returned wrong number of rows: \(rows.count) instead of 4")
                                                            XCTAssertEqual(titles.count, 3, "SELECT returned wrong number of columns: \(titles.count) instead of 3")
                                                            
                                                            KueryTests.printResultAsRows(result: result)
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
    
    
    static func printSuccess(result: QueryResult) {
        if result.success  {
            print("Success")
        }
        else if let queryError = result.asError {
            print("Error in Select: ", queryError)
        }
    }
    
    static func printResultAsRows(result: QueryResult) {
        if let (titles, rows) = result.asRows {
            for title in titles {
                print(title.padding(toLength: 10, withPad: " ", startingAt: 0), terminator: "")
            }
            print()
            for row in rows {
                for value in row {
                    var valueToPrint = ""
                    if value != nil {
                        valueToPrint = value as! String
                    }
                    print(valueToPrint.padding(toLength: 10, withPad: " ", startingAt: 0), terminator: "")
                }
                print()
            }
        }
        else if let queryError = result.asError {
            print("Error in Select: ", queryError)
        }
    }
}
