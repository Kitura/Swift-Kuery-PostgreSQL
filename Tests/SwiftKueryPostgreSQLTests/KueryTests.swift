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



public class MyTable : Table {
    let a = Column("a")
    let b = Column("b")
    
    public var name = "myTable"
}

class KueryTests: XCTestCase {
    
    static var allTests: [(String, (KueryTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
    
    func testExample() {
        let t = MyTable()
        
        let connection = PostgreSQLConnection(host: "localhost", port: 5432, queryBuilder: QueryBuilder(), options: [.userName("postgres"), .password("kitura")])
        connection.connect() { error in
            XCTAssertNil(error, "Error connecting to PostgreSQL server: \(error)")
            
            let d1 = Delete(from: t)
            print("=======\(connection.descriptionOf(query: d1))=======")
            connection.execute(query: d1) { result in
                XCTAssertEqual(result.success, true, "DELETE failed")
                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                
                KueryTests.printSuccess(result: result)
                
                let s1 = Select(from: t)
                print("=======\(connection.descriptionOf(query: s1))=======")
                connection.execute(query: s1) { result in
                    XCTAssertEqual(result.success, true, "SELECT failed")
                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                    let (_, rows) = result.asRows!
                    XCTAssertEqual(rows.count, 0, "Table not empty after DELETE all")
                    
                    KueryTests.printResultAsRows(result: result)
                    
                    let i1 = Insert(into: t, values: "Apple", 10)
                    print("=======\(connection.descriptionOf(query: i1))=======")
                    connection.execute(query: i1) { result in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                        
                        KueryTests.printSuccess(result: result)
                        
                        let i2 = Insert(into: t, valueTuples: (t.a, "appricot"), (t.b, "3"))
                        print("=======\(connection.descriptionOf(query: i2))=======")
                        connection.execute(query: i2) { result in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            KueryTests.printSuccess(result: result)
                            
                            let i3 = Insert(into: t, columns: [t.a, t.b], values: ["BANANA", 17])
                            print("=======\(connection.descriptionOf(query: i3))=======")
                            connection.execute(query: i3) { result in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                
                                KueryTests.printSuccess(result: result)
                                
                                let i4 = Insert(into: t, rows: [["Apple", 17], ["BANANA", -7], ["BANANA", 27]])
                                print("=======\(connection.descriptionOf(query: i4))=======")
                                connection.execute(query: i4) { result in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                    
                                    KueryTests.printSuccess(result: result)
                                    
                                    print("=======\(connection.descriptionOf(query: s1))=======")
                                    connection.execute(query: s1) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (_, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                        
                                        KueryTests.printResultAsRows(result: result)
                                        
                                        let sd1 = Select.distinct(t.a, from: t)
                                            .where(t.a.like("B%"))
                                        print("=======\(connection.descriptionOf(query: sd1))=======")
                                        connection.execute(query: sd1) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (_, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                            
                                            KueryTests.printResultAsRows(result: result)
                                            
                                            let s3 = Select(t.b, t.a, from: t)
                                                .where(((t.a == "BANANA") || (ucase(t.a) == "APPLE")) && (t.b == 27 || t.b == -7 || t.b == 17))
                                                .order(by: .ASCD(t.b), .DESC(t.a))
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
                                                    XCTAssertEqual(rows[0][0]! as! String, "BANANA", "Wrong column name: \(rows[0][0]) instead of BANANA")
                                                    XCTAssertEqual(rows[1][0]! as! String, "Apple", "Wrong column name: \(rows[1][0]) instead of Apple")
                                                    
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
                                                        XCTAssertEqual(rows[0][0]! as! String, "appricot", "Wrong column name: \(rows[0][0]) instead of appricot")
                                                        XCTAssertEqual(rows[1][0]! as! String, "BANANA", "Wrong column name: \(rows[1][0]) instead of BANANA")
                                                        KueryTests.printResultAsRows(result: result)
                                                        
                                                        let u1 = Update(table: t, set: [(t.a, "peach"), (t.b, 2)])
                                                            .where(t.a == "BANANA")
                                                        print("=======\(connection.descriptionOf(query: u1))=======")
                                                        connection.execute(query: u1) { result in
                                                            XCTAssertEqual(result.success, true, "UPDATE failed")
                                                            XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError)")
                                                            
                                                            KueryTests.printSuccess(result: result)
                                                            
                                                            let s6 = Select(t.a, t.b, from: t)
                                                                .where(t.a == "BANANA")
                                                            print("=======\(connection.descriptionOf(query: s6))=======")
                                                            connection.execute(query: s6) { result in
                                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                let (_, rows) = result.asRows!
                                                                XCTAssertEqual(rows.count, 0, "Result not empty")
                                                                
                                                                let d2 = Delete(from: t)
                                                                    .where(t.b == "2")
                                                                print("=======\(connection.descriptionOf(query: d2))=======")
                                                                connection.execute(query: d2) { result in
                                                                    XCTAssertEqual(result.success, true, "DELETE failed")
                                                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                                    
                                                                    KueryTests.printSuccess(result: result)
                                                                    
                                                                    let s7 = Select(ucase(t.a).as("upper"), t.b, from: t)
                                                                        .where(t.a.between("a", and: "b"))
                                                                    print("=======\(connection.descriptionOf(query: s7))=======")
                                                                    connection.execute(query: s7) { result in
                                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                        let (titles, rows) = result.asRows!
                                                                        XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                                                        XCTAssertEqual(titles[0], "upper", "Wrong column name: \(titles[0]) instead of upper")
                                                                        XCTAssertEqual(rows[0][0]! as! String, "APPRICOT", "Wrong column name: \(rows[0][0]) instead of APPRICOT")
                                                                        
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
                    }
                }
            }
        }
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
                    print((value as! String).padding(toLength: 10, withPad: " ", startingAt: 0), terminator: "")
                }
                print()
            }
        }
        else if let queryError = result.asError {
            print("Error in Select: ", queryError)
        }
    }
}
