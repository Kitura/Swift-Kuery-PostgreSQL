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
import Foundation

@testable import SwiftKueryPostgreSQL

#if os(Linux)
let tableNameSuffix = "Linux"
#else
let tableNameSuffix = "OSX"
#endif

class TestSchema: XCTestCase {
    
    static var allTests: [(String, (TestSchema) -> () throws -> Void)] {
        return [
            ("testCreateTable", testCreateTable),
            ("testForeignKeys", testForeignKeys),
            ("testPrimaryKeys", testPrimaryKeys),
            ("testTypes", testTypes),
        ]
    }
    
    class MyTable: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi", collate: "en_US")
        let b = Column("b", Int32.self, autoIncrement: true)
        let c = Column("c", Double.self, defaultValue: 4.95, check: "c > 0")
        
        let tableName = "MyTable" + tableNameSuffix
    }
    
    class MyNewTable: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self, autoIncrement: true)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)
        
        let tableName = "MyNewTable" + tableNameSuffix
    }
    
    
    func testCreateTable() {
        let t = MyTable()
        let tNew = MyNewTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t.tableName, connection: connection) { result in
                cleanUp(table: tNew.tableName, connection: connection) { result in
                    
                    t.create(connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        let i1 = Insert(into: t, values: "apple", 5)
                        executeQuery(query: i1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                
                                let resultSet = result.asResultSet!
                                XCTAssertEqual(resultSet.titles.count, 3, "SELECT returned wrong number of titles")
                                XCTAssertEqual(resultSet.titles[0], "a", "Wrong column name for column 0")
                                XCTAssertEqual(resultSet.titles[1], "b", "Wrong column name for column 1")
                                XCTAssertEqual(resultSet.titles[2], "c", "Wrong column name for column 2")
                                
                                XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                                XCTAssertEqual(rows![0].count, 3, "SELECT returned wrong number of columns")
                                XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                                XCTAssertEqual(rows![0][1]! as! Int32, 5, "Wrong value in row 0 column 1")
                                XCTAssertEqual(rows![0][2]! as! Double, 4.95, "Wrong value in row 0 column 2")
                                
                                var index = Index("index", on: t, columns: [tNew.a, desc(t.b)])
                                index.create(connection: connection) { result in
                                    XCTAssertEqual(result.success, false, "CREATE INDEX should fail")
                                    XCTAssertNotNil(result.asError, "CREATE INDEX should return an error")
                                    XCTAssertEqual("\(result.asError!)", "Index contains columns that do not belong to its table.")
                                    
                                    index = Index("index", on: t, columns: [t.a, desc(t.b)])
                                    index.create(connection: connection) { result in
                                        XCTAssertEqual(result.success, true, "CREATE INDEX failed")
                                        XCTAssertNil(result.asError, "Error in CREATE INDEX: \(result.asError!)")
                                        
                                        index.drop(connection: connection) { result in
                                            XCTAssertEqual(result.success, true, "DROP INDEX failed")
                                            XCTAssertNil(result.asError, "Error in DROP INDEX: \(result.asError!)")
                                            
                                            let migration = Migration(from: t, to: tNew, using: connection)
                                            migration.alterTableName() { result in
                                                XCTAssertEqual(result.success, true, "Migration failed")
                                                XCTAssertNil(result.asError, "Error in Migration: \(result.asError!)")
                                                
                                                migration.alterTableAdd(column: tNew.d) { result in
                                                    XCTAssertEqual(result.success, true, "Migration failed")
                                                    XCTAssertNil(result.asError, "Error in Migration: \(result.asError!)")
                                                    
                                                    let s2 = Select(from: tNew)
                                                    executeQuery(query: s2, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                                        
                                                        let resultSet = result.asResultSet!
                                                        XCTAssertEqual(resultSet.titles.count, 4, "SELECT returned wrong number of titles")
                                                        XCTAssertEqual(resultSet.titles[0], "a", "Wrong column name for column 0")
                                                        XCTAssertEqual(resultSet.titles[1], "b", "Wrong column name for column 1")
                                                        XCTAssertEqual(resultSet.titles[2], "c", "Wrong column name for column 2")
                                                        XCTAssertEqual(resultSet.titles[3], "d", "Wrong column name for column 3")
                                                        
                                                        XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                                                        XCTAssertEqual(rows![0].count, 4, "SELECT returned wrong number of columns")
                                                        XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                                                        XCTAssertEqual(rows![0][1]! as! Int32, 5, "Wrong value in row 0 column 1")
                                                        XCTAssertEqual(rows![0][2]! as! Double, 4.95, "Wrong value in row 0 column 2")
                                                        XCTAssertEqual(rows![0][3]! as! Int32, 123, "Wrong value in row 0 column 3")
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
            semaphore.wait()
            expectation.fulfill()
        })
    }
    
    class Table1: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self, primaryKey: true)
        let c = Column("c", Double.self, defaultValue: 4.95)
        
        let tableName = "Table1" + tableNameSuffix
    }
    
    class Table2: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)
        
        let tableName = "Table2" + tableNameSuffix
    }
    
    class Table3: Table {
        let a = Column("a", String.self, defaultValue: "qiwi")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)
        
        let tableName = "Table3" + tableNameSuffix
    }
    
    
    func testPrimaryKeys() {
        let t1 = Table1()
        let t2 = Table2()
        let t3 = Table3()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t1.tableName, connection: connection) { result in
                cleanUp(table: t2.tableName, connection: connection) { result in
                    cleanUp(table: t3.tableName, connection: connection) { result in
                        
                        t1.create(connection: connection) { result in
                            XCTAssertEqual(result.success, false, "CREATE TABLE with conflicting primary keys didn't fail")
                            XCTAssertEqual("\(result.asError!)", "Conflicting definitions of primary key. ", "Wrong error")
                            
                            t2.primaryKey(t2.c, t2.d).create(connection: connection) { result in
                                XCTAssertEqual(result.success, false, "CREATE TABLE with conflicting primary keys didn't fail")
                                XCTAssertEqual("\(result.asError!)", "Conflicting definitions of primary key. ", "Wrong error")
                                
                                t3.primaryKey(t3.c, t3.d).create(connection: connection) { result in
                                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                    semaphore.signal()
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
    
    
    class Table4: Table {
        let a = Column("a", String.self)
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self)
        
        let tableName = "Table4" + tableNameSuffix
    }
    
    class Table5: Table {
        let e = Column("e", String.self, primaryKey: true)
        let f = Column("f", Int32.self)
        
        let tableName = "Table5" + tableNameSuffix
    }
    
    func testForeignKeys() {
        let t4 = Table4()
        let t5 = Table5()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t5.tableName, connection: connection) { result in
                cleanUp(table: t4.tableName, connection: connection) { result in
                    
                    t4.primaryKey(t4.a, t4.b).create(connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        t5.foreignKey([t5.e, t5.f], references: [t4.a, t4.b]).create(connection: connection) { result in
                            XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                            XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                            semaphore.signal()
                        }
                    }
                }
            }
            semaphore.wait()
            expectation.fulfill()
        })
    }
    
    public struct TimestampWithTimezone: SQLDataType {
        /// Return database specific description of the type using `QueryBuilder`.
        ///
        /// - Parameter queryBuilder: The QueryBuilder to use.
        /// - Returns: A String representation of the type.
        public static func create(queryBuilder: QueryBuilder) -> String {
            return "timestamp with time zone"
        }
    }
    
    class TypesTable: Table {
        let a = Column("a", Varchar.self, length: 30, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Varchar.self, length: 10)
        let c = Column("c", Char.self, length: 10)
        
        let d = Column("d", Int16.self)
        let e = Column("e", Int32.self)
        let f = Column("f", Int64.self)
        
        let g = Column("g", Float.self)
        let h = Column("h", Double.self)
        
        let i = Column("i", SQLDate.self)
        let j = Column("j", Time.self)
        let k = Column("k", Timestamp.self)
        
        let l = Column("l", TimestampWithTimezone.self)
        
        let tableName = "TypesTable" + tableNameSuffix
    }
    
    
    func testTypes() {
        let t = TypesTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                t.create(connection: connection) { result in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let now = Date()
                    
                    let thenString = "2017-03-19 11:15:15 +0800"
                    let dateFormatter = DateFormatter()
                    dateFormatter.dateFormat = "yyyy-MM-dd hh:mm:ss Z"
                    dateFormatter.timeZone = TimeZone(abbreviation: "UTC")
                    let then: Date = dateFormatter.date(from: thenString)!
                    
                    let i1 = Insert(into: t, values: "apple", "passion fruit", "peach", 123456789, 123456789, 123456789, -0.53, 123.4567, now, now, now, then)
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, false, "INSERT should fail")
                        XCTAssertNotNil(result.asError, "No error in INSERT of too long value into varchar column.")
                        
                        let i2 = Insert(into: t, values: "apple", "banana", "peach", 123456789, 123456789, 123456789, -0.53, 123.4567, now, now, now, then)
                        executeQuery(query: i2, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, false, "INSERT should fail")
                            XCTAssertNotNil(result.asError, "No error in INSERT of too long value into smallint column.")
                            
                            let i3 = Insert(into: t, values: "apple", "banana", "peach", 1234, 123456789, 123456789, -0.53, 123.4567, now, now, now, then)
                            executeQuery(query: i3, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                                
                                let s1 = Select(from: t)
                                executeQuery(query: s1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                    
                                    XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                                    XCTAssertEqual(rows![0].count, 12, "SELECT returned wrong number of columns")
                                    XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                                    XCTAssertEqual(rows![0][1]! as! String, "banana", "Wrong value in row 0 column 1")
                                    XCTAssertEqual(rows![0][2]! as! String, "peach     ", "Wrong value in row 0 column 2")
                                    XCTAssertEqual(rows![0][3]! as! Int16, 1234, "Wrong value in row 0 column 3")
                                    XCTAssertEqual(rows![0][4]! as! Int32, 123456789, "Wrong value in row 0 column 4")
                                    XCTAssertEqual(rows![0][5]! as! Int64, 123456789, "Wrong value in row 0 column 5")
                                    XCTAssertEqual(rows![0][6]! as! Float, -0.53, "Wrong value in row 0 column 6")
                                    XCTAssertEqual(rows![0][7]! as! Double, 123.4567, "Wrong value in row 0 column 7")
                                    
                                    let date = extractDate(from: rows![0][8]! as! Date)
                                    let nowDate = extractDate(from: now)
                                    XCTAssertEqual(date, nowDate, "Wrong value in row 0 column 8")
                                    
                                    let time = extractTime(from: rows![0][9]! as! Date)
                                    let nowTime = extractTime(from: now)
                                    XCTAssertEqual(time, nowTime, "Wrong value in row 0 column 9")
                                    
                                    var timestamp = rows![0][10]! as! Date
                                    let nowTimeInterval = Int(now.timeIntervalSince1970)
                                    XCTAssertEqual(Int(timestamp.timeIntervalSince1970), nowTimeInterval, "Wrong value in row 0 column 10")
                                    
                                    timestamp = rows![0][11]! as! Date
                                    let thenTimeInterval = Int(then.timeIntervalSince1970)
                                    XCTAssertEqual(Int(timestamp.timeIntervalSince1970), thenTimeInterval, "Wrong value in row 0 column 11")
                                    semaphore.signal()
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
