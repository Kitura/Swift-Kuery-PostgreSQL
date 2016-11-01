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

import SwiftKuery
import CLibpq

import Foundation

// https://www.postgresql.org/docs/8.0/static/libpq-exec.html
public class PostgreSQLConnection : Connection {
    
    private var connection: OpaquePointer?
    private var connectionParameters: String
    public var queryBuilder: QueryBuilder
    
    public required init(host: String, port: Int32, options: [ConnectionOptions]?) {
        connectionParameters = "host = \(host) port = \(port)"
        if let options = options {
            for option in options {
                switch option {
                case .options(let value):
                    connectionParameters += " options = \(value)"
                case .databaseName(let value):
                    connectionParameters += " dbname = \(value)"
                case .userName(let value):
                    connectionParameters += " user = \(value)"
                case .password(let value):
                    connectionParameters += " password = \(value)"
                case .connectionTimeout(let value):
                    connectionParameters += " connect_timeout = \(value)"
                }
            }
        }
        self.queryBuilder = QueryBuilder()
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.ucase : "UPPER", QueryBuilder.QuerySubstitutionNames.lcase : "LOWER", QueryBuilder.QuerySubstitutionNames.len : "LENGTH", QueryBuilder.QuerySubstitutionNames.numberedParameter : "$", QueryBuilder.QuerySubstitutionNames.namedParameter : ""])
    }
    
    public func descriptionOf(query: Query) throws -> String {
        return try query.build(queryBuilder: queryBuilder)
    }
    
    public func connect(onCompletion: (QueryError?) -> ()) {
        connection = PQconnectdb(connectionParameters)
        
        let error: String? = String(validatingUTF8: PQerrorMessage(connection))
        var queryError: QueryError? = nil
        if error != nil && !error!.isEmpty {
            queryError = QueryError.connection(error!)
        }
        onCompletion(queryError)
    }
    
    public func closeConnection() {
        PQfinish(connection)
        connection = nil
    }
    
    public func execute(query: Query, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        do {
            let postgresQuery = try query.build(queryBuilder: queryBuilder)
            executeQueryWithParameters(query: postgresQuery, parameters: parameters, onCompletion: onCompletion)
        }
        catch QueryError.syntaxError(let error) {
            onCompletion(.error(QueryError.syntaxError(error)))
        }
        catch {
            onCompletion(.error(QueryError.syntaxError("Failed to build the query")))
        }
    }
    
    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        do {
            let postgresQuery = try query.build(queryBuilder: queryBuilder)
            executeQuery(query: postgresQuery, onCompletion: onCompletion)
        }
        catch QueryError.syntaxError(let error) {
            onCompletion(.error(QueryError.syntaxError(error)))
        }
        catch {
            onCompletion(.error(QueryError.syntaxError("Failed to build the query")))
        }
    }
    
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, onCompletion: onCompletion)
    }
    
    public func execute(_ raw: String, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        executeQueryWithParameters(query: raw, parameters: parameters, onCompletion: onCompletion)
    }
    
    private func executeQuery(query: String, onCompletion: @escaping ((QueryResult) -> ())) {
        let queryResult = PQexec(connection, query)
        processQueryResult(queryResult, onCompletion: onCompletion)
    }
    
    private func executeQueryWithParameters(query: String, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        var parameterData = [UnsafePointer<Int8>?]()
        // At the moment we only create string parameters. Binary parameters should be added.
        for parameter in parameters {
            let value = AnyCollection("\(parameter)".utf8CString)
            let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(value.count))
            for (index, byte) in value.enumerated() {
                pointer[index] = byte
            }
            parameterData.append(pointer)
        }
        let queryResult: OpaquePointer? =  parameterData.withUnsafeBufferPointer { buffer in
            return PQexecParams(connection, query, Int32(parameters.count), nil, buffer.isEmpty ? nil : buffer.baseAddress, nil, nil, 0)
        }
        processQueryResult(queryResult, onCompletion: onCompletion)
    }

    private func processQueryResult(_ queryResult: OpaquePointer?, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let result = queryResult else {
            onCompletion(.error(QueryError.noResult("No result returned for the query")))
            return
        }
        
        let status = PQresultStatus(result)
        if status == PGRES_COMMAND_OK {
            onCompletion(.successNoData)
        }
        else if status == PGRES_TUPLES_OK {
            let (titles, rows) = PostgreSQLConnection.getRows(queryResult: result)
            onCompletion(.rows(titles: titles, rows: rows))
        }
        else {
            onCompletion(.error(QueryError.databaseError(String(validatingUTF8: PQresultErrorMessage(result))!)))
        }
    }
    
    private static func getRows(queryResult: OpaquePointer) -> ([String], [[Any?]]) {
        var result = [[Any?]]()
        let rows = PQntuples(queryResult)
        let columns = PQnfields(queryResult)
        var columnNames = [String]()
        
        for column in 0 ..< columns {
            columnNames.append(String(validatingUTF8: PQfname(queryResult, column))!)
        }
        
        for rowIndex in 0 ..< rows {
            var row = [Any?]()
            
            for column in 0 ..< columns {
                if PQgetisnull(queryResult, rowIndex, column) == 1 {
                    row.append(nil)
                }
                else {
                    row.append(PostgreSQLConnection.convert(queryResult, row: rowIndex, column: column))
                }
            }
            result.append(row)
        }
        
        return (columnNames, result)
    }
    
    private static func convert(_ queryResult: OpaquePointer, row: Int32, column: Int32) -> Any {
        let data = Data(bytes: PQgetvalue(queryResult, row, column),
                        count: Int(PQgetlength(queryResult, row, column)))
        
        if PQfformat(queryResult, column) == 0 {
            return String(data: data, encoding: String.Encoding.utf8)
        }
        else {
            return data
        }
    }
}
