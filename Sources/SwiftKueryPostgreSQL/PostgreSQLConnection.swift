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

// MARK: PostgreSQLConnection

/// An implementation of `SwiftKuery.Connection` protocol for PostgreSQL.
/// Please see [PostgreSQL manual](https://www.postgresql.org/docs/8.0/static/libpq-exec.html) for details.
public class PostgreSQLConnection: Connection {
    
    private var connection: OpaquePointer?
    private var connectionParameters: String = ""
    
    /// The `QueryBuilder` with PostgreSQL specific substitutions.
    public var queryBuilder: QueryBuilder
    
    /// Initialize an instance of PostgreSQLConnection.
    ///
    /// - Parameter host: The host of the PostgreSQL server to connect to.
    /// - Parameter port: The port of the PostgreSQL server to connect to.
    /// - Parameter options: A set of `ConnectionOptions` to pass to the PostgreSQL server.
    public init(host: String, port: Int32, options: [ConnectionOptions]?) {
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
        queryBuilder = PostgreSQLConnection.createQuryBuilder()
    }
    
    /// Initialize an instance of PostgreSQLConnection.
    ///
    /// - Parameter url: A URL of the following form: Postgres://userid:pwd@host:port/db.
    public init(url: URL) {
        if let scheme = url.scheme, scheme == "Postgres", let host = url.host, let port = url.port {
            connectionParameters = "host = \(host) port = \(port)"
            if let user = url.user {
               connectionParameters += " user = \(user)"
            }
            if let password = url.password {
                connectionParameters += " password = \(password)"
            }
            if !url.lastPathComponent.isEmpty {
                connectionParameters += " dbname = \(url.lastPathComponent)"
            }
        }
        queryBuilder = PostgreSQLConnection.createQuryBuilder()
    }
    
    private static func createQuryBuilder() -> QueryBuilder {
        let queryBuilder = QueryBuilder(withDeleteRequiresUsing: true, withUpdateRequiresFrom: true)
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.ucase : "UPPER",
                                          QueryBuilder.QuerySubstitutionNames.lcase : "LOWER",
                                          QueryBuilder.QuerySubstitutionNames.len : "LENGTH",
                                          QueryBuilder.QuerySubstitutionNames.numberedParameter : "$",
                                          QueryBuilder.QuerySubstitutionNames.namedParameter : ""])
        return queryBuilder
    }
    
    /// Return a String representation of the query.
    ///
    /// - Parameter query: The query.
    /// - Returns: A String representation of the query.
    /// - Throws: QueryError.syntaxError if query build fails.
    public func descriptionOf(query: Query) throws -> String {
        return try query.build(queryBuilder: queryBuilder)
    }
    
    /// Establish a connection with the database.
    ///
    /// - Parameter onCompletion: The function to be called when the connection is established.
    public func connect(onCompletion: (QueryError?) -> ()) {
        if connectionParameters == "" {
            onCompletion(QueryError.connection("No connection parameters."))
        }
        connection = PQconnectdb(connectionParameters)
        
        let queryError: QueryError?
        if let error = String(validatingUTF8: PQerrorMessage(connection)), !error.isEmpty {
            queryError = QueryError.connection(error)
        }
        else {
            queryError = nil
        }
        onCompletion(queryError)
    }
    
    /// Close the connection to the database.
    public func closeConnection() {
        PQfinish(connection)
        connection = nil
    }
    
    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
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
    
    /// Execute a query.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
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
    
    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, onCompletion: onCompletion)
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        executeQueryWithParameters(query: raw, parameters: parameters, onCompletion: onCompletion)
    }
    
    private func executeQuery(query: String, onCompletion: @escaping ((QueryResult) -> ())) {
        PQsendQuery(connection, query)
        PQsetSingleRowMode(connection)
        processQueryResult(query: query, onCompletion: onCompletion)
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
        _ = parameterData.withUnsafeBufferPointer { buffer in
            PQsendQueryParams(connection, query, Int32(parameters.count), nil, buffer.isEmpty ? nil : buffer.baseAddress, nil, nil, 0)
        }
        PQsetSingleRowMode(connection)
        processQueryResult(query: query, onCompletion: onCompletion)
    }

    private func processQueryResult(query: String, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let result = PQgetResult(connection) else {
            let error = String(validatingUTF8: PQerrorMessage(connection))
            onCompletion(.error(QueryError.noResult("No result returned for query: \(query). Error: \(error).")))
            return
        }
        
        let status = PQresultStatus(result)
        if status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK {
            // Since we set the single row mode, PGRES_TUPLES_OK means the result is empty, i.e. there are 
            // no rows to return.
            clearResult(connection: connection)
            onCompletion(.successNoData)
        }
        else if status == PGRES_SINGLE_TUPLE {
            let resultFetcher = PostgreSQLResultFetcher(queryResult: result, connection: connection)
            onCompletion(.resultSet(ResultSet(resultFetcher)))
        }
        else {
            clearResult(connection: connection)
            onCompletion(.error(QueryError.databaseError("Query execution error:\n" + String(validatingUTF8: PQresultErrorMessage(result))! + "For query: " + query)))
        }
    }
    
    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters are not supported in PostgreSQL")))
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters are not supported in PostgreSQL")))
    }
}
