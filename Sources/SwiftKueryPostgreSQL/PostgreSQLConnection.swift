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

import SwiftKuery
import CLibpq

import Foundation

// MARK: PostgreSQLConnection

/// An implementation of `SwiftKuery.Connection` protocol for PostgreSQL.
/// Please see [PostgreSQL manual](https://www.postgresql.org/docs/8.0/static/libpq-exec.html) for details.
public class PostgreSQLConnection: Connection {
    
    private var connection: OpaquePointer?
    private var connectionParameters: String = ""
    private var inTransaction = false
    
    private var preparedStatements = Set<String>()
    
    /// An indication whether there is a connection to the database.
    public var isConnected: Bool {
        return connection != nil
    }
    
    /// The `QueryBuilder` with PostgreSQL specific substitutions.
    public var queryBuilder: QueryBuilder
    
    init(connectionParameters: String) {
        self.connectionParameters = connectionParameters
        queryBuilder = PostgreSQLConnection.createQueryBuilder()
    }
    
    /// Initialize an instance of PostgreSQLConnection.
    ///
    /// - Parameter host: The host of the PostgreSQL server to connect to.
    /// - Parameter port: The port of the PostgreSQL server to connect to.
    /// - Parameter options: A set of `ConnectionOptions` to pass to the PostgreSQL server.
    public convenience init(host: String, port: Int32, options: [ConnectionOptions]?) {
        self.init(connectionParameters: PostgreSQLConnection.extractConnectionParameters(host: host, port: port, options: options))
    }
    
    /// Initialize an instance of PostgreSQLConnection.
    ///
    /// - Parameter url: A URL of the following form: Postgres://userid:pwd@host:port/db.
    public convenience init(url: URL) {
        self.init(connectionParameters: PostgreSQLConnection.extractConnectionParameters(url: url))
    }

    private static func extractConnectionParameters(host: String, port: Int32, options: [ConnectionOptions]?) -> String {
        var result = "host = \(host) port = \(port)"
        if let options = options {
            for option in options {
                switch option {
                case .options(let value):
                    result += " options = \(value)"
                case .databaseName(let value):
                    result += " dbname = \(value)"
                case .userName(let value):
                    result += " user = \(value)"
                case .password(let value):
                    result += " password = \(value)"
                case .connectionTimeout(let value):
                    result += " connect_timeout = \(value)"
                }
            }
        }
        return result
    }
    
    private static func extractConnectionParameters(url: URL) -> String {
        var result = ""
        if let scheme = url.scheme, scheme == "Postgres", let host = url.host, let port = url.port {
            result = "host = \(host) port = \(port)"
            if let user = url.user {
                result += " user = \(user)"
            }
            if let password = url.password {
                result += " password = \(password)"
            }
            if !url.lastPathComponent.isEmpty {
                result += " dbname = \(url.lastPathComponent)"
            }
        }
        return result
    }
    
    
    static func createAutoIncrement(_ type: String) -> String {
        switch type {
        case "smallint":
            return "smallserial"
        case "integer":
            return "serial"
        case "bigint":
            return "bigserial"
        default:
            return ""
        }
    }
    
    private static func createQueryBuilder() -> QueryBuilder {
        let queryBuilder = QueryBuilder(withDeleteRequiresUsing: true, withUpdateRequiresFrom: true, createAutoIncrement: createAutoIncrement)
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.ucase : "UPPER",
                                          QueryBuilder.QuerySubstitutionNames.lcase : "LOWER",
                                          QueryBuilder.QuerySubstitutionNames.len : "LENGTH",
                                          QueryBuilder.QuerySubstitutionNames.numberedParameter : "$",
                                          QueryBuilder.QuerySubstitutionNames.namedParameter : "",
                                          QueryBuilder.QuerySubstitutionNames.double : "double precision"
            ])
        return queryBuilder
    }

    private static func createPool(_ connectionParameters: String, options: ConnectionPoolOptions) -> ConnectionPool {
        let connectionGenerator: () -> Connection? = {
            let connection = PostgreSQLConnection(connectionParameters: connectionParameters)
            connection.connection = PQconnectdb(connectionParameters)
            
            if let error = String(validatingUTF8: PQerrorMessage(connection.connection)), !error.isEmpty {
                return nil
            }
            else {
                return connection
            }
        }
        
        let connectionReleaser: (_ connection: Connection) -> () = { connection in
            connection.closeConnection()
        }
        
        return ConnectionPool(options: options, connectionGenerator: connectionGenerator, connectionReleaser: connectionReleaser)
    }
    
    /// Create a connection pool for PostgreSQLConnection's.
    ///
    /// - Parameter url: A URL of the PostgreSQL server of the following form: Postgres://userid:pwd@host:port/db.
    /// - Parameter poolOptions: A set of `ConnectionPoolOptions` to configure the created pool.
    /// - Returns: The `ConnectionPool` of `PostgreSQLConnection`.
    public static func createPool(url: URL, poolOptions: ConnectionPoolOptions) -> ConnectionPool {
        let connectionParameters = extractConnectionParameters(url: url)
        return createPool(connectionParameters, options: poolOptions)
    }

    /// Create a connection pool for PostgreSQLConnection's.
    ///
    /// - Parameter host: The host of the PostgreSQL server to connect to.
    /// - Parameter port: The port of the PostgreSQL server to connect to.
    /// - Parameter options: A set of `ConnectionOptions` to pass to the PostgreSQL server.
    /// - Parameter poolOptions: A set of `ConnectionPoolOptions` to configure the created pool.
    /// - Returns: The `ConnectionPool` of `PostgreSQLConnection`.
    public static func createPool(host: String, port: Int32, options: [ConnectionOptions]?, poolOptions: ConnectionPoolOptions) -> ConnectionPool {
        let connectionParameters = extractConnectionParameters(host: host, port: port, options: options)
        return createPool(connectionParameters, options: poolOptions)
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
            connection = nil
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
    public func execute(query: Query, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        do {
            let postgresQuery = try buildQuery(query)
            execute(query: postgresQuery, preparedStatement: nil, with: parameters, onCompletion: onCompletion)
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
            let postgresQuery = try buildQuery(query)
            execute(query: postgresQuery, preparedStatement: nil, with: [Any?](), onCompletion: onCompletion)
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
        execute(query: raw, preparedStatement: nil, with: [Any?](), onCompletion: onCompletion)
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(query: raw, preparedStatement: nil, with: parameters, onCompletion: onCompletion)
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters in raw queries are not supported in PostgreSQL")))
    }
    
    /// Prepare statement.
    ///
    /// - Parameter query: The query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
    public func prepareStatement(_ query: Query) throws -> PreparedStatement {
        let postgresQuery = try buildQuery(query)
        return try prepareStatement(postgresQuery)
    }

    /// Prepare statement.
    ///
    /// - Parameter raw: A String with the query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
    public func prepareStatement(_ raw: String) throws -> PreparedStatement  {
        let statementName = String.randomString()
        if let error = prepareStatement(name: statementName, for: raw) {
            throw QueryError.noResult(error)
        }
        return PostgreSQLPreparedStatement(name: statementName, query: raw)
    }
    
    private func prepareStatement(name: String, for query: String) -> String? {
        guard let result = PQprepare(connection, name, query, 0, nil),
            PQresultStatus(result) == PGRES_COMMAND_OK else {
                var errorMessage = "Failed to create prepared statement."
                if let error = String(validatingUTF8: PQerrorMessage(connection)) {
                    errorMessage += " Error: \(error)."
                }
                return errorMessage
        }
        preparedStatements.insert(name)
        return nil
    }

    /// Execute a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ()))  {
        execute(query: nil, preparedStatement: preparedStatement, with: [Any?](), onCompletion: onCompletion)
    }
    
    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(query: nil, preparedStatement: preparedStatement, with: parameters, onCompletion: onCompletion)
    }
    
    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters in prepared statemennts are not supported in PostgreSQL")))
    }

    /// Release a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to release.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func release(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        // No need to deallocate prepared statements in PostgreSQL.
        onCompletion(.successNoData)
    }

    private func execute(query: String?, preparedStatement: PreparedStatement?, with parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        guard let connection = connection else {
            onCompletion(.error(QueryError.connection("Connection is disconnected")))
            return
        }
        
        var parameterPointers = [UnsafeMutablePointer<Int8>?]()
        var parameterData = [UnsafePointer<Int8>?]()
        // At the moment we only create string parameters. Binary parameters should be added.
        for parameter in parameters {
            if let parameter = parameter {
                let parameterString = String(describing: parameter)
                let count = parameterString.lengthOfBytes(using: .utf8) + 1
                parameterPointers.append(UnsafeMutablePointer<Int8>.allocate(capacity: Int(count)))
                memcpy(parameterPointers[parameterPointers.count-1]!, UnsafeRawPointer(parameterString), count)
                parameterData.append(parameterPointers.last!)
            }
            else {
                parameterData.append(nil)
            }
        }

        if let query = query {
            _ = parameterData.withUnsafeBufferPointer { buffer in
                PQsendQueryParams(connection, query, Int32(parameters.count), nil, buffer.isEmpty ? nil : buffer.baseAddress, nil, nil, 1)
            }
        }
        else if let preparedStatement = preparedStatement {
            guard let statement = preparedStatement as? PostgreSQLPreparedStatement else {
                onCompletion(.error(QueryError.unsupported("Failed to execute unsupported prepared statement")))
                return
            }
            
            if !preparedStatements.contains(statement.name) {
                if let error = prepareStatement(name: statement.name, for: statement.query) {
                    onCompletion(.error(QueryError.databaseError(error)))
                    return

                }
            }

            _ = parameterData.withUnsafeBufferPointer { buffer in
                PQsendQueryPrepared(connection, statement.name, Int32(parameters.count), buffer.isEmpty ? nil : buffer.baseAddress, nil, nil, 1)
            }
        }
        for pointer in parameterPointers {
            free(pointer)
        }
        
        PQsetSingleRowMode(connection)
        processQueryResult(query: query ?? "Execution of prepared statement \(preparedStatement!)", onCompletion: onCompletion)
    }
    
    private func processQueryResult(query: String, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let result = PQgetResult(connection) else {
            var errorMessage = "No result returned for query: \(query)."
            if let error = String(validatingUTF8: PQerrorMessage(connection)) {
                errorMessage += " Error: \(error)."
            }
            onCompletion(.error(QueryError.noResult(errorMessage)))
            return
        }
        
        let status = PQresultStatus(result)
        if status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK {
            // Since we set the single row mode, PGRES_TUPLES_OK means the result is empty, i.e. there are
            // no rows to return.
            clearResult(result, connection: connection)
            onCompletion(.successNoData)
        }
        else if status == PGRES_SINGLE_TUPLE {
            let resultFetcher = PostgreSQLResultFetcher(queryResult: result, connection: connection)
            onCompletion(.resultSet(ResultSet(resultFetcher)))
        }
        else {
            let errorMessage = String(validatingUTF8: PQresultErrorMessage(result)) ?? "Unknown"
            clearResult(result, connection: connection)
            onCompletion(.error(QueryError.databaseError("Query execution error:\n" + errorMessage + " For query: " + query)))
        }
    }
    
    
    /// Start a transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of start transaction command has completed.
    public func startTransaction(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "BEGIN", inTransaction: false, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
    }
    
    /// Commit the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of commit transaction command has completed.
    public func commit(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "COMMIT", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
    }
    
    /// Rollback the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "ROLLBACK", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
    }
    
    /// Create a savepoint.
    ///
    /// - Parameter savepoint: The name to  be given to the created savepoint.
    /// - Parameter onCompletion: The function to be called when the execution of create savepoint command has completed.
    public func create(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to create the savepoint \(savepoint)", onCompletion: onCompletion)
    }
    
    /// Rollback the current transaction to the specified savepoint.
    ///
    /// - Parameter to savepoint: The name of the savepoint to rollback to.
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(to savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "ROLLBACK TO \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to rollback to the savepoint \(savepoint)", onCompletion: onCompletion)
    }
    
    /// Release a savepoint.
    ///
    /// - Parameter savepoint: The name of the savepoint to release.
    /// - Parameter onCompletion: The function to be called when the execution of release savepoint command has completed.
    public func release(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "RELEASE SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to release the savepoint \(savepoint)", onCompletion: onCompletion)
    }
    
    private func executeTransaction(command: String, inTransaction: Bool, changeTransactionState: Bool, errorMessage: String, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let connection = connection else {
            onCompletion(.error(QueryError.connection("Connection is disconnected")))
            return
        }
        
        guard self.inTransaction == inTransaction else {
            let error = self.inTransaction ? "Transaction already exists" : "No transaction exists"
            onCompletion(.error(QueryError.transactionError(error)))
            return
        }
        
        let result = PQexec(connection, command)
        let status = PQresultStatus(result)
        if status != PGRES_COMMAND_OK {
            var message = errorMessage
            if let error = String(validatingUTF8: PQerrorMessage(connection)) {
                message += " Error: \(error)."
            }

            PQclear(result)
            onCompletion(.error(QueryError.databaseError(message)))
            return
        }
        
        if changeTransactionState {
            self.inTransaction = !self.inTransaction
        }
        
        PQclear(result)
        onCompletion(.successNoData)
    }

    private func buildQuery(_ query: Query) throws  -> String {
      var postgresQuery = try query.build(queryBuilder: queryBuilder)

      if let insertQuery = query as? Insert, insertQuery.returnID {
        let columns = insertQuery.table.columns.filter { $0.isPrimaryKey && $0.autoIncrement }

        if (insertQuery.suffix == nil && columns.count == 1) {
           let insertQueryReturnID = insertQuery.suffix("Returning " + columns[0].name + " AS id")
           postgresQuery = try insertQueryReturnID.build(queryBuilder: queryBuilder)
        }

        if (insertQuery.suffix != nil) {
          throw QueryError.syntaxError("Suffix for query already set, could not add Returning suffix")
        }
      }
      return postgresQuery
    }
}
