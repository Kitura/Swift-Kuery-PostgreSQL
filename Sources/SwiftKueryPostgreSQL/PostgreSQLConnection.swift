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
    
    private var connection: OpaquePointer? = nil///???
    private var connectionParameters: String
    public var queryBuilder: QueryBuilder
    
    public required init(host: String, port: Int32, queryBuilder: QueryBuilder, options: [ConnectionOptions]?) {
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
        self.queryBuilder = queryBuilder
        queryBuilder.updateNames([QueryBuilder.QueryNames.ascd : "ASC", QueryBuilder.QueryNames.ucase : "UPPER", QueryBuilder.QueryNames.lcase : "LOWER"])
    }
    
    public func execute(query: Query, parameters: ValueType..., onCompletion: @escaping ((QueryResult) -> ())) {
        
    }
    
    public func descriptionOf(query: Query) -> String {
        return query.build(queryBuilder: queryBuilder)
    }

    public func connect(onCompletion: (String?) -> ()) {
        connection = PQconnectdb(connectionParameters)
        
        var error : String? = String(validatingUTF8: PQerrorMessage(connection))
        if error != nil && error!.isEmpty {
            error = nil
        }
        onCompletion(error)
    }
    
    public func closeConnection() {
        PQfinish(connection)
        connection = nil
    }
    
    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        let postgresQuery = query.build(queryBuilder: queryBuilder)
        let queryResult = PQexec(connection, postgresQuery)
        
        guard let result = queryResult else {
            onCompletion(.error(QueryError.noResult))
            return
        }
        
        let status = PQresultStatus(result)
        /*if status == 5 || status == 6 {
         onCompletion(.error(PQresultErrorMessage(queryResult)))
         }
         else */if status == PGRES_COMMAND_OK {
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
    
    private static func getRows(queryResult: OpaquePointer) -> ([String], [[ValueType?]]) {
        var result = [[ValueType?]]()
        let rows = PQntuples(queryResult)
        let columns = PQnfields(queryResult)
        var columnNames = [String]()
        
        for column in 0 ..< columns {
            columnNames.append(String(validatingUTF8: PQfname(queryResult, column))!)
        }
        
        for rowIndex in 0 ..< rows {
            var row = [ValueType?]()
            
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
    
    private static func convert(_ queryResult: OpaquePointer, row: Int32, column: Int32) -> ValueType {
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

//public enum Status: Int, ResultStatus {
//    case EmptyQuery
//    case CommandOK
//    case TuplesOK
//    case CopyOut
//    case CopyIn
//    case BadResponse
//    case NonFatalError
//    case FatalError
//    case CopyBoth
//    case SingleTuple
//    case Unknown
//
//    public init(status: ExecStatusType) {
//        switch status {
//        case PGRES_EMPTY_QUERY:
//            self = .EmptyQuery
//            break
//        case PGRES_COMMAND_OK:
//            self = .CommandOK
//            break
//        case PGRES_TUPLES_OK:
//            self = .TuplesOK
//            break
//        case PGRES_COPY_OUT:
//            self = .CopyOut
//            break
//        case PGRES_COPY_IN:
//            self = .CopyIn
//            break
//        case PGRES_BAD_RESPONSE:
//            self = .BadResponse
//            break
//        case PGRES_NONFATAL_ERROR:
//            self = .NonFatalError
//            break
//        case PGRES_FATAL_ERROR:
//            self = .FatalError
//            break
//        case PGRES_COPY_BOTH:
//            self = .CopyBoth
//            break
//        case PGRES_SINGLE_TUPLE:
//            self = .SingleTuple
//            break
//        default:
//            self = .Unknown
//            break
//        }
//    }
//}
//
