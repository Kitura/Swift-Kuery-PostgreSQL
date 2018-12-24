import Foundation

// MARK: PostgreSQLBinaryParameterConvertible

/// Interface for objects that can be sent as binary parameters of calls to the `PQsendQuery` family of functions
protocol PostgreSQLBinaryParameterConvertible {
    /// Format this object as binary Data for use as a PostgreSQL parameter.
    func asPostgreSQLBinaryParameter() -> Data
}

extension Data: PostgreSQLBinaryParameterConvertible {
    public func asPostgreSQLBinaryParameter() -> Data {
        return self
    }
}
