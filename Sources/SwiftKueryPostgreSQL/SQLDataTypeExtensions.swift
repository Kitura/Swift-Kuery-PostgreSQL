import SwiftKuery

// MARK: ByteA

/// Provides the PostgreSQL data type `bytea` for in SwiftKuery Tables.
struct ByteA: SQLDataType {
    public static func create(queryBuilder: QueryBuilder) -> String {
        return "bytea"
    }
}
