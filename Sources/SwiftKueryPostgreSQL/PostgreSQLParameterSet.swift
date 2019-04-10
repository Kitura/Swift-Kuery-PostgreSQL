import Foundation
import SwiftKuery

// MARK:  PostgreSQLParameterSet

/// Helper object which serializes binary and string parameters of `PQsendQueryParams()` or `PQsendQueryPrepared()`.
/// Handles all memory management associated with the process.
internal struct PostgreSQLParameterSet {
    let parameters: [Any?]

    /// ParameterSet.UnsafePointers
    ///
    /// Helper object only for use within `withUnsafeBufferPointers()`.
    internal struct UnsafePointers {
        let values: UnsafePointer<UnsafePointer<Int8>?>?
        let lengths: UnsafePointer<Int32>?
        let formats: UnsafePointer<Int32>?
    }

    /// Perform a block, yielding raw pointers to the parameters of this object.
    ///
    /// - Parameter body: A block which is called synchronously by this function.
    /// - Throws: QueryError.unsupported if parameter cannot be converted to data
    internal func withUnsafeBufferPointers(_ body: @escaping (UnsafePointers) -> Void) throws {
        let (values, lengths, formats) = try parameterData()
        defer { values.forEach({ free($0) }) }

        values.map({ UnsafePointer($0) }).withUnsafeBufferPointer { valuesBuffer in
            lengths.withUnsafeBufferPointer { lengthsBuffer in
                formats.withUnsafeBufferPointer { formatsBuffer in
                    let pointers = UnsafePointers(
                        values: valuesBuffer.isEmpty ? nil : valuesBuffer.baseAddress,
                        lengths: lengthsBuffer.isEmpty ? nil : lengthsBuffer.baseAddress,
                        formats: formatsBuffer.isEmpty ? nil : formatsBuffer.baseAddress
                    )

                    body(pointers)
                }
            }
        }
    }

    /// Helper function returning formatted data useful for Postgres API function calls.
    private func parameterData() throws -> ([UnsafeMutablePointer<Int8>?], [Int32], [Int32]) {
        var values = [UnsafeMutablePointer<Int8>?]()
        var lengths = [Int32]()
        var formats = [Int32]()

        try self.parameters.forEach {
            if let binaryConvertibleParameter = $0 as? PostgreSQLBinaryParameterConvertible {
                let parameterData = binaryConvertibleParameter.asPostgreSQLBinaryParameter()
                let count = parameterData.count
                let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(count))
                #if swift(>=5)
                _ = try parameterData.withUnsafeBytes( { (data: UnsafeRawBufferPointer) in
                    let basePointer = data.baseAddress?.assumingMemoryBound(to: Int8.self)
                    guard let source = basePointer else {
                        throw QueryError.databaseError("Unable to format binary parameter data")
                    }
                    memcpy(pointer, source, count)
                    return
                } )
                #else
                _ = parameterData.withUnsafeBytes { memcpy(pointer, $0, count) }
                #endif
                values.append(pointer)
                lengths.append(Int32(count))
                formats.append(1) // indicate binary data for the PQsendQuery APIs
            } else if let parameter = $0 {
                let parameterString = String(describing: parameter)

                guard let parameterData = parameterString.data(using: .utf8) else {
                  throw QueryError.unsupported("Could not convert \(parameter) to UTF8-encoded data")
                }

                // Copy memory and provide explicit null termination for the C-string
                let count = parameterData.count
                let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: count + 1)
                #if swift(>=5)
                _ = try parameterData.withUnsafeBytes( { (data: UnsafeRawBufferPointer) in
                    let basePointer = data.baseAddress?.assumingMemoryBound(to: Int8.self)
                    guard let source = basePointer else {
                        throw QueryError.databaseError("Unable to format parameter data")
                    }
                    memcpy(pointer, source, count)
                    return
                } )
                #else
                _ = parameterData.withUnsafeBytes { memcpy(pointer, $0, count) }
                #endif
                pointer[parameterData.count] = 0

                values.append(pointer)
                lengths.append(Int32(count))
                formats.append(0) // indicate string data for the PQsendQuery APIs
            } else {
                values.append(nil)
                lengths.append(0)
                formats.append(0)
            }
        }

        return (values, lengths, formats)
    }
}
