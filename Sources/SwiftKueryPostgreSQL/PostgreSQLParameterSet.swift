import Foundation

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
    internal func withUnsafeBufferPointers(_ body: @escaping (UnsafePointers) -> Void) {
        let (values, lengths, formats) = parameterData()
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
    private func parameterData() -> ([UnsafeMutablePointer<Int8>?], [Int32], [Int32]) {
        var values = [UnsafeMutablePointer<Int8>?]()
        var lengths = [Int32]()
        var formats = [Int32]()

        self.parameters.forEach {
            if let binaryConvertibleParameter = $0 as? PostgreSQLBinaryParameterConvertible {
                let parameterData = binaryConvertibleParameter.asPostgreSQLBinaryParameter()
                let count = parameterData.count
                let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(count))
                _ = parameterData.withUnsafeBytes { memcpy(pointer, $0, count) }

                values.append(pointer)
                lengths.append(Int32(count))
                formats.append(1) // binary data for the PQsendQuery APIs
            } else if let parameter = $0 {
                let parameterString = String(describing: parameter)
                let count = parameterString.lengthOfBytes(using: .utf8) + 1
                let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(count))
                memcpy(pointer, UnsafeRawPointer(parameterString), count)

                values.append(pointer)
                lengths.append(Int32(count))
                formats.append(0) // string data for the PQsendQuery APIs
            } else {
                values.append(nil)
                lengths.append(0)
                formats.append(0)
            }
        }

        return (values, lengths, formats)
    }
}
