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

import CLibpq
import Foundation

func clearResult(_ lastResult: OpaquePointer?, connection: PostgreSQLConnection) {
    if let lastResult = lastResult {
        PQclear(lastResult)
    }
    var result = PQgetResult(connection.connection)
    while result != nil {
        PQclear(result)
        result = PQgetResult(connection.connection)
    }
    connection.setState(.idle)
}


extension String {
    static func randomString() -> String {
        let base = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        var randomString: String = ""
        
        let max = UInt32(base.characters.count)
        for _ in 0..<20 {
            #if os(Linux)
                let randomValue =  Int(random() % Int(max))
            #else
                let randomValue = Int(arc4random_uniform(UInt32(max)))
            #endif
            randomString += "\(base[base.index(base.startIndex, offsetBy: Int(randomValue))])"
        }
        return randomString
    }
}

