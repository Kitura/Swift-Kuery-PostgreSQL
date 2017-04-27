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

// MARK: PostgreSQLTypes

// The list of OIDs for the various PostgreSQL types supported by Swift-Kuery-PostgreSQL.
enum PostgreSQLType: UInt32 {
    case int2 = 21
    case int4 = 23
    case int8 = 20

    case float4 = 700
    case float8 = 701

    case bool = 16

    case char = 18
    case name = 19
    case text = 25
    case bpchar = 1042
    case varchar = 1043
    case json = 114
    case xml = 142
    
    case numeric = 1700
    
    case date = 1082
    case time = 1083
    case timetz = 1266
    case timestamp = 1114
    case timestamptz = 1184
    
    case uuid = 2950
}
