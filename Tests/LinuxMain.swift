import XCTest
@testable import SwiftKueryPostgreSQLTests

XCTMain([
     testCase(TestSelect.allTests),
     testCase(TestInsert.allTests),
     testCase(TestUpdate.allTests),
     testCase(TestAlias.allTests),
     testCase(TestParameters.allTests),
     testCase(TestJoin.allTests),
     testCase(TestSubquery.allTests),
     testCase(TestWith.allTests),
])
