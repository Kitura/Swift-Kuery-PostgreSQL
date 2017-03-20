import XCTest
@testable import SwiftKueryPostgreSQLTests

XCTMain([
     testCase(TestAlias.allTests),
     testCase(TestInsert.allTests),
     testCase(TestJoin.allTests),
     testCase(TestParameters.allTests),
     testCase(TestSelect.allTests),
     testCase(TestSubquery.allTests),
     testCase(TestTransaction.allTests),
     testCase(TestTypes.allTests),
     testCase(TestUpdate.allTests),
     testCase(TestWith.allTests),
])
