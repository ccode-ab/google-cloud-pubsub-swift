import XCTest

import GoogleCloudDatastoreTests

var tests = [XCTestCaseEntry]()
tests += GoogleCloudPubSubTests.allTests()
XCTMain(tests)
