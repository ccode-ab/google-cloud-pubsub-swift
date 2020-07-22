import XCTest
import GoogleCloudPubSub

extension Topic {

    static let test = Topic(name: "test")
}

final class PublisherTests: XCTestCase {

    func testPublish() throws {
        let pubSub = try GoogleCloudPubSub.PublisherDriver.bootstrap(eventLoopGroupProvider: .createNew)
        defer { pubSub.shutdown() }

        _ = try PubSubPublisher.default(on: pubSub.eventLoopGroup.next())
            .publish(to: .test, data: "Hello".data(using: .utf8)!)
            .wait()
    }
}
