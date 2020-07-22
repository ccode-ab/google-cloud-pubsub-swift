import XCTest
import GoogleCloudPubSub

struct Message: Codable {

    let text: String
}

extension Topics {

    static let test = Topic<Message>(name: "test")
}

final class PublisherTests: XCTestCase {

    func testPublish() throws {
        let pubSub = try GoogleCloudPubSub.PublisherDriver.bootstrap(eventLoopGroupProvider: .createNew)
        defer { pubSub.shutdown() }

        _ = try PubSubPublisher.default(on: pubSub.eventLoopGroup.next())
            .publish(to: Topics.test, data: "Hello".data(using: .utf8)!)
            .wait()
    }
}
