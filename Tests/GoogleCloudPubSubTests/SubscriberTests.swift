import XCTest
import NIO
import GoogleCloudPubSub

extension Subscription {

    static let test = Subscription(name: "test", topic: .test)
}

final class SubscriberTestCase: XCTestCase {

    func testSubscribe() throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)

        let pubSubSubscriber = try GoogleCloudPubSub.SubscriberDriver.bootstrap(eventLoopGroupProvider: .shared(eventLoopGroup))
        defer { pubSubSubscriber.shutdown() }

        let pubSubPublisher = try GoogleCloudPubSub.PublisherDriver.bootstrap(eventLoopGroupProvider: .shared(eventLoopGroup))
        defer { pubSubPublisher.shutdown() }

        // Prepare
        let expectation = self.expectation(description: "Received message")
        var publishedMessage: PublisherMessage?
        var receivedMessage: SubscriberMessage?

        // Recive message
        PubSubSubscriber.receive(from: .test, use: { message in
            receivedMessage = message
            expectation.fulfill()
            return message.eventLoop.makeSucceededFuture(())
        })

        // Publish message
        publishedMessage = try! PubSubPublisher.default(on: eventLoopGroup.next())
            .publish(to: .test, data: "Hello".data(using: .utf8)!)
            .wait()

        // Wait
        waitForExpectations(timeout: 60, handler: nil)

        // Assert
        XCTAssertNotNil(publishedMessage)
        XCTAssertNotNil(receivedMessage)
        XCTAssertEqual(publishedMessage?.id, receivedMessage?.id)
    }
}
