import XCTest
@testable import GoogleCloudPubSub

extension Subscription {

    public static let userCreated = Subscription("projects/cfeedback-cloud/subscriptions/i-email-notifier-user-created")
}

final class TestCase: XCTestCase {

    func testasd() throws {
        let pubSub = try GoogleCloudPubSub.SubscriberDriver.bootstrap(eventLoopGroupProvider: .createNew)
        defer { pubSub.shutdown() }

        PubSubSubscriber.receive(from: .userCreated, use: { message in
            return message.eventLoop.makeSucceededFuture(())
        })

        sleep(120)
    }
}

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
    ]
}
#endif
