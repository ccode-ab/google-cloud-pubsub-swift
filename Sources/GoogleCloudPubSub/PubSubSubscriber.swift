import Foundation
import GRPC
import NIO

public final class PubSubSubscriber {

    let driver: SubscriberDriver
    let eventLoop: EventLoop

    init(driver: SubscriberDriver, eventLoop: EventLoop) {
        self.driver = driver
        self.eventLoop = eventLoop
    }

    // MARK: -

    public static func `default`(on eventLoop: EventLoop) -> PubSubSubscriber {
        SubscriberDriver.default.pubSubSubscriber(on: eventLoop)
    }

    // MARK: -

    public typealias Handler = (SubscriberMessage) -> EventLoopFuture<Void>

    private func acknowledge(subscription: Subscription, id: String) -> EventLoopFuture<Void> {
        let request = Google_Pubsub_V1_AcknowledgeRequest.with {
            $0.subscription = subscription.rawValue
            $0.ackIds = [id]
        }

        return driver.raw
            .acknowledge(request)
            .response
            .map { _ in () }
    }

    private func pull(from subscription: Subscription, use handler: @escaping Handler) {
        let request = Google_Pubsub_V1_PullRequest.with {
            $0.subscription = subscription.rawValue
            $0.maxMessages = 6
        }

        let callOptions = CallOptions(timeLimit: .none)
        _ = driver.raw
            .pull(request, callOptions: callOptions)
            .response
            .hop(to: eventLoop)
            .map { response in

                var allFutures: EventLoopFuture<Void>?

                for receivedMessage in response.receivedMessages {
                    let rawMessage = receivedMessage.message
                    let message = SubscriberMessage(
                        id: rawMessage.messageID,
                        published: rawMessage.publishTime.date,
                        data: rawMessage.data,
                        attributes: rawMessage.attributes,
                        eventLoop: self.eventLoop.next()
                    )

                    // Handle message
                    let future = handler(message)

                    // Acknowledge when succeeded
                    let ackID = receivedMessage.ackID
                    future.whenSuccess { _ in
                        _ = self.acknowledge(subscription: subscription, id: ackID)
                    }

                    // Keep track of all handler futures
                    if let existing = allFutures {
                        allFutures = existing.and(future).map { _, _ in () }
                    } else {
                        allFutures = future
                    }
                }

                // Wait for all futures to complete before pulling again
                // If there were no recived messages, start polling again immediately
                if let allFutures = allFutures {
                    allFutures.whenComplete { _ in
                        self.pull(from: subscription, use: handler)
                    }
                } else {
                    self.pull(from: subscription, use: handler)
                }
            }
    }

    public func receive(from subscription: Subscription, use handler: @escaping Handler) {
        pull(from: subscription, use: handler)
    }
}
