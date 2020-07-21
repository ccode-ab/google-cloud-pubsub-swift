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

    public typealias Handler = (SubscriberMessage) throws -> EventLoopFuture<Void>

    private func acknowledge(subscription: Subscription, id: String) -> EventLoopFuture<Void> {
        let request = Google_Pubsub_V1_AcknowledgeRequest.with {
            $0.subscription = subscription.rawValue
            $0.ackIds = [id]
        }

        return driver.raw
            .acknowledge(request)
            .response
            .hop(to: eventLoop)
            .map { _ in () }
    }

    private func pull(from subscription: Subscription, use handler: @escaping Handler) throws {
        print("Pulling...")

        let stream = driver.raw
            .streamingPull(handler: { response in
                print("Received messages: \(response.receivedMessages.count)")

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
                    self.eventLoop.execute {
                        do {
                            let future = try handler(message)

                            // Acknowledge when succeeded
                            let ackID = receivedMessage.ackID
                            future.whenSuccess { _ in
                                _ = self.acknowledge(subscription: subscription, id: ackID)
                            }
                            future.whenFailure { error in
                                print("Failed to handle message: \(error)")
                            }

                            // Keep track of all handler futures
                            if let existing = allFutures {
                                allFutures = existing.and(future).map { _, _ in () }
                            } else {
                                allFutures = future
                            }
                        } catch {
                            print("Failed to handle message: \(error)")
                        }
                    }
                }
            })

        let request = Google_Pubsub_V1_StreamingPullRequest.with {
            $0.subscription = subscription.rawValue
            $0.streamAckDeadlineSeconds = 60
            $0.maxOutstandingMessages = 100
            $0.maxOutstandingBytes = 1000
        }

        try stream.sendMessage(request).wait()
    }

    public static func receive(from subscription: Subscription, use handler: @escaping Handler) throws {
        let pubSubSubscriber = SubscriberDriver.default.pubSubSubscriber()
        try pubSubSubscriber.pull(from: subscription, use: handler)
    }
}
