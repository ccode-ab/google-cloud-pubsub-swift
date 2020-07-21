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

    private func pull(from subscription: Subscription, use handler: @escaping Handler) throws -> EventLoopFuture<Void> {
        print("Pulling...")

        let request = Google_Pubsub_V1_PullRequest.with {
            $0.subscription = subscription.rawValue
            $0.maxMessages = 6
        }

        let callOptions = CallOptions(timeLimit: .none)

        return driver.raw
            .pull(request, callOptions: callOptions)
            .response
            .hop(to: eventLoop)
            .flatMap { response in
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

                if let allFutures = allFutures {
                    return allFutures
                } else {
                    return self.eventLoop.makeSucceededFuture(())
                }
            }
    }

    public static func receive(from subscription: Subscription, use handler: @escaping Handler, on eventLoop: EventLoop) {
        let subscriber = PubSubSubscriber(driver: .default, eventLoop: eventLoop)
        eventLoop.execute {
            while true {
                do {
                    try subscriber.pull(from: subscription, use: handler).wait()
                } catch {
                    print("Failed to pull: \(error)")
                }
            }
        }
    }
}
