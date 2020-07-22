import Foundation
import GRPC
import NIO

public final class PubSubSubscriber {

    public typealias Handler = (SubscriberMessage) throws -> EventLoopFuture<Void>

    let driver: SubscriberDriver
    let subscription: Subscription
    let handler: Handler

    init(driver: SubscriberDriver, subscription: Subscription, handler: @escaping Handler) {
        self.driver = driver
        self.subscription = subscription
        self.handler = handler
    }

    public static func receive(from subscription: Subscription, use handler: @escaping Handler, driver: SubscriberDriver = .default) {
        let subscriber = PubSubSubscriber(driver: driver, subscription: subscription, handler: handler)
        driver.subscribers.append(subscriber)

        DispatchQueue(label: "Pub/Sub Subscriber: " + subscription.rawValue, qos: .default).async {
            subscriber.run()
        }
    }

    // MARK: - State

    var isShutdown = false

    func run() {
        while !isShutdown {
            do {
                try pull().wait()
            } catch {
                if !isShutdown {
                    driver.logger.error("Failed to pull: \(error)")
                }
            }
        }
    }

    // MARK: - Acknowledge

    private func acknowledge(id: String) -> EventLoopFuture<Void> {
        let request = Google_Pubsub_V1_AcknowledgeRequest.with {
            $0.subscription = subscription.rawValue
            $0.ackIds = [id]
        }

        return driver.rawClient
            .acknowledge(request)
            .response
            .map { _ in () }
    }

    // MARK: - Pull

    private func pull() -> EventLoopFuture<Void> {
        let request = Google_Pubsub_V1_PullRequest.with {
            $0.subscription = subscription.rawValue
            $0.maxMessages = 100
        }

        let pull = driver.rawClient.pull(request)
        let eventLoop = pull.eventLoop

        return pull
            .response
            .map { response in
                guard !response.receivedMessages.isEmpty
                    else { return () }

                print("Received messages: \(response.receivedMessages.count)")

                var allFutures: EventLoopFuture<Void>?

                for receivedMessage in response.receivedMessages {
                    let rawMessage = receivedMessage.message
                    let message = SubscriberMessage(
                        id: rawMessage.messageID,
                        published: rawMessage.publishTime.date,
                        data: rawMessage.data,
                        attributes: rawMessage.attributes,
                        eventLoop: eventLoop
                    )

                    // Handle message
                    self.driver.logger.info("Handling message", metadata: ["message-id": .string(rawMessage.messageID)])

                    do {
                        let future = try self.handler(message)

                        // Acknowledge when succeeded
                        let ackID = receivedMessage.ackID
                        future.whenSuccess { _ in
                            _ = self.acknowledge(id: ackID)
                        }
                        future.whenFailure { error in
                            self.driver.logger.error("Failed to handle message: \(error)", metadata: ["message-id": .string(rawMessage.messageID)])
                        }

                        // Keep track of all handler futures
                        if let existing = allFutures {
                            allFutures = existing.and(future).map { _, _ in () }
                        } else {
                            allFutures = future
                        }
                    } catch {
                        self.driver.logger.error("Failed to handle message: \(error)", metadata: ["message-id": .string(rawMessage.messageID)])
                    }
                }

                return ()
        }
    }
}
