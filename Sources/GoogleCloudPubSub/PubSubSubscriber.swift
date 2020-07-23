import Foundation
import GRPC
import NIO

private var verifiedSubscriptions = [Int]()

public final class PubSubSubscriber<Element: Codable> {

    public typealias Handler = (SubscriberMessage) throws -> EventLoopFuture<Void>

    let driver: SubscriberDriver
    let subscription: Subscription<Element>
    let handler: Handler

    init(driver: SubscriberDriver, subscription: Subscription<Element>, handler: @escaping Handler) {
        self.driver = driver
        self.subscription = subscription
        self.handler = handler
    }

    public static func receive(from subscription: Subscription<Element>, use handler: @escaping Handler, driver: SubscriberDriver = .default) {
        let subscriber = PubSubSubscriber(driver: driver, subscription: subscription, handler: handler)
        driver.subscriberShutdowns.append({
            subscriber.isShutdown = true
        })

        DispatchQueue(label: "Pub/Sub Subscriber: " + subscription.rawValue, qos: .default).async {
            subscriber.run()
        }
    }

    // MARK: - State

    var isShutdown = false

    func run() {
        do {
            try verify(subscription: subscription).wait()
        } catch {
            driver.logger.error("Failed to verify subscription: \(error)")
            return
        }

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

    // MARK: - Verify

    private func verify<Element>(subscription: Subscription<Element>) -> EventLoopFuture<Void> {
        let hashValue = subscription.rawValue.hashValue
        let eventLoop = driver.eventLoopGroup.next()
        if verifiedSubscriptions.contains(hashValue) {
            return eventLoop.makeSucceededFuture(())
        }

        let request = Google_Pubsub_V1_Subscription.with {
            $0.name = subscription.rawValue
            $0.labels = subscription.labels
            $0.topic = subscription.topic.rawValue
            $0.ackDeadlineSeconds = Int32(subscription.acknowledgeDeadline)
            $0.retainAckedMessages = subscription.retainAcknowledgedMessages
            $0.messageRetentionDuration = .with {
                $0.seconds = Int64(subscription.messageRetentionDuration)
            }
            $0.expirationPolicy = .with {
                $0.ttl = .with {
                    $0.seconds = Int64(subscription.expirationPolicyDuration)
                }
            }
            if let deadLetterPolicy = subscription.deadLetterPolicy {
                $0.deadLetterPolicy = .with {
                    $0.deadLetterTopic = deadLetterPolicy.topic.rawValue
                    $0.maxDeliveryAttempts = deadLetterPolicy.maxDeliveryAttempts
                }
            }
        }

        let promise = eventLoop.makePromise(of: Void.self)

        driver.rawClient
            .createSubscription(request)
            .response
            .whenComplete { result in
                switch result {
                case .success:
                    verifiedSubscriptions.append(hashValue)
                    promise.succeed(())
                case .failure(let error):
                    if "\(error)".hasPrefix("alreadyExists (6):") {
                        verifiedSubscriptions.append(hashValue)
                        promise.succeed(())
                    } else {
                        promise.fail(error)
                    }
                }
        }

        return promise.futureResult
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
