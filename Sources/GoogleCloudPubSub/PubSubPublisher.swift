import Foundation
import GRPC
import NIO

public final class PubSubPublisher {

    let driver: PublisherDriver
    let eventLoop: EventLoop

    init(driver: PublisherDriver, eventLoop: EventLoop) {
        self.driver = driver
        self.eventLoop = eventLoop
    }

    // MARK: -

    public static func `default`(on eventLoop: EventLoop) -> PubSubPublisher {
        PublisherDriver.default.pubSubPublisher(on: eventLoop)
    }

    // MARK: - Verify

    private static var verifiedTopics = [Int]()

    private func verify<Element>(topic: Topic<Element>) -> EventLoopFuture<Void> {
        let hashValue = topic.rawValue.hashValue
        if Self.verifiedTopics.contains(hashValue) {
            return eventLoop.makeSucceededFuture(())
        }

        let request = Google_Pubsub_V1_Topic.with {
            $0.name = topic.rawValue
            $0.labels = topic.labels
        }

        let promise = eventLoop.makePromise(of: Void.self)

        driver.rawClient
            .createTopic(request)
            .response
            .whenComplete { result in
                switch result {
                case .success:
                    Self.verifiedTopics.append(hashValue)
                    promise.succeed(())
                case .failure(let error):
                    if "\(error)" == "alreadyExists (6): Topic already exists" {
                        Self.verifiedTopics.append(hashValue)
                        promise.succeed(())
                    } else {
                        promise.fail(error)
                    }
                }
        }

        return promise.futureResult
    }

    // MARK: - Publish

    public func publish<Element>(to topic: Topic<Element>, messages: [PublisherMessage]) -> EventLoopFuture<[PublisherMessage]> {
        let request = Google_Pubsub_V1_PublishRequest.with {
            $0.topic = topic.rawValue
            $0.messages = messages.map { message in
                Google_Pubsub_V1_PubsubMessage.with {
                    $0.data = message.data
                    $0.attributes = message.attributes
                }
            }
        }

        return verify(topic: topic)
            .flatMap {
                self.driver.rawClient
                    .publish(request)
                    .response
                    .hop(to: self.eventLoop)
                    .map { response in
                        var messages = messages
                        for (index, id) in response.messageIds.enumerated(){
                            self.driver.logger.info("Published message", metadata: ["message-id": .string(id)])

                            messages[index].id = id
                        }
                        return messages
                }
        }
    }

    public func publish<Element>(to topic: Topic<Element>, message: PublisherMessage) -> EventLoopFuture<PublisherMessage> {
        publish(to: topic, messages: [message]).map { $0[0] }
    }

    public func publish<Element>(to topic: Topic<Element>, data: Data, attributes: [String: String] = [:]) -> EventLoopFuture<PublisherMessage> {
        publish(to: topic, messages: [PublisherMessage(data: data, attributes: attributes)]).map { $0[0] }
    }

    public func publish<Element: Encodable>(to topic: Topic<Element>, data element: Element, attributes: [String: String] = [:]) -> EventLoopFuture<PublisherMessage> {
        do {
            let message = try PublisherMessage(data: element, attributes: attributes)
            return publish(to: topic, messages: [message]).map { $0[0] }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
