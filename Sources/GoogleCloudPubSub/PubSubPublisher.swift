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

    private static var verifiedTopics = [Topic]()

    private func verify(topic: Topic) -> EventLoopFuture<Void> {
        if Self.verifiedTopics.contains(topic) {
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
                    promise.succeed(())
                case .failure(let error):
                    if "\(error)" == "alreadyExists (6): Topic already exists" {
                        promise.succeed(())
                    } else {
                        promise.fail(error)
                    }
                }
        }

        return promise.futureResult
    }

    // MARK: - Publish

    public func publish(to topic: Topic, messages: [PublisherMessage]) -> EventLoopFuture<[PublisherMessage]> {
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

    public func publish(to topic: Topic, message: PublisherMessage) -> EventLoopFuture<PublisherMessage> {
        publish(to: topic, messages: [message]).map { $0[0] }
    }

    public func publish(to topic: Topic, data: Data, attributes: [String: String] = [:]) -> EventLoopFuture<PublisherMessage> {
        publish(to: topic, messages: [PublisherMessage(data: data, attributes: attributes)]).map { $0[0] }
    }

    public func publish<Element: Encodable>(to topic: Topic, dataEncoding element: Element, attributes: [String: String] = [:]) -> EventLoopFuture<PublisherMessage> {
        do {
            let message = try PublisherMessage(dataEncoding: element, attributes: attributes)
            return publish(to: topic, messages: [message]).map { $0[0] }
        } catch {
            return eventLoop.makeFailedFuture(error)
        }
    }
}
