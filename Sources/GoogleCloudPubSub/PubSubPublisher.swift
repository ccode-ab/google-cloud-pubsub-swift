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

    // MARK: -

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

        return driver.rawClient
            .publish(request)
            .response
            .hop(to: eventLoop)
            .map { response in
                var messages = messages
                for (index, id) in response.messageIds.enumerated(){
                    self.driver.logger.info("Published message", metadata: ["message-id": .string(id)])

                    messages[index].id = id
                }
                return messages
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
