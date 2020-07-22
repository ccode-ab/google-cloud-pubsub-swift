import Foundation
import GRPC
import NIO
import OAuth2
import Logging

public final class PublisherDriver: Driver {

    var rawClient: Google_Pubsub_V1_PublisherClient!

    let logger = Logger(label: "Pub/Sub Publisher")

    public required init(eventLoopGroupProvider: EventLoopGroupProvider) throws {
        try super.init(eventLoopGroupProvider: eventLoopGroupProvider)

        let channel = ClientConnection
            .secure(group: eventLoopGroup)
            .connect(host: "pubsub.googleapis.com", port: 443)

        let callOptions = CallOptions(
            customMetadata: ["authorization": "Bearer \(try accessToken())"]
        )

        self.rawClient = Google_Pubsub_V1_PublisherClient(channel: channel, defaultCallOptions: callOptions)

        Self.`default` = self
    }

    public private(set) static var `default`: PublisherDriver!

    // MARK: - PubSub

    public func pubSubPublisher(on eventLoop: EventLoop) -> PubSubPublisher {
        PubSubPublisher(driver: self, eventLoop: eventLoop)
    }
}
