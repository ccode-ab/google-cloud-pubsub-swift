import Foundation
import GRPC
import NIO

public final class SubscriberDriver: Driver {

    var rawClient: Google_Pubsub_V1_SubscriberClient!

    public required init(eventLoopGroupProvider: EventLoopGroupProvider) throws {
        try super.init(eventLoopGroupProvider: eventLoopGroupProvider)

        let channel = ClientConnection
            .secure(group: eventLoopGroup)
            .connect(host: "pubsub.googleapis.com", port: 443)

        let callOptions = CallOptions(
            customMetadata: ["authorization": "Bearer \(try accessToken())"],
            timeLimit: .deadline(.distantFuture)
        )

        self.rawClient = Google_Pubsub_V1_SubscriberClient(channel: channel, defaultCallOptions: callOptions)

        Self.`default` = self
    }

    public private(set) static var `default`: SubscriberDriver!

    // MARK: - Shutdown

    override public func shutdown() {
        if !subscribers.isEmpty {
            print("Shutting down Pub/Sub subscribers...")
            subscribers.forEach { $0.isShutdown = true }
        }

        super.shutdown()
    }

    // MARK: - Subscribers

    var subscribers = [PubSubSubscriber]()
}
