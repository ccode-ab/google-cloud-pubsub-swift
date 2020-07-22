import Foundation
import GRPC
import NIO
import Logging

public final class SubscriberDriver: Driver {

    var rawClient: Google_Pubsub_V1_SubscriberClient!

    let logger = Logger(label: "Pub/Sub Subscriber")

    public required init(eventLoopGroupProvider: EventLoopGroupProvider) throws {
        try super.init(eventLoopGroupProvider: eventLoopGroupProvider)

        // Emulator
        if let host = ProcessInfo.processInfo.environment["PUBSUB_EMULATOR_HOST"] {
            let components = host.components(separatedBy: ":")
            let port = Int(components[1])!

            let channel = ClientConnection
                .insecure(group: eventLoopGroup)
                .connect(host: components[0], port: port)

            self.rawClient = Google_Pubsub_V1_SubscriberClient(channel: channel)
        }

        // Production
        else {
            let channel = ClientConnection
                .secure(group: eventLoopGroup)
                .connect(host: "pubsub.googleapis.com", port: 443)

            let callOptions = CallOptions(
                customMetadata: ["authorization": "Bearer \(try accessToken())"],
                timeLimit: .deadline(.distantFuture)
            )

            self.rawClient = Google_Pubsub_V1_SubscriberClient(channel: channel, defaultCallOptions: callOptions)
        }

        Self.`default` = self
    }

    public private(set) static var `default`: SubscriberDriver!

    // MARK: - Shutdown

    override public func shutdown() {
        logger.info("Shutting down...")

        if !subscribers.isEmpty {
            subscribers.forEach { $0.isShutdown = true }
        }

        super.shutdown()
    }

    // MARK: - Subscribers

    var subscribers = [PubSubSubscriber]()
}
