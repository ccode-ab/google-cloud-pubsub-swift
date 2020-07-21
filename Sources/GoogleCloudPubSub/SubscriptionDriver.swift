import Foundation
import GRPC
import NIO
import OAuth2

public final class SubscriberDriver {

    let raw: Google_Pubsub_V1_SubscriberClient
    static let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)

    private init(raw: Google_Pubsub_V1_SubscriberClient) {
        self.raw = raw

        Self.default = self
    }

    public private(set) static var `default`: SubscriberDriver!

    // MARK: - Bootstrap

    private enum BootstrapError: Error {
        case noTokenProvider
        case tokenProviderFailed
    }

    @discardableResult
    public static func bootstrap() -> EventLoopFuture<SubscriberDriver> {
        bootstrapForProduction()
    }

    private static func bootstrapForProduction() -> EventLoopFuture<SubscriberDriver> {
        let promise = eventLoopGroup.next().makePromise(of: SubscriberDriver.self)

        guard let provider = DefaultTokenProvider(scopes: ["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/pubsub"]) else {
            promise.fail(BootstrapError.noTokenProvider)
            return promise.futureResult
        }

        do {
            try provider.withToken { token, error in
                guard let token = token, let accessToken = token.AccessToken else {
                    promise.fail(error ?? BootstrapError.tokenProviderFailed)
                    return
                }

                let channel = ClientConnection
                    .secure(group: eventLoopGroup)
                    .connect(host: "pubsub.googleapis.com", port: 443)

                let callOptions = CallOptions(
                    customMetadata: ["authorization": "Bearer \(accessToken)"]
                )
                let client = Google_Pubsub_V1_SubscriberClient(channel: channel, defaultCallOptions: callOptions)

                promise.succeed(SubscriberDriver(raw: client))
            }
        } catch {
            promise.fail(error)
        }

        return promise.futureResult
    }

    // MARK: - Shutdown

    public func shutdown() -> EventLoopFuture<Void> {
        Self.eventLoopGroup.shutdownGracefully { _ in }
        return raw.channel.close()
    }

    // MARK: - PubSub

    private var subscribers = [PubSubSubscriber]()

    public func pubSubSubscriber() -> PubSubSubscriber {
        let subscriber = PubSubSubscriber(driver: self, eventLoop: Self.eventLoopGroup.next())
        subscribers.append(subscriber)
        return subscriber
    }
}
