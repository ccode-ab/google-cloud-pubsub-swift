import Foundation
import GRPC
import NIO
import OAuth2

public final class PublisherDriver {

    let raw: Google_Pubsub_V1_PublisherClient

    private init(raw: Google_Pubsub_V1_PublisherClient) {
        self.raw = raw

        Self.default = self
    }

    /// Default datastore. `nil` if bootstrap has not been called yet.
    public private(set) static var `default`: PublisherDriver!

    // MARK: - Bootstrap

    private enum BootstrapError: Error {
        case noTokenProvider
        case tokenProviderFailed
    }

    /// Bootstrap the datastore. Authroizes and prepares connection to remote. See `bootstrap(configuration:,group:)` for more options.
    /// - Parameters:
    ///   - eventLoopGroup: Event loop group to use.
    /// - Returns: Future for datastore.
    public static func bootstrap(on eventLoopGroup: EventLoopGroup) -> EventLoopFuture<PublisherDriver> {
        bootstrap(group: eventLoopGroup)
    }

    /// Bootstrap the datastore. Authroizes and prepares connection to remote.
    /// - Parameters:
    ///   - eventLoopGroup: Event loop group to use.
    /// - Returns: Future for datastore.
    @discardableResult
    public static func bootstrap(group eventLoopGroup: EventLoopGroup) -> EventLoopFuture<PublisherDriver> {
        bootstrapForProduction(on: eventLoopGroup)
    }

    private static func bootstrapForProduction(on eventLoopGroup: EventLoopGroup) -> EventLoopFuture<PublisherDriver> {
        let promise = eventLoopGroup.next().makePromise(of: PublisherDriver.self)

        guard let provider = DefaultTokenProvider(scopes: ["https://www.googleapis.com/auth/pubsub"]) else {
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
                let client = Google_Pubsub_V1_PublisherClient(channel: channel, defaultCallOptions: callOptions)

                promise.succeed(PublisherDriver(raw: client))
            }
        } catch {
            promise.fail(error)
        }

        return promise.futureResult
    }

    // MARK: - Shutdown

    public func shutdown() -> EventLoopFuture<Void> {
        raw.channel.close()
    }

    // MARK: - PubSub

    public func pubSubPublisher(on eventLoop: EventLoop) -> PubSubPublisher {
        PubSubPublisher(driver: self, eventLoop: eventLoop)
    }
}
