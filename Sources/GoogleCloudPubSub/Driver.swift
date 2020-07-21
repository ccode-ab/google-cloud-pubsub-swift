import Foundation
import GRPC
import NIO
import OAuth2

protocol DriverRawClient {

    init(channel: GRPCChannel, defaultCallOptions: CallOptions)
}

extension Google_Pubsub_V1_PublisherClient: DriverRawClient {}
extension Google_Pubsub_V1_SubscriberClient: DriverRawClient {}

public class Driver {

    public enum EventLoopGroupProvider {
        case shared(EventLoopGroup)
        case createNew
    }

    public let eventLoopGroupProvider: EventLoopGroupProvider
    public let eventLoopGroup: EventLoopGroup

    required init(eventLoopGroupProvider: EventLoopGroupProvider) throws {
        self.eventLoopGroupProvider = eventLoopGroupProvider

        switch eventLoopGroupProvider {
        case .createNew:
            self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        case .shared(let eventLoopGroup):
            self.eventLoopGroup = eventLoopGroup
        }
    }

    private enum AccessTokenError: Error {
        case noTokenProvider
        case tokenProviderFailed
    }

    func accessToken() throws -> String {
        guard let provider = DefaultTokenProvider(scopes: ["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/pubsub"]) else { throw AccessTokenError.noTokenProvider }

        let accessTokenPromise = eventLoopGroup.next().makePromise(of: String.self)

        try provider.withToken { token, error in
            guard let token = token, let accessToken = token.AccessToken else {
                accessTokenPromise.fail(error ?? AccessTokenError.tokenProviderFailed)
                return
            }

            accessTokenPromise.succeed(accessToken)
        }

        return try accessTokenPromise.futureResult.wait()
    }

    // MARK: - Bootstrap

    @discardableResult
    public static func bootstrap(eventLoopGroupProvider: EventLoopGroupProvider) throws -> Self {
        try Self.init(eventLoopGroupProvider: eventLoopGroupProvider)
    }

    // MARK: - Shutdown

    public func shutdown() {
        switch eventLoopGroupProvider {
        case .createNew:
            do {
                try eventLoopGroup.syncShutdownGracefully()
            } catch {
                print("Shutting down EventLoopGroup failed: \(error)")
            }
        case .shared:
            break
        }
    }
}
