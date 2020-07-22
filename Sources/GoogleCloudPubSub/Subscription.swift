import Foundation

public struct Subscriptions {}

public struct Subscription<Element: Codable>: Equatable, Hashable {

    public let name: String
    public let topic: Topic<Element>

    public let labels: [String: String]

    public let retainAcknowledgedMessages: Bool
    public let acknowledgeDeadline: TimeInterval
    public let expirationPolicyDuration: TimeInterval
    public let messageRetentionDuration: TimeInterval

    public struct DeadLetterPolicy: Equatable, Hashable {

        public let topic: Topic<Element>
        public let maxDeliveryAttempts: Int32

        public init(topic: Topic<Element>, maxDeliveryAttempts: Int32) {
            self.topic = topic
            self.maxDeliveryAttempts = maxDeliveryAttempts
        }
    }

    public let deadLetterPolicy: DeadLetterPolicy?

    public init(name: String, topic: Topic<Element>, labels: [String: String] = [:], retainAcknowledgedMessages: Bool = false, acknowledgeDeadline: TimeInterval = 10, expirationPolicyDuration: TimeInterval = 3600 * 24 * 31, messageRetentionDuration: TimeInterval = 3600 * 24 * 6, deadLetterPolicy: DeadLetterPolicy? = nil) {
        self.name = name
        self.topic = topic
        self.labels = labels
        self.retainAcknowledgedMessages = retainAcknowledgedMessages
        self.acknowledgeDeadline = acknowledgeDeadline
        self.expirationPolicyDuration = expirationPolicyDuration
        self.messageRetentionDuration = messageRetentionDuration
        self.deadLetterPolicy = deadLetterPolicy
    }

    // MARK: -

    public var rawValue: String {
        let projectID = ProcessInfo.processInfo.environment["GCP_PROJECT_ID"] ?? ""
        return "projects/\(projectID)/subscriptions/\(name)"
    }

    // MARK: - Equatable

    public static func == (lhs: Subscription, rhs: Subscription) -> Bool {
        lhs.rawValue == rhs.rawValue
    }
}
