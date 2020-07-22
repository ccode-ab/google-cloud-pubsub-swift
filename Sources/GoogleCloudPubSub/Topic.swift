import Foundation

public struct Topic: Equatable, Hashable {

    public let name: String

    public let labels: [String: String]

    public init(name: String, labels: [String: String] = [:]) {
        self.name = name
        self.labels = labels
    }

    // MARK: -

    public var rawValue: String {
        let projectID = ProcessInfo.processInfo.environment["GCP_PROJECT_ID"] ?? ""
        return "projects/\(projectID)/topics/\(name)"
    }

    // MARK: - Equatable

    public static func == (lhs: Topic, rhs: Topic) -> Bool {
        lhs.rawValue == rhs.rawValue
    }
}
