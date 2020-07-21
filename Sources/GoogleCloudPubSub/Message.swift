import Foundation

public struct Message {

    public internal(set) var id: String
    public let published: Date

    public var data: Data
    public var attributes: [String: String]

    public init(data: Data, attributes: [String: String] = [:]) {
        self.id = ""
        self.published = Date(timeIntervalSince1970: 0)
        self.data = data
        self.attributes = attributes
    }

    public init<Element: Encodable>(dataEncoding element: Element, attributes: [String: String] = [:]) throws {
        let data = try JSONEncoder().encode(element)
        self.init(data: data, attributes: attributes)
    }
}
