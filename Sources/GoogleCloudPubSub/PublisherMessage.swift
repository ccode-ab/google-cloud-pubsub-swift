import Foundation
import NIO

public struct PublisherMessage {

    public internal(set) var id: String

    public var data: Data
    public var attributes: [String: String]

    public init(data: Data, attributes: [String: String] = [:]) {
        self.id = ""
        self.data = data
        self.attributes = attributes
    }

    public init<Element: Encodable>(dataEncoding element: Element, attributes: [String: String] = [:]) throws {
        let data = try JSONEncoder().encode(element)
        self.init(data: data, attributes: attributes)
    }
}

public struct SubscriberMessage {

    public let id: String
    public let published: Date

    public let data: Data
    public let attributes: [String: String]

    public let eventLoop: EventLoop

    init(id: String, published: Date, data: Data, attributes: [String: String], eventLoop: EventLoop) {
        self.id = id
        self.published = published
        self.data = data
        self.attributes = attributes
        self.eventLoop = eventLoop
    }

    public func decode<Element: Decodable>(_ elementType: Element.Type) throws -> Element {
        try JSONDecoder().decode(elementType, from: data)
    }
}
