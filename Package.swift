// swift-tools-version:5.2
import PackageDescription

let package = Package(
    name: "GoogleCloudPubSub",
    products: [
        .library(name: "GoogleCloudPubSub", targets: ["GoogleCloudPubSub"]),
    ],
    dependencies: [
        .package(name: "grpc-swift", url: "https://github.com/grpc/grpc-swift.git", from: "1.0.0-alpha.12"),
        .package(name: "Auth", url: "https://github.com/googleapis/google-auth-library-swift.git", from: "0.5.2"),
        .package(name: "swift-log", url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
    ],
    targets: [
        .target(name: "GoogleCloudPubSub", dependencies: [
            .product(name: "GRPC", package: "grpc-swift"),
            .product(name: "OAuth2", package: "Auth"),
            .product(name: "Logging", package: "swift-log"),
        ]),
        .testTarget(name: "GoogleCloudPubSubTests", dependencies: [
            "GoogleCloudPubSub",
        ]),
    ]
)
