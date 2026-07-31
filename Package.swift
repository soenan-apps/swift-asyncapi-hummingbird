// swift-tools-version: 6.2

import PackageDescription

let package = Package(
  name: "swift-asyncapi-hummingbird",
  platforms: [
    .macOS(.v15),
    .iOS(.v18),
  ],
  products: [
    .library(name: "AsyncAPIHummingbird", targets: ["AsyncAPIHummingbird"])
  ],
  dependencies: [
    .package(
      url: "https://github.com/soenan-apps/swift-asyncapi-runtime.git",
      exact: "0.1.0"
    ),
    .package(
      url: "https://github.com/hummingbird-project/hummingbird.git",
      from: "2.25.1"
    ),
    .package(
      url: "https://github.com/hummingbird-project/hummingbird-websocket.git",
      from: "2.6.0"
    ),
    .package(
      url: "https://github.com/apple/swift-nio.git",
      from: "2.100.0"
    ),
  ],
  targets: [
    .target(
      name: "AsyncAPIHummingbird",
      dependencies: [
        .product(name: "AsyncAPIRuntime", package: "swift-asyncapi-runtime"),
        .product(name: "Hummingbird", package: "hummingbird"),
        .product(name: "HummingbirdWebSocket", package: "hummingbird-websocket"),
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOWebSocket", package: "swift-nio"),
      ]
    ),
    .testTarget(
      name: "AsyncAPIHummingbirdTests",
      dependencies: [
        "AsyncAPIHummingbird",
        .product(name: "AsyncAPIRuntime", package: "swift-asyncapi-runtime"),
        .product(name: "Hummingbird", package: "hummingbird"),
        .product(name: "HummingbirdTesting", package: "hummingbird"),
        .product(name: "HummingbirdWebSocket", package: "hummingbird-websocket"),
        .product(name: "HummingbirdWSTesting", package: "hummingbird-websocket"),
        .product(name: "NIOCore", package: "swift-nio"),
        .product(name: "NIOWebSocket", package: "swift-nio"),
      ]
    ),
  ],
  swiftLanguageModes: [.v6]
)
