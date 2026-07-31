# Swift AsyncAPI Hummingbird

Use AsyncAPI-generated Swift server interfaces with a Hummingbird 2.x
WebSocket router. This package implements the `AsyncAPIServerTransport`
boundary from
[`swift-asyncapi-runtime`](https://github.com/soenan-apps/swift-asyncapi-runtime)
and does not depend on any generated API.

## Requirements

| Component | Supported version |
| --- | --- |
| Swift | 6.2 or newer |
| `swift-asyncapi-runtime` | Exactly 0.1.0 |
| Hummingbird | 2.25.1 up to the next major version |
| Hummingbird WebSocket | 2.6.0 up to the next major version |
| SwiftNIO | 2.100.0 up to the next major version |
| Apple platforms | macOS 15+, iOS 18+ |
| Linux | Swift 6.2+ toolchain |

The package is intended for server-side Hummingbird applications. SwiftPM's
Apple platform declarations are deployment minimums; Linux has no equivalent
manifest declaration.

## Add the package

Add the tagged release to your package dependencies:

```swift
dependencies: [
  .package(
    url: "https://github.com/soenan-apps/swift-asyncapi-hummingbird.git",
    exact: "0.1.0"
  )
]
```

Then add the library to your server target:

```swift
.target(
  name: "Server",
  dependencies: [
    .product(
      name: "AsyncAPIHummingbird",
      package: "swift-asyncapi-hummingbird"
    )
  ]
)
```

## Register a generated server

Create one WebSocket router, register the generated server on the transport,
and pass that same router to Hummingbird's WebSocket upgrade server.

```swift
import AsyncAPIHummingbird
import Hummingbird
import HummingbirdWebSocket

let webSocketRouter = Router(context: BasicWebSocketRequestContext.self)
let transport = HummingbirdWebSocketTransport(
  router: webSocketRouter,
  upgradePolicy: { request in
    // Authenticate the request and enforce Origin policy here.
    .upgrade()
  }
)

struct EventHandler: GeneratedAPIProtocol {
  func events(_ session: EventsServerSession<Void>) async throws {
    for try await message in session.incoming {
      // Handle generated message types and send typed responses.
    }
  }
}

try GeneratedAPIServerRegistration(handler: EventHandler())
  .register(on: transport)

let application = Application(
  router: Router(),
  server: .http1WebSocketUpgrade(
    webSocketRouter: webSocketRouter,
    configuration: .init(validateUTF8: true)
  )
)
try await application.runService()
```

`GeneratedAPIProtocol`, `EventsServerSession`, and
`GeneratedAPIServerRegistration` are placeholders for names emitted from your
AsyncAPI document. Register every generated handler before starting the
application, and enable Hummingbird's UTF-8 validation as shown.

## Carry upgrade evidence into a handler

The upgrade policy runs before WebSocket acceptance. A separate context
factory can move typed, `Sendable` evidence from request-scoped state into the
generated handler without casts:

```swift
let transport = HummingbirdWebSocketTransport(
  router: webSocketRouter,
  upgradePolicy: { request in
    let authorization = try await authorize(request)
    await request.context.pendingAuthorization.store(authorization)
    return .upgrade()
  },
  connectionContext: { request in
    guard let authorization =
      await request.context.pendingAuthorization.take()
    else {
      throw MissingUpgradeAuthorization()
    }
    return authorization
  }
)
```

The request context is shared by Hummingbird's upgrade and connection phases.
Store pending evidence there and consume it with take semantics. If no typed
context is needed, omit `connectionContext`; the connection context is
`Void`.

## Adapter responsibilities

The adapter:

- registers generated channel addresses and rejects duplicate addresses;
- extracts declared path parameters into `AsyncAPIConnection.parameters`;
- invokes the application's asynchronous upgrade policy;
- creates the application-defined connection context after acceptance;
- converts complete Hummingbird text and binary messages to runtime messages;
- enforces inbound and outbound message-size and queue limits;
- serializes concurrent writes and preserves their accepted order;
- forwards runtime close codes and reasons to Hummingbird; and
- ties the inbound reader and generated handler to one connection lifetime.

Authentication, authorization, Origin checks, rate limiting, and validation
beyond the generated channel contract remain application responsibilities.

## Backpressure and lifecycle

`AsyncAPIHummingbirdConfiguration` makes every in-memory boundary explicit:

| Setting | Default | Behavior |
| --- | ---: | --- |
| `maximumMessageSize` | 1 MiB | Bounds inbound messages through Hummingbird and outbound messages by byte count. |
| `inboundMessageBufferCapacity` | 16 | Buffers complete messages. Overflow terminates the stream with `inboundBufferOverflow` and cancels the handler. |
| `outboundWriteBufferCapacity` | 16 | Bounds writes waiting behind the active write. Overflow rejects the new write with `outboundBufferOverflow`. |

All values must be greater than zero. Text size is measured in UTF-8 bytes.
The outbound writer accepts one active write plus the configured number of
waiting writes.

When close begins, new sends fail with `connectionClosed`; already accepted
writes complete in order before the close frame. A write failure closes the
writer and fails queued writers. When either the peer-facing inbound reader or
the generated handler ends, the adapter cancels the other task, finishes the
message stream, and closes its writer state. This is per-connection lifecycle
management; the application still owns listener startup, graceful server
shutdown, and process-level resource cleanup.

## Non-goals

This package does not:

- parse AsyncAPI documents or generate source;
- provide HTTP routes, persistence, pub/sub, or reconnect behavior;
- choose authentication, authorization, Origin, or rate-limit policy;
- configure TLS, logging, metrics, or distributed tracing;
- manage Hummingbird application startup or shutdown; or
- expose WebSocket fragments instead of complete messages.

## Versioning

Releases use semantic versioning. Before 1.0, a minor release may contain
breaking public API changes. Each adapter release pins an exact
`swift-asyncapi-runtime` version so generated code, runtime protocols, and the
framework adapter cannot silently drift apart. Hummingbird and SwiftNIO use
bounded major-version ranges in `Package.swift`.

Upgrade the generator, runtime, and adapter as one reviewed compatibility set.
Do not replace the runtime pin with a local path in a release commit.

## Development

Run formatting validation and the test suite from this repository:

```sh
swift format lint --recursive --strict Package.swift Sources Tests
swift test
git diff --check
```

Tests cover route registration, path parameters, upgrade decisions, typed
connection context, text/binary bridging, close propagation, bounded inbound
and outbound queues, cancellation, and connection lifecycle.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE).
