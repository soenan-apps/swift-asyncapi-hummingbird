import AsyncAPIRuntime
import Hummingbird
import HummingbirdTesting
import HummingbirdWSTesting
import HummingbirdWebSocket
import NIOCore
import NIOWebSocket
import XCTest

@testable import AsyncAPIHummingbird

final class AsyncAPIHummingbirdTests: XCTestCase {
  func testUpgradeAuthorizationMovesIntoTypedApplicationContext() async throws {
    let router = Router(context: AuthorizationWebSocketRequestContext.self)
    let recorder = ApplicationContextRecorder()
    let transport = HummingbirdWebSocketTransport(
      router: router,
      upgradePolicy: { request in
        guard let roomID = request.parameters["roomId"] else {
          return .dontUpgrade
        }
        await request.context.authorization.store(
          FixtureApplicationContext(
            subject: "user_fixture",
            roomID: roomID
          )
        )
        return .upgrade()
      },
      connectionContext: { request in
        guard let authorization = await request.context.authorization.take()
        else {
          throw FixtureError.missingAuthorization
        }
        return authorization
      }
    )
    try transport.register(
      channel: AsyncAPIChannel(
        name: "roomEvents",
        address: "/rooms/{roomId}/events",
        parameterNames: ["roomId"]
      )
    ) { connection in
      await recorder.record(connection.applicationContext)
      try await connection.close(
        AsyncAPICloseSignal(code: .normalClosure, reason: "")
      )
    }

    let app = Application(
      router: Router(),
      server: .http1WebSocketUpgrade(
        webSocketRouter: router,
        configuration: .init(validateUTF8: true)
      )
    )
    try await app.test(.live) { client in
      let close = try await client.ws("/rooms/room-7/events") {
        inbound,
        _,
        _ in
        for try await _ in inbound {}
      }
      XCTAssertEqual(close?.closeCode, .normalClosure)
    }

    let applicationContext = await recorder.applicationContext
    XCTAssertEqual(
      applicationContext,
      FixtureApplicationContext(
        subject: "user_fixture",
        roomID: "room-7"
      )
    )
  }

  func testMissingApplicationContextFailsClosedBeforeHandler() async throws {
    let router = Router(context: AuthorizationWebSocketRequestContext.self)
    let recorder = HandlerInvocationRecorder()
    let transport = HummingbirdWebSocketTransport(
      router: router,
      connectionContext: { request in
        guard let authorization = await request.context.authorization.take()
        else {
          throw FixtureError.missingAuthorization
        }
        return authorization
      }
    )
    try transport.register(
      channel: AsyncAPIChannel(
        name: "events",
        address: "/events",
        parameterNames: []
      )
    ) { _ in
      await recorder.recordInvocation()
    }

    let app = Application(
      router: Router(),
      server: .http1WebSocketUpgrade(
        webSocketRouter: router,
        configuration: .init(validateUTF8: true)
      )
    )
    try await app.test(.live) { client in
      let close = try await client.ws("/events") { inbound, _, _ in
        for try await _ in inbound {}
      }
      XCTAssertEqual(close?.closeCode, .unexpectedServerError)
    }

    let invoked = await recorder.invoked
    XCTAssertFalse(invoked)
  }

  func testRouteBridgesParametersTextBinaryAndCustomClose() async throws {
    let router = Router(context: BasicWebSocketRequestContext.self)
    let transport = HummingbirdWebSocketTransport(router: router)
    let channel = try AsyncAPIChannel(
      name: "roomLive",
      address: "/rooms/{roomId}/live",
      parameterNames: ["roomId"]
    )
    let recorder = ConnectionRecorder()
    try transport.register(channel: channel) { connection in
      await recorder.record(parameters: connection.parameters)
      var iterator = connection.messages.makeAsyncIterator()
      guard let first = try await iterator.next(),
        let second = try await iterator.next()
      else {
        return
      }
      await recorder.record(messages: [first, second])
      try await connection.send(first)
      try await connection.send(second)
      try await connection.close(
        AsyncAPICloseSignal(
          code: AsyncAPICloseCode(rawValue: 3999)!,
          reason: "fixture_complete"
        )
      )
    }

    let app = Application(
      router: Router(),
      server: .http1WebSocketUpgrade(
        webSocketRouter: router,
        configuration: .init(validateUTF8: true)
      )
    )
    try await app.test(.live) { client in
      let close = try await client.ws("/rooms/room-7/live") {
        inbound,
        outbound,
        _ in
        try await outbound.write(.text("hello"))
        try await outbound.write(.binary(ByteBuffer(bytes: [1, 2, 3])))
        var iterator = inbound.messages(maxSize: 1024).makeAsyncIterator()
        let first = try await iterator.next()
        let second = try await iterator.next()
        XCTAssertEqual(first, .text("hello"))
        XCTAssertEqual(second, .binary(ByteBuffer(bytes: [1, 2, 3])))
        while try await iterator.next() != nil {}
      }
      XCTAssertEqual(
        close.map { UInt16(webSocketErrorCode: $0.closeCode) },
        3999
      )
      XCTAssertEqual(close?.reason, "fixture_complete")
    }

    let parameters = await recorder.parameters
    let messages = await recorder.messages
    XCTAssertEqual(parameters, ["roomId": "room-7"])
    XCTAssertEqual(messages, [.text("hello"), .binary([1, 2, 3])])
  }

  func testUpgradePolicyReceivesTypedChannelAndParameters() async throws {
    let router = Router(context: BasicWebSocketRequestContext.self)
    let recorder = UpgradeRecorder()
    let transport = HummingbirdWebSocketTransport(
      router: router,
      upgradePolicy: { request in
        await recorder.record(
          channel: request.channel,
          parameters: request.parameters
        )
        return request.request.uri.queryParameters["allow"] == "1"
          ? .upgrade()
          : .dontUpgrade
      }
    )
    try transport.register(
      channel: AsyncAPIChannel(
        name: "roomLive",
        address: "/rooms/{roomId}/live",
        parameterNames: ["roomId"]
      )
    ) { connection in
      try await connection.close(
        AsyncAPICloseSignal(code: .normalClosure, reason: "")
      )
    }

    let app = Application(
      router: Router(),
      server: .http1WebSocketUpgrade(
        webSocketRouter: router,
        configuration: .init(validateUTF8: true)
      )
    )
    try await app.test(.live) { client in
      do {
        try await client.ws("/rooms/denied/live") { _, _, _ in }
        XCTFail("upgrade should be rejected")
      } catch {}

      let close = try await client.ws("/rooms/allowed/live?allow=1") {
        inbound,
        _,
        _ in
        for try await _ in inbound {}
      }
      XCTAssertEqual(close?.closeCode, .normalClosure)
    }

    let channelName = await recorder.channelName
    let parameters = await recorder.parameters
    XCTAssertEqual(channelName, "roomLive")
    XCTAssertEqual(parameters?["roomId"], "allowed")
  }

  func testPeerCloseCancelsConnectionHandler() async throws {
    let router = Router(context: BasicWebSocketRequestContext.self)
    let recorder = CancellationRecorder()
    let transport = HummingbirdWebSocketTransport(router: router)
    try transport.register(
      channel: AsyncAPIChannel(
        name: "events",
        address: "/events",
        parameterNames: []
      )
    ) { _ in
      try await withTaskCancellationHandler {
        try await Task.sleep(for: .seconds(60))
      } onCancel: {
        Task { await recorder.recordCancellation() }
      }
    }

    let app = Application(
      router: Router(),
      server: .http1WebSocketUpgrade(
        webSocketRouter: router,
        configuration: .init(validateUTF8: true)
      )
    )
    try await app.test(.live) { client in
      _ = try await client.ws("/events") { _, _, _ in }
    }

    for _ in 0..<100 {
      guard !(await recorder.cancelled) else { break }
      try await Task.sleep(for: .milliseconds(10))
    }
    let cancelled = await recorder.cancelled
    XCTAssertTrue(cancelled)
  }

  func testInboundBufferOverflowClosesConnectionAndCancelsHandler() async throws {
    let router = Router(context: BasicWebSocketRequestContext.self)
    let recorder = CancellationRecorder()
    let configuration = try XCTUnwrap(
      AsyncAPIHummingbirdConfiguration(
        maximumMessageSize: 1024,
        inboundMessageBufferCapacity: 1,
        outboundWriteBufferCapacity: 1
      )
    )
    let transport = HummingbirdWebSocketTransport(
      router: router,
      configuration: configuration
    )
    try transport.register(
      channel: AsyncAPIChannel(
        name: "events",
        address: "/events",
        parameterNames: []
      )
    ) { _ in
      try await withTaskCancellationHandler {
        try await Task.sleep(for: .seconds(60))
      } onCancel: {
        Task { await recorder.recordCancellation() }
      }
    }

    let app = Application(
      router: Router(),
      server: .http1WebSocketUpgrade(
        webSocketRouter: router,
        configuration: .init(validateUTF8: true)
      )
    )
    try await app.test(.live) { client in
      let close = try await client.ws("/events") { inbound, outbound, _ in
        try await outbound.write(.text("one"))
        try await outbound.write(.text("two"))
        for try await _ in inbound {}
      }
      XCTAssertEqual(close?.closeCode, .unexpectedServerError)
    }

    let cancelled = await recorder.cancelled
    XCTAssertTrue(cancelled)
  }

  func testOutboundWritesAreBoundedAndCancellationAware() async throws {
    let sink = SuspendedSink()
    let writer = BoundedHummingbirdWebSocketWriter(
      sink: HummingbirdWebSocketSink(
        writeText: { try await sink.write($0) },
        writeBinary: { _ in },
        close: { _ in }
      ),
      maximumMessageSize: 16,
      bufferCapacity: 1
    )

    let first = Task { try await writer.send(.text("first")) }
    await sink.waitUntilWriting()
    let second = Task { try await writer.send(.text("second")) }
    for _ in 0..<100 {
      guard await writer.pendingWriteCount() == 0 else { break }
      await Task.yield()
    }
    let pendingWriteCount = await writer.pendingWriteCount()
    XCTAssertEqual(pendingWriteCount, 1)

    do {
      try await writer.send(.text("third"))
      XCTFail("third write should exceed the bounded queue")
    } catch let error as AsyncAPIHummingbirdError {
      XCTAssertEqual(error, .outboundBufferOverflow(capacity: 1))
    }

    second.cancel()
    await sink.resumeWrites()
    try await first.value
    do {
      try await second.value
      XCTFail("cancelled queued write should fail")
    } catch is CancellationError {}
    let writtenValues = await sink.values
    XCTAssertEqual(writtenValues, ["first"])
  }

  func testCloseWaitsForAcceptedWritesAndRejectsNewWrites() async throws {
    let sink = SuspendedSink()
    let writer = BoundedHummingbirdWebSocketWriter(
      sink: HummingbirdWebSocketSink(
        writeText: { try await sink.write($0) },
        writeBinary: { _ in },
        close: { await sink.close($0) }
      ),
      maximumMessageSize: 16,
      bufferCapacity: 1
    )
    let signal = try AsyncAPICloseSignal(
      code: .serviceRestart,
      reason: "restart"
    )

    let write = Task { try await writer.send(.text("accepted")) }
    await sink.waitUntilWriting()
    let close = Task { try await writer.close(signal) }
    for _ in 0..<100 {
      guard await writer.pendingWriteCount() == 0 else { break }
      await Task.yield()
    }

    do {
      try await writer.send(.text("too late"))
      XCTFail("writes after close begins should fail")
    } catch let error as AsyncAPIHummingbirdError {
      XCTAssertEqual(error, .connectionClosed)
    }

    await sink.resumeWrites()
    try await write.value
    try await close.value
    let writtenValues = await sink.values
    let closeSignals = await sink.closeSignals
    XCTAssertEqual(writtenValues, ["accepted"])
    XCTAssertEqual(closeSignals, [signal])
  }

  func testOutboundLimitCountsUTF8Bytes() async throws {
    let writer = BoundedHummingbirdWebSocketWriter(
      sink: HummingbirdWebSocketSink(
        writeText: { _ in },
        writeBinary: { _ in },
        close: { _ in }
      ),
      maximumMessageSize: 3,
      bufferCapacity: 1
    )

    do {
      try await writer.send(.text("éé"))
      XCTFail("UTF-8 payload over the byte limit should fail")
    } catch let error as AsyncAPIHummingbirdError {
      XCTAssertEqual(error, .outboundMessageTooLarge(limit: 3))
    }
  }

  func testDuplicateChannelAddressIsRejected() throws {
    let router = Router(context: BasicWebSocketRequestContext.self)
    let transport = HummingbirdWebSocketTransport(router: router)
    let channel = try AsyncAPIChannel(
      name: "events",
      address: "/events",
      parameterNames: []
    )
    try transport.register(channel: channel) { _ in }
    XCTAssertThrowsError(try transport.register(channel: channel) { _ in }) {
      XCTAssertEqual(
        $0 as? AsyncAPIHummingbirdError,
        .duplicateChannelAddress("/events")
      )
    }
  }
}

private struct AuthorizationWebSocketRequestContext:
  RequestContext,
  WebSocketRequestContext
{
  var coreContext: CoreRequestContextStorage
  let webSocket: WebSocketHandlerReference<Self>
  let authorization: AuthorizationTransfer

  init(source: ApplicationRequestContextSource) {
    self.coreContext = .init(source: source)
    self.webSocket = .init()
    self.authorization = .init()
  }
}

private struct FixtureApplicationContext: Equatable, Sendable {
  let subject: String
  let roomID: String
}

private enum FixtureError: Error {
  case missingAuthorization
}

private actor AuthorizationTransfer {
  private var value: FixtureApplicationContext?

  func store(_ value: FixtureApplicationContext) {
    precondition(self.value == nil)
    self.value = value
  }

  func take() -> FixtureApplicationContext? {
    defer { value = nil }
    return value
  }
}

private actor ApplicationContextRecorder {
  var applicationContext: FixtureApplicationContext?

  func record(_ applicationContext: FixtureApplicationContext) {
    self.applicationContext = applicationContext
  }
}

private actor HandlerInvocationRecorder {
  var invoked = false

  func recordInvocation() {
    invoked = true
  }
}

private actor ConnectionRecorder {
  var parameters: [String: String]?
  var messages: [AsyncAPITransportMessage]?

  func record(parameters: [String: String]) {
    self.parameters = parameters
  }

  func record(messages: [AsyncAPITransportMessage]) {
    self.messages = messages
  }
}

private actor UpgradeRecorder {
  var channelName: String?
  var parameters: [String: String]?

  func record(channel: AsyncAPIChannel, parameters: [String: String]) {
    channelName = channel.name
    self.parameters = parameters
  }
}

private actor CancellationRecorder {
  var cancelled = false

  func recordCancellation() {
    cancelled = true
  }
}

private actor SuspendedSink {
  private var writing = false
  private var writeWaiter: CheckedContinuation<Void, Never>?
  private var startedWaiters: [CheckedContinuation<Void, Never>] = []
  var values: [String] = []
  var closeSignals: [AsyncAPICloseSignal] = []

  func write(_ value: String) async throws {
    writing = true
    for waiter in startedWaiters { waiter.resume() }
    startedWaiters.removeAll()
    await withCheckedContinuation { continuation in
      writeWaiter = continuation
    }
    values.append(value)
  }

  func waitUntilWriting() async {
    guard !writing else { return }
    await withCheckedContinuation { continuation in
      startedWaiters.append(continuation)
    }
  }

  func resumeWrites() {
    writeWaiter?.resume()
    writeWaiter = nil
  }

  func close(_ signal: AsyncAPICloseSignal) {
    closeSignals.append(signal)
  }
}
