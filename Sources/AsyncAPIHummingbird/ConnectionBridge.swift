import AsyncAPIRuntime
import HummingbirdWebSocket
import NIOCore
import NIOWebSocket

struct HummingbirdWebSocketSink: Sendable {
  let writeText: @Sendable (String) async throws -> Void
  let writeBinary: @Sendable ([UInt8]) async throws -> Void
  let close: @Sendable (AsyncAPICloseSignal) async throws -> Void

  init(outbound: WebSocketOutboundWriter) {
    self.writeText = { try await outbound.writeTextMessage($0) }
    self.writeBinary = {
      try await outbound.writeBinaryMessage(ByteBuffer(bytes: $0))
    }
    self.close = {
      try await outbound.close(
        WebSocketErrorCode(codeNumber: Int($0.code.rawValue)),
        reason: $0.reason
      )
    }
  }

  init(
    writeText: @escaping @Sendable (String) async throws -> Void,
    writeBinary: @escaping @Sendable ([UInt8]) async throws -> Void,
    close: @escaping @Sendable (AsyncAPICloseSignal) async throws -> Void
  ) {
    self.writeText = writeText
    self.writeBinary = writeBinary
    self.close = close
  }
}

actor BoundedHummingbirdWebSocketWriter {
  private enum State {
    case open
    case closing
    case closed
  }

  private struct Waiter {
    let id: UInt64
    let continuation: CheckedContinuation<Void, any Error>
  }

  private let sink: HummingbirdWebSocketSink
  private let maximumMessageSize: Int
  private let bufferCapacity: Int
  private var state = State.open
  private var isWriting = false
  private var nextWaiterID: UInt64 = 0
  private var waiters: [Waiter] = []

  init(
    sink: HummingbirdWebSocketSink,
    maximumMessageSize: Int,
    bufferCapacity: Int
  ) {
    self.sink = sink
    self.maximumMessageSize = maximumMessageSize
    self.bufferCapacity = bufferCapacity
  }

  func send(_ message: AsyncAPITransportMessage) async throws {
    guard case .open = state else {
      throw AsyncAPIHummingbirdError.connectionClosed
    }
    try Self.validate(message, maximumSize: maximumMessageSize)
    try await acquire(force: false)
    do {
      try Task.checkCancellation()
      switch message {
      case .text(let text):
        try await sink.writeText(text)
      case .binary(let bytes):
        try await sink.writeBinary(bytes)
      }
      release()
    } catch is CancellationError {
      release()
      throw CancellationError()
    } catch {
      fail(with: error)
      throw error
    }
  }

  func close(_ signal: AsyncAPICloseSignal) async throws {
    guard case .open = state else {
      throw AsyncAPIHummingbirdError.connectionClosed
    }
    state = .closing
    do {
      try await acquire(force: true)
      try Task.checkCancellation()
      try await sink.close(signal)
      state = .closed
      release()
    } catch {
      fail(with: error)
      throw error
    }
  }

  func finish() {
    guard case .closed = state else {
      state = .closed
      failWaiters(with: AsyncAPIHummingbirdError.connectionClosed)
      return
    }
  }

  func pendingWriteCount() -> Int {
    waiters.count
  }

  private func acquire(force: Bool) async throws {
    try Task.checkCancellation()
    if !isWriting {
      isWriting = true
      return
    }
    guard force || waiters.count < bufferCapacity else {
      throw AsyncAPIHummingbirdError.outboundBufferOverflow(
        capacity: bufferCapacity
      )
    }
    let id = nextWaiterID
    nextWaiterID &+= 1
    try await withTaskCancellationHandler(
      operation: {
        try await withCheckedThrowingContinuation {
          (continuation: CheckedContinuation<Void, any Error>) in
          if Task.isCancelled {
            continuation.resume(throwing: CancellationError())
          } else {
            waiters.append(.init(id: id, continuation: continuation))
          }
        }
      },
      onCancel: {
        Task { await self.cancelWaiter(id: id) }
      })
  }

  private func cancelWaiter(id: UInt64) {
    guard let index = waiters.firstIndex(where: { $0.id == id }) else {
      return
    }
    let waiter = waiters.remove(at: index)
    waiter.continuation.resume(throwing: CancellationError())
  }

  private func release() {
    guard !waiters.isEmpty else {
      isWriting = false
      return
    }
    let waiter = waiters.removeFirst()
    waiter.continuation.resume()
  }

  private func fail(with error: any Error) {
    state = .closed
    isWriting = false
    failWaiters(with: error)
  }

  private func failWaiters(with error: any Error) {
    let current = waiters
    waiters.removeAll(keepingCapacity: false)
    for waiter in current {
      waiter.continuation.resume(throwing: error)
    }
  }

  private static func validate(
    _ message: AsyncAPITransportMessage,
    maximumSize: Int
  ) throws {
    let size =
      switch message {
      case .text(let text): text.utf8.count
      case .binary(let bytes): bytes.count
      }
    guard size <= maximumSize else {
      throw AsyncAPIHummingbirdError.outboundMessageTooLarge(
        limit: maximumSize
      )
    }
  }
}

enum HummingbirdConnectionTaskResult: Sendable {
  case inboundEnded
  case handlerEnded
}

func runHummingbirdConnection<ApplicationContext: Sendable>(
  inbound: WebSocketInboundStream,
  outbound: WebSocketOutboundWriter,
  applicationContext: ApplicationContext,
  parameters: [String: String],
  configuration: AsyncAPIHummingbirdConfiguration,
  handler: @escaping AsyncAPIConnectionHandler<ApplicationContext>
) async throws {
  let (messages, continuation) = AsyncThrowingStream<
    AsyncAPITransportMessage,
    any Error
  >.makeStream(
    bufferingPolicy: .bufferingOldest(
      configuration.inboundMessageBufferCapacity
    )
  )
  let writer = BoundedHummingbirdWebSocketWriter(
    sink: .init(outbound: outbound),
    maximumMessageSize: configuration.maximumMessageSize,
    bufferCapacity: configuration.outboundWriteBufferCapacity
  )
  let connection = AsyncAPIConnection(
    applicationContext: applicationContext,
    parameters: parameters,
    messages: messages,
    send: { try await writer.send($0) },
    close: { try await writer.close($0) }
  )

  do {
    try await withThrowingTaskGroup(of: HummingbirdConnectionTaskResult.self) {
      group in
      group.addTask {
        do {
          for try await message in inbound.messages(
            maxSize: configuration.maximumMessageSize
          ) {
            let transportMessage: AsyncAPITransportMessage =
              switch message {
              case .text(let text): .text(text)
              case .binary(let buffer):
                .binary(Array(buffer.readableBytesView))
              }
            switch continuation.yield(transportMessage) {
            case .enqueued:
              continue
            case .dropped:
              let error = AsyncAPIHummingbirdError.inboundBufferOverflow(
                capacity: configuration.inboundMessageBufferCapacity
              )
              continuation.finish(throwing: error)
              throw error
            case .terminated:
              return .inboundEnded
            @unknown default:
              return .inboundEnded
            }
          }
          continuation.finish()
          return .inboundEnded
        } catch {
          continuation.finish(throwing: error)
          throw error
        }
      }
      group.addTask {
        try await handler(connection)
        return .handlerEnded
      }

      do {
        guard try await group.next() != nil else { return }
        group.cancelAll()
        do {
          while try await group.next() != nil {}
        } catch is CancellationError {
          // The first completed side owns the connection lifetime.
        }
      } catch {
        group.cancelAll()
        throw error
      }
    }
    continuation.finish()
    await writer.finish()
  } catch {
    continuation.finish(throwing: error)
    await writer.finish()
    throw error
  }
}
