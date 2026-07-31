import AsyncAPIRuntime
import HummingbirdWebSocket
import NIOCore
import NIOWebSocket
import Synchronization

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

final class HummingbirdConnectionByteBudget: Sendable {
  let limit: Int
  private let reservedBytes = Mutex(0)

  init(limit: Int) {
    precondition(limit > 0)
    self.limit = limit
  }

  func reserve(_ byteCount: Int) -> Bool {
    precondition(byteCount >= 0)
    return reservedBytes.withLock { reservedBytes in
      guard byteCount <= limit - reservedBytes else { return false }
      reservedBytes += byteCount
      return true
    }
  }

  func release(_ byteCount: Int) {
    precondition(byteCount >= 0)
    reservedBytes.withLock { reservedBytes in
      precondition(byteCount <= reservedBytes)
      reservedBytes -= byteCount
    }
  }

  var reservedByteCount: Int {
    reservedBytes.withLock { $0 }
  }
}

actor BoundedHummingbirdInboundMessageBuffer {
  enum EnqueueResult: Equatable, Sendable {
    case enqueued
    case terminated
    case countOverflow
    case byteOverflow
  }

  private struct BufferedMessage: Sendable {
    let message: AsyncAPITransportMessage
    let byteCount: Int
  }

  private struct Waiter {
    let id: UInt64
    let continuation:
      CheckedContinuation<
        AsyncAPITransportMessage?,
        any Error
      >
  }

  private enum TerminalState {
    case finished
    case failed(any Error)
  }

  private let capacity: Int
  private let byteBudget: HummingbirdConnectionByteBudget
  private var messages: [BufferedMessage] = []
  private var waiters: [Waiter] = []
  private var terminalState: TerminalState?
  private var nextWaiterID: UInt64 = 0

  init(
    capacity: Int,
    byteBudget: HummingbirdConnectionByteBudget
  ) {
    self.capacity = capacity
    self.byteBudget = byteBudget
  }

  func enqueue(
    _ message: AsyncAPITransportMessage,
    byteCount: Int
  ) -> EnqueueResult {
    guard terminalState == nil else { return .terminated }
    guard waiters.isEmpty else {
      let waiter = waiters.removeFirst()
      waiter.continuation.resume(returning: message)
      return .enqueued
    }
    guard messages.count < capacity else { return .countOverflow }
    guard byteBudget.reserve(byteCount) else { return .byteOverflow }
    messages.append(.init(message: message, byteCount: byteCount))
    return .enqueued
  }

  func next() async throws -> AsyncAPITransportMessage? {
    try Task.checkCancellation()
    if !messages.isEmpty {
      let buffered = messages.removeFirst()
      byteBudget.release(buffered.byteCount)
      return buffered.message
    }
    if let terminalState {
      switch terminalState {
      case .finished:
        return nil
      case .failed(let error):
        throw error
      }
    }

    let id = nextWaiterID
    nextWaiterID &+= 1
    return try await withTaskCancellationHandler(
      operation: {
        let message = try await withCheckedThrowingContinuation {
          (
            continuation: CheckedContinuation<
              AsyncAPITransportMessage?,
              any Error
            >
          ) in
          if Task.isCancelled {
            continuation.resume(throwing: CancellationError())
          } else {
            waiters.append(.init(id: id, continuation: continuation))
          }
        }
        try Task.checkCancellation()
        return message
      },
      onCancel: {
        Task { await self.cancelWaiter(id: id) }
      }
    )
  }

  func finish(throwing error: (any Error)? = nil) {
    guard terminalState == nil else { return }
    terminalState = error.map(TerminalState.failed) ?? .finished

    let bufferedMessages = messages
    messages.removeAll(keepingCapacity: false)
    for buffered in bufferedMessages {
      byteBudget.release(buffered.byteCount)
    }

    let currentWaiters = waiters
    waiters.removeAll(keepingCapacity: false)
    for waiter in currentWaiters {
      if let error {
        waiter.continuation.resume(throwing: error)
      } else {
        waiter.continuation.resume(returning: nil)
      }
    }
  }

  func bufferedMessageCount() -> Int {
    messages.count
  }

  private func cancelWaiter(id: UInt64) {
    guard let index = waiters.firstIndex(where: { $0.id == id }) else {
      return
    }
    let waiter = waiters.remove(at: index)
    waiter.continuation.resume(throwing: CancellationError())
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
    let bufferedByteCount: Int
    let continuation: CheckedContinuation<Void, any Error>
  }

  private let sink: HummingbirdWebSocketSink
  private let maximumMessageSize: Int
  private let bufferCapacity: Int
  private let byteBudget: HummingbirdConnectionByteBudget
  private var state = State.open
  private var isWriting = false
  private var nextWaiterID: UInt64 = 0
  private var waiters: [Waiter] = []

  init(
    sink: HummingbirdWebSocketSink,
    maximumMessageSize: Int,
    bufferCapacity: Int,
    byteBudget: HummingbirdConnectionByteBudget
  ) {
    self.sink = sink
    self.maximumMessageSize = maximumMessageSize
    self.bufferCapacity = bufferCapacity
    self.byteBudget = byteBudget
  }

  func send(_ message: AsyncAPITransportMessage) async throws {
    guard case .open = state else {
      throw AsyncAPIHummingbirdError.connectionClosed
    }
    let byteCount = try Self.validatedByteCount(
      message,
      maximumSize: maximumMessageSize
    )
    try await acquire(force: false, bufferedByteCount: byteCount)
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
      try await acquire(force: true, bufferedByteCount: 0)
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

  private func acquire(
    force: Bool,
    bufferedByteCount: Int
  ) async throws {
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
    guard force || byteBudget.reserve(bufferedByteCount) else {
      throw AsyncAPIHummingbirdError.connectionBufferOverflow(
        limit: byteBudget.limit
      )
    }
    let id = nextWaiterID
    nextWaiterID &+= 1
    try await withTaskCancellationHandler(
      operation: {
        try await withCheckedThrowingContinuation {
          (continuation: CheckedContinuation<Void, any Error>) in
          if Task.isCancelled {
            if !force { byteBudget.release(bufferedByteCount) }
            continuation.resume(throwing: CancellationError())
          } else {
            waiters.append(
              .init(
                id: id,
                bufferedByteCount: force ? 0 : bufferedByteCount,
                continuation: continuation
              ))
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
    byteBudget.release(waiter.bufferedByteCount)
    waiter.continuation.resume(throwing: CancellationError())
  }

  private func release() {
    guard !waiters.isEmpty else {
      isWriting = false
      return
    }
    let waiter = waiters.removeFirst()
    byteBudget.release(waiter.bufferedByteCount)
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
      byteBudget.release(waiter.bufferedByteCount)
      waiter.continuation.resume(throwing: error)
    }
  }

  private static func validatedByteCount(
    _ message: AsyncAPITransportMessage,
    maximumSize: Int
  ) throws -> Int {
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
    return size
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
  let byteBudget = HummingbirdConnectionByteBudget(
    limit: configuration.maximumBufferedBytesPerConnection
  )
  let inboundBuffer = BoundedHummingbirdInboundMessageBuffer(
    capacity: configuration.inboundMessageBufferCapacity,
    byteBudget: byteBudget
  )
  let messages = AsyncThrowingStream<
    AsyncAPITransportMessage,
    any Error
  >(unfolding: { try await inboundBuffer.next() })
  let writer = BoundedHummingbirdWebSocketWriter(
    sink: .init(outbound: outbound),
    maximumMessageSize: configuration.maximumMessageSize,
    bufferCapacity: configuration.outboundWriteBufferCapacity,
    byteBudget: byteBudget
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
            let (transportMessage, byteCount):
              (
                AsyncAPITransportMessage,
                Int
              ) =
                switch message {
                case .text(let text): (.text(text), text.utf8.count)
                case .binary(let buffer):
                  (
                    .binary(Array(buffer.readableBytesView)),
                    buffer.readableBytes
                  )
                }
            switch await inboundBuffer.enqueue(
              transportMessage,
              byteCount: byteCount
            ) {
            case .enqueued:
              continue
            case .countOverflow:
              let error = AsyncAPIHummingbirdError.inboundBufferOverflow(
                capacity: configuration.inboundMessageBufferCapacity
              )
              await inboundBuffer.finish(throwing: error)
              throw error
            case .byteOverflow:
              let error = AsyncAPIHummingbirdError.connectionBufferOverflow(
                limit: configuration.maximumBufferedBytesPerConnection
              )
              await inboundBuffer.finish(throwing: error)
              throw error
            case .terminated:
              return .inboundEnded
            }
          }
          await inboundBuffer.finish()
          return .inboundEnded
        } catch {
          await inboundBuffer.finish(throwing: error)
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
    await inboundBuffer.finish()
    await writer.finish()
  } catch {
    await inboundBuffer.finish(throwing: error)
    await writer.finish()
    throw error
  }
}
