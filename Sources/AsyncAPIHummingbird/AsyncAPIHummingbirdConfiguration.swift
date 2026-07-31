public struct AsyncAPIHummingbirdConfiguration: Equatable, Sendable {
  public static let `default` = Self(
    uncheckedMaximumMessageSize: 1 << 20,
    inboundMessageBufferCapacity: 16,
    outboundWriteBufferCapacity: 16,
    maximumBufferedBytesPerConnection: 1 << 20
  )

  public let maximumMessageSize: Int
  public let inboundMessageBufferCapacity: Int
  public let outboundWriteBufferCapacity: Int
  public let maximumBufferedBytesPerConnection: Int

  private init(
    uncheckedMaximumMessageSize maximumMessageSize: Int,
    inboundMessageBufferCapacity: Int,
    outboundWriteBufferCapacity: Int,
    maximumBufferedBytesPerConnection: Int
  ) {
    self.maximumMessageSize = maximumMessageSize
    self.inboundMessageBufferCapacity = inboundMessageBufferCapacity
    self.outboundWriteBufferCapacity = outboundWriteBufferCapacity
    self.maximumBufferedBytesPerConnection =
      maximumBufferedBytesPerConnection
  }

  public init?(
    maximumMessageSize: Int = 1 << 20,
    inboundMessageBufferCapacity: Int = 16,
    outboundWriteBufferCapacity: Int = 16,
    maximumBufferedBytesPerConnection: Int = 1 << 20
  ) {
    guard maximumMessageSize > 0,
      inboundMessageBufferCapacity > 0,
      outboundWriteBufferCapacity > 0,
      maximumBufferedBytesPerConnection > 0
    else {
      return nil
    }
    self.init(
      uncheckedMaximumMessageSize: maximumMessageSize,
      inboundMessageBufferCapacity: inboundMessageBufferCapacity,
      outboundWriteBufferCapacity: outboundWriteBufferCapacity,
      maximumBufferedBytesPerConnection: maximumBufferedBytesPerConnection
    )
  }
}
