public enum AsyncAPIHummingbirdError: Error, Equatable, Sendable {
  case duplicateChannelAddress(String)
  case inboundBufferOverflow(capacity: Int)
  case outboundBufferOverflow(capacity: Int)
  case outboundMessageTooLarge(limit: Int)
  case connectionClosed
}
