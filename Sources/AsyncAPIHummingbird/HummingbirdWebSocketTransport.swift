import AsyncAPIRuntime
import Foundation
import Hummingbird
import HummingbirdWebSocket

public struct AsyncAPIHummingbirdRequest<Context>: Sendable
where Context: RequestContext, Context: WebSocketRequestContext {
  public let request: Request
  public let context: Context
  public let channel: AsyncAPIChannel
  public let parameters: [String: String]
}

public typealias AsyncAPIHummingbirdUpgradePolicy<Context> =
  @Sendable (AsyncAPIHummingbirdRequest<Context>) async throws ->
  RouterShouldUpgrade
where Context: RequestContext, Context: WebSocketRequestContext

public typealias AsyncAPIHummingbirdConnectionContextFactory<
  Context,
  ApplicationContext: Sendable
> =
  @Sendable (AsyncAPIHummingbirdRequest<Context>) async throws ->
  ApplicationContext
where Context: RequestContext, Context: WebSocketRequestContext

public final class HummingbirdWebSocketTransport<
  Context,
  ApplicationContext: Sendable
>:
  AsyncAPIServerTransport,
  @unchecked Sendable
where Context: RequestContext, Context: WebSocketRequestContext {
  private let router: Router<Context>
  private let configuration: AsyncAPIHummingbirdConfiguration
  private let upgradePolicy: AsyncAPIHummingbirdUpgradePolicy<Context>
  private let connectionContext:
    AsyncAPIHummingbirdConnectionContextFactory<Context, ApplicationContext>
  private let registrationLock = NSLock()
  private var registeredAddresses: Set<String> = []

  public init(
    router: Router<Context>,
    configuration: AsyncAPIHummingbirdConfiguration = .default,
    upgradePolicy: @escaping AsyncAPIHummingbirdUpgradePolicy<Context> = {
      _ in .upgrade()
    },
    connectionContext:
      @escaping AsyncAPIHummingbirdConnectionContextFactory<
        Context,
        ApplicationContext
      >
  ) {
    self.router = router
    self.configuration = configuration
    self.upgradePolicy = upgradePolicy
    self.connectionContext = connectionContext
  }

  public func register(
    channel: AsyncAPIChannel,
    handler: @escaping AsyncAPIConnectionHandler<ApplicationContext>
  ) throws {
    registrationLock.lock()
    defer { registrationLock.unlock() }

    guard registeredAddresses.insert(channel.address).inserted else {
      throw AsyncAPIHummingbirdError.duplicateChannelAddress(channel.address)
    }

    router.ws(
      RouterPath(channel.address),
      shouldUpgrade: { [upgradePolicy] request, context in
        guard
          let parameters = try? Self.extractParameters(
            channel: channel,
            context: context
          )
        else {
          return .dontUpgrade
        }
        return try await upgradePolicy(
          .init(
            request: request,
            context: context,
            channel: channel,
            parameters: parameters
          )
        )
      },
      onUpgrade: {
        [configuration, connectionContext]
        inbound,
        outbound,
        context in
        let parameters = try Self.extractParameters(
          channel: channel,
          context: context.requestContext
        )
        let applicationContext = try await connectionContext(
          .init(
            request: context.request,
            context: context.requestContext,
            channel: channel,
            parameters: parameters
          )
        )
        try await runHummingbirdConnection(
          inbound: inbound,
          outbound: outbound,
          applicationContext: applicationContext,
          parameters: parameters,
          configuration: configuration,
          handler: handler
        )
      }
    )
  }

  private static func extractParameters(
    channel: AsyncAPIChannel,
    context: Context
  ) throws -> [String: String] {
    var parameters: [String: String] = [:]
    parameters.reserveCapacity(channel.parameterNames.count)
    for name in channel.parameterNames {
      guard let value = context.parameters.get(name) else {
        throw AsyncAPIRuntimeError.missingChannelParameter(name)
      }
      parameters[name] = value
    }
    return parameters
  }
}

extension HummingbirdWebSocketTransport where ApplicationContext == Void {
  public convenience init(
    router: Router<Context>,
    configuration: AsyncAPIHummingbirdConfiguration = .default,
    upgradePolicy: @escaping AsyncAPIHummingbirdUpgradePolicy<Context> = {
      _ in .upgrade()
    }
  ) {
    self.init(
      router: router,
      configuration: configuration,
      upgradePolicy: upgradePolicy,
      connectionContext: { _ in () }
    )
  }
}
