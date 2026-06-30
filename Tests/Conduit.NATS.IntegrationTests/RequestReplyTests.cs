using Conduit.NATS.IntegrationTests.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using TUnit.Core;

namespace Conduit.NATS.IntegrationTests;

[ClassDataSource<NatsFixture>(Shared = SharedType.PerTestSession)]
public sealed class RequestReplyTests
{
    private readonly NatsFixture _nats;

    public RequestReplyTests(NatsFixture nats) => _nats = nats;

    [Test]
    [Timeout(60_000)]
    public async Task Request_Gets_Reply_From_Responder(CancellationToken cancellationToken)
    {
        await using var sp = _nats.BuildProvider();
        var bus = sp.GetRequiredService<IMessageBus>();

        await using var sub = await bus.SubscribeAsync<PingMsg>(
            "rpc.ping",
            async ctx => await ctx.RespondAsync(new PongMsg($"{ctx.Message.Value}-pong")),
            cancellationToken: cancellationToken);

        // Core NATS subscriptions are not persisted, so poll until the responder is live.
        PongMsg? response = null;
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(20);
        while (DateTime.UtcNow < deadline && response is null)
        {
            response = await bus.RequestAsync<PingMsg, PongMsg>("rpc.ping", new PingMsg("hi"), TimeSpan.FromSeconds(2), cancellationToken);
            if (response is null)
                await Task.Delay(200, cancellationToken);
        }

        response.ShouldNotBeNull();
        response!.Value.ShouldBe("hi-pong");
    }

    [Test]
    [Timeout(60_000)]
    public async Task Request_With_No_Responder_Returns_Default(CancellationToken cancellationToken)
    {
        await using var sp = _nats.BuildProvider();
        var bus = sp.GetRequiredService<IMessageBus>();

        var response = await bus.RequestAsync<PingMsg, PongMsg>("rpc.no.responder", new PingMsg("x"), TimeSpan.FromSeconds(2), cancellationToken);
        response.ShouldBeNull();
    }
}
