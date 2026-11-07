/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The in-process host's handshake completes the same way every time (#8134). An SDK client on its defaults opens
/// with a <c>server/discover</c> probe on the newest protocol revision, and the server pins its session to that
/// revision as it reads the probe. A probe the client gives up on, whether its probe timeout fires or the server
/// answers with a protocol error, sends the client on to <c>initialize</c>, and the pinned session refuses that:
/// "Method 'initialize' is not available on protocol version '2026-07-28'". The host sends no probe, so the server
/// never pins a session ahead of the handshake.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpInProcessHostHandshakeTests
{
    /// <summary>
    /// A server that refuses every <c>server/discover</c> stands in for a probe the client abandoned: the SDK's own
    /// metadata filter has already read the probe's protocol version by the time the refusal runs. The host's
    /// handshake completes anyway, and the session serves a tool call.
    /// </summary>
    [Fact]
    public async Task TheHandshake_Completes_WhenTheServerRefusesTheDiscoverProbe()
    {
        var ct = TestContext.Current.CancellationToken;
        McpInProcessHost? host = null;

        try
        {
            host = await McpInProcessHost.StartAsync(
                new[] { typeof(HandshakeProbeTools) }, _ => false, _ => null, installGuard: true, ct,
                configureServer: builder => builder.WithMessageFilters(filters => filters.AddIncomingFilter(RefuseDiscover)));
        }
        catch (McpException ex)
        {
            Assert.Fail($"the handshake did not complete: {ex.Message}");
        }

        Assert.NotNull(host);

        await using (host)
        {
            var tools = await host.Client.ListToolsAsync(cancellationToken: ct);
            Assert.Contains(tools, t => t.Name == "handshake_probe");
        }
    }

    private static McpMessageHandler RefuseDiscover(McpMessageHandler next) =>
        (context, cancellationToken) =>
            context.JsonRpcMessage is JsonRpcRequest { Method: "server/discover" }
                ? throw new McpProtocolException("The client gave up on this probe.", McpErrorCode.InternalError)
                : next(context, cancellationToken);
}

/// <summary>One tool with no arguments, so the handshake test registers a tool without any service.</summary>
[McpServerToolType]
internal static class HandshakeProbeTools
{
    [McpServerTool(Name = "handshake_probe"), Description("Answers the word probe.")]
    public static string Probe() => "probe";
}
