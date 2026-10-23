/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>generate_webhook_signing_key</c> (#6394) returns the new key only over TLS or to a caller on the service's machine: plain HTTP from
/// another address is refused before the store is touched, HTTPS or loopback over HTTP is served, and a call with no HTTP context
/// (in process) is served. The store connections here point at a closed port, so a call that got past the refusal would fail on connect.
/// </summary>
public sealed class McpWebhookSigningKeyTransportTests
{
    private static DefaultHttpContext Context(string scheme, string? remote)
    {
        var context = new DefaultHttpContext();
        context.Request.Scheme = scheme;
        context.Connection.RemoteIpAddress = remote is null ? null : IPAddress.Parse(remote);
        return context;
    }

    private static async Task<string> GenerateAsync(HttpContext? context)
    {
        await using var dead = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=mcp;Password=x;Database=none;Timeout=1;Pooling=false");
        using var key = PasswordPrivateKey.Generate();
        return await DarlingMcpAlertTools.GenerateWebhookSigningKeyAsync(dead, dead, null, false, context, DarlingPasswordKey.FromPrivateKey(key), null, TestContext.Current.CancellationToken);
    }

    private static string Status(string answer) => JsonDocument.Parse(answer).RootElement.GetProperty("status").GetString()!;

    [Theory]
    [InlineData("203.0.113.9")]
    [InlineData("10.1.2.3")]
    [InlineData("2001:db8::1")]
    [InlineData(null)]
    public async Task PlainHttpFromANonLoopbackAddress_IsRefused_AndTheStoreIsNeverTouched(string? remote)
    {
        var answer = await GenerateAsync(Context("http", remote));
        Assert.Equal("refused", Status(answer));
        Assert.Contains("mcp.network.tls", answer, StringComparison.Ordinal);
        Assert.Contains("Nothing was written", answer, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("https", "203.0.113.9")]
    [InlineData("http", "127.0.0.1")]
    [InlineData("http", "::1")]
    [InlineData("http", "::ffff:127.0.0.1")]
    public async Task TlsOrLoopback_IsServed_PastTheTransportCheck(string scheme, string remote)
    {
        /* Served means past the transport check: the dead store connection then fails, which is not the transport refusal. */
        var answer = await GenerateAsync(Context(scheme, remote));
        Assert.DoesNotContain("mcp.network.tls", answer, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnInProcessCall_WithNoHttpContext_IsServed()
    {
        var answer = await GenerateAsync(null);
        Assert.DoesNotContain("mcp.network.tls", answer, StringComparison.Ordinal);
    }
}
