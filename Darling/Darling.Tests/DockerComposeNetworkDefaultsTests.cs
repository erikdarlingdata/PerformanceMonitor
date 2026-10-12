/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The shipped compose distribution's network defaults. The sample serves plain HTTP (TLS is opt-in), so the
/// only safe out-of-the-box shape publishes both ports on the docker host's loopback address and admits only
/// the docker bridge in <c>allowFrom</c>: a token that crosses a network in the clear is not a default anyone
/// chose. These pins keep a copy-paste "5153:5153" or "0.0.0.0/0" from coming back.
/// </summary>
public sealed class DockerComposeNetworkDefaultsTests
{
    private static string ReadFixture(string name)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", name);
        Assert.True(File.Exists(path), $"{name} was not copied beside the test binary — check the csproj None/Link item.");
        return File.ReadAllText(path);
    }

    [Fact]
    public void ThePorts_ArePublishedOnTheHostsLoopbackAddressOnly()
    {
        var compose = ReadFixture("docker-compose.yml");

        Assert.Contains("\"127.0.0.1:5153:5153\"", compose, StringComparison.Ordinal);
        Assert.Contains("\"127.0.0.1:5152:5152\"", compose, StringComparison.Ordinal);
        Assert.DoesNotContain("\"5153:5153\"", compose, StringComparison.Ordinal);
        Assert.DoesNotContain("\"5152:5152\"", compose, StringComparison.Ordinal);
    }

    [Fact]
    public void TheQuickstart_SaysHowToOpenTheDashboardToANetworkDeliberately()
    {
        var compose = ReadFixture("docker-compose.yml");

        Assert.Contains("http://127.0.0.1:5153", compose, StringComparison.Ordinal);
        Assert.Contains("web.network.tls", compose, StringComparison.Ordinal);
        Assert.Contains("web.network.trustedProxies", compose, StringComparison.Ordinal);
    }

    [Fact]
    public void TheComposeSample_DoesNotAllowEverySource()
    {
        /* The compose distribution's own sample (Darling/compose/darling.sample.json, linked as a fixture),
           not the service's darling.sample.json that lands beside the binary. */
        using var document = JsonDocument.Parse(ReadFixture("compose.darling.sample.json"), new JsonDocumentOptions
        {
            CommentHandling = JsonCommentHandling.Skip,
            AllowTrailingCommas = true,
        });

        foreach (var surface in new[] { "web", "mcp" })
        {
            var network = document.RootElement.GetProperty(surface).GetProperty("network");
            var allowFrom = network.GetProperty("allowFrom").GetString();
            Assert.False(string.IsNullOrWhiteSpace(allowFrom));
            Assert.NotEqual("0.0.0.0/0", allowFrom);
            Assert.NotEqual("::/0", allowFrom);
            /* The listen stays a wildcard on purpose: inside the container a loopback bind is unreachable
               through the published port, and the runtime never falls back to loopback on its own. */
            Assert.Equal("0.0.0.0", network.GetProperty("listen").GetString());
        }
    }
}
