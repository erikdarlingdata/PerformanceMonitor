/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Net;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#8184: the health config block, its defaults, and its validation.</summary>
public class HealthConfigTests
{
    private static DarlingConfig Valid()
    {
        return new DarlingConfig { Postgres = new PostgresConfig { ConnectionString = "Host=x" } };
    }

    [Fact]
    public void Defaults_AreOffOn5154WithEmptyListen()
    {
        var health = new DarlingConfig().Health;

        Assert.False(health.Enabled);
        Assert.Equal(5154, health.Port);
        Assert.Equal("", health.Listen);
    }

    [Fact]
    public void ParsesTheBlock()
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-health-{Guid.NewGuid():N}.json");
        File.WriteAllText(path, "{\"postgres\":{\"connectionString\":\"Host=x\"},\"health\":{\"enabled\":true,\"port\":6000,\"listen\":\"10.0.0.1\"}}");
        try
        {
            var health = DarlingConfig.Load(path).Health;

            Assert.True(health.Enabled);
            Assert.Equal(6000, health.Port);
            Assert.Equal("10.0.0.1", health.Listen);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("", true, "0.0.0.0")]
    [InlineData("", false, "127.0.0.1")]
    [InlineData("127.0.0.1", true, "127.0.0.1")]
    [InlineData("::", false, "::")]
    [InlineData("not-an-ip", true, null)]
    public void ResolveListenAddress_Cases(string listen, bool inContainer, string? expected)
    {
        var address = HealthConfig.ResolveListenAddress(listen, inContainer);

        Assert.Equal(expected is null ? null : IPAddress.Parse(expected), address);
    }

    [Theory]
    [InlineData("web")]
    [InlineData("mcp")]
    public void HealthPortClash_IsAProblem(string surface)
    {
        var config = Valid();
        config.Health.Enabled = true;
        config.Health.Port = surface == "web" ? config.Web.Port : config.Mcp.Port;

        var problem = Assert.Single(config.Validate());

        Assert.Contains("health.port", problem);
        Assert.Contains($"{surface}.port", problem);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(65536)]
    public void HealthPortOutOfRange_IsAProblem(int port)
    {
        var config = Valid();
        config.Health.Enabled = true;
        config.Health.Port = port;

        var problem = Assert.Single(config.Validate());

        Assert.Contains("health.port", problem);
        Assert.Contains("1 and 65535", problem);
    }

    [Fact]
    public void HealthListenUnparsable_IsAProblem()
    {
        var config = Valid();
        config.Health.Enabled = true;
        config.Health.Listen = "nope";

        var problem = Assert.Single(config.Validate());

        Assert.Contains("health.listen", problem);
    }

    [Fact]
    public void HealthDisabled_IsNeverAProblem()
    {
        var config = Valid();
        config.Health.Enabled = false;
        config.Health.Port = 0;
        config.Health.Listen = "nope";

        Assert.DoesNotContain(config.Validate(), p => p.Contains("health.", StringComparison.Ordinal));
    }
}
