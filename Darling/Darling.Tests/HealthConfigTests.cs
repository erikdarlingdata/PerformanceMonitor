/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#8184: the health config block, its defaults and its parse. Its refusals live in the host (ruling R9, D-validate):
/// <see cref="DarlingHealthHostTests"/>.</summary>
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
        var health = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=x\"},\"health\":{\"enabled\":true,\"port\":6000,\"listen\":\"10.0.0.1\"}}").Health;

        Assert.True(health.Enabled);
        Assert.Equal(6000, health.Port);
        Assert.Equal("10.0.0.1", health.Listen);
    }

    /// <summary>#8184 I2: <c>"health": null</c> is the absent block (the #5460 rule), never a null that faults the start.</summary>
    [Fact]
    public void NullHealth_ParsesToTheDefaultOffBlock()
    {
        var health = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=x\"},\"health\":null}").Health;

        Assert.NotNull(health);
        Assert.False(health.Enabled);
        Assert.Equal(5154, health.Port);
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

    [Fact]
    public void HealthBlock_IsNeverAValidateProblem()
    {
        /* #8184 ruling R9: an optional, default-off endpoint never stops collection, so Validate() does not check the block, on or off. */
        var config = Valid();
        config.Health.Enabled = true;
        config.Health.Port = config.Web.Port;
        config.Health.Listen = "nope";

        Assert.DoesNotContain(config.Validate(), p => p.Contains("health.", StringComparison.Ordinal));
        config.Health.Enabled = false;
        config.Health.Port = 0;

        Assert.DoesNotContain(config.Validate(), p => p.Contains("health.", StringComparison.Ordinal));
    }
}
