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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>#9013: the metrics config block, its defaults, its parse and its checks. Its refusals live in the host.</summary>
public class MetricsConfigTests
{
    private const string GoodToken = "abcdefghijklmnop1234";
    private const string ShortToken = "abcdefghijklmno";

    private static DarlingConfig Valid()
    {
        return new DarlingConfig { Postgres = new PostgresConfig { ConnectionString = "Host=x" } };
    }

    private static MetricsConfig Block(string metricsJson)
    {
        return DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=x\"},\"metrics\":" + metricsJson + "}").Metrics;
    }

    [Fact]
    public void Defaults_AreOffOn5155WithEmptyListenAndNoToken()
    {
        var metrics = new MetricsConfig();

        Assert.False(metrics.Enabled);
        Assert.Equal(5155, metrics.Port);
        Assert.Equal("", metrics.Listen);
        Assert.Null(metrics.Token);
        Assert.Null(metrics.AllowFrom);
        Assert.False(DarlingConfig.Parse("{}").Metrics.Enabled);
    }

    [Fact]
    public void NullMetrics_ParsesToTheDefaultOffBlock()
    {
        var metrics = DarlingConfig.Parse("{\"postgres\":{\"connectionString\":\"Host=x\"},\"metrics\":null}").Metrics;

        Assert.NotNull(metrics);
        Assert.False(metrics.Enabled);
    }

    [Fact]
    public void ParsesTheBlock()
    {
        var metrics = Block("{\"enabled\":true,\"port\":6001,\"listen\":\"10.0.0.1\",\"token\":\"env:SOME_VAR\",\"allowFrom\":\"10.0.0.0/8\"}");

        Assert.True(metrics.Enabled);
        Assert.Equal(6001, metrics.Port);
        Assert.Equal("10.0.0.1", metrics.Listen);
        Assert.Equal("env:SOME_VAR", metrics.Token);
        Assert.Equal("10.0.0.0/8", metrics.AllowFrom);
    }

    /// <summary>#9013: allowFrom takes the array form web and MCP take, and it yields the same allow list as the comma string.</summary>
    [Fact]
    public void AllowFromArray_ParsesToTheSameAllowListAsTheCommaString()
    {
        var fromArray = Block("{\"enabled\":true,\"token\":\"" + GoodToken + "\",\"allowFrom\":[\"10.0.0.0/8\", \"192.168.1.0/24\"]}");
        var fromString = Block("{\"enabled\":true,\"token\":\"" + GoodToken + "\",\"allowFrom\":\"10.0.0.0/8,192.168.1.0/24\"}");

        Assert.Equal("10.0.0.0/8,192.168.1.0/24", fromArray.AllowFrom);
        var arrayCheck = MetricsConfig.Check(fromArray, 5100, 5200, null);
        var stringCheck = MetricsConfig.Check(fromString, 5100, 5200, null);
        Assert.Empty(arrayCheck.Problems);
        Assert.Equal(2, arrayCheck.AllowList.Count);
        Assert.Equal(stringCheck.AllowList.ToString(), arrayCheck.AllowList.ToString());
    }

    /// <summary>#9013: as on web and MCP, the converter checks shape only. A bad CIDR inside the array loads and is a
    /// metrics.allowFrom problem; an element that is not a string fails the load.</summary>
    [Fact]
    public void AllowFromArray_BadElement_IsAnAllowFromProblem_ANonStringElementFailsTheLoad()
    {
        var block = Block("{\"enabled\":true,\"token\":\"" + GoodToken + "\",\"allowFrom\":[\"10.0.0.0/8\", \"not-a-cidr\"]}");

        var check = MetricsConfig.Check(block, 5100, 5200, null);

        Assert.Contains("metrics.allowFrom", Assert.Single(check.Problems), StringComparison.Ordinal);
        Assert.Null(check.Token);
        Assert.ThrowsAny<JsonException>(() => Block("{\"enabled\":true,\"allowFrom\":[\"10.0.0.0/8\", 5]}"));
    }

    [Theory]
    [InlineData("{\"enabled\":true,\"port\":0,\"token\":\"" + GoodToken + "\"}", "metrics.port")]
    [InlineData("{\"enabled\":true,\"port\":65536,\"token\":\"" + GoodToken + "\"}", "metrics.port")]
    [InlineData("{\"enabled\":true,\"port\":5100,\"token\":\"" + GoodToken + "\"}", "metrics.port")]
    [InlineData("{\"enabled\":true,\"port\":5200,\"token\":\"" + GoodToken + "\"}", "metrics.port")]
    [InlineData("{\"enabled\":true,\"port\":5154,\"token\":\"" + GoodToken + "\"}", "metrics.port")]
    [InlineData("{\"enabled\":true,\"listen\":\"nope\",\"token\":\"" + GoodToken + "\"}", "metrics.listen")]
    [InlineData("{\"enabled\":true}", "metrics.token")]
    [InlineData("{\"enabled\":true,\"token\":\"env:DARLING_TEST_UNSET_9013\"}", "metrics.token")]
    [InlineData("{\"enabled\":true,\"token\":\"file:darling-test-9013-no-such-dir/metrics-token\"}", "metrics.token")]
    [InlineData("{\"enabled\":true,\"token\":\"short\"}", "metrics.token")]
    [InlineData("{\"enabled\":true,\"token\":\"" + GoodToken + "\",\"allowFrom\":\"10.0.0.0/33\"}", "metrics.allowFrom")]
    public void Problems_Cases(string metricsJson, string expected)
    {
        /* Web 5100, MCP 5200 and an enabled health port 5154 are the ports the rows clash with. */
        var check = MetricsConfig.Check(Block(metricsJson), webPort: 5100, mcpPort: 5200, healthPort: 5154);

        Assert.Contains(expected, Assert.Single(check.Problems), StringComparison.Ordinal);
        Assert.Null(check.Token);
    }

    [Fact]
    public void HealthPortClash_OnlyWhenHealthIsEnabled()
    {
        var block = Block("{\"enabled\":true,\"port\":5154,\"token\":\"" + GoodToken + "\"}");

        Assert.Empty(MetricsConfig.Check(block, webPort: 5100, mcpPort: 5200, healthPort: null).Problems);
    }

    [Fact]
    public void DisabledBlock_HasNoProblemsAndNoToken()
    {
        var check = MetricsConfig.Check(Block("{\"enabled\":false,\"port\":0,\"listen\":\"nope\",\"token\":\"short\"}"), 5100, 5200, null);

        Assert.Empty(check.Problems);
        Assert.Null(check.Token);
    }

    [Fact]
    public void ProblemText_NeverCarriesTheTokenValue()
    {
        var check = MetricsConfig.Check(Block("{\"enabled\":true,\"token\":\"" + ShortToken + "\"}"), 5100, 5200, null);

        var problem = Assert.Single(check.Problems);
        Assert.DoesNotContain(ShortToken, problem, StringComparison.Ordinal);

        /* #9013: the floor in the port's own words, not the network wizard's hint for the web and MCP binds. */
        Assert.Contains("metrics port", problem, StringComparison.Ordinal);
        Assert.DoesNotContain("--configure-network", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void GoodBlock_ResolvesTokenAndAllowList()
    {
        var path = Path.Combine(Path.GetTempPath(), "metrics-token-" + Guid.NewGuid().ToString("N") + ".txt");
        File.WriteAllText(path, GoodToken);
        try
        {
            var block = new MetricsConfig { Enabled = true, Token = "file:" + path, AllowFrom = "10.0.0.0/8,192.168.1.0/24" };

            var check = MetricsConfig.Check(block, 5100, 5200, null);

            Assert.Empty(check.Problems);
            Assert.Equal(GoodToken, check.Token);
            Assert.Equal(2, check.AllowList.Count);
            Assert.False(check.TokenIsLiteral);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>#9013: the check holds the resolved token, so its string form must not print it into a log line or a failed
    /// assertion.</summary>
    [Fact]
    public void Check_ToString_DoesNotCarryTheToken()
    {
        var check = MetricsConfig.Check(new MetricsConfig { Enabled = true, Token = GoodToken, AllowFrom = "10.0.0.0/8" }, 5100, 5200, null);
        Assert.Equal(GoodToken, check.Token);

        Assert.DoesNotContain(GoodToken, check.ToString() ?? "", StringComparison.Ordinal);
    }

    [Fact]
    public void LiteralTokenWithBlankAllowFrom_IsLiteralWithNoAddressFilter()
    {
        var block = new MetricsConfig { Enabled = true, Token = GoodToken, AllowFrom = "  " };

        var check = MetricsConfig.Check(block, 5100, 5200, null);

        Assert.Empty(check.Problems);
        Assert.True(check.TokenIsLiteral);
        Assert.Equal(0, check.AllowList.Count);
    }

    [Fact]
    public void MetricsBlock_IsNeverAValidateProblem()
    {
        /* #9013: an optional, default-off endpoint never stops collection, so Validate() does not check the block, on or off. */
        var config = Valid();
        config.Metrics.Enabled = true;
        config.Metrics.Port = config.Web.Port;
        config.Metrics.Listen = "nope";
        config.Metrics.Token = "short";
        config.Metrics.AllowFrom = "10.0.0.0/33";

        Assert.DoesNotContain(config.Validate(), p => p.Contains("metrics.", StringComparison.Ordinal));
        config.Metrics.Enabled = false;
        config.Metrics.Port = 0;

        Assert.DoesNotContain(config.Validate(), p => p.Contains("metrics.", StringComparison.Ordinal));
    }
}
