/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>#8737: <c>mcp.personalTokens</c>: whether a service serves personal tokens, and with which limits.</summary>
public sealed class DarlingPersonalTokenSettingsTests
{
    private static DarlingConfig Served(McpPersonalTokensConfig? tokens = null) => new()
    {
        Web = new WebConfig
        {
            Enabled = true,
            Network = new WebNetworkConfig { Oidc = new WebOidcConfig { Authority = "https://idp.example", ClientId = "c" } },
        },
        Mcp = new McpConfig { PersonalTokens = tokens ?? new McpPersonalTokensConfig { Enabled = true } },
    };

    private static (PersonalTokenSettings? Settings, DarlingSelfAlertTests.CapturingLogger Log) Resolve(DarlingConfig config)
    {
        var log = new DarlingSelfAlertTests.CapturingLogger();
        return (DarlingPersonalTokenSettings.Resolve(config, log), log);
    }

    private static int Warnings(DarlingSelfAlertTests.CapturingLogger log) => log.Entries.Count(e => e.Level == LogLevel.Warning);

    [Fact]
    public void Resolve_Off_ByDefault()
    {
        var (settings, log) = Resolve(new DarlingConfig());

        Assert.Null(settings);
        Assert.Empty(log.Entries);
    }

    /// <summary>I4: an admin's personal token is a reader unless the operator opts in with <c>adminRole: operator</c>.</summary>
    [Fact]
    public void Resolve_Served_WithOidcWebAndStore_AdminsGetReaderByDefault()
    {
        var (settings, log) = Resolve(Served());

        Assert.Equal(new PersonalTokenSettings(30, 5, "reader", null), settings);
        Assert.False(settings!.AdminMayMintOperator);
        Assert.Empty(log.Entries);
    }

    [Fact]
    public void Resolve_OperatorAdminRole_IsOptIn()
    {
        var (settings, log) = Resolve(Served(new McpPersonalTokensConfig { Enabled = true, AdminRole = "operator" }));

        Assert.True(settings!.AdminMayMintOperator);
        Assert.Empty(log.Entries);
    }

    [Fact]
    public void TheSampleFile_LeavesAdminsReaderTokens()
    {
        var sample = DarlingConfig.Parse(System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "darling.sample.json")));

        Assert.Equal("reader", sample.Mcp.PersonalTokens!.AdminRole);
    }

    [Theory]
    [InlineData("noOidc")]
    [InlineData("webOff")]
    [InlineData("frontDoor")]
    [InlineData("tokenlessLoopback")]
    public void Resolve_NotServed_LogsOneWarning(string refusal)
    {
        var config = Served();
        switch (refusal)
        {
            case "noOidc":
                config.Web.Network!.Oidc = null;
                break;
            case "webOff":
                config.Web.Enabled = false;
                break;
            case "frontDoor":
                config.FrontDoor.Enabled = true;
                break;
            default:
                config.Mcp.AllowTokenlessLoopback = true;
                break;
        }

        var (settings, log) = Resolve(config);

        Assert.Null(settings);
        Assert.Equal(1, Warnings(log));
        Assert.Contains("mcp.personalTokens", log.Entries.Single().Message, StringComparison.Ordinal);
        Assert.NotEmpty(DarlingPersonalTokenSettings.Problems(config));
    }

    [Theory]
    [InlineData("maxLifetimeDays", 0, 1)]
    [InlineData("maxLifetimeDays", 400, 365)]
    [InlineData("maxTokensPerPerson", 0, 1)]
    [InlineData("maxTokensPerPerson", 99, 50)]
    public void Resolve_OutOfRangeNumber_IsClampedWithAWarning(string setting, int given, int served)
    {
        var tokens = new McpPersonalTokensConfig { Enabled = true };
        if (setting == "maxLifetimeDays")
        {
            tokens.MaxLifetimeDays = given;
        }
        else
        {
            tokens.MaxTokensPerPerson = given;
        }

        var (settings, log) = Resolve(Served(tokens));

        Assert.NotNull(settings);
        Assert.Equal(served, setting == "maxLifetimeDays" ? settings!.MaxLifetimeDays : settings!.MaxTokensPerPerson);
        Assert.Equal(1, Warnings(log));
        Assert.Contains(setting, log.Entries.Single().Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolve_UnknownAdminRole_ServesReader()
    {
        var (settings, log) = Resolve(Served(new McpPersonalTokensConfig { Enabled = true, AdminRole = "admin" }));

        Assert.Equal("reader", settings!.AdminRole);
        Assert.False(settings.AdminMayMintOperator);
        Assert.Equal(1, Warnings(log));
    }

    [Fact]
    public void Resolve_ReaderAdminRole_ServesReaderQuietly()
    {
        var (settings, log) = Resolve(Served(new McpPersonalTokensConfig { Enabled = true, AdminRole = "reader" }));

        Assert.False(settings!.AdminMayMintOperator);
        Assert.Empty(log.Entries);
    }

    [Fact]
    public void Resolve_DataDetail_IsParsedToTheMcpLevel()
    {
        var (settings, _) = Resolve(Served(new McpPersonalTokensConfig { Enabled = true, DataDetail = "maskLiterals" }));

        Assert.Equal(McpDataDetail.MaskLiterals, settings!.DataDetail);
    }

    [Fact]
    public void Resolve_BadDataDetail_StaysOff()
    {
        var config = Served(new McpPersonalTokensConfig { Enabled = true, DataDetail = "everything" });

        var (settings, log) = Resolve(config);

        Assert.Null(settings);
        Assert.Equal(1, Warnings(log));
        Assert.NotEmpty(DarlingPersonalTokenSettings.Problems(config));
    }

    [Fact]
    public void Problems_AreEmpty_ForAServedConfigAndForAnOffOne()
    {
        Assert.Empty(DarlingPersonalTokenSettings.Problems(Served()));
        Assert.Empty(DarlingPersonalTokenSettings.Problems(new DarlingConfig()));
    }

    [Fact]
    public void UnknownKeys_FlagsTypoInsidePersonalTokens()
    {
        var config = DarlingConfig.Parse("""{ "mcp": { "personalTokens": { "enabled": true, "maxLifetimeDay": 3 } } }""");

        Assert.Equal(new[] { "mcp.personalTokens.maxLifetimeDay" }, config.UnknownNetworkKeys);
    }

    [Fact]
    public void Parse_ReadsThePersonalTokensBlock()
    {
        var config = DarlingConfig.Parse("""{ "mcp": { "personalTokens": { "enabled": true, "maxLifetimeDays": 7, "maxTokensPerPerson": 2, "adminRole": "reader", "dataDetail": "metadataOnly" } } }""");

        var tokens = config.Mcp.PersonalTokens!;
        Assert.True(tokens.Enabled);
        Assert.Equal(7, tokens.MaxLifetimeDays);
        Assert.Equal(2, tokens.MaxTokensPerPerson);
        Assert.Equal("reader", tokens.AdminRole);
        Assert.Equal("metadataOnly", tokens.DataDetail);
    }
}
