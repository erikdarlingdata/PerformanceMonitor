/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9245: malformed JSON from a caller gets an error that names the line and byte where it broke, and nothing
/// from the input. The bad token is followed by a distinctive value that System.Text.Json's own message would
/// have quoted back.
/// </summary>
public sealed class DarlingJsonParseErrorTests
{
    private const string Distinct = "Zq7-distinct-9";

    private static void AssertPositionOnly(string answer, string what)
    {
        Assert.Contains(what + " is not valid JSON (line ", answer, System.StringComparison.Ordinal);
        Assert.Matches(@"\(line \d+, byte \d+\)", answer);
        Assert.DoesNotContain(Distinct, answer, System.StringComparison.Ordinal);
        Assert.DoesNotContain("nil", answer, System.StringComparison.Ordinal);
    }

    [Fact]
    public void ServersJson_BadLiteral_NamesThePosition_NotTheInput()
    {
        var (entries, invalid, wholeError) = DarlingMcpServerAdminTools.ParseRequest(
            "[{\"host\":\"sql01\",\"password\":nil, \"x\":\"" + Distinct + "\"}]");

        Assert.Empty(entries);
        Assert.Empty(invalid);
        Assert.NotNull(wholeError);
        AssertPositionOnly(wholeError!, "servers_json");
    }

    [Fact]
    public void ServersJson_PositionIsCountedFromOne_AndFollowsTheLine()
    {
        var (_, _, wholeError) = DarlingMcpServerAdminTools.ParseRequest("[\n  {\"host\":\"sql01\",\n  \"password\":nil, \"x\":\"" + Distinct + "\"}]");

        Assert.NotNull(wholeError);
        Assert.Contains("(line 3, byte ", wholeError!, System.StringComparison.Ordinal);
        Assert.DoesNotContain(Distinct, wholeError!, System.StringComparison.Ordinal);
    }

    [Fact]
    public void TagChangesJson_BadLiteral_NamesThePosition_NotTheInput()
    {
        var (edit, error, _) = DarlingMcpServerTagTools.ParseChanges("{\"name\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.Null(edit);
        Assert.NotNull(error);
        AssertPositionOnly(error!, "changes_json");
    }

    [Fact]
    public async Task MuteRuleCreateBody_BadLiteral_NamesThePosition_NotTheInput()
    {
        var answer = await DarlingMcpAlertTools.CreateMuteRuleCore(new FakeMuteRuleStore(), "{\"reason\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(answer));
        AssertPositionOnly(answer, "the request body");
    }

    [Fact]
    public async Task MuteRuleUpdateChanges_BadLiteral_NamesThePosition_NotTheInput()
    {
        var answer = await DarlingMcpAlertTools.UpdateMuteRuleCore(new FakeMuteRuleStore(), "rule-1", "{\"reason\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(answer));
        AssertPositionOnly(answer, "changes_json");
    }

    /* The two reads below parse before they touch the store, so a data source that never connects reaches them. */
    [Fact]
    public async Task AlertSettingsJson_BadLiteral_NamesThePosition_NotTheInput()
    {
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none");

        var answer = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, "{\"cpu\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(answer));
        AssertPositionOnly(answer, "settings_json");
    }

    [Fact]
    public async Task CustomViewSpec_BadLiteral_NamesThePosition_NotTheInput()
    {
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none");

        var answer = await DarlingMcpCustomViewTools.RunCustomViewPanel(postgres, "{\"panel\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(answer));
        AssertPositionOnly(answer, "spec");
    }
}
