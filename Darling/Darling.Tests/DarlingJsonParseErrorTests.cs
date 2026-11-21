/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
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

    /* A lone surrogate is the one input that reaches the ArgumentException arm: the string cannot become UTF-8. */
    private const string LoneSurrogate = "{\"name\":\"\uD800\"}";

    [Fact]
    /* The reader stops on the second character of the bad literal: "[nil]" fails at index 2, so a person counting from 1 reads byte 3. */
    public void Describe_PinsTheOneBasedLineAndByte()
    {
        var ex = Assert.ThrowsAny<JsonException>(() => JsonDocument.Parse("[nil]"));

        Assert.Equal("changes_json is not valid JSON (line 1, byte 3).", JsonParseError.Describe("changes_json", ex));
        Assert.Equal("not valid JSON (line 1, byte 3).", JsonParseError.Bare(ex));
    }

    [Fact]
    public void TagChangesJson_UnpairedSurrogate_GetsTheFixedText()
    {
        var (edit, error, _) = DarlingMcpServerTagTools.ParseChanges(LoneSurrogate);

        Assert.Null(edit);
        Assert.Equal("changes_json is not valid JSON.", error);
    }

    [Fact]
    public void CustomAlertRuleDefinition_BadLiteral_NamesThePosition_NotTheInput()
    {
        var (definition, error) = CustomAlertRuleDefinition.TryParse("{\"metric\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.Null(definition);
        Assert.NotNull(error);
        AssertPositionOnly(error!, "definition");
    }

    [Fact]
    public void CustomAlertRuleDefinition_UnpairedSurrogate_GetsTheFixedText()
    {
        var (definition, error) = CustomAlertRuleDefinition.TryParse(LoneSurrogate);

        Assert.Null(definition);
        Assert.Equal("definition is not valid JSON.", error);
    }

    [Fact]
    public void InvestigationEvidence_BadLiteral_NamesThePosition_NotTheInput()
    {
        var problem = InvestigationToolCore.ValidateEvidence("[nil, \"" + Distinct + "\"]", out var json);

        Assert.Null(json);
        Assert.Equal("not valid JSON (line 1, byte 3).", problem);
    }

    [Fact]
    public void InvestigationQueriesRun_BadLiteral_NamesThePosition_NotTheInput()
    {
        var problem = InvestigationReportFields.Validate("[nil, \"" + Distinct + "\"]", out var json);

        Assert.Null(json);
        Assert.Equal("not valid JSON (line 1, byte 3).", problem);
    }

    [Fact]
    public void InvestigationLinks_BadLiteral_NamesThePosition_NotTheInput()
    {
        var problem = InvestigationReportLinks.Validate("[nil, \"" + Distinct + "\"]", out _);

        Assert.Equal("not valid JSON (line 1, byte 3).", problem);
    }

    [Fact]
    public void WebhookHeaders_BadLiteral_NamesThePosition_NotTheToken()
    {
        var ok = WebhookAlertService.TryParseHeaders("{\"Authorization\":nil, \"x\":\"" + Distinct + "\"}", out _, out var error);

        Assert.False(ok);
        Assert.NotNull(error);
        AssertPositionOnly(error!, "Webhook headers");
    }

    [Fact]
    public void WebhookBodyTemplate_BadLiteral_NamesThePosition_NotTheBody()
    {
        var error = WebhookAlertService.ValidateGenericConfig("", "{\"a\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.NotNull(error);
        AssertPositionOnly(error!, "Webhook body template output");
    }
}
