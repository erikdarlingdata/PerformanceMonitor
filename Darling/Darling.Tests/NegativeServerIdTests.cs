/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Store server ids are SIGNED FNV-1a hashes, so about half are negative. Only 0 (or null) means "no id"; a
/// negative id must key a silence, a snooze, a self-alert context and an MCP update exactly as a positive one
/// does, or the silence is lost on that half of the fleet.
/// </summary>
public sealed class NegativeServerIdTests
{
    private const int NegativeId = -616758012;
    private static readonly DateTime Now = new(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static ViewerAlertRow Row(int serverId) => new()
    {
        AlertTime = Now,
        ServerId = serverId,
        ServerName = "host1",
        MetricName = "High CPU",
        CurrentValue = 1,
        ThresholdValue = 1,
        AlertSent = false,
        NotificationType = "tray",
        Muted = false,
    };

    [Fact]
    public void ToMuteContext_CarriesANegativeId_AndZeroMapsToNull()
    {
        Assert.Equal(NegativeId, Row(NegativeId).ToMuteContext().ServerId);
        Assert.Null(Row(0).ToMuteContext().ServerId);
    }

    [Fact]
    public void ASilenceOnANegativeIdServer_SuppressesThatServersToast()
    {
        var rule = ViewerDataService.BuildServerSilenceRule(NegativeId, "host1");

        Assert.Equal(NegativeId, rule.ServerId);
        Assert.True(rule.MatchesAt(Row(NegativeId).ToMuteContext(), Now));
        Assert.False(rule.MatchesAt(Row(5).ToMuteContext(), Now));
    }

    [Fact]
    public void TraySnooze_OnANegativeId_IsIdKeyed_AndZeroIsNameKeyed()
    {
        var rule = ViewerDataService.BuildTraySnoozeRule("host1", "High CPU", TimeSpan.FromHours(1), Now, NegativeId);
        Assert.Equal(NegativeId, rule.ServerId);
        Assert.True(rule.MatchesAt(Row(NegativeId).ToMuteContext(), Now));

        Assert.Null(ViewerDataService.BuildTraySnoozeRule("host1", "High CPU", TimeSpan.FromHours(1), Now, 0).ServerId);
    }

    [Fact]
    public void MuteThis_KeysOnAnyNonZeroRowId()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AlertsHistoryTab.xaml.cs");
        Assert.Contains("context.ServerId is not (null or 0)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("context.ServerId is > 0", source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("-616758012", -616758012)]
    [InlineData("-5", -5)]
    public void SelfAlert_ANegativeServerKey_GivesTheId(string key, int expected) =>
        Assert.Equal(expected, DarlingSelfAlertEvaluator.ServerIdFromKey(key));

    [Theory]
    [InlineData("0")]
    [InlineData("-0")]
    [InlineData(" 1")]
    [InlineData("1 ")]
    [InlineData("+5")]
    [InlineData("store")]
    [InlineData("compressjob:12")]
    [InlineData("cost:3:x")]
    public void SelfAlert_ZeroAndFleetKeys_GiveNull(string key) =>
        Assert.Null(DarlingSelfAlertEvaluator.ServerIdFromKey(key));

    private static Task<string?> Known(int id) => Task.FromResult<string?>(id == NegativeId ? "Display Neg" : null);

    [Fact]
    public async Task UpdateMuteRule_AcceptsAKnownNegativeId_AndRefusesZero()
    {
        var store = new FakeMuteRuleStore().Seed(new MuteRule { Id = "r1", ServerName = "n", CreatedAtUtc = DateTime.UtcNow });

        var ok = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"server_id\":-616758012}", Known);
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(ok));
        Assert.Equal(NegativeId, store.Row("r1")!.ServerId);

        var zero = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "r1", "{\"server_id\":0}", Known);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(zero));
        Assert.Contains("non-zero", zero, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateMuteRule_AcceptsAKnownNegativeId()
    {
        var store = new FakeMuteRuleStore();
        var result = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"server_id\":-616758012}", Known);
        Assert.Equal("created", DarlingMcpTestData.StatusOf(result));
    }

    [Fact]
    public void PlanUnsilence_ARetry_DoesNotDuplicateAnExistingIdKeyedSilence()
    {
        var servers = new[] { (7, "host1"), (8, "host1"), (9, "host1") };
        var legacy = new MuteRule { Id = "legacy", ServerName = "host1", Enabled = true };
        var existing = new MuteRule { Id = "idk", ServerId = 8, ServerName = "host1", Enabled = true };

        var plan = ViewerDataService.PlanUnsilence(new[] { legacy, existing }, 7, "host1", servers);

        Assert.Contains("legacy", plan.DeleteRuleIds);
        Assert.Single(plan.CreateRules);
        Assert.Equal(9, plan.CreateRules[0].ServerId);
    }

    [Theory]
    [InlineData("disabled")]
    [InlineData("expired")]
    public void PlanUnsilence_ADisabledOrExpiredIdKeyedSilence_DoesNotStandInForTheReplacement(string state)
    {
        /* An id-keyed silence that no longer mutes anything cannot stand in for the legacy rule being deleted:
           server 8 would be unsilenced. Only an enabled, unexpired one counts as "already silenced". */
        var now = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
        var servers = new[] { (7, "host1"), (8, "host1") };
        var legacy = new MuteRule { Id = "legacy", ServerName = "host1", Enabled = true };
        var stale = new MuteRule
        {
            Id = "stale", ServerId = 8, ServerName = "host1",
            Enabled = state != "disabled",
            ExpiresAtUtc = state == "expired" ? now.AddMinutes(-1) : null,
        };

        var plan = ViewerDataService.PlanUnsilence(new[] { legacy, stale }, 7, "host1", servers, now);

        Assert.Contains("legacy", plan.DeleteRuleIds);
        var created = Assert.Single(plan.CreateRules);
        Assert.Equal(8, created.ServerId);
    }

    [Fact]
    public void Summary_AnIdKeyedRuleWithAName_ShowsTheId()
    {
        Assert.Equal("on host1 (#7)", new MuteRule { ServerId = 7, ServerName = "host1" }.Summary);
        Assert.Equal("on server #7", new MuteRule { ServerId = 7 }.Summary);
        Assert.Equal("on host1", new MuteRule { ServerName = "host1" }.Summary);
    }
}
