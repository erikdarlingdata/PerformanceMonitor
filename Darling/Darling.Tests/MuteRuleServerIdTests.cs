/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// F14: a mute rule that carries a server's store id matches on the id, not the display name. A blank
/// display name falls back to the host, so two databases on one Azure SQL Database logical server share a
/// name; a name-keyed silence on one silenced both. A rule WITHOUT an id keeps the name match it always had,
/// so every stored rule keeps its effect.
/// </summary>
public sealed class MuteRuleServerIdTests
{
    private static readonly DateTime Now = new(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

    private static AlertMuteContext Ctx(string name, int? id, string metric = "High CPU") =>
        new() { ServerName = name, ServerId = id, MetricName = metric };

    [Fact]
    public void IdKeyedRule_MatchesItsOwnId_AndNotASameNamedSibling()
    {
        var rule = new MuteRule { ServerId = 7, ServerName = "host1" };

        Assert.True(rule.MatchesAt(Ctx("host1", 7), Now));
        Assert.False(rule.MatchesAt(Ctx("host1", 8), Now));
    }

    [Fact]
    public void IdKeyedRule_IgnoresTheNameOnceTheIdMatches()
    {
        /* The name is only a label: a rename must not un-silence the server. */
        var rule = new MuteRule { ServerId = 7, ServerName = "host1" };

        Assert.True(rule.MatchesAt(Ctx("renamed", 7), Now));
    }

    [Fact]
    public void IdKeyedRule_DoesNotMatchAContextWithoutAnId()
    {
        var rule = new MuteRule { ServerId = 7, ServerName = "host1" };

        Assert.False(rule.MatchesAt(Ctx("host1", null), Now));
    }

    [Fact]
    public void IdOnlyRule_StillRequiresTheId()
    {
        var rule = new MuteRule { ServerId = 7 };

        Assert.True(rule.MatchesAt(Ctx("anything", 7), Now));
        Assert.False(rule.MatchesAt(Ctx("anything", 8), Now));
    }

    [Fact]
    public void LegacyNameRule_StillMatchesEveryServerWithThatName()
    {
        var rule = new MuteRule { ServerName = "host1" };

        Assert.True(rule.MatchesAt(Ctx("host1", 7), Now));
        Assert.True(rule.MatchesAt(Ctx("HOST1", 8), Now));
        Assert.True(rule.MatchesAt(Ctx("host1", null), Now));
        Assert.False(rule.MatchesAt(Ctx("host2", 7), Now));
    }

    [Fact]
    public void IdKeyedRule_StillHonoursItsOtherDimensions()
    {
        var rule = new MuteRule { ServerId = 7, ServerName = "host1", MetricName = "Deadlocks Detected" };

        Assert.False(rule.MatchesAt(Ctx("host1", 7, "High CPU"), Now));
        Assert.True(rule.MatchesAt(Ctx("host1", 7, "Deadlocks Detected"), Now));
    }

    [Fact]
    public void IdOnlyRule_IsNotMatchesEveryAlert_AndNamesTheId()
    {
        var rule = new MuteRule { ServerId = 7 };

        Assert.False(rule.MatchesEveryAlert);
        Assert.Equal("on server #7", rule.Summary);
    }

    [Fact]
    public void IdKeyedRuleWithAName_SummaryReadsTheNameAndId()
    {
        Assert.Equal("on host1 (#7)", new MuteRule { ServerId = 7, ServerName = "host1" }.Summary);
    }

    [Fact]
    public void NamedRule_SummaryIsUnchanged()
    {
        Assert.Equal("High CPU, on host1, db≈Sales",
            new MuteRule { MetricName = "High CPU", ServerName = "host1", DatabasePattern = "Sales" }.Summary);
        Assert.Equal("(matches all alerts)", new MuteRule().Summary);
    }

    [Fact]
    public void Clone_CopiesServerId()
    {
        Assert.Equal(7, new MuteRule { ServerId = 7, ServerName = "host1" }.Clone().ServerId);
    }

    private static AlertServerSnapshot WithId(int? id) =>
        new("101", "SRV-A", true, null, null, false, false, null) { ServerId = id };

    [Fact]
    public void Snapshot_CarriesAnOptionalServerId_DefaultingToNull()
    {
        Assert.Null(new AlertServerSnapshot("1", "n", true, null, null, false, false, null).ServerId);
        Assert.Equal(7, WithId(7).ServerId);
    }

    [Fact]
    public void EveryMuteContextInTheEngine_SetsServerId()
    {
        var source = ReadRepoFileLf("PerformanceMonitor.Alerting", "AlertEngine.cs");
        var sites = Regex.Matches(source, @"new AlertMuteContext\b[^;]*;");

        Assert.True(sites.Count >= 14, "expected the engine's mute-context sites to be found");
        foreach (Match site in sites)
        {
            Assert.Contains("ServerId = ", site.Value);
        }
    }

    [Fact]
    public void EveryMuteContextInDarlingProducers_SetsServerId()
    {
        foreach (var file in new[] { "DarlingWorker.cs", "CustomAlertEvaluator.cs" })
        {
            var source = ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", file);
            var sites = Regex.Matches(source, @"new AlertMuteContext\b[^;]*;", RegexOptions.Singleline);

            Assert.NotEmpty(sites);
            foreach (Match site in sites)
            {
                Assert.Contains("ServerId = ", site.Value);
            }
        }
    }

    [Theory]
    [InlineData("42", 42)]
    [InlineData("101", 101)]
    [InlineData("-5", -5)]
    public void SelfAlert_ANumericServerKey_GivesTheId(string key, int expected) =>
        Assert.Equal(expected, DarlingSelfAlertEvaluator.ServerIdFromKey(key));

    [Theory]
    [InlineData("store")]
    [InlineData("compressjob:12")]
    [InlineData("storejob:5")]
    [InlineData("cost:3:collector")]
    [InlineData("")]
    [InlineData("1 ")]
    public void SelfAlert_AFleetLevelKey_GivesNull(string key) =>
        Assert.Null(DarlingSelfAlertEvaluator.ServerIdFromKey(key));

    [Fact]
    public void RepeatBudget_TwoSameNamedServers_FoldIndependently()
    {
        const string metric = "Failed Agent Job";
        var window = TimeSpan.FromMinutes(15);
        var budget = new RepeatDeliveryBudget();
        // The fingerprint hashes the server NAME, so two same-named servers carry the same one.
        var fp = new[] { "samefingerprint0123456789" };

        IncidentCooldown.Decision Repeat(DateTime at) => new(true, new[] { "k" }, at, fp, false);

        var carrier = budget.Evaluate(metric, "host1", Repeat(Now), window, true, serverKey: "1");
        Assert.True(carrier.ShouldSend);
        budget.Commit(carrier);

        Assert.False(budget.Evaluate(metric, "host1", Repeat(Now.AddMinutes(1)), window, true, serverKey: "1").ShouldSend);
        var second = budget.Evaluate(metric, "host1", Repeat(Now.AddMinutes(2)), window, true, serverKey: "2");
        Assert.False(second.ShouldSend);

        // Both folds are on the roster: two entries, not one merged line.
        Assert.Equal(2, second.RosterEntryCount);
    }
}
