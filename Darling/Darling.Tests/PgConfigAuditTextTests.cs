/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7447: <c>audit_config</c> on a PostgreSQL target printed advice written for the fired case beside a healthy
/// value ("autovacuum is OFF" on a server where it is on). A healthy row now states the setting and its value and
/// nothing else, a fired row keeps its advice, and the advice only names tools that exist.
/// </summary>
public sealed class PgConfigAuditTextTests
{
    /* The CONFIG_PG_* keys under test: a healthy fact and, where the scorer can fire the key, a fired one. */
    private sealed record Case(string Key, string Source, double Healthy, double? Fired, bool Context,
        Dictionary<string, double>? HealthyMeta = null, Dictionary<string, double>? FiredMeta = null);

    private static readonly Case[] Cases =
    [
        new(PgTargetFactKeys.ConfigSharedBuffers, PgTargetSources.ConfigSource, 4096, 128, false),
        new(PgTargetFactKeys.ConfigMaxWalSize, PgTargetSources.ConfigSource, 8192, 1024, false),
        new(PgTargetFactKeys.ConfigEffectiveCacheSize, PgTargetSources.ConfigSource, 16384, PgTargetScorer.EffectiveCacheSizeDefaultMb, false),
        new(PgTargetFactKeys.ConfigRandomPageCost, PgTargetSources.ConfigSource, 1.1, PgTargetScorer.RandomPageCostDefault, false),
        new(PgTargetFactKeys.ConfigTrackIoTiming, PgTargetSources.ConfigSource, 1, 0, false),
        new(PgTargetFactKeys.ConfigCheckpointTimeout, PgTargetSources.ConfigSource, 300, null, true),
        new(PgTargetFactKeys.ConfigWalCompression, PgTargetSources.ConfigSource, 1, null, true),
        new(PgTargetFactKeys.ConfigStatStatementsMissing, PgTargetSources.ConfigSource, 0, null, false),
        new(PgTargetFactKeys.ConfigStatStatementsEviction, PgTargetSources.ConfigSource, 0, 3, false,
            FiredMeta: new() { [EvictionFinding.MaxEntriesKey] = 5000 }),
        new(PgTargetFactKeys.ConfigMaxConnections, PgTargetSources.ConfigSource, 100, null, false),
        new(PgTargetFactKeys.ConfigSuperuserReserved, PgTargetSources.ConfigSource, 3, null, false),
        new(PgTargetFactKeys.ConfigReservedConnections, PgTargetSources.ConfigSource, 0, null, false),
        new(PgTargetFactKeys.ConfigAutovacuumOff, PgTargetSources.ConfigSource, 0, 1, false),
        new(PgTargetFactKeys.ConfigMaintWorkMem, PgTargetSources.ConfigSource, 64, 64, false,
            FiredMeta: new() { [PgTargetScorer.MaintWorkMemBacklogRatioKey] = 2.0 }),
        new(PgTargetFactKeys.ConfigWorkMem, PgTargetSources.ConfigSource, 4, null, true),
        new(PgTargetFactKeys.ConfigMemoryOvercommit, PgTargetSources.MemorySource, 0.3, 1.5, false),
        new(PgTargetFactKeys.ConfigAutovacuumDisabled, PgTargetSources.VacuumSource, 0, 5.0, false,
            FiredMeta: new()
            {
                [PgTargetScorer.BacklogTrailingSamplesKey] = PgTargetScorer.BacklogPersistenceSamples,
                [PgTargetScorer.BacklogRatioKey] = 5.0,
            }),
    ];

    public static IEnumerable<object[]> Keys() => Cases.Select(c => new object[] { c.Key });

    private static Fact Build(Case c, bool fired)
    {
        var fact = new Fact
        {
            Source = c.Source,
            Key = c.Key,
            Value = fired ? c.Fired!.Value : c.Healthy,
            ServerId = 1,
        };
        foreach (var (k, v) in (fired ? c.FiredMeta : c.HealthyMeta) ?? new())
            fact.Metadata[k] = v;
        fact.BaseSeverity = PgTargetScorer.ScoreBase(fact);
        fact.Severity = fact.BaseSeverity;
        return fact;
    }

    private static string Row(Fact fact, string setting = "the_setting", string value = "THE_VALUE")
    {
        var advice = FactAdvice.Compose(fact.Key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [fact.Key] = fact });
        return PgConfigAuditText.Compose(fact, setting, value, advice, notApplicable: false, isAuroraCheckpointTimeout: false);
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public void AHealthyRow_StatesTheSettingAndItsValue_AndCarriesNoAdviceWrittenForTheFiredCase(string key)
    {
        var c = Cases.Single(x => x.Key == key);
        var fact = Build(c, fired: false);
        Assert.Equal(0.0, fact.Severity);

        var text = Row(fact);

        if (c.Context)
        {
            var advice = FactAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = fact })!;
            Assert.Contains(advice.Headline, text, StringComparison.Ordinal);
            Assert.Contains(PgConfigAuditText.DropRestatedLead(advice.Headline, advice.Remediation), text, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("THE_VALUE", text, StringComparison.Ordinal);
            Assert.EndsWith(".", text, StringComparison.Ordinal);
            Assert.DoesNotContain("No composed advice", text, StringComparison.Ordinal);
            /* A plain statement is one sentence: whatever the advice block says about a fault must not appear. */
            var advice = FactAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = fact });
            if (advice is not null)
            {
                Assert.DoesNotContain(advice.Remediation, text, StringComparison.Ordinal);
                Assert.DoesNotContain(advice.Headline, text, StringComparison.Ordinal);
            }
        }
    }

    [Theory]
    [MemberData(nameof(Keys))]
    public void AFiredRow_KeepsItsAdvice_JoinedWithAFullStop(string key)
    {
        var c = Cases.Single(x => x.Key == key);
        if (c.Fired is null)
            return;

        var fact = Build(c, fired: true);
        Assert.True(fact.Severity > 0, key + " should fire in the fired state");

        var advice = FactAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = fact })!;
        var text = Row(fact);

        /* A remediation that restates the setting and its value is joined without that sentence (#7670). */
        var remediation = PgConfigAuditText.DropRestatedLead(advice.Headline, advice.Remediation).Trim();
        Assert.StartsWith(advice.Headline.TrimEnd().TrimEnd('.'), text, StringComparison.Ordinal);
        Assert.EndsWith(remediation, text, StringComparison.Ordinal);
        Assert.DoesNotContain("  ", text, StringComparison.Ordinal);
        /* The sentence break: a mark, then one space, then the remediation. */
        Assert.Matches(new Regex(@"[.!?] " + Regex.Escape(remediation[..Math.Min(12, remediation.Length)])), text);
    }

    [Fact]
    public void AutovacuumOn_IsNotReportedAsOff()
    {
        var fact = Build(Cases.Single(x => x.Key == PgTargetFactKeys.ConfigAutovacuumOff), fired: false);

        var text = Row(fact, "autovacuum", "on");

        Assert.Equal("autovacuum is on.", text);
        Assert.DoesNotContain("OFF", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AutovacuumOff_StillTellsTheReaderToTurnItOn()
    {
        var fact = Build(Cases.Single(x => x.Key == PgTargetFactKeys.ConfigAutovacuumOff), fired: true);

        var text = Row(fact, "autovacuum", "off");

        Assert.Contains("autovacuum is OFF server-wide.", text, StringComparison.Ordinal);
        Assert.Contains("Turn it back on", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAdviceBlocks_AreHonestWhenAskedForDirectlyOnAHealthyValue()
    {
        /* get_analysis_findings and analyze_server reach the same composers, so the blocks themselves must not
           claim a fault the value does not have. */
        foreach (var key in new[] { PgTargetFactKeys.ConfigAutovacuumOff, PgTargetFactKeys.ConfigTrackIoTiming })
        {
            var fact = Build(Cases.Single(x => x.Key == key), fired: false);
            var advice = FactAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = fact })!;
            var all = advice.Headline + advice.Investigation + advice.Remediation;
            Assert.DoesNotContain("OFF", all, StringComparison.Ordinal);
            Assert.DoesNotContain("not being measured", all, StringComparison.Ordinal);
            Assert.DoesNotContain("Turn", all, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("a headline", "Then this.", "a headline. Then this.")]
    [InlineData("a headline.", "Then this.", "a headline. Then this.")]
    [InlineData("a headline?", "Then this.", "a headline? Then this.")]
    [InlineData("a headline (x = 1)", "Then this.", "a headline (x = 1). Then this.")]
    [InlineData("a headline ", " Then this.", "a headline. Then this.")]
    public void Join_AddsAFullStopOnlyWhereTheFirstSentenceHasNone(string first, string second, string expected) =>
        Assert.Equal(expected, PgConfigAuditText.Join(first, second));

    [Theory]
    [InlineData(PgTargetFactKeys.ConfigMaxWalSize, "max_wal_size")]
    [InlineData(PgTargetFactKeys.ConfigSharedBuffers, "shared_buffers")]
    public void AFiredSettingRow_StatesTheSettingAndItsValueOnce(string key, string setting)
    {
        /* #7670: the headline says "max_wal_size is 1 GB" and the remediation opened with it again. */
        var fact = Build(Cases.Single(x => x.Key == key), fired: true);

        var text = Row(fact, setting, "x");

        Assert.True(Regex.Matches(text, Regex.Escape(setting + " is ")).Count == 1, "the setting and its value are stated once, but the row reads: " + text);
        Assert.DoesNotContain("checkpoint-pressure card", text, StringComparison.Ordinal);
        Assert.DoesNotContain("the composite", text, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("max_wal_size is 1 GB: the shipped default", "max_wal_size is 1 GB. No measured WAL rate.", "No measured WAL rate.")]
    [InlineData("shared_buffers is at the initdb default of 128 MB", "shared_buffers is 128 MB (assumed: no config snapshot). Raise it.", "Raise it.")]
    [InlineData("shared_buffers is 1 GB", "shared_buffers is 1 GB, already sized above the default. Look first.", "Already sized above the default. Look first.")]
    [InlineData("shared_buffers is 4 GB", "shared_buffers is 4 GB, already sized above the default, and the cache is still short. Look first.", "Already sized above the default, and the cache is still short. Look first.")]
    [InlineData("shared_buffers is 1 GB", "shared_buffers is 1 GB; keep it. Look first.", "shared_buffers is 1 GB; keep it. Look first.")]
    [InlineData("a headline", "Then this.", "Then this.")]
    public void DropRestatedLead_DropsOnlyABareRestatement(string headline, string remediation, string expected) =>
        Assert.Equal(expected, PgConfigAuditText.DropRestatedLead(headline, remediation));

    [Fact]
    public void ARowWithNoAdviceBlock_NeverPrintsTheOldNoComposedAdviceText()
    {
        var fact = Build(Cases.Single(x => x.Key == PgTargetFactKeys.ConfigMaxConnections), fired: false);

        var text = PgConfigAuditText.Compose(fact, "max_connections", "100", null, notApplicable: false, isAuroraCheckpointTimeout: false);

        Assert.Equal("max_connections is 100.", text);
    }

    [Fact]
    public void EveryToolTheCompositeAdviceNames_IsATool()
    {
        var tools = typeof(DarlingMcpTools).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .ToHashSet(StringComparer.Ordinal);
        Assert.Contains("get_pg_top_queries", tools);

        var keys = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Concat(Cases.Select(c => c.Key));
        var noFacts = new Dictionary<string, Fact>(StringComparer.Ordinal);
        var wrong = new List<string>();
        foreach (var key in keys.Distinct(StringComparer.Ordinal))
        {
            var states = new List<IReadOnlyDictionary<string, Fact>> { noFacts };
            var c = Cases.FirstOrDefault(x => x.Key == key);
            if (c is not null)
            {
                states.Add(new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = Build(c, fired: false) });
                if (c.Fired is not null)
                    states.Add(new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = Build(c, fired: true) });
            }
            foreach (var facts in states)
            {
                var advice = FactAdvice.Compose(key, facts);
                if (advice is null)
                    continue;
                foreach (Match m in Regex.Matches(advice.Headline + " " + advice.Investigation + " " + advice.Remediation, @"\bget_[a-z0-9_]+"))
                    if (!tools.Contains(m.Value))
                        wrong.Add(key + " names " + m.Value);
            }
        }

        Assert.Empty(wrong.Distinct());
    }
}
