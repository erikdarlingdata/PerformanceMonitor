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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The durability posture family (#3542 step 8, D6): <c>fsync</c>, <c>full_page_writes</c> and
/// <c>synchronous_commit</c> as <c>pg_posture</c> facts.
///
/// <para><b>What is pinned.</b> The two grades and their exact values (1.5 CRITICAL for the two corruption
/// settings, 0.4 advisory for the bounded-loss one), that NO amplifier and NO tuning-class cap touches a posture
/// grade even when every other PostgreSQL fact in the vocabulary is present and lit (the D6 isolation, exercised
/// at runtime through the real <c>FactScorer.ScoreAll</c> — the source-text half is
/// <c>PgTargetPostureIsolationTests</c>), that the Aurora-managed pair grade 0 while staying present, that the
/// 0.4 advisory roots its own card through <c>ConfigAdvisoryRoots</c>, and that the advice states the value,
/// the durability consequence, the change mechanism read from the fact, and nothing about how the server
/// runs.</para>
///
/// <para><b>The exit criterion (gated on <c>DARLING_TEST_PG</c>).</b> A stock PostgreSQL 18 target whose latest
/// <c>pg_server_config</c> snapshot says <c>fsync = off</c>, <c>full_page_writes = on</c>,
/// <c>synchronous_commit = off</c> gets, from the REAL <c>analyze_server</c>, a CRITICAL card rooted on
/// <c>PG_POSTURE_FSYNC</c> whose text carries none of the optimisation vocabulary and a 0.4 card rooted on
/// <c>PG_POSTURE_SYNCHRONOUS_COMMIT</c> naming the loss window; an Aurora sibling with <c>fsync = off</c> and
/// <c>full_page_writes = off</c> planted gets both facts back from <c>get_analysis_facts</c> with
/// <c>managed_by_platform = 1</c> and severity 0, and no card for either.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetPostureTests
{
    private const string StockServerName = "pgtarget-posture-stock";
    private static readonly int StockServerId = ServerIdHelper.GetDeterministicHashCode(StockServerName);
    private const string AuroraServerName = "pgtarget-posture-aurora";
    private static readonly int AuroraServerId = ServerIdHelper.GetDeterministicHashCode(AuroraServerName);

    /// <summary>The five words the advice may never use. Shared with the isolation census by value on purpose —
    /// two lists that could drift would each be pinning a different rule.</summary>
    internal static readonly string[] OptimisationVocabulary = { "faster", "performance", "throughput", "speed", "latency" };

    /* ---------------- the scorer ---------------- */

    [Theory]
    [InlineData(PgTargetFactKeys.PostureFsync, 1.5)]
    [InlineData(PgTargetFactKeys.PostureFullPageWrites, 1.5)]
    [InlineData(PgTargetFactKeys.PostureSynchronousCommit, 0.4)]
    public void ASettingThatIsOff_GradesItsBand_AndOneThatIsOn_GradesZero(string key, double expected)
    {
        Assert.Equal(expected, PgTargetScorer.ScoreBase(Posture(key, off: true)));
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(Posture(key, off: false)));
    }

    /// <summary>
    /// The runtime half of D6. Every declared PostgreSQL key is present at a loud value beside the posture
    /// fact, and the posture fact's FINAL severity is exactly its base: no amplifier matched (none exists for
    /// the key), and the tuning-class cap did not pull 1.5 down to 1.49 (a posture key is not tuning-class —
    /// a server with fsync off must be able to notify).
    /// </summary>
    [Fact]
    public void FsyncOff_ReachesTheCriticalBandAlone_NoAmplifierMatches_NoCapApplies()
    {
        var fsync = Posture(PgTargetFactKeys.PostureFsync, off: true);
        var facts = new List<Fact> { fsync };
        facts.AddRange(EveryOtherPgKeyLit());

        new FactScorer().ScoreAll(facts);

        Assert.Equal(1.5, fsync.BaseSeverity);
        Assert.Equal(1.5, fsync.Severity);
        Assert.Empty(fsync.AmplifierResults);
    }

    [Fact]
    public void SynchronousCommitOff_StaysAtTheAdvisoryBand_WhateverElseFires()
    {
        var sync = Posture(PgTargetFactKeys.PostureSynchronousCommit, off: true);
        var facts = new List<Fact> { sync };
        facts.AddRange(EveryOtherPgKeyLit());

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.4, sync.Severity);
        Assert.Empty(sync.AmplifierResults);
    }

    [Theory]
    [InlineData(PgTargetFactKeys.PostureFsync)]
    [InlineData(PgTargetFactKeys.PostureFullPageWrites)]
    public void OnAurora_ThePlatformManagedPair_GradesZeroWhileOff(string key)
    {
        var fact = Posture(key, off: true, aurora: true);
        Assert.Equal(1, fact.Metadata["managed_by_platform"]);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(fact));
    }

    [Fact]
    public void OnAurora_SynchronousCommit_IsStillTheOperatorsPolicy_AndStillGrades()
    {
        var fact = Posture(PgTargetFactKeys.PostureSynchronousCommit, off: true, aurora: true);
        Assert.Equal(0, fact.Metadata["managed_by_platform"]);
        Assert.Equal(0.4, PgTargetScorer.ScoreBase(fact));
    }

    [Fact]
    public void TheAdvisoryBand_RootsItsOwnCard_BelowTheIncidentThreshold()
    {
        var sync = Posture(PgTargetFactKeys.PostureSynchronousCommit, off: true);
        var facts = new List<Fact> { sync };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.4, sync.Severity);

        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        var story = Assert.Single(stories);
        Assert.Equal(PgTargetFactKeys.PostureSynchronousCommit, story.RootFactKey);
    }

    /* ---------------- the advice ---------------- */

    [Theory]
    [InlineData(PgTargetFactKeys.PostureFsync, "fsync = off")]
    [InlineData(PgTargetFactKeys.PostureFullPageWrites, "full_page_writes = off")]
    [InlineData(PgTargetFactKeys.PostureSynchronousCommit, "synchronous_commit = off")]
    public void TheOffBlock_StatesTheValue_TheDefault_TheMechanism_AndNoneOfTheOptimisationVocabulary(string key, string valueStatement)
    {
        var fact = Posture(key, off: true);
        var block = PgTargetAdvice.Compose(key, Lookup(fact));

        Assert.NotNull(block);
        Assert.Contains(valueStatement, block!.Investigation, StringComparison.Ordinal);
        Assert.Contains("default is on", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("operator", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("pg_reload_conf()", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("a reload, not a restart", block.Remediation, StringComparison.Ordinal);
        AssertNoOptimisationVocabulary(block);
    }

    [Fact]
    public void FsyncOff_NamesTheCorruptionConsequence()
    {
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.PostureFsync, Lookup(Posture(PgTargetFactKeys.PostureFsync, off: true)))!;
        Assert.Contains("unrecoverable", block.Headline, StringComparison.Ordinal);
        Assert.Contains("crash recovery cannot repair", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("ALTER SYSTEM SET fsync = on", block.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void SynchronousCommitOff_NamesTheBoundedLossWindow_AndThatItIsNotCorruption()
    {
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.PostureSynchronousCommit, Lookup(Posture(PgTargetFactKeys.PostureSynchronousCommit, off: true)))!;
        Assert.Contains("wal_writer_delay", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("600 ms", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("not corruption", block.Investigation, StringComparison.Ordinal);
        /* The per-session scope is read from the fact, not assumed. */
        Assert.Contains("pg_settings.context = user", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("a session that has SET its own value keeps it", block.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void AManagedFact_SaysSo_AndAnOnFact_IsAStatementNotAFinding()
    {
        var managed = PgTargetAdvice.Compose(PgTargetFactKeys.PostureFsync, Lookup(Posture(PgTargetFactKeys.PostureFsync, off: true, aurora: true)))!;
        Assert.Contains("managed by Aurora", managed.Headline, StringComparison.Ordinal);
        Assert.Contains("not graded", managed.Headline, StringComparison.Ordinal);
        AssertNoOptimisationVocabulary(managed);

        var on = PgTargetAdvice.Compose(PgTargetFactKeys.PostureFullPageWrites, Lookup(Posture(PgTargetFactKeys.PostureFullPageWrites, off: false)))!;
        Assert.Contains("full_page_writes is on", on.Headline, StringComparison.Ordinal);
        Assert.Contains("not a finding", on.Investigation, StringComparison.Ordinal);
        AssertNoOptimisationVocabulary(on);
    }

    [Fact]
    public void ThePendingRestartAndRestartRequiredArms_ReadTheFact()
    {
        var pending = Posture(PgTargetFactKeys.PostureFsync, off: true);
        pending.Metadata["pending_restart"] = 1;
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.PostureFsync, Lookup(pending))!;
        Assert.Contains("already shows pending_restart", block.Remediation, StringComparison.Ordinal);

        var restart = Posture(PgTargetFactKeys.PostureFullPageWrites, off: true);
        restart.Metadata["requires_restart"] = 1;
        block = PgTargetAdvice.Compose(PgTargetFactKeys.PostureFullPageWrites, Lookup(restart))!;
        Assert.Contains("only after a server restart", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("a reload, not a restart", block.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSnapshotAge_IsStatedFromTheFact_InMinutesThenHours()
    {
        var fresh = Posture(PgTargetFactKeys.PostureFsync, off: true);
        fresh.Metadata["snapshot_age_minutes"] = 0.2;
        Assert.Contains("taken at the end of the analysis window", PgTargetAdvice.Compose(PgTargetFactKeys.PostureFsync, Lookup(fresh))!.Investigation, StringComparison.Ordinal);

        var minutes = Posture(PgTargetFactKeys.PostureFsync, off: true);
        minutes.Metadata["snapshot_age_minutes"] = 47;
        Assert.Contains("taken 47 minutes before", PgTargetAdvice.Compose(PgTargetFactKeys.PostureFsync, Lookup(minutes))!.Investigation, StringComparison.Ordinal);

        var hours = Posture(PgTargetFactKeys.PostureFsync, off: true);
        hours.Metadata["snapshot_age_minutes"] = 150;
        Assert.Contains("taken 2.5 hours before", PgTargetAdvice.Compose(PgTargetFactKeys.PostureFsync, Lookup(hours))!.Investigation, StringComparison.Ordinal);
    }

    /// <summary>Value-stated or nothing: without the fact there is no honest posture sentence (the file's
    /// summary says why), so the composed block and the static fallback are both null.</summary>
    [Fact]
    public void WithoutTheFact_ThereIsNoBlock_AndNoStaticFallback()
    {
        var empty = new Dictionary<string, Fact>(StringComparer.Ordinal);
        foreach (var key in PostureKeys())
        {
            Assert.Null(PgTargetAdvice.Compose(key, empty));
            Assert.Null(PgTargetAdvice.Static(key));
        }
    }

    /* ---------------- the collector, by construction ---------------- */

    [Fact]
    public void TheSql_ReadsTheNewestSnapshot_ExcludesSessionScopedRows_AndSelectsExactlyTheThreeSettings()
    {
        var sql = PgTargetFactCollector.PgTargetPostureSql;
        Assert.Contains(sql, PgTargetFactCollector.AllSql);
        Assert.Contains("c.collection_time = (", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT MAX(collection_time)", sql, StringComparison.Ordinal);
        /* The reader's exclusion list, verbatim (DarlingPgServerConfigReader.SessionScopedSources). */
        Assert.Contains("NOT IN (" + DarlingPgServerConfigReader.SessionScopedSources + ")", sql, StringComparison.Ordinal);
        Assert.Contains("c.name IN ('fsync', 'full_page_writes', 'synchronous_commit')", sql, StringComparison.Ordinal);
        /* The engine kind comes from the registry join, not from the metadata fact emitted a step earlier. */
        Assert.Contains("JOIN servers AS s", sql, StringComparison.Ordinal);
        Assert.Contains("s.engine_kind", sql, StringComparison.Ordinal);
        /* No other family's table, and no rate: the read is the snapshot and nothing else. */
        Assert.DoesNotContain("pg_database_stats", sql, StringComparison.Ordinal);
    }

    /* ---------------- gated: the exit criterion ---------------- */

    [Fact]
    public async Task FsyncOffIsACriticalCardWithNoOptimisationWords_SyncCommitOffIsAnAdvisory_AuroraIsManagedAndUngraded()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the posture family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, StockServerId, StockServerName, "postgres", 18, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, AuroraServerId, AuroraServerName, "aurora-postgres", 17, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* 25 hours of the coverage witness for both, one row a minute across the window (the plumbing's
               planting shape, so the span gate opens and coverage is whole). */
            foreach (var (id, name) in new[] { (StockServerId, StockServerName), (AuroraServerId, AuroraServerName) })
            {
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, windowEnd.AddHours(-25), ct);
                for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                    await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, windowStart.AddMinutes(minute - 1), ct);
            }

            /* Two config snapshots for the stock server: an OLDER one where everything is on, and the NEWEST
               (40 minutes before the window end) with fsync and synchronous_commit off — proving the read
               takes the latest snapshot, not the first. Plus one session-scoped row saying fsync = off under
               source 'session' in the older snapshot, which must not be read as the server's. */
            var older = windowEnd.AddMinutes(-100);
            var newest = windowEnd.AddMinutes(-40);
            await PlantConfigAsync(connection, StockServerId, StockServerName, older, "fsync", "on", "sighup", "default", false, ct);
            await PlantConfigAsync(connection, StockServerId, StockServerName, older, "full_page_writes", "on", "sighup", "default", false, ct);
            await PlantConfigAsync(connection, StockServerId, StockServerName, older, "synchronous_commit", "on", "user", "default", false, ct);
            await PlantConfigAsync(connection, StockServerId, StockServerName, newest, "fsync", "off", "sighup", "configuration file", false, ct);
            await PlantConfigAsync(connection, StockServerId, StockServerName, newest, "full_page_writes", "on", "sighup", "default", false, ct);
            await PlantConfigAsync(connection, StockServerId, StockServerName, newest, "synchronous_commit", "off", "user", "configuration file", false, ct);
            await PlantConfigAsync(connection, StockServerId, StockServerName, newest, "fsync", "off", "sighup", "session", false, ct);

            /* Aurora: both platform-managed settings planted OFF, synchronous_commit on. */
            await PlantConfigAsync(connection, AuroraServerId, AuroraServerName, newest, "fsync", "off", "sighup", "override", false, ct);
            await PlantConfigAsync(connection, AuroraServerId, AuroraServerName, newest, "fsync", "off", "sighup", "default", false, ct);
            await PlantConfigAsync(connection, AuroraServerId, AuroraServerName, newest, "full_page_writes", "off", "sighup", "default", false, ct);
            await PlantConfigAsync(connection, AuroraServerId, AuroraServerName, newest, "synchronous_commit", "on", "user", "default", false, ct);

            /* ── the collector alone, on the stock server: three posture facts from the NEWEST snapshot. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = StockServerId,
                ServerName = StockServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            var posture = facts.Where(f => f.Source == PgTargetSources.PostureSource).ToDictionary(f => f.Key, StringComparer.Ordinal);
            Assert.Equal(3, posture.Count);
            Assert.Equal(1, posture[PgTargetFactKeys.PostureFsync].Value);
            Assert.Equal(0, posture[PgTargetFactKeys.PostureFullPageWrites].Value);
            Assert.Equal(1, posture[PgTargetFactKeys.PostureSynchronousCommit].Value);
            Assert.All(posture.Values, f => Assert.Equal(0, f.Metadata["managed_by_platform"]));
            Assert.All(posture.Values, f => Assert.Equal(40.0, f.Metadata["snapshot_age_minutes"], precision: 3));
            Assert.Equal(0, posture[PgTargetFactKeys.PostureFsync].Metadata["is_default"]);
            Assert.Equal(1, posture[PgTargetFactKeys.PostureFullPageWrites].Metadata["is_default"]);
            Assert.Equal(1, posture[PgTargetFactKeys.PostureSynchronousCommit].Metadata["session_settable"]);
            Assert.Equal(0, posture[PgTargetFactKeys.PostureFsync].Metadata["session_settable"]);
            Assert.All(posture.Values, f => Assert.Equal(0, f.Metadata["requires_restart"]));

            /* ── THE EXIT CRITERION: the real analyze_server tool on the stock server. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, StockServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();

                var fsync = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.PostureFsync);
                Assert.Equal(1.5, fsync.GetProperty("severity").GetDouble(), precision: 2);
                var advice = fsync.GetProperty("advice");
                var text = advice.GetProperty("headline").GetString() + " " + advice.GetProperty("investigation").GetString() + " " + advice.GetProperty("remediation").GetString();
                Assert.Contains("fsync = off", text, StringComparison.Ordinal);
                /* The tool's window ends at ITS now, a minute or two past the collector run above, so the exact
                   figure is pinned there (40, against a controlled TimeRangeEnd) and only the shape is pinned here. */
                Assert.Matches(new System.Text.RegularExpressions.Regex(@"taken 4\d minutes before the end of the analysis window"), text);
                foreach (var word in OptimisationVocabulary)
                    Assert.DoesNotContain(word, text, StringComparison.OrdinalIgnoreCase);

                var sync = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.PostureSynchronousCommit);
                Assert.Equal(0.4, sync.GetProperty("severity").GetDouble(), precision: 2);
                Assert.Contains("wal_writer_delay", sync.GetProperty("advice").GetProperty("investigation").GetString(), StringComparison.Ordinal);

                /* full_page_writes is on: a posture statement, never a card. */
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.PostureFullPageWrites);
            }

            /* ── Aurora: facts present under pg_posture, managed, graded 0; no card for either. */
            var auroraFacts = await DarlingMcpTools.GetAnalysisFacts(service, postgres, AuroraServerName, 4, PgTargetSources.PostureSource);
            using (var doc = JsonDocument.Parse(auroraFacts))
            {
                var rows = doc.RootElement.GetProperty("facts").EnumerateArray().ToDictionary(f => f.GetProperty("key").GetString()!, StringComparer.Ordinal);
                Assert.Equal(3, rows.Count);
                foreach (var key in new[] { PgTargetFactKeys.PostureFsync, PgTargetFactKeys.PostureFullPageWrites })
                {
                    Assert.Equal(1, rows[key].GetProperty("value").GetDouble());
                    Assert.Equal(1, rows[key].GetProperty("metadata").GetProperty("managed_by_platform").GetDouble());
                    Assert.Equal(1, rows[key].GetProperty("metadata").GetProperty("is_aurora").GetDouble());
                    Assert.Equal(0, rows[key].GetProperty("severity").GetDouble());
                }
                Assert.Equal(0, rows[PgTargetFactKeys.PostureSynchronousCommit].GetProperty("value").GetDouble());
                Assert.Equal(0, rows[PgTargetFactKeys.PostureSynchronousCommit].GetProperty("metadata").GetProperty("managed_by_platform").GetDouble());
            }

            var auroraAnalysis = await DarlingMcpTools.AnalyzeServer(service, postgres, AuroraServerName, 4);
            using (var doc = JsonDocument.Parse(auroraAnalysis))
            {
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ---------------- helpers ---------------- */

    internal static IEnumerable<string> PostureKeys() =>
        [PgTargetFactKeys.PostureFsync, PgTargetFactKeys.PostureFullPageWrites, PgTargetFactKeys.PostureSynchronousCommit];

    /// <summary>A posture fact as the collector emits it (see <c>PgTargetFactCollector.Posture.cs</c> for the
    /// metadata contract), with <c>pg_settings.context</c> per setting: <c>sighup</c> for the two
    /// corruption settings, <c>user</c> for <c>synchronous_commit</c>.</summary>
    private static Fact Posture(string key, bool off, bool aurora = false)
    {
        var sessionSettable = key == PgTargetFactKeys.PostureSynchronousCommit;
        var managed = aurora && !sessionSettable;
        return new Fact
        {
            Source = PgTargetSources.PostureSource,
            Key = key,
            Value = off ? 1 : 0,
            ServerId = 1,
            Metadata =
            {
                ["managed_by_platform"] = managed ? 1 : 0,
                ["is_aurora"] = aurora ? 1 : 0,
                ["requires_restart"] = 0,
                ["session_settable"] = sessionSettable ? 1 : 0,
                ["pending_restart"] = 0,
                ["is_default"] = off ? 0 : 1,
                ["snapshot_age_minutes"] = 12,
            },
        };
    }

    private static IReadOnlyDictionary<string, Fact> Lookup(params Fact[] facts) =>
        facts.ToDictionary(f => f.Key, StringComparer.Ordinal);

    /// <summary>Every declared PostgreSQL key except the three posture keys, each at a loud value under its
    /// source, plus one of each dynamic family — the universe an amplifier could match against.</summary>
    private static IEnumerable<Fact> EveryOtherPgKeyLit()
    {
        var keys = typeof(PgTargetFactKeys).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(PgTargetFactKeys.IsPgKey)
            .Where(k => !PostureKeys().Contains(k, StringComparer.Ordinal))
            .Append(PgTargetFactKeys.WaitKey("Lock", "relation"))
            .Append(PgTargetFactKeys.BadActorKey(7));

        foreach (var key in keys)
        {
            var source = key.StartsWith(PgTargetFactKeys.WaitKeyPrefix, StringComparison.Ordinal) ? PgTargetSources.WaitsSource
                : key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal) ? PgTargetSources.QueriesSource
                : key.StartsWith(PgTargetFactKeys.ConfigPrefix, StringComparison.Ordinal) ? PgTargetSources.ConfigSource
                : PgTargetSources.DatabaseSource;
            yield return new Fact { Source = source, Key = key, Value = 99, ServerId = 1, Metadata = { ["period_duration_ms"] = 3_600_000, ["coverage_fraction"] = 1 } };
        }
    }

    private static void AssertNoOptimisationVocabulary(AdviceBlock block)
    {
        var text = block.Headline + " " + block.Investigation + " " + block.Remediation;
        foreach (var word in OptimisationVocabulary)
            Assert.DoesNotContain(word, text, StringComparison.OrdinalIgnoreCase);
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One <c>pg_server_config</c> row as the collector writes it (V102 shape).</summary>
    private static async Task PlantConfigAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at,
        string name, string setting, string context, string source, bool pendingRestart, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype,
     source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, NULL, 'Write-Ahead Log / Settings', $7, 'bool', $8, 'on', $6, NULL, NULL, $9, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(setting);
        command.Parameters.AddWithValue(context);
        command.Parameters.AddWithValue(source);
        command.Parameters.AddWithValue(pendingRestart);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_server_config WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM pg_database_stats WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({StockServerId}, {AuroraServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({StockServerId}, {AuroraServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
