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
/// The session family of the PostgreSQL-target analysis pass (#3542 lane 3, design §3.6): <c>PG_CONNECTION_SATURATION</c>
/// as <c>peak_sessions / (max_connections − superuser_reserved_connections)</c>, and <c>PG_MONITORING_PERMISSIONS</c>
/// in its place when the monitoring login could not see session state.
///
/// <para><b>What is pinned.</b> The scorer's bands — 0.79 scores ZERO (not a fraction of the bar: a fired saturation
/// fact must mean the pool is near its cliff), 0.8 scores 0.5, 0.9 scores 1.0 — with <c>threshold_lineage = 0</c>
/// on every graded fact; the permissions advisory at the story line, only past the majority; the two amplifiers
/// (the fact's own parked-connections share; a fired <c>PG_CPU_PERCENT</c>) and their inertness through the real
/// <see cref="FactScorer.ScoreAll"/> while the CPU family is a stub; the advice stating the division with all
/// three numbers (<c>90 / (100 − 3) = 93%</c>), the state breakdown of the peak capture, both levers with their
/// counter-objectives, and this server's own <c>work_mem</c> when lane 2's fact is in the set; the permissions
/// block stating the redacted share and NO ratio; the collector's SQL shape (the collector table only — never
/// <c>collection_log</c>, never <c>pg_server_config</c>) and its read of the ceiling off the in-memory fact list;
/// and the saturation chain adding no edge in v1.</para>
///
/// <para><b>Gated e2e</b> (<c>DARLING_TEST_PG</c>): a planted session series peaking at 90 sessions against a planted
/// <c>pg_server_config</c> snapshot (<c>max_connections 100</c>, <c>superuser_reserved_connections 3</c> — lane 2's
/// REAL emitter, not hand-built facts) yields the saturation fact at 90 / 97, scored 1.0 and amplified by the
/// parked share to 1.25, and — through the REAL <c>analyze_server</c> — a card whose prose states the division and
/// the breakdown; a sibling server whose stored rows are two-thirds redacted yields the permissions advisory and no
/// saturation fact at all.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetSessionsTests
{
    private const string BusyServerName = "darling-pg-target-sessions-busy";
    private static readonly int BusyServerId = ServerIdHelper.GetDeterministicHashCode(BusyServerName);
    private const string BlindServerName = "darling-pg-target-sessions-blind";
    private static readonly int BlindServerId = ServerIdHelper.GetDeterministicHashCode(BlindServerName);

    /// <summary>A saturation fact as the collector composes it: 100 / 3 → 97 usable; the breakdown and the
    /// window shape are the e2e's planted figures unless overridden.</summary>
    private static Fact Saturation(double peak, double active = 40, double idleInTransaction = 30, double maxConnections = 100, double reserved = 3,
        double captures = 48, double latest = 60, bool pendingRestart = false)
    {
        var usable = maxConnections - reserved;
        return new Fact
        {
            Source = PgTargetSources.SessionsSource,
            Key = PgTargetFactKeys.ConnectionSaturation,
            Value = peak / usable,
            ServerId = 1,
            Metadata =
            {
                ["saturation_ratio"] = peak / usable,
                ["peak_total_sessions"] = peak,
                ["peak_active_sessions"] = active,
                ["peak_idle_in_transaction_sessions"] = idleInTransaction,
                ["peak_other_sessions"] = Math.Max(0, peak - active - idleInTransaction),
                ["peak_idle_in_transaction_share"] = idleInTransaction / peak,
                ["peak_age_s"] = 7_200,
                ["latest_total_sessions"] = latest,
                ["latest_active_sessions"] = 20,
                ["latest_idle_in_transaction_sessions"] = 5,
                ["latest_age_s"] = 0,
                ["max_connections"] = maxConnections,
                ["superuser_reserved_connections"] = reserved,
                ["usable_connections"] = usable,
                ["max_connections_pending_restart"] = pendingRestart ? 1 : 0,
                ["config_snapshot_age_s"] = 1_800,
                ["captures_with_rows"] = captures,
                ["rows_redacted_share"] = 0,
            },
        };
    }

    private static Fact Permissions(double redacted, double stored, double captures = 48, double peak = 95) => new()
    {
        Source = PgTargetSources.SessionsSource,
        Key = PgTargetFactKeys.MonitoringPermissions,
        Value = redacted / stored,
        ServerId = 1,
        Metadata =
        {
            ["rows_redacted_share"] = redacted / stored,
            ["rows_redacted"] = redacted,
            ["rows_stored"] = stored,
            ["captures_with_rows"] = captures,
            ["peak_total_sessions"] = peak,
            ["peak_age_s"] = 600,
        },
    };

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) => facts.ToFactLookup();

    /* ───────────────────────── the scorer ───────────────────────── */

    [Theory]
    [InlineData(0.79, 0.0)]     // below the warning band: ZERO, not 0.5 × 0.79 / 0.8 — a fired fact means near the cliff
    [InlineData(0.80, 0.5)]     // at the warning band: the story threshold
    [InlineData(0.85, 0.75)]    // between: 0.5 + 0.5 × (0.85 − 0.8) / (0.9 − 0.8)
    [InlineData(0.90, 1.0)]     // critical
    [InlineData(0.97, 1.0)]     // past it: pinned at 1.0, the amplifiers do the rest
    public void ScoreSessionsFact_IsZeroBelowTheWarningBand_HalfAtIt_FullAtCritical_AndStampsTheUnmeasuredLineage(double ratio, double expected)
    {
        /* The ratio is planted EXACTLY (not derived from a peak, so 0.8 is 0.8 and not 77.6 / 97's last bit):
           the band edges are the thing under test. */
        var fact = Saturation(peak: ratio * 97);
        fact.Value = ratio;
        fact.Metadata["saturation_ratio"] = ratio;

        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        /* The number that decided is unmeasured whichever side of it the fact fell — the stamp lands on the 0.79 too. */
        Assert.Equal(0, fact.Metadata["threshold_lineage"]);

        Assert.Equal(0.8, PgTargetScorer.ConnectionSaturationWarning);
        Assert.Equal(0.9, PgTargetScorer.ConnectionSaturationCritical);
    }

    [Fact]
    public void ScoreSessionsFact_IsZeroWithoutARatio_AndForAnyOtherKeyUnderTheSource()
    {
        /* No ratio, no grade — and no stamp, because nothing was graded. */
        var bare = new Fact { Source = PgTargetSources.SessionsSource, Key = PgTargetFactKeys.ConnectionSaturation, Value = 0.95 };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(bare));
        Assert.False(bare.Metadata.ContainsKey("threshold_lineage"));

        /* The routing probe the shared-switch test uses: an unknown key under pg_sessions scores 0. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.SessionsSource, Key = "PG_PROBE", Value = 99 }));
    }

    [Theory]
    [InlineData(50, 100, 0.5)]   // exactly the majority line: the advisory roots
    [InlineData(96, 96, 0.5)]    // every row blank — the least-privileged login's real shape
    [InlineData(49, 100, 0.0)]   // a minority redacted: the collector would have emitted a ratio instead; a stray fact scores nothing
    public void ScoreSessionsFact_MonitoringPermissions_RootsAtTheStoryLine_OnlyAtOrPastTheMajority(double redacted, double stored, double expected)
    {
        var fact = Permissions(redacted, stored);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        /* Its only line is definitional (a majority), so it carries no unmeasured stamp. */
        Assert.False(fact.Metadata.ContainsKey("threshold_lineage"));

        Assert.Equal(0.5, PgTargetScorer.RedactedShareMajority);
        Assert.Equal(0.5, PgTargetScorer.MonitoringPermissionsBase);
        /* A blank claim: no share at all. */
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(new Fact { Source = PgTargetSources.SessionsSource, Key = PgTargetFactKeys.MonitoringPermissions, Value = 1 }));
    }

    [Fact]
    public void MonitoringPermissions_RootsAOneCardStory_ThroughTheRealScorerAndEngine_AndNeverClimbs()
    {
        var facts = new List<Fact> { Permissions(64, 96) };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.5, facts[0].BaseSeverity, precision: 9);
        Assert.Equal(0.5, facts[0].Severity, precision: 9);
        /* No amplifier arm claims the key: the advisory cannot be boosted into the CRITICAL band. */
        Assert.Empty(facts[0].AmplifierResults);

        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        var story = Assert.Single(stories);
        Assert.Equal(PgTargetFactKeys.MonitoringPermissions, story.RootFactKey);
        Assert.Equal(PgTargetSources.SessionsSource, story.Category);
        /* Not an advisory root by registration — it clears the story line on its own severity. */
        Assert.False(PgTargetFactKeys.IsConfigAdvisoryRoot(PgTargetFactKeys.MonitoringPermissions));
    }

    [Fact]
    public void SessionsAmplifiers_TheParkedShareReadsTheFactItself_TheCpuArmAsksWhetherTheSiblingFired_AndBothAreInertThroughScoreAllToday()
    {
        var key = PgTargetFactKeys.ConnectionSaturation;
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        var definitions = ((System.Collections.IEnumerable)amplifiers.Invoke(null, [key])!).Cast<object>().ToList();
        Assert.Equal(2, definitions.Count);

        static (double Boost, Func<Dictionary<string, Fact>, bool> Predicate) Read(object definition)
        {
            var type = definition.GetType();
            return (
                (double)type.GetProperty("Boost")!.GetValue(definition)!,
                (Func<Dictionary<string, Fact>, bool>)type.GetProperty("Predicate")!.GetValue(definition)!);
        }

        var parked = Read(definitions[0]);
        var cpu = Read(definitions[1]);
        Assert.Equal(PgTargetScorer.ConnectionSaturationCoFireBoost, parked.Boost);
        Assert.Equal(PgTargetScorer.ConnectionSaturationCoFireBoost, cpu.Boost);
        Assert.Equal(0.25, PgTargetScorer.IdleInTransactionShareBar);

        /* Parked: 30 of 90 idle in transaction (0.333) fires; 22 of 90 (0.244) does not; exactly a quarter does. */
        Assert.True(parked.Predicate(Lookup(Saturation(90, idleInTransaction: 30))));
        Assert.False(parked.Predicate(Lookup(Saturation(90, idleInTransaction: 22))));
        Assert.True(parked.Predicate(Lookup(Saturation(100, idleInTransaction: 25))));
        Assert.False(parked.Predicate(Lookup()));

        var firedCpu = new Fact { Source = PgTargetSources.CpuSource, Key = PgTargetFactKeys.CpuPercent, BaseSeverity = 0.9 };
        var quietCpu = new Fact { Source = PgTargetSources.CpuSource, Key = PgTargetFactKeys.CpuPercent, BaseSeverity = 0.0 };
        Assert.True(cpu.Predicate(Lookup(Saturation(90), firedCpu)));
        Assert.False(cpu.Predicate(Lookup(Saturation(90), quietCpu)));
        Assert.False(cpu.Predicate(Lookup(Saturation(90))));

        /* Through the real scorer: the parked share is the fact's own and fires; a CPU fact whose 95 is the RAW
           percent-of-allocated reading (no capacity_measured flag — lane 9's scorer grades only the capacity
           percent, #3281) scores 0 and its arm is wired and inert. 1.0 × (1 + 0.25) = 1.25. */
        var facts = new List<Fact>
        {
            Saturation(90, idleInTransaction: 30),
            new() { Source = PgTargetSources.CpuSource, Key = PgTargetFactKeys.CpuPercent, Value = 95 },
        };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(1.0, facts[0].BaseSeverity, precision: 9);
        Assert.Equal(1.25, facts[0].Severity, precision: 9);
        Assert.Equal(2, facts[0].AmplifierResults.Count);
        Assert.True(facts[0].AmplifierResults[0].Matched);
        Assert.False(facts[0].AmplifierResults[1].Matched);

        /* And with the capacity reading measured at 95% of the configured ceiling the CPU fact fires (1.0) and the
           arm lifts saturation to 1.0 × (1 + 0.25 + 0.25) = 1.5 — the D7 "queueing at the cliff" shape. */
        var atCapacity = new List<Fact>
        {
            Saturation(90, idleInTransaction: 30),
            new()
            {
                Source = PgTargetSources.CpuSource, Key = PgTargetFactKeys.CpuPercent, Value = 95,
                Metadata = { [PgTargetScorer.CpuCapacityMeasuredKey] = 1 },
            },
        };
        new FactScorer().ScoreAll(atCapacity);
        Assert.Equal(1.0, atCapacity[1].BaseSeverity, precision: 9);
        Assert.Equal(1.5, atCapacity[0].Severity, precision: 9);
        Assert.True(atCapacity[0].AmplifierResults[1].Matched);

        /* And below the warning band nothing fires at all — the fact is context, and no sibling can lift it. */
        var calm = new List<Fact> { Saturation(70, idleInTransaction: 60), firedCpu };
        new FactScorer().ScoreAll(calm);
        Assert.Equal(0.0, calm[0].Severity);
        Assert.Empty(calm[0].AmplifierResults);
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void ComposeSessions_StatesTheDivisionWithAllThreeNumbers_TheBreakdown_BothLevers_AndTheirCounterObjectives()
    {
        var fact = Saturation(90, active: 40, idleInTransaction: 30);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.ConnectionSaturation, Lookup(fact));

        Assert.NotNull(block);
        Assert.Equal("Connections peaked at 93% of the usable ceiling (90 of 97) — PostgreSQL refuses the next one, it does not queue it", block!.Headline);

        /* THE EXIT CRITERION: sessions / (max − reserved) stated as arithmetic the reader can redo. */
        Assert.Contains("90 / (100 − 3) = 93%", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("90 sessions were connected against 97 usable connections — max_connections 100 minus superuser_reserved_connections 3", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("2 h before the window's end", block.Investigation, StringComparison.Ordinal);
        /* The state breakdown of the PEAK capture, so the operator sees which population fills the pool. */
        Assert.Contains("40 active, 30 idle in transaction, 20 other", block.Investigation, StringComparison.Ordinal);
        /* 30 / 90 is past the parked bar: the population is named. */
        Assert.Contains("33% of the peak — the pool is being filled by PARKED connections", block.Investigation, StringComparison.Ordinal);
        /* The cliff is an outage, not a wait; the count's known bias; the exception-table caveat; the lineage. */
        Assert.Contains("FATAL: too many clients already", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("a refusal is an outage, not a slowdown", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("ratio reads a few points high, the safe direction for a cliff", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("seen over 48 captures that stored session rows", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("the newest capture in the window had 60 sessions", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("threshold_lineage = 0", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("pending a restart on this server", block.Investigation, StringComparison.Ordinal);

        /* Both levers, each with its counter-objective, and the parked-connections lever first because the
           breakdown pointed at it. */
        Assert.StartsWith("30 of the 90 peak sessions were idle in transaction", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("idle_in_transaction_session_timeout", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("A bigger pool only defers the same refusal", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("pgbouncer in transaction mode, or RDS Proxy", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("does not preserve session-level state", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("Raise max_connections above 100", block.Remediation, StringComparison.Ordinal);
        /* work_mem is NOT in the set: the memory cost is stated as a shape, never an assumed figure (D4). */
        Assert.Contains("the ceiling multiplies work_mem against the host's memory", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("work_mem is ", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("takes a restart (pg_settings.context = postmaster)", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_session_states", block.Remediation, StringComparison.Ordinal);
        /* D8: no DDL, ever. */
        Assert.DoesNotContain("CREATE INDEX", block.Investigation + block.Remediation, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ComposeSessions_StatesThisServersWorkMem_WhenLaneTwosFactIsInTheSet_AndThePendingRestart()
    {
        var fact = Saturation(80, active: 70, idleInTransaction: 5, pendingRestart: true);
        var workMem = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigWorkMem, Value = 4, Metadata = { ["bytes"] = 4L * 1024 * 1024 } };
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.ConnectionSaturation, Lookup(fact, workMem));

        Assert.NotNull(block);
        Assert.Equal("Connections peaked at 82% of the usable ceiling (80 of 97) — PostgreSQL refuses the next one, it does not queue it", block!.Headline);
        /* Value-stated from THIS server's work_mem: 4 MB × 97 usable connections. */
        Assert.Contains("work_mem is 4 MB here, so 97 connections each running one such node could claim 388 MB", block.Remediation, StringComparison.Ordinal);
        /* 5 of 80 is under the parked bar: the breakdown is stated, the parked population is not claimed. */
        Assert.Contains("70 active, 5 idle in transaction, 5 other", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("PARKED", block.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("Two capacity levers", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("A max_connections change is already pending a restart on this server", block.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeSessions_Permissions_StatesTheRedactedShareAndThePeakCount_AndClaimsNoRatio()
    {
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.MonitoringPermissions, Lookup(Permissions(64, 96)));

        Assert.NotNull(block);
        Assert.Equal("The monitoring login cannot see session state on this server — 67% of the stored session rows are redacted", block!.Headline);
        Assert.Contains("64 of the 96 pg_session_states rows stored across 48 captures in the window (67%) came back state_is_redacted", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("the privileged role saw four idle-in-transaction sessions and the unprivileged one zero of the same nine backends", block.Investigation, StringComparison.Ordinal);
        /* The peak COUNT is stated; its share of the ceiling is not — no ratio, no ceiling, no percentage of it. */
        Assert.Contains("The peak capture counted 95 sessions — a bare count, which survives redaction", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("emits no connection-saturation ratio", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("usable", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("max_connections", block.Investigation, StringComparison.Ordinal);
        /* The lever and its counter-objective. */
        Assert.Contains("Grant the monitoring login pg_monitor (or the narrower pg_read_all_stats)", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("lets the login SEE other roles' query text, which pg_session_states deliberately never stores", block.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeSessions_StaticBlocks_ClaimNoFigure_AndEveryOtherKeyIsNull()
    {
        var saturation = PgTargetAdvice.Static(PgTargetFactKeys.ConnectionSaturation);
        var permissions = PgTargetAdvice.Static(PgTargetFactKeys.MonitoringPermissions);
        Assert.NotNull(saturation);
        Assert.NotNull(permissions);
        Assert.Equal(saturation, PgTargetAdvice.Compose(PgTargetFactKeys.ConnectionSaturation, Lookup()));
        Assert.Equal(permissions, PgTargetAdvice.Compose(PgTargetFactKeys.MonitoringPermissions, Lookup()));

        /* The fallback for a finding read without its facts: the mechanism and the levers, no number it did not
           read. The only digits are the two bands it names as a judgment. */
        Assert.StartsWith("Connections peaked near the usable ceiling", saturation!.Headline, StringComparison.Ordinal);
        Assert.Contains("max_connections minus superuser_reserved_connections", saturation.Investigation, StringComparison.Ordinal);
        Assert.Contains("80% / 90% bands on the ratio are an unmeasured judgment", saturation.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain(" of ", saturation.Headline, StringComparison.Ordinal);
        Assert.Contains("pgbouncer", saturation.Remediation, StringComparison.Ordinal);
        Assert.Contains("takes a restart", saturation.Remediation, StringComparison.Ordinal);
        Assert.Equal("The monitoring login cannot see session state on this server", permissions!.Headline);
        Assert.Contains("pg_monitor", permissions.Remediation, StringComparison.Ordinal);

        /* The dispatcher routes only the two keys here; the partial answers null for anything else it is handed. */
        var compose = typeof(PgTargetAdvice).GetMethod("ComposeSessions", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.Null(compose.Invoke(null, [PgTargetFactKeys.IdleInTransaction, Lookup()]));
    }

    /* ───────────────────────── the collector's shape, by source ───────────────────────── */

    [Fact]
    public void TheSessionRead_IsOneQueryOverTheCollectorTable_PicksTheDenormalisedTotals_AndNeverReadsTheConfigSnapshot()
    {
        var sql = PgTargetFactCollector.PgTargetSessionPeakSql;
        Assert.Contains(sql, PgTargetFactCollector.AllSql);
        Assert.Contains("FROM pg_session_states", sql, StringComparison.Ordinal);
        /* The reader's honest-empty denominator lives in collection_log; that is not an analysis table and the
           FROM/JOIN census would refuse it — the capture count here is over the rows the table has. */
        Assert.DoesNotContain("collection_log", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_server_config", sql, StringComparison.Ordinal);
        /* The totals are a PICK per capture (they repeat on every row), not a count of the stored rows. */
        Assert.Contains("MAX(total_sessions)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(active_sessions)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(idle_in_transaction_sessions)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);
        /* Peak by total, ties to the newest; the newest capture beside it; the redacted share over ROWS. */
        Assert.Contains("ORDER BY total_sessions DESC NULLS LAST, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) FILTER (WHERE coalesce(state_is_redacted, false)) AS rows_redacted", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Sessions.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        /* The ceiling comes off lane 2's context facts in the in-memory list — by KEY, never by re-reading the
           snapshot; the two config keys are the whole dependency between the lanes. */
        Assert.Contains("facts.Find(f => f.Key == PgTargetFactKeys.ConfigMaxConnections)", code, StringComparison.Ordinal);
        Assert.Contains("facts.Find(f => f.Key == PgTargetFactKeys.ConfigSuperuserReserved)", code, StringComparison.Ordinal);
        Assert.DoesNotContain("PgTargetConfigSnapshotSql", code, StringComparison.Ordinal);
        /* Redaction gates on the scorer's ONE majority constant, and takes precedence: the advisory returns
           before the ceiling is looked up. */
        var redactionGate = code.IndexOf("PgTargetScorer.RedactedShareMajority", StringComparison.Ordinal);
        var ceilingLookup = code.IndexOf("PgTargetFactKeys.ConfigMaxConnections", StringComparison.Ordinal);
        Assert.True(redactionGate > 0 && redactionGate < ceilingLookup, "the redaction gate must precede the ceiling lookup");
        Assert.Contains("Key = PgTargetFactKeys.MonitoringPermissions", code, StringComparison.Ordinal);
        Assert.Contains("Key = PgTargetFactKeys.ConnectionSaturation", code, StringComparison.Ordinal);
        /* One command, one const, the shared deadline, the shared degrade. */
        Assert.Equal(1, Count(code, "new NpgsqlCommand("));
        Assert.Contains("CommandTimeout = FactCommandTimeoutSeconds", code, StringComparison.Ordinal);
        Assert.Contains("ReportCollectionFailure(ex, context)", code, StringComparison.Ordinal);
        Assert.Contains("filled by lane 3", source, StringComparison.Ordinal);
        /* No clock of its own: every age is measured from the window's end the caller asked for. */
        Assert.DoesNotContain("DateTime.UtcNow", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// Lane 3 shipped this chain with NO edge and this pin held that; lane 5 (the wait family, #3542 step 5)
    /// moved it deliberately when it wrote the saturation ↔ Lock-wait mesh into the file: saturation's only
    /// edges are the two wait keys, in BOTH directions (a saturation root walks to a fired Lock wait, a Lock wait
    /// root walks to fired saturation — one story whichever outranks), the permissions fact still has none, and
    /// the v2 <c>PG_IDLE_IN_TRANSACTION</c> hook stays a comment. The wait-side pins (predicates, both walks) are
    /// <c>PgTargetWaitTests</c>'.
    /// </summary>
    [Fact]
    public void TheSaturationChain_MeshesWithTheLockWaitsOnly_AndThePermissionsFactHasNoEdge()
    {
        var graph = new PgTargetRelationshipGraph();
        Assert.Equal(
            new[] { PgTargetFactKeys.WaitKey("Lock", null), PgTargetFactKeys.WaitKey("Lock", "relation") }.Order(StringComparer.Ordinal),
            graph.GetAllEdges(PgTargetFactKeys.ConnectionSaturation).Select(e => e.Destination).Order(StringComparer.Ordinal));
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.MonitoringPermissions));
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.IdleInTransaction));

        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Saturation.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Equal(4, Count(code, "AddEdge("));
        /* The v2 hook is still written out, as a comment, for the lane that owns its destination. */
        Assert.Contains("PgTargetFactKeys.IdleInTransaction", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PgTargetFactKeys.IdleInTransaction", code, StringComparison.Ordinal);
        Assert.Contains("Lane 3 added no edge of its own, deliberately", source, StringComparison.Ordinal);
    }

    /* ───────────────────────── gated: the exit criterion ───────────────────────── */

    [Fact]
    public async Task APlantedSeriesPeakingAtNinetyOfNinetySevenUsable_YieldsTheSaturationStory_AndARedactedMajorityYieldsThePermissionsAdvisoryInstead()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the session-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, BusyServerId, BusyServerName, "postgres", 18, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, BlindServerId, BlindServerName, "postgres", 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            foreach (var (serverId, serverName) in new[] { (BusyServerId, BusyServerName), (BlindServerId, BlindServerName) })
            {
                /* The coverage witness and the span gate read pg_database_stats: 25 h of span, one row a minute. */
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, serverId, serverName, windowEnd.AddHours(-25), ct);
                for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                    await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, serverId, serverName, windowStart.AddMinutes(minute - 1), ct);

                /* The ceiling, through lane 2's REAL emitter: one pg_server_config snapshot half an hour before
                   the window's end — max_connections 100, superuser_reserved_connections 3, work_mem 4 MB. */
                await PlantConfigSnapshotAsync(connection, serverId, serverName, windowEnd.AddMinutes(-30), ct);
            }

            /* Busy: a capture every five minutes, 48 in the window, two stored rows each, none redacted. The pool
               sits at 60 and peaks at 90 two hours before the window's end — 40 active, 30 idle in transaction, 20
               other — so the ratio is 90 / 97 and the parked share 30 / 90. */
            for (var capture = 1; capture <= 48; capture++)
            {
                var at = windowStart.AddMinutes(capture * 5);
                var isPeak = capture == 24;
                await PlantSessionCaptureAsync(connection, BusyServerId, BusyServerName, at,
                    total: isPeak ? 90 : 60, active: isPeak ? 40 : 20, idleInTransaction: isPeak ? 30 : 5, rows: 2, redactedRows: 0, ct);
            }

            /* Blind: the same rhythm with three stored rows per capture, two of them redacted — the monitoring
               login owns one backend and sees every other one blank. The count (95) survives; the state does not. */
            for (var capture = 1; capture <= 48; capture++)
            {
                await PlantSessionCaptureAsync(connection, BlindServerId, BlindServerName, windowStart.AddMinutes(capture * 5),
                    total: 95, active: 1, idleInTransaction: 0, rows: 3, redactedRows: 2, ct);
            }

            /* ── the collector alone, on the exact planted window. */
            var collector = new PgTargetFactCollector(postgres);
            var busyContext = new AnalysisContext
            {
                ServerId = BusyServerId,
                ServerName = BusyServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var busyFacts = await collector.CollectFactsAsync(busyContext);
            Assert.Equal(14_400_000, busyContext.ObservedDurationMs, precision: 3);

            /* Lane 2's two context facts are in the list ahead of the session family, by KEY. */
            Assert.Equal(100, Assert.Single(busyFacts, f => f.Key == PgTargetFactKeys.ConfigMaxConnections).Value);
            Assert.Equal(3, Assert.Single(busyFacts, f => f.Key == PgTargetFactKeys.ConfigSuperuserReserved).Value);

            var sessionFacts = busyFacts.Where(f => f.Source == PgTargetSources.SessionsSource).ToList();
            var saturation = Assert.Single(sessionFacts);
            Assert.Equal(PgTargetFactKeys.ConnectionSaturation, saturation.Key);
            Assert.Equal(90 / 97.0, saturation.Value, precision: 9);
            Assert.Equal(saturation.Value, saturation.Metadata["saturation_ratio"]);
            Assert.Equal(90, saturation.Metadata["peak_total_sessions"]);
            Assert.Equal(40, saturation.Metadata["peak_active_sessions"]);
            Assert.Equal(30, saturation.Metadata["peak_idle_in_transaction_sessions"]);
            Assert.Equal(20, saturation.Metadata["peak_other_sessions"]);
            Assert.Equal(30 / 90.0, saturation.Metadata["peak_idle_in_transaction_share"], precision: 9);
            /* Capture 24 of 48 at five-minute spacing: two hours before the window's end. */
            Assert.Equal(7_200, saturation.Metadata["peak_age_s"]);
            Assert.Equal(60, saturation.Metadata["latest_total_sessions"]);
            Assert.Equal(0, saturation.Metadata["latest_age_s"]);
            Assert.Equal(100, saturation.Metadata["max_connections"]);
            Assert.Equal(3, saturation.Metadata["superuser_reserved_connections"]);
            Assert.Equal(97, saturation.Metadata["usable_connections"]);
            Assert.Equal(0, saturation.Metadata["max_connections_pending_restart"]);
            Assert.Equal(1_800, saturation.Metadata["config_snapshot_age_s"]);
            Assert.Equal(48, saturation.Metadata["captures_with_rows"]);
            Assert.Equal(0, saturation.Metadata["rows_redacted_share"]);

            /* ── scored through the real scorer: 0.928 is past critical; the parked share amplifies it. */
            new FactScorer().ScoreAll(busyFacts);
            Assert.Equal(1.0, saturation.BaseSeverity, precision: 9);
            Assert.Equal(1.25, saturation.Severity, precision: 9);
            Assert.Equal(0, saturation.Metadata["threshold_lineage"]);

            /* ── the blind server: the advisory, and NO saturation fact. */
            var blindContext = new AnalysisContext
            {
                ServerId = BlindServerId,
                ServerName = BlindServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var blindFacts = await collector.CollectFactsAsync(blindContext);
            var advisory = Assert.Single(blindFacts, f => f.Source == PgTargetSources.SessionsSource);
            Assert.Equal(PgTargetFactKeys.MonitoringPermissions, advisory.Key);
            Assert.DoesNotContain(blindFacts, f => f.Key == PgTargetFactKeys.ConnectionSaturation);
            Assert.Equal(2 / 3.0, advisory.Value, precision: 9);
            Assert.Equal(96, advisory.Metadata["rows_redacted"]);
            Assert.Equal(144, advisory.Metadata["rows_stored"]);
            Assert.Equal(48, advisory.Metadata["captures_with_rows"]);
            Assert.Equal(95, advisory.Metadata["peak_total_sessions"]);
            Assert.False(advisory.Metadata.ContainsKey("saturation_ratio"));
            Assert.False(advisory.Metadata.ContainsKey("usable_connections"));

            /* ── THE EXIT CRITERION: the real analyze_server tool, anchored at the planted window's end (as_of) so
               every number is arithmetic, not timing. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);

            var busy = await DarlingMcpTools.AnalyzeServer(service, postgres, BusyServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(busy))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConnectionSaturation);

                Assert.Equal(1.25, card.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal(PgTargetSources.SessionsSource, card.GetProperty("category").GetString());
                Assert.Equal(90 / 97.0, card.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 9);

                var advice = card.GetProperty("advice");
                Assert.Equal("Connections peaked at 93% of the usable ceiling (90 of 97) — PostgreSQL refuses the next one, it does not queue it", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("90 / (100 − 3) = 93%", investigation, StringComparison.Ordinal);
                Assert.Contains("40 active, 30 idle in transaction, 20 other", investigation, StringComparison.Ordinal);
                Assert.Contains("2 h before the window's end", investigation, StringComparison.Ordinal);
                /* Lane 2's work_mem fact is in the same pass, so the memory cost is THIS server's: 4 MB × 97. */
                Assert.Contains("work_mem is 4 MB here, so 97 connections each running one such node could claim 388 MB", advice.GetProperty("remediation").GetString(), StringComparison.Ordinal);

                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()).ToList();
                Assert.Contains("get_pg_session_states", tools);

                /* No permissions card on the server that could see. */
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.MonitoringPermissions);
            }

            var blind = await DarlingMcpTools.AnalyzeServer(service, postgres, BlindServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(blind))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.MonitoringPermissions);
                Assert.Equal(0.5, card.GetProperty("severity").GetDouble(), precision: 9);
                var advice = card.GetProperty("advice");
                Assert.Equal("The monitoring login cannot see session state on this server — 67% of the stored session rows are redacted", advice.GetProperty("headline").GetString());
                Assert.Contains("96 of the 144 pg_session_states rows stored across 48 captures", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("The peak capture counted 95 sessions", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConnectionSaturation);
            }

            /* ── get_analysis_facts under the family's source: one fact per server, the lineage stamp on the ratio
               and none on the advisory. */
            var busyFactsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, BusyServerName, 4, PgTargetSources.SessionsSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(busyFactsJson))
            {
                var root = doc.RootElement;
                Assert.Equal(1, root.GetProperty("shown").GetInt32());
                var fact = Assert.Single(root.GetProperty("facts").EnumerateArray());
                Assert.Equal(PgTargetFactKeys.ConnectionSaturation, fact.GetProperty("key").GetString());
                Assert.Equal(0, fact.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                Assert.Equal(97, fact.GetProperty("metadata").GetProperty("usable_connections").GetDouble());
            }

            var blindFactsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, BlindServerName, 4, PgTargetSources.SessionsSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(blindFactsJson))
            {
                var root = doc.RootElement;
                Assert.Equal(1, root.GetProperty("shown").GetInt32());
                var fact = Assert.Single(root.GetProperty("facts").EnumerateArray());
                Assert.Equal(PgTargetFactKeys.MonitoringPermissions, fact.GetProperty("key").GetString());
                Assert.False(fact.GetProperty("metadata").TryGetProperty("saturation_ratio", out _));
                Assert.False(fact.GetProperty("metadata").TryGetProperty("threshold_lineage", out _));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static int Count(string text, string token)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(token, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += token.Length;
        }

        return count;
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>
    /// One <c>pg_server_config</c> snapshot carrying ONLY the three names this family's story reads —
    /// <c>max_connections</c>, <c>superuser_reserved_connections</c>, <c>work_mem</c> — so lane 2's emitter yields
    /// exactly those context facts and no convention check roots a card of its own beside the saturation story.
    /// </summary>
    private static async Task PlantConfigSnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        var rows = new (string Name, string Setting, string? Unit, string BootVal, string Source)[]
        {
            ("max_connections", "100", null, "100", "configuration file"),
            ("superuser_reserved_connections", "3", null, "3", "default"),
            ("work_mem", "4096", "kB", "4096", "default"),
        };

        var collectionId = CollectionIdGenerator.Next();
        foreach (var row in rows)
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Test', 'postmaster', 'integer', $8, $9, $9, NULL, NULL, false, NULL)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(serverName);
            command.Parameters.AddWithValue(row.Name);
            command.Parameters.AddWithValue(row.Setting);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.AddWithValue(row.Source);
            command.Parameters.AddWithValue(row.BootVal);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// One <c>pg_session_states</c> capture as the collector writes it: <paramref name="rows"/> stored rows sharing one
    /// <c>collection_id</c> / <c>collection_time</c>, each carrying the SAME four denormalised totals (the V86
    /// design), the first <paramref name="redactedRows"/> of them <c>state_is_redacted</c> with the privileged
    /// columns NULL as redaction leaves them.
    /// </summary>
    private static async Task PlantSessionCaptureAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime at,
        int total, int active, int idleInTransaction, int rows, int redactedRows, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        for (var row = 0; row < rows; row++)
        {
            var redacted = row < redactedRows;
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, backend_id, pid, database_name, username, application_name, client_addr,
     backend_type, state, wait_event_type, wait_event, command_tag, query_id,
     state_duration_ms, xact_duration_ms, query_duration_ms, backend_duration_ms, xmin_age, xid_age, horizon_age,
     is_idle_in_transaction, is_horizon_holder, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, $5, $6, 'appdb', 'app', 'pool-worker', NULL,
        $7, $8, NULL, NULL, $9, NULL,
        $10, $11, NULL, 600000, -1, -1, -1,
        $12, false, $13,
        $14, $15, $16, $17)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(serverName);
            command.Parameters.AddWithValue(1000L + row);
            command.Parameters.AddWithValue(5000 + row);
            command.Parameters.Add(new NpgsqlParameter { Value = redacted ? DBNull.Value : "client backend", NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = redacted ? DBNull.Value : "idle in transaction", NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = redacted ? DBNull.Value : "UPDATE", NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = redacted ? DBNull.Value : 45_000L, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            command.Parameters.Add(new NpgsqlParameter { Value = redacted ? DBNull.Value : 45_000L, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            command.Parameters.AddWithValue(!redacted);
            command.Parameters.AddWithValue(redacted);
            command.Parameters.AddWithValue(total);
            command.Parameters.AddWithValue(active);
            command.Parameters.AddWithValue(idleInTransaction);
            command.Parameters.AddWithValue(rows);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_session_states WHERE server_id IN ({BusyServerId}, {BlindServerId}); " +
            $"DELETE FROM pg_server_config WHERE server_id IN ({BusyServerId}, {BlindServerId}); " +
            $"DELETE FROM pg_database_stats WHERE server_id IN ({BusyServerId}, {BlindServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({BusyServerId}, {BlindServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({BusyServerId}, {BlindServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({BusyServerId}, {BlindServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
