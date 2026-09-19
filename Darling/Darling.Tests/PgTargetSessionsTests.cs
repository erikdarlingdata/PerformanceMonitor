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
/// fact must mean the pool is near its cliff), 0.8 scores 0.5, 0.9 scores 1.0 — with <c>threshold_lineage = 1</c>
/// on every graded fact (engine-defined ceiling, bands measured against it in the #3691 calibration); the permissions advisory at the story line, only past the majority; the two amplifiers
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
///
/// <para><b><c>PG_IDLE_IN_TRANSACTION</c> (#3691 lane 14, design §3.10).</b> Pinned: the duration bars (59.999 s is
/// ZERO, 60 s is 0.5, 10 min is 1.0) with <c>threshold_lineage = 1</c> (measured — the 2026-09-19 empty interval);
/// the horizon escalation one band up on <c>horizon_age &gt; 0</c> only — the V86 <c>-1</c> sentinel, 0, and an
/// absent key never escalate; recurrence as the one amplifier (×1.2 at 3 captures) that lifts a fired fact and
/// never grades one; the saturation ↔ idle edges active ONLY at or over <see cref="PgTargetScorer.IdleInTransactionShareBar"/>,
/// both directions, with a saturation finding that has NO idle fact byte-identical to lane 5's mesh; the
/// <c>PG_XMIN_HOLD</c> edge into the leaf for a SESSION holder with a positive horizon age; the collector's second
/// read (identity grouping, the parametrised floor, the sentinel through the aggregate, its place between the
/// redaction gate and the ceiling lookup, the unobservable stamp on the permissions advisory); and the advice
/// naming the holder, the duration, the horizon claim, the recurrence, and each of the three chains only when its
/// sibling fired. Gated: a third server whose one holder — 15 min, pinning, six captures — yields the fact at
/// 1.0 × 1.2 through the real <c>analyze_server</c>, headline naming the application and the horizon.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetSessionsTests
{
    private const string BusyServerName = "darling-pg-target-sessions-busy";
    private static readonly int BusyServerId = ServerIdHelper.GetDeterministicHashCode(BusyServerName);
    private const string BlindServerName = "darling-pg-target-sessions-blind";
    private static readonly int BlindServerId = ServerIdHelper.GetDeterministicHashCode(BlindServerName);
    private const string ParkedServerName = "darling-pg-target-sessions-parked";
    private static readonly int ParkedServerId = ServerIdHelper.GetDeterministicHashCode(ParkedServerName);

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
    public void ScoreSessionsFact_IsZeroBelowTheWarningBand_HalfAtIt_FullAtCritical_AndStampsTheLineage(double ratio, double expected)
    {
        /* The ratio is planted EXACTLY (not derived from a peak, so 0.8 is 0.8 and not 77.6 / 97's last bit):
           the band edges are the thing under test. */
        var fact = Saturation(peak: ratio * 97);
        fact.Value = ratio;
        fact.Metadata["saturation_ratio"] = ratio;

        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        /* The number that decided — the engine's ceiling, bands measured against it (2026-09-19) — is stated
           whichever side of it the fact fell: the stamp lands on the 0.79 too. */
        Assert.Equal(1, fact.Metadata["threshold_lineage"]);

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
        Assert.Contains("fleet maximum 10.3% of ceiling over 7 days; threshold_lineage = 1", block.Investigation, StringComparison.Ordinal);
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
        Assert.Contains("80% / 90% bands on the ratio sit far above anything the measured fleet reached", saturation.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain(" of ", saturation.Headline, StringComparison.Ordinal);
        Assert.Contains("pgbouncer", saturation.Remediation, StringComparison.Ordinal);
        Assert.Contains("takes a restart", saturation.Remediation, StringComparison.Ordinal);
        Assert.Equal("The monitoring login cannot see session state on this server", permissions!.Headline);
        Assert.Contains("pg_monitor", permissions.Remediation, StringComparison.Ordinal);

        /* The dispatcher routes the three keys here (lane 14 moved PG_IDLE_IN_TRANSACTION from null to its static
           block); the partial answers null for anything else it is handed. */
        var parked = PgTargetAdvice.Static(PgTargetFactKeys.IdleInTransaction);
        Assert.NotNull(parked);
        Assert.Equal(parked, PgTargetAdvice.Compose(PgTargetFactKeys.IdleInTransaction, Lookup()));
        Assert.Equal(parked, FactAdvice.GetForFactKey(PgTargetFactKeys.IdleInTransaction));
        Assert.StartsWith("A transaction sat idle past the floor", parked!.Headline, StringComparison.Ordinal);
        Assert.Contains("60 s (WARNING) and 10 min (CRITICAL)", parked.Investigation, StringComparison.Ordinal);
        Assert.Contains("idle_in_transaction_session_timeout", parked.Remediation, StringComparison.Ordinal);
        Assert.Contains("rolls its work back", parked.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", parked.Remediation, StringComparison.Ordinal);
        var compose = typeof(PgTargetAdvice).GetMethod("ComposeSessions", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.Null(compose.Invoke(null, ["PG_PROBE", Lookup()]));
    }

    /* ───────────────────────── the collector's shape, by source ───────────────────────── */

    [Fact]
    public void TheSessionReads_AreTwoQueriesOverTheCollectorTable_PickTheDenormalisedTotals_AndNeverReadTheConfigSnapshot()
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
        /* Two commands (lane 14 added the holders read — per-identity rows cannot join a one-row peak SELECT), two
           consts, the shared deadline on both, the shared degrade. */
        Assert.Equal(2, Count(code, "new NpgsqlCommand("));
        Assert.Equal(2, Count(code, "CommandTimeout = FactCommandTimeoutSeconds"));
        Assert.Contains("CommandTimeout = FactCommandTimeoutSeconds", code, StringComparison.Ordinal);
        Assert.Contains("ReportCollectionFailure(ex, context)", code, StringComparison.Ordinal);
        Assert.Contains("filled by lane 3", source, StringComparison.Ordinal);
        /* No clock of its own: every age is measured from the window's end the caller asked for. */
        Assert.DoesNotContain("DateTime.UtcNow", code, StringComparison.Ordinal);

        /* ── the holders read (lane 14). */
        var holders = PgTargetFactCollector.PgTargetIdleInTransactionSql;
        Assert.Contains(holders, PgTargetFactCollector.AllSql);
        Assert.Contains("FROM pg_session_states", holders, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_log", holders, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_server_config", holders, StringComparison.Ordinal);
        /* The floor is a PARAMETER — the scorer's warning bar, never a second copy of 60000 in SQL. */
        Assert.Contains("xact_duration_ms >= $4", holders, StringComparison.Ordinal);
        Assert.DoesNotContain("60000", holders, StringComparison.Ordinal);
        Assert.DoesNotContain("60_000", holders, StringComparison.Ordinal);
        Assert.Contains("(long)PgTargetScorer.IdleInTransactionWarningMs", code, StringComparison.Ordinal);
        /* Idle rows only, redacted rows excluded by the flag; identity grouping; recurrence is DISTINCT captures. */
        Assert.Contains("coalesce(is_idle_in_transaction, false)", holders, StringComparison.Ordinal);
        Assert.Contains("NOT coalesce(state_is_redacted, false)", holders, StringComparison.Ordinal);
        Assert.Contains("GROUP BY application_name, username, database_name", holders, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT collection_time) AS captures_seen", holders, StringComparison.Ordinal);
        /* The -1 sentinel survives the aggregate: coalesce to -1, MAX over it. */
        Assert.Contains("coalesce(horizon_age, -1)", holders, StringComparison.Ordinal);
        Assert.Contains("MAX(horizon_age)", holders, StringComparison.Ordinal);
        Assert.Contains("ORDER BY i.max_xact_duration_ms DESC", holders, StringComparison.Ordinal);
        Assert.Contains("LIMIT 25", holders, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", holders, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", holders, StringComparison.Ordinal);
        /* Ordering in the collector: redaction gate, THEN the holders read, THEN the ceiling — the idle fact needs
           no ceiling and must not be lost to the ceiling's early returns; the redaction branch stamps the
           unobservable flag instead of running the read. */
        var holdersRead = code.IndexOf("ReadIdleInTransactionAsync(connection", StringComparison.Ordinal);
        Assert.True(redactionGate > 0 && redactionGate < holdersRead && holdersRead < ceilingLookup, "redaction gate < holders read < ceiling lookup");
        var unobservable = code.IndexOf("PgTargetScorer.IdleInTransactionUnobservableKey] = 1", StringComparison.Ordinal);
        Assert.True(unobservable > redactionGate && unobservable < holdersRead, "the unobservable stamp belongs to the redaction branch");
        Assert.Contains("Key = PgTargetFactKeys.IdleInTransaction", code, StringComparison.Ordinal);
    }

    /// <summary>
    /// Lane 3 shipped this chain with NO edge and this pin held that; lane 5 (the wait family, #3542 step 5)
    /// moved it when it wrote the saturation ↔ Lock-wait mesh; lane 14 (#3691) moved it again when the
    /// <c>PG_IDLE_IN_TRANSACTION</c> hook became edges: saturation's edges are the two wait keys AND the idle fact,
    /// the idle fact's are saturation and the two wait keys, <c>PG_XMIN_HOLD</c> gains one edge into the idle fact
    /// (declared from this file, the vacuum chain's category), and the permissions fact still has none. The
    /// wait-side pins (a Lock wait's edges: saturation, and since the #3691 between-waves batch the idle leaf too)
    /// are <c>PgTargetWaitTests</c>'; the file's <c>AddEdge</c> count moved 9 → 11 with that pair. Lane 17 moved it
    /// 11 → 14 and the idle fact's destinations 3 → 4: the two Lock keys and the idle fact each walk INTO
    /// <c>PG_BLOCKING_CHAIN</c> (declared from this file because it owns those source nodes' edges; the chain's own
    /// edges and the traversal pins are <c>PgTargetBlockingTests</c>').
    /// </summary>
    [Fact]
    public void TheSaturationChain_MeshesWithTheLockWaits_AndTheIdleInTransactionLeaf_AndThePermissionsFactHasNoEdge()
    {
        var graph = new PgTargetRelationshipGraph();
        Assert.Equal(
            new[] { PgTargetFactKeys.WaitKey("Lock", null), PgTargetFactKeys.WaitKey("Lock", "relation"), PgTargetFactKeys.IdleInTransaction }.Order(StringComparer.Ordinal),
            graph.GetAllEdges(PgTargetFactKeys.ConnectionSaturation).Select(e => e.Destination).Order(StringComparer.Ordinal));
        Assert.Equal(
            new[] { PgTargetFactKeys.WaitKey("Lock", null), PgTargetFactKeys.WaitKey("Lock", "relation"), PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.BlockingChain }.Order(StringComparer.Ordinal),
            graph.GetAllEdges(PgTargetFactKeys.IdleInTransaction).Select(e => e.Destination).Order(StringComparer.Ordinal));
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.MonitoringPermissions));
        var xminLeaf = Assert.Single(graph.GetAllEdges(PgTargetFactKeys.XminHold), e => e.Destination == PgTargetFactKeys.IdleInTransaction);
        Assert.Equal("vacuum_starvation", xminLeaf.Category);
        Assert.All(graph.GetAllEdges(PgTargetFactKeys.IdleInTransaction).Where(e => e.Destination != PgTargetFactKeys.BlockingChain), e => Assert.Equal("connection_saturation", e.Category));
        Assert.Equal("blocking", Assert.Single(graph.GetAllEdges(PgTargetFactKeys.IdleInTransaction), e => e.Destination == PgTargetFactKeys.BlockingChain).Category);

        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Saturation.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Equal(14, Count(code, "AddEdge("));
        Assert.Contains("PgTargetFactKeys.IdleInTransaction", code, StringComparison.Ordinal);
        /* Every predicate reads a verdict (BaseSeverity) or the saturation fact's own share — never a bar of its own. */
        Assert.DoesNotContain("Severity > 0.", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.IdleInTransactionShareBar", code, StringComparison.Ordinal);
        Assert.Contains("Lane 3 added no edge of its own, deliberately", source, StringComparison.Ordinal);
    }

    /* ───────────────────────── PG_IDLE_IN_TRANSACTION (lane 14) ───────────────────────── */

    /// <summary>An idle-in-transaction fact as the collector composes it: one holder identity, its longest
    /// transaction in ms, its horizon claim (-1 pins nothing), how many captures it and the most-recurring identity
    /// were seen in.</summary>
    private static Fact Parked(double heldMs, double horizonAge = -1, double holderCaptures = 1, double recurring = 1,
        double identities = 1, double peakConcurrent = 1, string application = "billing-worker", string user = "billing", string database = "appdb")
        => new()
        {
            Source = PgTargetSources.SessionsSource,
            Key = PgTargetFactKeys.IdleInTransaction,
            Value = heldMs / 1_000.0,
            ServerId = 1,
            DatabaseName = database,
            ObjectName = $"{application} as {user}",
            Metadata =
            {
                [PgTargetScorer.IdleInTransactionDurationMsKey] = heldMs,
                [PgTargetScorer.IdleInTransactionHolderHorizonAgeKey] = horizonAge,
                ["holder_is_horizon_holder"] = horizonAge > 0 ? 1 : 0,
                ["holder_captures_seen"] = holderCaptures,
                [PgTargetScorer.IdleInTransactionRecurringCapturesKey] = recurring,
                ["holder_identities"] = identities,
                ["holder_identities_pinning_horizon"] = horizonAge > 0 ? 1 : 0,
                ["captures_with_holders"] = recurring,
                ["captures_with_rows"] = 48,
                ["holder_rows"] = recurring * peakConcurrent,
                ["peak_concurrent_holders"] = peakConcurrent,
                ["peak_age_s"] = 1_800,
                ["holder_last_seen_age_s"] = 300,
                ["floor_ms"] = PgTargetScorer.IdleInTransactionWarningMs,
                ["rows_redacted_share"] = 0,
            },
        };

    [Theory]
    [InlineData(59_999, -1, 0.0)]      // under the floor: ZERO, never a fraction of the bar (the read floor makes this a fact built elsewhere)
    [InlineData(60_000, -1, 0.5)]      // at the floor: the story line
    [InlineData(330_000, -1, 0.75)]    // halfway: 0.5 + 0.5 × (330 − 60) / (600 − 60)
    [InlineData(600_000, -1, 1.0)]     // critical
    [InlineData(900_000, -1, 1.0)]     // past it: pinned
    [InlineData(60_000, 5_000_000, 1.0)]   // 60 s AND pinning the horizon: one band up, WARNING → CRITICAL
    [InlineData(330_000, 12, 1.0)]         // 0.75 + 0.5 clamps at 1.0
    [InlineData(900_000, 5_000_000, 1.0)]  // already critical: the escalation cannot exceed the cap
    [InlineData(60_000, 0, 0.5)]           // horizon_age 0 is not "> 0": no escalation
    [InlineData(168_000, -1, 0.6)]         // -1 (pins nothing, V86) never escalates — and never lowers: 0.5 + 0.5 × (168 − 60) / 540
    public void ScoreSessionsFact_IdleInTransaction_GradesDurationBetweenTheMeasuredBars_AndEscalatesOneBandOnlyOnAPositiveHorizonAge(double heldMs, double horizonAge, double expected)
    {
        var fact = Parked(heldMs, horizonAge);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        /* Measured bars (the empty interval, 2026-09-19): the stamp lands on every graded fact, the 59.999 s too. */
        Assert.Equal(1, fact.Metadata["threshold_lineage"]);
        if (heldMs >= PgTargetScorer.IdleInTransactionWarningMs)
            Assert.Equal(horizonAge > 0 ? 1 : 0, fact.Metadata[PgTargetScorer.IdleInTransactionHorizonEscalatedKey]);
        else
            Assert.False(fact.Metadata.ContainsKey(PgTargetScorer.IdleInTransactionHorizonEscalatedKey));

        Assert.Equal(60_000, PgTargetScorer.IdleInTransactionWarningMs);
        Assert.Equal(600_000, PgTargetScorer.IdleInTransactionCriticalMs);
        Assert.Equal(0.5, PgTargetScorer.IdleInTransactionHorizonEscalation);
        Assert.Equal(3, PgTargetScorer.IdleInTransactionRecurrenceCaptures);
        Assert.Equal(0.2, PgTargetScorer.IdleInTransactionRecurrenceBoost);
    }

    [Fact]
    public void ScoreSessionsFact_IdleInTransaction_IsZeroWithoutADuration_AndTheSentinelIsTheDefaultWhenTheKeyIsAbsent()
    {
        var bare = new Fact { Source = PgTargetSources.SessionsSource, Key = PgTargetFactKeys.IdleInTransaction, Value = 900 };
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(bare));
        Assert.False(bare.Metadata.ContainsKey("threshold_lineage"));

        /* No horizon key at all reads as -1: a fact from an older collector cannot escalate by omission. */
        var noHorizon = Parked(600_000);
        noHorizon.Metadata.Remove(PgTargetScorer.IdleInTransactionHolderHorizonAgeKey);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(noHorizon), precision: 9);
        Assert.Equal(0, noHorizon.Metadata[PgTargetScorer.IdleInTransactionHorizonEscalatedKey]);
    }

    [Fact]
    public void IdleInTransactionAmplifiers_RecurrenceIsTheOnlyArm_ReadsTheFactItself_AndLiftsOnlyAFiredFact()
    {
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        var definitions = ((System.Collections.IEnumerable)amplifiers.Invoke(null, [PgTargetFactKeys.IdleInTransaction])!).Cast<object>().ToList();
        var definition = Assert.Single(definitions);
        var type = definition.GetType();
        Assert.Equal(PgTargetScorer.IdleInTransactionRecurrenceBoost, (double)type.GetProperty("Boost")!.GetValue(definition)!);
        var predicate = (Func<Dictionary<string, Fact>, bool>)type.GetProperty("Predicate")!.GetValue(definition)!;

        /* Three captures fires; two does not; the witness is the MOST-recurring identity, not the longest holder's. */
        Assert.True(predicate(Lookup(Parked(900_000, holderCaptures: 1, recurring: 3))));
        Assert.False(predicate(Lookup(Parked(900_000, holderCaptures: 2, recurring: 2))));
        Assert.False(predicate(Lookup()));

        /* Through the real scorer: 15 min, pinning, seen in 6 captures — base 1.0 (already critical; the escalation
           is stamped and clamps), ×1.2 = 1.2; the recurrence is the one AmplifierResult and it matched. */
        var chronic = new List<Fact> { Parked(900_000, horizonAge: 5_000_000, holderCaptures: 6, recurring: 6) };
        new FactScorer().ScoreAll(chronic);
        Assert.Equal(1.0, chronic[0].BaseSeverity, precision: 9);
        Assert.Equal(1.2, chronic[0].Severity, precision: 9);
        Assert.Equal(1, chronic[0].Metadata[PgTargetScorer.IdleInTransactionHorizonEscalatedKey]);
        var result = Assert.Single(chronic[0].AmplifierResults);
        Assert.True(result.Matched);

        /* A one-off at the floor: 0.5, no lift — persistence amplifies, it never grades. */
        var oneOff = new List<Fact> { Parked(60_000) };
        new FactScorer().ScoreAll(oneOff);
        Assert.Equal(0.5, oneOff[0].Severity, precision: 9);
        Assert.False(Assert.Single(oneOff[0].AmplifierResults).Matched);

        /* Under the floor with recurrence: nothing — an amplifier cannot make a finding out of context. */
        var context = new List<Fact> { Parked(45_000, recurring: 10) };
        new FactScorer().ScoreAll(context);
        Assert.Equal(0.0, context[0].Severity);
        Assert.Empty(context[0].AmplifierResults);
    }

    /// <summary>
    /// The saturation hook: the idle fact joins the saturation story ONLY when the saturation fact's own peak share
    /// of idle-in-transaction sessions is at or over <see cref="PgTargetScorer.IdleInTransactionShareBar"/> — and a
    /// saturation finding with NO idle fact in the set is byte-identical to lane 5's mesh (same edges active, same
    /// path, same severity, same amplifier results).
    /// </summary>
    [Fact]
    public void TheSaturationIdleEdge_IsActiveOnlyAboveTheShareBar_InBothDirections_AndSaturationAloneIsByteIdentical()
    {
        var graph = new PgTargetRelationshipGraph();

        /* Saturation alone (30 of 90 parked, fired): exactly what lane 5 left — no active edge (no wait, no idle
           fact), a one-card story at 1.25 with the two amplifier results the v1 pin recorded. */
        var alone = new List<Fact> { Saturation(90, idleInTransaction: 30) };
        new FactScorer().ScoreAll(alone);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.ConnectionSaturation, alone.ToFactLookup()));
        var soloStory = Assert.Single(new InferenceEngine(graph).BuildStories(alone));
        Assert.Equal(new[] { PgTargetFactKeys.ConnectionSaturation }, soloStory.Path);
        Assert.Equal(1.25, alone[0].Severity, precision: 9);
        Assert.Equal(2, alone[0].AmplifierResults.Count);
        Assert.True(alone[0].AmplifierResults[0].Matched);
        Assert.False(alone[0].AmplifierResults[1].Matched);

        /* Saturation at 1.25 + a fired idle fact, share 30/90: both directions active; saturation outranks and walks
           to the idle leaf; ONE story. The saturation fact's severity is unchanged by the idle fact's presence — the
           scorer's idle arm stays parked (double-counting), the join is the story. */
        var meshed = new List<Fact> { Saturation(90, idleInTransaction: 30), Parked(600_000) };
        new FactScorer().ScoreAll(meshed);
        var lookup = meshed.ToFactLookup();
        Assert.Equal(1.25, meshed[0].Severity, precision: 9);
        Assert.Equal(2, meshed[0].AmplifierResults.Count);
        Assert.Equal(PgTargetFactKeys.IdleInTransaction, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.ConnectionSaturation, lookup)).Destination);
        Assert.Equal(PgTargetFactKeys.ConnectionSaturation, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.IdleInTransaction, lookup)).Destination);
        var story = Assert.Single(new InferenceEngine(graph).BuildStories(meshed));
        Assert.Equal(new[] { PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.IdleInTransaction }, story.Path);

        /* BELOW the bar (22 of 90 parked, share 0.244) neither direction is active, whichever outranks: the pool is
           near its ceiling for some other reason and the parked transaction is its own finding — two stories. */
        var underBar = new List<Fact> { Saturation(90, idleInTransaction: 22), Parked(900_000, horizonAge: 5_000_000, recurring: 6) };
        new FactScorer().ScoreAll(underBar);
        var underLookup = underBar.ToFactLookup();
        Assert.Equal(1.0, underBar[0].Severity, precision: 9);
        Assert.Equal(1.2, underBar[1].Severity, precision: 9);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.ConnectionSaturation, underLookup));
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.IdleInTransaction, underLookup));
        var split = new InferenceEngine(graph).BuildStories(underBar);
        Assert.Equal(2, split.Count);
        Assert.Equal(PgTargetFactKeys.IdleInTransaction, split[0].RootFactKey);
        Assert.Equal(PgTargetFactKeys.ConnectionSaturation, split[1].RootFactKey);

        /* At the bar with the idle fact OUTRANKING (78 / 97 = 0.804 → base 0.52, share 20 / 78 = 0.256 → ×1.25 =
           0.65; the idle fact at 1.2): the idle root walks to saturation — the reverse direction, one story. */
        var idleLeads = new List<Fact> { Saturation(78, idleInTransaction: 20), Parked(900_000, horizonAge: 5_000_000, recurring: 6) };
        new FactScorer().ScoreAll(idleLeads);
        Assert.True(idleLeads[1].Severity > idleLeads[0].Severity);
        var reversed = Assert.Single(new InferenceEngine(graph).BuildStories(idleLeads));
        Assert.Equal(new[] { PgTargetFactKeys.IdleInTransaction, PgTargetFactKeys.ConnectionSaturation }, reversed.Path);
        Assert.Equal(PgTargetSources.SessionsSource, reversed.Category);

        /* Saturation present but NOT fired (context, 70/97) with a fired idle fact: no edge either way; the idle
           fact roots alone and the context fact roots nothing. */
        var quietPool = new List<Fact> { Saturation(70, idleInTransaction: 60), Parked(600_000) };
        new FactScorer().ScoreAll(quietPool);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.IdleInTransaction, quietPool.ToFactLookup()));
        Assert.Equal(new[] { PgTargetFactKeys.IdleInTransaction }, Assert.Single(new InferenceEngine(graph).BuildStories(quietPool)).Path);
    }

    [Fact]
    public void TheXminHoldEdge_IntoTheIdleLeaf_NeedsASessionHolder_AndAPositiveHorizonAge()
    {
        var graph = new PgTargetRelationshipGraph();
        static Fact Hold(string source) => new()
        {
            Source = PgTargetSources.VacuumSource, Key = PgTargetFactKeys.XminHold, Value = 5_000_000, BaseSeverity = 1.0, Severity = 1.0,
            Metadata = { [PgTargetScorer.XminHolderSourceKey] = PgTargetAdvice.HolderSourceCode(source) },
        };
        var pinning = Parked(900_000, horizonAge: 5_000_000);
        pinning.BaseSeverity = 1.0;
        var notPinning = Parked(900_000, horizonAge: -1);
        notPinning.BaseSeverity = 1.0;

        Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.XminHold, Lookup(Hold("session"), pinning)), e => e.Destination == PgTargetFactKeys.IdleInTransaction);
        /* The -1 sentinel: the session pins nothing, so it is not the holder PG_XMIN_HOLD names. */
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.XminHold, Lookup(Hold("session"), notPinning)), e => e.Destination == PgTargetFactKeys.IdleInTransaction);
        /* A slot holding the horizon is a different story, whatever the parked session is doing. */
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.XminHold, Lookup(Hold("replication_slot"), pinning)), e => e.Destination == PgTargetFactKeys.IdleInTransaction);
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.XminHold, Lookup(Hold("session"))), e => e.Destination == PgTargetFactKeys.IdleInTransaction);
    }

    [Fact]
    public void ComposeSessions_IdleInTransaction_NamesTheHolder_TheDuration_TheHorizonClaim_TheRecurrence_AndOnlyTheChainsInTheSet()
    {
        /* The exit shape: 15 min, pinning, 6 of 48 captures, one holder. */
        var chronic = Parked(900_000, horizonAge: 5_000_000, holderCaptures: 6, recurring: 6);
        new FactScorer().ScoreAll([chronic]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.IdleInTransaction, Lookup(chronic))!;
        Assert.Equal("billing-worker as billing idle in transaction for 15 min — the application never committed or rolled back, and it is holding the xmin horizon back", block.Headline);
        Assert.Contains("billing-worker as billing in appdb — had held its transaction open for 15 min, last seen 5 min before the window's end, and was over the 1 min floor in 6 of the 48 captures", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("1 session was parked past the floor at once; 6 of the captures had at least one, from 1 distinct holder", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PINS THE XMIN HORIZON: PostgreSQL reports the xid / xmin it holds at an age of 5,000,000 transactions", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("graded one band higher", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("recurred in 6 captures", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("zero idle-in-transaction rows reached 60 s over 7 days across 50 clusters", block.Investigation, StringComparison.Ordinal);
        /* No sibling in the set: none of the three chains is claimed. */
        Assert.DoesNotContain("PG_XMIN_HOLD fired", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Lock waits fired", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("PG_CONNECTION_SATURATION fired", block.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("The lever is the application behind billing-worker as billing: commit or roll back", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("it recurred in 6 captures, so it is a path that runs", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("idle_in_transaction_session_timeout terminates any session idle in a transaction past the interval and ROLLS ITS WORK BACK", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("pg_terminate_backend(pid)", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("lets autovacuum reclaim the dead tuples", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_session_states", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_xmin_horizon", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("get_pg_blocking", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("900000", block.Investigation + block.Headline, StringComparison.Ordinal);

        /* A one-off that pins nothing, three parked at once, with every sibling fired: the -1 is stated as NOTHING,
           no escalation clause, no recurrence clause, and all three chains are named with the saturation share. */
        var oneOff = Parked(252_000, horizonAge: -1, holderCaptures: 1, recurring: 1, identities: 3, peakConcurrent: 3, application: "reporting", user: "analyst");
        var saturation = Saturation(90, idleInTransaction: 30);
        var hold = new Fact
        {
            Source = PgTargetSources.VacuumSource, Key = PgTargetFactKeys.XminHold, Value = 4_000_000, BaseSeverity = 0.8,
            Metadata = { [PgTargetScorer.XminHolderSourceKey] = PgTargetAdvice.HolderSourceCode("session"), [PgTargetScorer.XminAgeKey] = 4_000_000 },
        };
        var lockWait = new Fact { Source = PgTargetSources.WaitsSource, Key = PgTargetFactKeys.WaitKey("Lock", "relation"), BaseSeverity = 0.6 };
        new FactScorer().ScoreAll([oneOff, saturation]);
        var busy = PgTargetAdvice.Compose(PgTargetFactKeys.IdleInTransaction, Lookup(oneOff, saturation, hold, lockWait))!;
        Assert.Equal("reporting as analyst idle in transaction for 4 min 12 s (3 parked at once at the peak) — the application never committed or rolled back", busy.Headline);
        Assert.Contains("This holder pins NOTHING", busy.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("graded one band higher", busy.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("recurred in", busy.Investigation, StringComparison.Ordinal);
        Assert.Contains("3 sessions were parked past the floor at once", busy.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_XMIN_HOLD fired on this server with a SESSION as the winning holder (xmin age 4,000,000)", busy.Investigation, StringComparison.Ordinal);
        Assert.Contains("Lock waits fired in the same window", busy.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_CONNECTION_SATURATION fired: the pool peaked at 93% of its usable ceiling with 33% of the peak capture idle in transaction — parked transactions are the population filling the slots", busy.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_xmin_horizon", busy.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_blocking", busy.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("it recurred in", busy.Remediation, StringComparison.Ordinal);

        /* The xmin chain is named only for a SESSION holder: a slot holding the horizon says nothing here. */
        hold.Metadata[PgTargetScorer.XminHolderSourceKey] = PgTargetAdvice.HolderSourceCode("replication_slot");
        var slot = PgTargetAdvice.Compose(PgTargetFactKeys.IdleInTransaction, Lookup(oneOff, hold))!;
        Assert.DoesNotContain("PG_XMIN_HOLD fired", slot.Investigation, StringComparison.Ordinal);

        /* The permissions block says WHY the family is silent on parked transactions when the stamp is there. */
        var blind = Permissions(64, 96);
        blind.Metadata[PgTargetScorer.IdleInTransactionUnobservableKey] = 1;
        Assert.Contains("idle_in_transaction_unobservable = 1", PgTargetAdvice.Compose(PgTargetFactKeys.MonitoringPermissions, Lookup(blind))!.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("idle_in_transaction_unobservable", PgTargetAdvice.Compose(PgTargetFactKeys.MonitoringPermissions, Lookup(Permissions(64, 96)))!.Investigation, StringComparison.Ordinal);
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
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ParkedServerId, ParkedServerName, "postgres", 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            foreach (var (serverId, serverName) in new[] { (BusyServerId, BusyServerName), (BlindServerId, BlindServerName), (ParkedServerId, ParkedServerName) })
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

            /* Parked (lane 14's exit shape): the same rhythm, one stored row per capture, a quiet pool of 30 (30 / 97 is
               context, never a saturation finding). In captures 10–15 the stored row is ONE holder — billing-worker
               as billing in appdb — idle in transaction for 15 minutes with horizon_age 5,000,000 (it pins the
               horizon); every other capture stores the busy server's 45 s row, under the 60 s floor. */
            for (var capture = 1; capture <= 48; capture++)
            {
                var isHolder = capture is >= 10 and <= 15;
                await PlantSessionCaptureAsync(connection, ParkedServerId, ParkedServerName, windowStart.AddMinutes(capture * 5),
                    total: 30, active: 10, idleInTransaction: isHolder ? 2 : 1, rows: 1, redactedRows: 0, ct,
                    xactDurationMs: isHolder ? 900_000L : 45_000L, horizonAge: isHolder ? 5_000_000L : -1L,
                    applicationName: isHolder ? "billing-worker" : "pool-worker", username: isHolder ? "billing" : "app");
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
            /* One fact: the busy server's parked sessions are all at 45 s, UNDER the idle-in-transaction floor. */
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
            Assert.Equal(1, saturation.Metadata["threshold_lineage"]);

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
            /* The idle-in-transaction read was not run on blank rows, and the advisory says so. */
            Assert.Equal(1, advisory.Metadata[PgTargetScorer.IdleInTransactionUnobservableKey]);
            Assert.DoesNotContain(blindFacts, f => f.Key == PgTargetFactKeys.IdleInTransaction);

            /* ── the parked server: the idle fact, named for its holder, beside a context-only saturation fact. */
            var parkedContext = new AnalysisContext
            {
                ServerId = ParkedServerId,
                ServerName = ParkedServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var parkedFacts = await collector.CollectFactsAsync(parkedContext);
            var parkedSessionFacts = parkedFacts.Where(f => f.Source == PgTargetSources.SessionsSource).ToList();
            Assert.Equal(2, parkedSessionFacts.Count);
            var parked = Assert.Single(parkedSessionFacts, f => f.Key == PgTargetFactKeys.IdleInTransaction);
            var quietPool = Assert.Single(parkedSessionFacts, f => f.Key == PgTargetFactKeys.ConnectionSaturation);
            /* Emission order: the idle fact before the ceiling lookup. */
            Assert.True(parkedSessionFacts.IndexOf(parked) < parkedSessionFacts.IndexOf(quietPool));
            Assert.Equal(30 / 97.0, quietPool.Value, precision: 9);
            Assert.Equal(900, parked.Value, precision: 9);
            Assert.Equal("billing-worker as billing", parked.ObjectName);
            Assert.Equal("appdb", parked.DatabaseName);
            Assert.Equal(900_000, parked.Metadata[PgTargetScorer.IdleInTransactionDurationMsKey]);
            Assert.Equal(5_000_000, parked.Metadata[PgTargetScorer.IdleInTransactionHolderHorizonAgeKey]);
            Assert.Equal(1, parked.Metadata["holder_is_horizon_holder"]);
            Assert.Equal(6, parked.Metadata["holder_captures_seen"]);
            Assert.Equal(6, parked.Metadata[PgTargetScorer.IdleInTransactionRecurringCapturesKey]);
            Assert.Equal(1, parked.Metadata["holder_identities"]);
            Assert.Equal(1, parked.Metadata["holder_identities_pinning_horizon"]);
            Assert.Equal(6, parked.Metadata["captures_with_holders"]);
            Assert.Equal(48, parked.Metadata["captures_with_rows"]);
            Assert.Equal(6, parked.Metadata["holder_rows"]);
            Assert.Equal(1, parked.Metadata["peak_concurrent_holders"]);
            Assert.Equal(60_000, parked.Metadata["floor_ms"]);
            Assert.Equal(0, parked.Metadata["rows_redacted_share"]);
            /* Capture 15 of 48 at five-minute spacing: 33 captures × 5 min = 9,900 s before the window's end — the
               holder's last sighting and (ties broken by time) the peak capture alike. */
            Assert.Equal(9_900, parked.Metadata["holder_last_seen_age_s"]);
            Assert.Equal(9_900, parked.Metadata["peak_age_s"]);

            /* Scored: 15 min is past critical (1.0), the horizon claim is stamped (and clamps), recurrence ×1.2. */
            new FactScorer().ScoreAll(parkedFacts);
            Assert.Equal(1.0, parked.BaseSeverity, precision: 9);
            Assert.Equal(1.2, parked.Severity, precision: 9);
            Assert.Equal(1, parked.Metadata["threshold_lineage"]);
            Assert.Equal(1, parked.Metadata[PgTargetScorer.IdleInTransactionHorizonEscalatedKey]);
            Assert.Equal(0.0, quietPool.Severity);

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
                Assert.Contains("idle_in_transaction_unobservable = 1", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConnectionSaturation);
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.IdleInTransaction);
            }

            /* ── THE LANE-14 EXIT CRITERION: PG_IDLE_IN_TRANSACTION CRITICAL through the real analyze_server, with the
               vacuum escalation and advice in PostgreSQL nouns naming the holder. */
            var parkedJson = await DarlingMcpTools.AnalyzeServer(service, postgres, ParkedServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(parkedJson))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.IdleInTransaction);
                Assert.Equal(1.2, card.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal(PgTargetSources.SessionsSource, card.GetProperty("category").GetString());
                Assert.Equal(900, card.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 9);

                var advice = card.GetProperty("advice");
                Assert.Equal("billing-worker as billing idle in transaction for 15 min — the application never committed or rolled back, and it is holding the xmin horizon back", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("was over the 1 min floor in 6 of the 48 captures", investigation, StringComparison.Ordinal);
                Assert.Contains("PINS THE XMIN HORIZON: PostgreSQL reports the xid / xmin it holds at an age of 5,000,000 transactions", investigation, StringComparison.Ordinal);
                Assert.Contains("graded one band higher", investigation, StringComparison.Ordinal);
                Assert.Contains("recurred in 6 captures", investigation, StringComparison.Ordinal);
                /* The pool is context here (30 / 97): no saturation chain is claimed, and no saturation card exists. */
                Assert.DoesNotContain("PG_CONNECTION_SATURATION fired", investigation, StringComparison.Ordinal);
                var remediation = advice.GetProperty("remediation").GetString()!;
                Assert.Contains("idle_in_transaction_session_timeout", remediation, StringComparison.Ordinal);
                Assert.Contains("get_pg_xmin_horizon", remediation, StringComparison.Ordinal);

                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()).ToList();
                Assert.Contains("get_pg_session_states", tools);
                Assert.All(tools, t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.ConnectionSaturation);
            }

            var parkedFactsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ParkedServerName, 4, PgTargetSources.SessionsSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(parkedFactsJson))
            {
                var fact = Assert.Single(doc.RootElement.GetProperty("facts").EnumerateArray(), f => f.GetProperty("key").GetString() == PgTargetFactKeys.IdleInTransaction);
                var metadata = fact.GetProperty("metadata");
                Assert.Equal(1, metadata.GetProperty("threshold_lineage").GetDouble());
                Assert.Equal(1, metadata.GetProperty(PgTargetScorer.IdleInTransactionHorizonEscalatedKey).GetDouble());
                Assert.Equal(6, metadata.GetProperty(PgTargetScorer.IdleInTransactionRecurringCapturesKey).GetDouble());
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
                Assert.Equal(1, fact.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
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
        int total, int active, int idleInTransaction, int rows, int redactedRows, CancellationToken ct,
        long xactDurationMs = 45_000L, long horizonAge = -1L, string applicationName = "pool-worker", string username = "app")
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
VALUES ($1, $2, $3, $4, $5, $6, 'appdb', $18, $19, NULL,
        $7, $8, NULL, NULL, $9, NULL,
        $10, $11, NULL, 600000, -1, -1, $20,
        $12, $21, $13,
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
            command.Parameters.Add(new NpgsqlParameter { Value = redacted ? DBNull.Value : xactDurationMs, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            command.Parameters.Add(new NpgsqlParameter { Value = redacted ? DBNull.Value : xactDurationMs, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            command.Parameters.AddWithValue(!redacted);
            command.Parameters.AddWithValue(redacted);
            command.Parameters.AddWithValue(total);
            command.Parameters.AddWithValue(active);
            command.Parameters.AddWithValue(idleInTransaction);
            command.Parameters.AddWithValue(rows);
            /* The holder identity and its horizon claim (lane 14): username, application_name, horizon_age,
               is_horizon_holder — the collector's -1 sentinel when the session pins nothing. */
            command.Parameters.AddWithValue(username);
            command.Parameters.AddWithValue(applicationName);
            command.Parameters.AddWithValue(horizonAge);
            command.Parameters.AddWithValue(horizonAge > 0);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_session_states WHERE server_id IN ({BusyServerId}, {BlindServerId}, {ParkedServerId}); " +
            $"DELETE FROM pg_server_config WHERE server_id IN ({BusyServerId}, {BlindServerId}, {ParkedServerId}); " +
            $"DELETE FROM pg_database_stats WHERE server_id IN ({BusyServerId}, {BlindServerId}, {ParkedServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({BusyServerId}, {BlindServerId}, {ParkedServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({BusyServerId}, {BlindServerId}, {ParkedServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({BusyServerId}, {BlindServerId}, {ParkedServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
