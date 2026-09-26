/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3653 A5 — identity-epoch detection. The pure comparator's NULL rules, the persisted form's round trip,
/// the observation's effects on a REAL <see cref="CollectorDeltaCalculator"/> (baselines and pass window
/// forgotten, the count measured, old and new pairs persisted, the sentence queued for the host), the
/// change-only persistence that keeps an ordinary pass from writing state, and the four carriers: the
/// wait-stats and CPU collectors' second result sets (the former first in both hosts' order and pinned as
/// forgetting BEFORE its first subtraction; the latter kept for the operator who disables wait_stats and
/// pinned as NOT forgetting twice on the same calculator), the statements collector's trailing column and
/// the Aurora wait collector's trailing column — each pinned as forgetting its own groups before its first
/// subtraction, which is the property that makes a carrier's own family honest on the pass that sees the epoch.
///
/// <para>Written against the shared calculator rather than a double wherever the claim is about what the
/// calculator forgets, because "ClearGroups leaves the other groups alone" is a claim about the real cache;
/// the recording double is used where the claim is about ORDER, which only a recorder can see.</para>
/// </summary>
public sealed class ServerEpochTests
{
    private const int ServerId = 7;

    private static readonly DateTime T0 = new(2026, 9, 1, 3, 4, 5, DateTimeKind.Unspecified);
    private static readonly DateTime T1 = new(2026, 9, 19, 1, 2, 3, DateTimeKind.Unspecified);
    private static readonly DateTime Pass = new(2026, 9, 19, 4, 0, 0, DateTimeKind.Utc);

    private static CollectorContext Context(
        ICollectorDeltaCalculator deltas,
        IReadOnlyDictionary<string, string>? state = null,
        bool isAzureSqlDb = false,
        bool postgres = false,
        DateTime? collectionTime = null)
        => new()
        {
            ServerId = ServerId,
            ServerName = "test-server",
            CollectionTime = collectionTime ?? Pass,
            Deltas = deltas,
            Target = postgres
                ? new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, PostgresVersionNum = 170000 }
                : new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            State = state ?? CollectorContext.NoState,
        };

    private static Dictionary<string, string> StateWith(string key, ServerEpoch.Stamp stamp)
        => new(StringComparer.Ordinal) { [key] = ServerEpoch.Serialize(stamp) };

    /* ---------------- the comparator ---------------- */

    /// <summary>
    /// Unknown is not different: only a value → DIFFERENT value on some component is an epoch. Every
    /// transition through a null — on either side, on either component — and a null prior are no evidence.
    /// </summary>
    [Fact]
    public void IsNewEpoch_OnlyAKnownToDifferentKnownTransitionCounts()
    {
        var a = new ServerEpoch.Stamp(T0, "SRV01");

        Assert.False(ServerEpoch.IsNewEpoch(null, a), "a null prior is the first observation, never an epoch");
        Assert.False(ServerEpoch.IsNewEpoch(a, a), "the same pair is the same instance");
        Assert.False(ServerEpoch.IsNewEpoch(a, new ServerEpoch.Stamp(null, null)), "an all-unknown observation says nothing");
        Assert.False(ServerEpoch.IsNewEpoch(a, new ServerEpoch.Stamp(null, "SRV01")), "value → null on the start time is not a restart");
        Assert.False(ServerEpoch.IsNewEpoch(new ServerEpoch.Stamp(null, "SRV01"), new ServerEpoch.Stamp(T1, "SRV01")), "null → value on the start time is not a restart");
        Assert.False(ServerEpoch.IsNewEpoch(new ServerEpoch.Stamp(T0, null), new ServerEpoch.Stamp(null, "SRV02")), "two components each known on one side only compare nothing");

        Assert.True(ServerEpoch.IsNewEpoch(a, new ServerEpoch.Stamp(T1, "SRV01")), "a new start time on the same name is a restart");
        Assert.True(ServerEpoch.IsNewEpoch(a, new ServerEpoch.Stamp(T0, "SRV02")), "a new name on the same start time is a different instance");
        Assert.True(ServerEpoch.IsNewEpoch(a, new ServerEpoch.Stamp(T1, "SRV02")), "both moving is the failover shape");
        Assert.True(ServerEpoch.IsNewEpoch(a, new ServerEpoch.Stamp(T1, null)), "a known-to-different start time is an epoch even when the name went unknown");
    }

    /// <summary>@@SERVERNAME is a SQL Server identifier — a case change is the same instance.</summary>
    [Fact]
    public void IsNewEpoch_ComparesNamesCaseInsensitively()
        => Assert.False(ServerEpoch.IsNewEpoch(new ServerEpoch.Stamp(T0, "srv01"), new ServerEpoch.Stamp(T0, "SRV01")));

    /// <summary>
    /// Ticks decide the start time, not Kind: a SQL Server datetime2 arrives Unspecified and a PostgreSQL
    /// timestamptz arrives Utc, and each source is only ever compared against itself.
    /// </summary>
    [Fact]
    public void IsNewEpoch_ComparesStartTimesByTicks()
    {
        var unspecified = new ServerEpoch.Stamp(T0, null);
        var sameTicksUtc = new ServerEpoch.Stamp(DateTime.SpecifyKind(T0, DateTimeKind.Utc), null);

        Assert.False(ServerEpoch.IsNewEpoch(unspecified, sameTicksUtc));
        Assert.True(ServerEpoch.IsNewEpoch(unspecified, new ServerEpoch.Stamp(T0.AddTicks(1), null)));
    }

    [Fact]
    public void Merge_KeepsTheLastKnownComponent()
    {
        var prior = new ServerEpoch.Stamp(T0, "SRV01");

        Assert.Equal(prior, ServerEpoch.Merge(prior, new ServerEpoch.Stamp(null, null)));
        Assert.Equal(new ServerEpoch.Stamp(T1, "SRV01"), ServerEpoch.Merge(prior, new ServerEpoch.Stamp(T1, null)));
        Assert.Equal(new ServerEpoch.Stamp(T0, "SRV02"), ServerEpoch.Merge(prior, new ServerEpoch.Stamp(null, "SRV02")));
        Assert.Equal(new ServerEpoch.Stamp(T1, null), ServerEpoch.Merge(null, new ServerEpoch.Stamp(T1, null)));
    }

    /* ---------------- the persisted form ---------------- */

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void Serialize_RoundTrips_WithTicksAndKindIntact(bool withStartTime, bool withName)
    {
        var stamp = new ServerEpoch.Stamp(
            withStartTime ? new DateTime(2026, 9, 19, 1, 2, 3, DateTimeKind.Utc).AddTicks(1234567) : null,
            withName ? "SRV01\\INST" : null);

        var text = ServerEpoch.Serialize(stamp);

        Assert.True(ServerEpoch.TryParse(text, out var parsed), text);
        Assert.Equal(stamp, parsed);
        if (withStartTime)
        {
            Assert.Equal(DateTimeKind.Utc, parsed.StartTime!.Value.Kind);
        }
    }

    /// <summary>A SQL Server start time is Unspecified and must stay so — RoundtripKind, not AssumeUniversal.</summary>
    [Fact]
    public void Serialize_KeepsAnUnspecifiedKindUnspecified()
    {
        Assert.True(ServerEpoch.TryParse(ServerEpoch.Serialize(new ServerEpoch.Stamp(T0, "SRV01")), out var parsed));
        Assert.Equal(DateTimeKind.Unspecified, parsed.StartTime!.Value.Kind);
        Assert.Equal(T0, parsed.StartTime.Value);
    }

    /// <summary>
    /// An unreadable prior is "no prior": the conservative direction, because a malformed value can then
    /// only make one observation late, never manufacture an epoch.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("no separator here")]
    [InlineData("not-a-date|SRV01")]
    [InlineData("2026-09-19|SRV01")]
    public void TryParse_RefusesWhatItCannotRead(string? text)
        => Assert.False(ServerEpoch.TryParse(text, out _));

    [Fact]
    public void TryParse_ReadsAnEmptyPairAsEmpty()
    {
        Assert.True(ServerEpoch.TryParse("|", out var parsed));
        Assert.True(parsed.IsEmpty);
    }

    /* ---------------- the instance observation, on the real calculator ---------------- */

    /// <summary>
    /// The whole point, on the shared calculator both SKUs run: a known → different pair forgets EVERY
    /// baseline and pass window under the server (the next delta on an old key is a first sighting, (0, 0)),
    /// measures the marker onto the run's note, persists the new pair and the pair it replaced, and queues
    /// one sentence naming old and new for the host to log — once.
    /// </summary>
    [Fact]
    public void ObserveInstance_OnAnEpoch_ForgetsEverythingAndRecordsTheChange()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDelta(ServerId, "wait_stats_time", "PAGEIOLATCH_SH", 1_000, Pass.AddMinutes(-1), CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDelta(ServerId, "perfmon", "Batch Requests/sec", 5_000, Pass.AddMinutes(-1), CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDelta(ServerId + 1, "wait_stats_time", "PAGEIOLATCH_SH", 1_000, Pass.AddMinutes(-1), CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var prior = new ServerEpoch.Stamp(T0, "SRV01");
        var current = new ServerEpoch.Stamp(T1, "SRV02");
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, prior));

        Assert.True(ServerEpoch.ObserveInstance(context, current));

        /* Forgotten: both families under the server re-baseline; the other server is untouched. */
        Assert.Equal(0, deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_time", "PAGEIOLATCH_SH", 9_000, out var interval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(0, interval);
        Assert.Equal(0, deltas.CalculateDeltaWithInterval(ServerId, "perfmon", "Batch Requests/sec", 9_000, out interval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(0, interval);
        Assert.Equal(500, deltas.CalculateDeltaWithInterval(ServerId + 1, "wait_stats_time", "PAGEIOLATCH_SH", 1_500, out interval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(60, interval);

        /* Marked: the count on the run's note, in the label grammar the seam enforces. */
        var measurement = Assert.Single(context.Measurements);
        Assert.Equal(ServerEpoch.IdentityChangesMeasurement, measurement.Label);
        Assert.Equal(1, measurement.Value);
        Assert.Equal(ServerEpoch.IdentityChangesMeasurement + "=1", CollectorMeasurementNote.Render(context.Measurements));

        /* Persisted: the new pair under the key, the replaced pair beside it. */
        Assert.Equal(ServerEpoch.Serialize(current), context.PendingState[ServerEpoch.IdentityStateKey]);
        Assert.Equal(ServerEpoch.Serialize(prior), context.PendingState[ServerEpoch.IdentityPreviousStateKey]);

        /* Queued for the host, naming both sides, and read-once. */
        var line = Assert.Single(deltas.DrainDiscontinuities(ServerId));
        Assert.Contains("2026-09-01 03:04:05", line, StringComparison.Ordinal);
        Assert.Contains("2026-09-19 01:02:03", line, StringComparison.Ordinal);
        Assert.Contains("SRV01", line, StringComparison.Ordinal);
        Assert.Contains("SRV02", line, StringComparison.Ordinal);
        Assert.Empty(deltas.DrainDiscontinuities(ServerId));
        Assert.Empty(deltas.DrainDiscontinuities(ServerId + 1));
    }

    /// <summary>The first observation ever (no store row) persists the pair and changes nothing else.</summary>
    [Fact]
    public void ObserveInstance_WithNoPrior_PersistsAndForgetsNothing()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDelta(ServerId, "wait_stats_time", "PAGEIOLATCH_SH", 1_000, Pass.AddMinutes(-1), CollectorDeltaCalculator.DefaultMaxGapSeconds);
        var context = Context(deltas);

        Assert.False(ServerEpoch.ObserveInstance(context, new ServerEpoch.Stamp(T0, "SRV01")));

        Assert.Equal(500, deltas.CalculateDelta(ServerId, "wait_stats_time", "PAGEIOLATCH_SH", 1_500, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Empty(context.Measurements);
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(T0, "SRV01")), Assert.Single(context.PendingState).Value);
        Assert.Empty(deltas.DrainDiscontinuities(ServerId));
    }

    /// <summary>
    /// Change-only persistence: the carrier runs every minute on every target, and the steady state must
    /// write NO state row — a same-pair pass leaves PendingState empty, so the host's save never runs.
    /// </summary>
    [Fact]
    public void ObserveInstance_WithTheSamePair_WritesNoState()
    {
        var deltas = new CollectorDeltaCalculator();
        var pair = new ServerEpoch.Stamp(T0, "SRV01");
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, pair));

        Assert.False(ServerEpoch.ObserveInstance(context, pair));

        Assert.Empty(context.PendingState);
        Assert.Empty(context.Measurements);
    }

    /// <summary>
    /// A pass that observes nothing (both components null — the permission-poor or Azure shape) neither
    /// changes nor erases: the known pair stays in the store for the next KNOWN observation to compare against.
    /// </summary>
    [Fact]
    public void ObserveInstance_WithAnUnknownPair_NeitherChangesNorErases()
    {
        var deltas = new CollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")));

        Assert.False(ServerEpoch.ObserveInstance(context, new ServerEpoch.Stamp(null, null)));

        Assert.Empty(context.PendingState);
    }

    /// <summary>A component becoming known for the first time is persisted (so it can be compared later) but is not an epoch.</summary>
    [Fact]
    public void ObserveInstance_WhenAComponentBecomesKnown_PersistsTheMergedPairWithoutAnEpoch()
    {
        var deltas = new CollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(null, "SRV01")));

        Assert.False(ServerEpoch.ObserveInstance(context, new ServerEpoch.Stamp(T0, null)));

        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(T0, "SRV01")), context.PendingState[ServerEpoch.IdentityStateKey]);
        Assert.False(context.PendingState.ContainsKey(ServerEpoch.IdentityPreviousStateKey));
        Assert.Empty(context.Measurements);
    }

    /// <summary>
    /// The host-restart case the item names: the host seeded baselines from its store for a target that
    /// restarted while the host was down. The prior is the PERSISTED pair, so the first pass sees the epoch
    /// and drops the seeded baselines rather than subtracting from them.
    /// </summary>
    [Fact]
    public void ObserveInstance_CatchesATargetThatRestartedWhileTheHostWasDown()
    {
        var deltas = new SeedingCalculator();
        deltas.SeedForTest(ServerId, "wait_stats_time", "PAGEIOLATCH_SH", 1_000_000, Pass.AddMinutes(-3));

        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")));

        Assert.True(ServerEpoch.ObserveInstance(context, new ServerEpoch.Stamp(T1, "SRV01")));

        /* The restarted instance's counter is SMALL, but that is not what saves us here: it is that the
           seeded baseline is gone, so this is a first sighting (0, 0) rather than a reset-branch (0, 0) —
           and had the counter been LARGER (a failover to a hotter replica), the same forget is the only
           thing that stops `new minus old` from being stored as work. */
        Assert.Equal(0, deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_time", "PAGEIOLATCH_SH", 5_000_000, out var interval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(0, interval);
    }

    /* ---------------- two carriers, one forget ---------------- */

    /// <summary>
    /// The second SQL Server carrier on the same calculator does NOT forget again. wait_stats (first in the
    /// order) observes the epoch and forgets; the families between the carriers re-baseline on the new
    /// instance; then cpu_utilization observes the same current pair against ITS OWN persisted prior (a
    /// separate collector_state row, so it too sees a change) — and the baselines just re-established must
    /// survive it, or the five families pay one more (0, 0) pass, the very pass the first carrier bought back.
    /// The second observer still records: its marker measured, its pair and the replaced pair persisted — it
    /// observed a change — but exactly one forget and one sentence.
    /// </summary>
    [Fact]
    public void ObserveInstance_ASecondCarrierOnTheSameCalculator_DoesNotForgetTwice()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDelta(ServerId, "latch_stats_time", "BUFFER", 1_000, Pass.AddMinutes(-1), CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var prior = new ServerEpoch.Stamp(T0, "SRV01");
        var current = new ServerEpoch.Stamp(T1, "SRV01");

        /* wait_stats: the forget. */
        var first = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, prior));
        Assert.True(ServerEpoch.ObserveInstance(first, current));
        Assert.Single(deltas.DrainDiscontinuities(ServerId));

        /* latch_stats, between the carriers: re-baselines on the new instance this pass. */
        Assert.Equal(0, deltas.CalculateDeltaWithInterval(ServerId, "latch_stats_time", "BUFFER", 50, out var interval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(0, interval);

        /* cpu_utilization: its own prior is the old pair too, so it sees the change — and must not forget. */
        var second = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, prior));
        Assert.True(ServerEpoch.ObserveInstance(second, current));

        Assert.Equal(20, deltas.CalculateDeltaWithInterval(ServerId, "latch_stats_time", "BUFFER", 70, out interval, Pass.AddMinutes(1), CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(60, interval);
        Assert.Empty(deltas.DrainDiscontinuities(ServerId));

        Assert.Equal(ServerEpoch.IdentityChangesMeasurement + "=1", CollectorMeasurementNote.Render(second.Measurements));
        Assert.Equal(ServerEpoch.Serialize(current), second.PendingState[ServerEpoch.IdentityStateKey]);
        Assert.Equal(ServerEpoch.Serialize(prior), second.PendingState[ServerEpoch.IdentityPreviousStateKey]);
    }

    /// <summary>
    /// The memo remembers an IDENTITY, not "forgot once": a second carrier that reads a FURTHER move (the
    /// instance restarted again between the two collectors of one pass) forgets again, because the baselines
    /// the first forget re-established belong to the instance in between.
    /// </summary>
    [Fact]
    public void ObserveInstance_ASecondCarrierThatSeesAFurtherMove_ForgetsAgain()
    {
        var deltas = new CollectorDeltaCalculator();
        var prior = new ServerEpoch.Stamp(T0, "SRV01");

        Assert.True(ServerEpoch.ObserveInstance(Context(deltas, StateWith(ServerEpoch.IdentityStateKey, prior)), new ServerEpoch.Stamp(T1, "SRV01")));
        deltas.CalculateDelta(ServerId, "latch_stats_time", "BUFFER", 1_000, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds);

        Assert.True(ServerEpoch.ObserveInstance(Context(deltas, StateWith(ServerEpoch.IdentityStateKey, prior)), new ServerEpoch.Stamp(T1.AddMinutes(2), "SRV01")));

        Assert.Equal(0, deltas.CalculateDeltaWithInterval(ServerId, "latch_stats_time", "BUFFER", 1_500, out var interval, Pass.AddMinutes(1), CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(0, interval);
        Assert.Equal(2, deltas.DrainDiscontinuities(ServerId).Count);
    }

    /// <summary>
    /// The memo is the CALCULATOR's, not the process's: a different calculator holding the same server id
    /// (a test, a host that rebuilt its runtime) knows nothing and forgets on first sight — the conservative
    /// direction — and one calculator's memo for one server says nothing about its other servers.
    /// </summary>
    [Fact]
    public void ObserveInstance_TheMemoIsPerCalculatorAndPerServer()
    {
        var prior = new ServerEpoch.Stamp(T0, "SRV01");
        var current = new ServerEpoch.Stamp(T1, "SRV01");

        var one = new CollectorDeltaCalculator();
        Assert.True(ServerEpoch.ObserveInstance(Context(one, StateWith(ServerEpoch.IdentityStateKey, prior)), current));
        Assert.Single(one.DrainDiscontinuities(ServerId));

        var other = new CollectorDeltaCalculator();
        Assert.True(ServerEpoch.ObserveInstance(Context(other, StateWith(ServerEpoch.IdentityStateKey, prior)), current));
        Assert.Single(other.DrainDiscontinuities(ServerId));

        /* Same calculator, another server that happens to report the same pair: its own first forget. */
        var sibling = new CollectorContext
        {
            ServerId = ServerId + 1,
            ServerName = "sibling",
            CollectionTime = Pass,
            Deltas = one,
            Target = new CollectorTargetInfo(),
            State = StateWith(ServerEpoch.IdentityStateKey, prior),
        };
        Assert.True(ServerEpoch.ObserveInstance(sibling, current));
        Assert.Single(one.DrainDiscontinuities(ServerId + 1));
    }

    /* ---------------- the statements observation ---------------- */

    /// <summary>
    /// A moved stats_reset forgets the statements family's groups — and ONLY those: a sibling PostgreSQL
    /// family's baseline on the same server survives and keeps subtracting honestly.
    /// </summary>
    [Fact]
    public void ObserveStatements_OnAnEpoch_ForgetsOnlyTheNamedGroups()
    {
        var deltas = new CollectorDeltaCalculator();
        var before = Pass.AddMinutes(-1);
        deltas.CalculateDelta(ServerId, "pg_statement_stats_calls", "1|1|1|1", 100, before, CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDelta(ServerId, "pg_statement_stats_time", "1|1|1|1", 100, before, CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDelta(ServerId, "pg_statement_stats_rows", "1|1|1|1", 100, before, CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDelta(ServerId, "pg_wait_stats_waits", "IO:DataFileRead", 100, before, CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var resetBefore = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var resetNow = new DateTime(2026, 9, 19, 3, 59, 0, DateTimeKind.Utc);
        var context = Context(deltas, StateWith(ServerEpoch.StatementsStateKey, new ServerEpoch.Stamp(resetBefore, null)), postgres: true);

        Assert.True(ServerEpoch.ObserveStatements(context, resetNow, "pg_statement_stats_calls", "pg_statement_stats_time", "pg_statement_stats_rows"));

        foreach (var group in new[] { "pg_statement_stats_calls", "pg_statement_stats_time", "pg_statement_stats_rows" })
        {
            Assert.Equal(0, deltas.CalculateDeltaWithInterval(ServerId, group, "1|1|1|1", 5, out var interval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
            Assert.Equal(0, interval);
        }

        Assert.Equal(50, deltas.CalculateDeltaWithInterval(ServerId, "pg_wait_stats_waits", "IO:DataFileRead", 150, out var waitInterval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(60, waitInterval);

        Assert.Equal(ServerEpoch.StatementsChangesMeasurement + "=1", CollectorMeasurementNote.Render(context.Measurements));
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(resetNow, null)), context.PendingState[ServerEpoch.StatementsStateKey]);
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(resetBefore, null)), context.PendingState[ServerEpoch.StatementsPreviousStateKey]);

        var line = Assert.Single(deltas.DrainDiscontinuities(ServerId));
        Assert.Contains("stats_reset", line, StringComparison.Ordinal);
        Assert.Contains("2026-09-01 00:00:00", line, StringComparison.Ordinal);
        Assert.Contains("2026-09-19 03:59:00", line, StringComparison.Ordinal);
    }

    /// <summary>Below pg_stat_statements 1.9 the column is a typed NULL: unknown, never a change, never an erasure.</summary>
    [Fact]
    public void ObserveStatements_WithANullReset_IsInert()
    {
        var deltas = new CollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.StatementsStateKey, new ServerEpoch.Stamp(new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc), null)), postgres: true);

        Assert.False(ServerEpoch.ObserveStatements(context, null, "pg_statement_stats_calls"));

        Assert.Empty(context.PendingState);
        Assert.Empty(context.Measurements);
    }

    /* ---------------- the postmaster observation ---------------- */

    /// <summary>
    /// A moved pg_postmaster_start_time forgets the wait family's groups — and ONLY those: the statements
    /// family's baseline on the same server survives, because pg_stat_statements' counters may well have
    /// survived the restart that zeroed the wait counters (they do, on a clean one).
    /// </summary>
    [Fact]
    public void ObservePostmaster_OnAnEpoch_ForgetsOnlyTheWaitGroups()
    {
        var deltas = new CollectorDeltaCalculator();
        var before = Pass.AddMinutes(-1);
        deltas.CalculateDelta(ServerId, "pg_wait_stats_waits", "167772160", 100, before, CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDelta(ServerId, "pg_wait_stats_time", "167772160", 100, before, CollectorDeltaCalculator.DefaultMaxGapSeconds);
        deltas.CalculateDelta(ServerId, "pg_statement_stats_calls", "1|1|1|1", 100, before, CollectorDeltaCalculator.DefaultMaxGapSeconds);

        var startedBefore = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var startedNow = new DateTime(2026, 9, 19, 3, 59, 0, DateTimeKind.Utc);
        var context = Context(deltas, StateWith(ServerEpoch.PostmasterStateKey, new ServerEpoch.Stamp(startedBefore, null)), postgres: true);

        Assert.True(ServerEpoch.ObservePostmaster(context, startedNow, "pg_wait_stats_waits", "pg_wait_stats_time"));

        foreach (var group in new[] { "pg_wait_stats_waits", "pg_wait_stats_time" })
        {
            Assert.Equal(0, deltas.CalculateDeltaWithInterval(ServerId, group, "167772160", 5, out var interval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
            Assert.Equal(0, interval);
        }

        Assert.Equal(50, deltas.CalculateDeltaWithInterval(ServerId, "pg_statement_stats_calls", "1|1|1|1", 150, out var callsInterval, Pass, CollectorDeltaCalculator.DefaultMaxGapSeconds));
        Assert.Equal(60, callsInterval);

        Assert.Equal(ServerEpoch.PostmasterChangesMeasurement + "=1", CollectorMeasurementNote.Render(context.Measurements));
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(startedNow, null)), context.PendingState[ServerEpoch.PostmasterStateKey]);
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(startedBefore, null)), context.PendingState[ServerEpoch.PostmasterPreviousStateKey]);

        var line = Assert.Single(deltas.DrainDiscontinuities(ServerId));
        Assert.Contains("pg_postmaster_start_time", line, StringComparison.Ordinal);
        Assert.Contains("2026-09-01 00:00:00", line, StringComparison.Ordinal);
        Assert.Contains("2026-09-19 03:59:00", line, StringComparison.Ordinal);
    }

    /// <summary>A NULL start time is unknown: never a change, never an erasure of the known prior.</summary>
    [Fact]
    public void ObservePostmaster_WithANullStart_IsInert()
    {
        var deltas = new CollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.PostmasterStateKey, new ServerEpoch.Stamp(new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc), null)), postgres: true);

        Assert.False(ServerEpoch.ObservePostmaster(context, null, "pg_wait_stats_waits"));

        Assert.Empty(context.PendingState);
        Assert.Empty(context.Measurements);
    }

    /* ---------------- the carriers ---------------- */

    /// <summary>
    /// The wait-stats collector reads the identity off its SECOND result set at the end of ReadAsync, and its
    /// subtractions all live in WritePayload — so driven the way both hosts drive it (read the whole list, then
    /// write each row), the forget lands with ZERO delta calls recorded, and every wait_stats row of the pass
    /// then subtracts under the groups the forget covered. This is the ordering that closes #3694's carrier-order
    /// residue: first in the order, honest on its own pass.
    /// </summary>
    [Fact]
    public async Task WaitStats_ReadsTheIdentityOffItsSecondResultSet_AndForgetsBeforeItsFirstSubtraction()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")));

        using var reader = FakeCollectorDataReader.WithResultSets(
            new object[][]
            {
                new object[] { "PAGEIOLATCH_SH", 7L, 300L, 20L },
                new object[] { "SOS_SCHEDULER_YIELD", 10L, 200L, 150L },
            },
            new object[][] { new object[] { T1, "SRV02" } });
        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        foreach (var row in rows)
        {
            WaitStatsCollector.Instance.WritePayload(row, new RecordingCollectorRowWriter(), context);
        }

        Assert.Equal(2, rows.Count);
        var clear = Assert.Single(deltas.Clears);
        Assert.Equal("server", clear.Kind);
        Assert.Equal(0, clear.DeltaCallsBefore);
        Assert.Equal(6, deltas.Calls.Count);
        Assert.Equal(ServerId, deltas.LastServerId);

        Assert.Equal(ServerEpoch.IdentityChangesMeasurement + "=1", CollectorMeasurementNote.Render(context.Measurements));
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(T1, "SRV02")), context.PendingState[ServerEpoch.IdentityStateKey]);
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(T0, "SRV01")), context.PendingState[ServerEpoch.IdentityPreviousStateKey]);
    }

    /// <summary>A batch with no second set — the shape every pre-#3653 wait_stats pin drives — observes nothing and reads the rows exactly as before.</summary>
    [Fact]
    public async Task WaitStats_WithoutASecondResultSet_ObservesNothing()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")));

        using var reader = new FakeCollectorDataReader(new object[] { "PAGEIOLATCH_SH", 7L, 300L, 20L });
        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Empty(deltas.Clears);
        Assert.Empty(context.PendingState);
        Assert.Empty(context.Measurements);
    }

    /// <summary>
    /// Azure SQL DB: the same engine gate as the CPU carrier, so the two carriers observe under one rule — a
    /// second set that somehow arrived is ignored, and the batch itself carries none (the definition test pins
    /// the Azure text verbatim).
    /// </summary>
    [Fact]
    public async Task WaitStats_OnAzureSqlDb_IgnoresASecondResultSetEvenIfOneArrived()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")), isAzureSqlDb: true);

        using var reader = FakeCollectorDataReader.WithResultSets(
            new object[][] { new object[] { "PAGEIOLATCH_SH", 7L, 300L, 20L } },
            new object[][] { new object[] { T1, "SRV02" } });
        await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(deltas.Clears);
        Assert.Empty(context.PendingState);
        Assert.DoesNotContain("dm_os_sys_info", WaitStatsCollector.Instance.BuildQuery(context).Text, StringComparison.Ordinal);
    }

    /// <summary>The wait-stats batch returns the identity as its second statement, after the payload SELECT, in the CPU carrier's column names.</summary>
    [Fact]
    public void WaitStats_Batch_CarriesTheIdentitySetAfterThePayload()
    {
        var sql = WaitStatsCollector.Instance.BuildQuery(Context(new RecordingCollectorDeltaCalculator())).Text;

        Assert.Contains("server_start_time = dosi.sqlserver_start_time", sql, StringComparison.Ordinal);
        Assert.Contains("server_name = @@SERVERNAME", sql, StringComparison.Ordinal);
        Assert.True(sql.IndexOf("FROM sys.dm_os_wait_stats", StringComparison.Ordinal) < sql.IndexOf("FROM sys.dm_os_sys_info", StringComparison.Ordinal));
    }

    /// <summary>
    /// The Aurora wait collector forgets BEFORE its first subtraction of the pass, on the same groups it
    /// subtracts under — the recorder sees zero delta calls at the moment of the forget, and the groups it
    /// named are exactly the groups the pass then used. The first row here is an IGNORED type, so this also
    /// pins that the observation is made off the first row read, not the first row kept.
    /// </summary>
    [Fact]
    public async Task PgWaitStats_ForgetsItsOwnGroupsBeforeItsFirstSubtraction()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var startedBefore = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var startedNow = new DateTime(2026, 9, 19, 3, 59, 0, DateTimeKind.Utc);
        var context = Context(deltas, StateWith(ServerEpoch.PostmasterStateKey, new ServerEpoch.Stamp(startedBefore, null)), postgres: true);

        using var reader = new FakeCollectorDataReader(
            new object[] { 6, 100663296L, "Client", "ClientRead", 3283144470L, 565758023440000L, startedNow },
            new object[] { 10, 167772160L, "IO", "DataFileRead", 100L, 5000L, startedNow },
            new object[] { 3, 50331648L, "Lock", "transactionid", 4395L, 11393070000L, startedNow });
        var rows = await PgWaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        var clear = Assert.Single(deltas.Clears);
        Assert.Equal("groups", clear.Kind);
        Assert.Equal(0, clear.DeltaCallsBefore);
        Assert.Equal(
            deltas.Calls.Select(c => c.Group).Distinct(StringComparer.Ordinal).OrderBy(g => g, StringComparer.Ordinal).ToList(),
            clear.Groups.OrderBy(g => g, StringComparer.Ordinal).ToList());
        Assert.Equal(4, deltas.Calls.Count);

        Assert.Equal(ServerEpoch.PostmasterChangesMeasurement + "=1", CollectorMeasurementNote.Render(context.Measurements));
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(startedNow, null)), context.PendingState[ServerEpoch.PostmasterStateKey]);
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(startedBefore, null)), context.PendingState[ServerEpoch.PostmasterPreviousStateKey]);
    }

    /// <summary>The same rows with the same postmaster start as the store holds: no forget, no note, no state write.</summary>
    [Fact]
    public async Task PgWaitStats_WithAnUnchangedPostmasterStart_ObservesQuietly()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var started = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var context = Context(deltas, StateWith(ServerEpoch.PostmasterStateKey, new ServerEpoch.Stamp(started, null)), postgres: true);

        using var reader = new FakeCollectorDataReader(new object[] { 10, 167772160L, "IO", "DataFileRead", 100L, 5000L, started });
        await PgWaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(deltas.Clears);
        Assert.Empty(context.Measurements);
        Assert.Empty(context.PendingState);
    }

    /// <summary>
    /// The statements collector forgets BEFORE its first subtraction of the pass, on the same groups it
    /// subtracts under — the recorder sees zero delta calls at the moment of the forget, and the groups it
    /// named are exactly the groups the pass then used. This is the property that makes the carrier's own
    /// family honest on the pass that sees the epoch; a forget after the loop would leave the rows already
    /// subtracted from the dead baseline.
    /// </summary>
    [Fact]
    public async Task PgStatementStats_ForgetsItsOwnGroupsBeforeItsFirstSubtraction()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var resetBefore = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var resetNow = new DateTime(2026, 9, 19, 3, 59, 0, DateTimeKind.Utc);
        var context = Context(deltas, StateWith(ServerEpoch.StatementsStateKey, new ServerEpoch.Stamp(resetBefore, null)), postgres: true);

        using var reader = new FakeCollectorDataReader(
            StatementRow(queryId: 1, calls: 10, statsReset: resetNow),
            StatementRow(queryId: 2, calls: 20, statsReset: resetNow));
        await PgStatementStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var clear = Assert.Single(deltas.Clears);
        Assert.Equal("groups", clear.Kind);
        Assert.Equal(0, clear.DeltaCallsBefore);
        Assert.Equal(
            deltas.Calls.Select(c => c.Group).Distinct(StringComparer.Ordinal).OrderBy(g => g, StringComparer.Ordinal).ToList(),
            clear.Groups.OrderBy(g => g, StringComparer.Ordinal).ToList());
        Assert.Equal(6, deltas.Calls.Count);

        Assert.Equal(ServerEpoch.StatementsChangesMeasurement + "=1", CollectorMeasurementNote.Render(context.Measurements));
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(resetNow, null)), context.PendingState[ServerEpoch.StatementsStateKey]);
    }

    /// <summary>The same rows with the same stats_reset as the store holds: no forget, no note, no state write.</summary>
    [Fact]
    public async Task PgStatementStats_WithAnUnchangedReset_ObservesQuietly()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var reset = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var context = Context(deltas, StateWith(ServerEpoch.StatementsStateKey, new ServerEpoch.Stamp(reset, null)), postgres: true);

        using var reader = new FakeCollectorDataReader(StatementRow(queryId: 1, calls: 10, statsReset: reset));
        await PgStatementStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(deltas.Clears);
        Assert.Empty(context.Measurements);
        Assert.Empty(context.PendingState);
    }

    /// <summary>
    /// The CPU collector reads the identity off its SECOND result set, after the samples; a changed pair
    /// forgets the whole server. CPU samples are gauges, so nothing here is ordered before a delta call.
    /// </summary>
    [Fact]
    public async Task CpuUtilization_ReadsTheIdentityOffItsSecondResultSet_AndForgetsTheServerOnAChange()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")));

        /* Four payload ordinals since V134 / v63 (#3653 item 13): the UTC twin rides last, and the identity
           set is still the SECOND result set after it. */
        using var reader = FakeCollectorDataReader.WithResultSets(
            new object[][] { new object[] { Pass.AddSeconds(-30), 12, 3, Pass.AddSeconds(-30) } },
            new object[][] { new object[] { T1, "SRV02" } });
        var rows = await CpuUtilizationCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        var clear = Assert.Single(deltas.Clears);
        Assert.Equal("server", clear.Kind);
        Assert.Equal(ServerId, deltas.LastServerId);
        Assert.Equal(ServerEpoch.IdentityChangesMeasurement + "=1", CollectorMeasurementNote.Render(context.Measurements));
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(T1, "SRV02")), context.PendingState[ServerEpoch.IdentityStateKey]);
        Assert.Equal(ServerEpoch.Serialize(new ServerEpoch.Stamp(T0, "SRV01")), context.PendingState[ServerEpoch.IdentityPreviousStateKey]);
    }

    /// <summary>
    /// A batch with no second set — the shape every pre-#3653 CPU pin drives — observes nothing and changes
    /// nothing: the samples are read exactly as before.
    /// </summary>
    [Fact]
    public async Task CpuUtilization_WithoutASecondResultSet_ObservesNothing()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")));

        using var reader = new FakeCollectorDataReader(new object[] { Pass.AddSeconds(-30), 12, 3, Pass.AddSeconds(-30) });
        var rows = await CpuUtilizationCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Empty(deltas.Clears);
        Assert.Empty(context.PendingState);
        Assert.Empty(context.Measurements);
    }

    /// <summary>
    /// Azure SQL DB carries no identity by design (its batch never touches the DMV, and a logical server keeps
    /// its name through failover): the gate is the engine flag, not the accident of a missing result set.
    /// </summary>
    [Fact]
    public async Task CpuUtilization_OnAzureSqlDb_IgnoresASecondResultSetEvenIfOneArrived()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = Context(deltas, StateWith(ServerEpoch.IdentityStateKey, new ServerEpoch.Stamp(T0, "SRV01")), isAzureSqlDb: true);

        using var reader = FakeCollectorDataReader.WithResultSets(
            new object[][] { new object[] { Pass.AddSeconds(-30), 12, 0, Pass.AddSeconds(-30) } },
            new object[][] { new object[] { T1, "SRV02" } });
        await CpuUtilizationCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(deltas.Clears);
        Assert.Empty(context.PendingState);
    }

    /// <summary>The CPU collector's ring-buffer batch returns the identity as its second statement; the Azure batches do not.</summary>
    [Fact]
    public void CpuUtilization_RingBufferBatch_CarriesTheIdentitySet_AndTheAzureBatchesDoNot()
    {
        var deltas = new RecordingCollectorDeltaCalculator();

        var ringBuffer = CpuUtilizationCollector.Instance.BuildQuery(Context(deltas)).Text;
        Assert.Contains("server_start_time = @start_time", ringBuffer, StringComparison.Ordinal);
        Assert.Contains("server_name = @@SERVERNAME", ringBuffer, StringComparison.Ordinal);
        /* After the payload SELECT, so ReadAsync's NextResult lands on it. */
        Assert.True(ringBuffer.IndexOf("RING_BUFFER_SCHEDULER_MONITOR", StringComparison.Ordinal) < ringBuffer.IndexOf("server_start_time = @start_time", StringComparison.Ordinal));

        var azure = CpuUtilizationCollector.Instance.BuildQuery(Context(deltas, isAzureSqlDb: true)).Text;
        Assert.DoesNotContain("@@SERVERNAME", azure, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_os_sys_info", azure, StringComparison.Ordinal);
    }

    /// <summary>
    /// All four carriers declare the keys they persist, which is what makes both hosts load the prior before
    /// the run and save the observation after it (#1962's wiring, pinned in CollectorStateContractTests). The
    /// two SQL Server carriers declare the SAME keys: the store keys state by (server_id, collector_name), so
    /// each holds its own prior under its own name, which is why the second needs the memo above.
    /// </summary>
    [Fact]
    public void TheCarriersDeclareTheirStateKeys()
    {
        Assert.Equal(
            new[] { ServerEpoch.IdentityStateKey, ServerEpoch.IdentityPreviousStateKey },
            WaitStatsCollector.Instance.StateKeys);
        Assert.Equal(
            new[] { ServerEpoch.IdentityStateKey, ServerEpoch.IdentityPreviousStateKey },
            CpuUtilizationCollector.Instance.StateKeys);
        Assert.Equal(
            new[] { ServerEpoch.StatementsStateKey, ServerEpoch.StatementsPreviousStateKey },
            PgStatementStatsCollector.Instance.StateKeys);
        Assert.Equal(
            new[] { ServerEpoch.PostmasterStateKey, ServerEpoch.PostmasterPreviousStateKey },
            PgWaitStatsCollector.Instance.StateKeys);
    }

    /* ---------------- the calculator's own contract ---------------- */

    /// <summary>The interface default is a no-op; a stateless double keeps compiling and forgets nothing, loudly documented on the member.</summary>
    [Fact]
    public void ADefaultImplementerForgetsNothing()
    {
        ICollectorDeltaCalculator legacy = new StatelessCalculator();

        legacy.ClearServer(ServerId, "anything");
        legacy.ClearGroups(ServerId, "anything", "g");
    }

    /// <summary>A forget with no sentence (the hosts' remove paths) queues nothing to log.</summary>
    [Fact]
    public void ClearServer_WithoutADiscontinuity_QueuesNothing()
    {
        var deltas = new CollectorDeltaCalculator();
        deltas.CalculateDelta(ServerId, "g", "k", 1, Pass);

        deltas.ClearServer(ServerId);
        deltas.ClearGroups(ServerId, null, "g");

        Assert.Empty(deltas.DrainDiscontinuities(ServerId));
    }

    /// <summary>Several sentences in one run drain oldest-first, once.</summary>
    [Fact]
    public void DrainDiscontinuities_ReturnsInOrder_ThenEmpty()
    {
        var deltas = new CollectorDeltaCalculator();

        deltas.ClearGroups(ServerId, "first", "g");
        deltas.ClearServer(ServerId, "second");

        Assert.Equal(new[] { "first", "second" }, deltas.DrainDiscontinuities(ServerId));
        Assert.Empty(deltas.DrainDiscontinuities(ServerId));
    }

    /* ---------------- helpers ---------------- */

    /// <summary>
    /// A pg_statement_stats reader row in ordinal order, the statements epoch at 27. Since #4428, ordinal
    /// 28 is <c>stats_since</c> (NULL here — these tests are about the epoch forget, not the row-coherent
    /// restart placement, which <c>Darling.Tests</c> covers separately) and ordinal 29 is <c>target_now</c>,
    /// defaulted to the same moment as <paramref name="statsReset"/> when it is a <see cref="DateTime"/>.
    /// </summary>
    private static object[] StatementRow(long queryId, long calls, object statsReset) => new object[]
    {
        queryId, 1L, 1L, true, calls, 100d,
        0d, 0d, 0d, 0L,
        0L, 0L, 0L, 0L,
        0L, 0L, 0d, 0d,
        DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
        0L, 0L, 0L,
        DBNull.Value, DBNull.Value,
        statsReset,
        DBNull.Value,
        statsReset is DateTime resetTime ? resetTime : new DateTime(2026, 9, 19, 3, 59, 0, DateTimeKind.Utc),
    };

    /// <summary>Reaches the protected restart-seed hook, to stage the host-restart case.</summary>
    private sealed class SeedingCalculator : CollectorDeltaCalculator
    {
        public void SeedForTest(int serverId, string group, string key, long value, DateTime timestamp)
        {
            Seed(serverId, group, key, value, timestamp);
            SeedPass(serverId, group, timestamp);
        }
    }

    /// <summary>Implements only the two original members — the interface defaults supply the rest.</summary>
    private sealed class StatelessCalculator : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => 0;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return 0;
        }
    }
}
