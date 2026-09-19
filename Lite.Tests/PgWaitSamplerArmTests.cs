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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The service-sampler arm of <see cref="PgWaitSamplingCollector"/> (#3604) — the floor under stock
/// PostgreSQL targets without the <c>pg_wait_sampling</c> extension — and the three-tier selection that
/// decides which instrument a target gets.
///
/// <para><b>What is pinned here and why.</b> The arm's whole correctness rests on four things a live run
/// cannot cheaply prove: that the batch really re-reads <c>pg_stat_activity</c> (each snapshot is its own
/// statement behind a <c>pg_stat_clear_snapshot()</c>), that the tally it writes is CUMULATIVE across cycles
/// (the read differences newest against oldest, so a per-window count would be silently wrong), that it
/// excludes its own connections and the shared idle types, and that exactly ONE wait collector applies to
/// any PostgreSQL shape. Each is a test below driven with a fixture reader, no server.</para>
/// </summary>
public sealed class PgWaitSamplerArmTests
{
    private static CollectorContext MakeContext(bool hasExtension, int major = 17, IReadOnlyDictionary<string, string>? state = null) =>
        new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 9, 18, 12, 0, 0, DateTimeKind.Utc),
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
                PostgresVersionNum = major * 10000,
                HasPgWaitSamplingExtension = hasExtension,
            },
            State = state ?? new Dictionary<string, string>(),
        };

    /* ───────────────────────── tier selection ───────────────────────── */

    /// <summary>
    /// Aurora native &gt; extension &gt; service sampler, and exactly one path per target. Asserted over the
    /// engine-capability sweep's own shapes rather than three hand-picked targets, so a fourth fact added to
    /// the gate later is swept too.
    /// </summary>
    [Fact]
    public void ExactlyOneWaitCollectorAppliesToEveryPostgresShape()
    {
        foreach (var kind in new[] { "postgres", "aurora-postgres" })
        {
            foreach (var target in CollectorEngineCapability.TargetsWithEngineKind(kind))
            {
                var aurora = CollectorCatalog.AppliesTo("pg_wait_stats", target);
                var sampling = CollectorCatalog.AppliesTo("pg_wait_sampling", target);
                Assert.True(aurora ^ sampling,
                    $"IsAurora={target.IsAurora} HasExt={target.HasPgWaitSamplingExtension}: pg_wait_stats={aurora}, pg_wait_sampling={sampling} — exactly one must run");
                Assert.Equal(target.IsAurora, aurora);
            }
        }
    }

    [Fact]
    public void TheArmFollowsTheExtensionFact_AndTheAuroraTierNeverReachesEither()
    {
        var extension = PgWaitSamplingCollector.Instance.BuildQuery(MakeContext(hasExtension: true)).Text;
        var sampler = PgWaitSamplingCollector.Instance.BuildQuery(MakeContext(hasExtension: false)).Text;

        Assert.Contains("pg_wait_sampling_profile", extension, StringComparison.Ordinal);
        /* The profile query mentions pg_stat_activity in a comment; what it must not do is READ it. */
        Assert.DoesNotContain("FROM pg_stat_activity", extension, StringComparison.Ordinal);

        Assert.Contains("pg_stat_activity", sampler, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_wait_sampling_profile", sampler, StringComparison.Ordinal);

        Assert.False(PgWaitSamplingCollector.Instance.AppliesTo(new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql, IsAurora = true, HasPgWaitSamplingExtension = false,
        }), "Aurora must take pg_wait_stats alone; the sampler arm beside it would be two answers to one question");
    }

    /// <summary>The Aurora gap now points at the instrument that DOES answer there — the mirror of the
    /// pointer that already ran from pg_wait_stats to pg_wait_sampling.</summary>
    [Fact]
    public void OnAurora_TheSamplingReadIsAPermanentGapThatNamesPgWaitStats()
    {
        Assert.False(CollectorEngineCapability.IsCollectedOnEngineKind("pg_wait_sampling", "aurora-postgres"));
        Assert.True(CollectorEngineCapability.IsCollectedOnEngineKind("pg_wait_sampling", "postgres"));

        var message = CollectorEngineCapability.NotCollectedMessage("db-01", 0, "aurora-postgres", "pg_wait_sampling");
        Assert.NotNull(message);
        Assert.Contains("pg_wait_stats", message, StringComparison.Ordinal);
        Assert.Contains("never will", message, StringComparison.Ordinal);
    }

    /* ───────────────────────── the batch ───────────────────────── */

    [Fact]
    public void TheBatchIsOneSnapshotThenSleepClearSnapshot_RepeatedToTheConstant()
    {
        var sql = PgWaitSamplingCollector.SamplerBatchSql(hasQueryId: true);

        var snapshots = Regex.Matches(sql, @"FROM pg_stat_activity AS a").Count;
        var sleeps = Regex.Matches(sql, @"SELECT pg_sleep\(").Count;
        var clears = Regex.Matches(sql, @"SELECT pg_stat_clear_snapshot\(\)").Count;

        Assert.Equal(PgWaitSamplingCollector.SamplerSnapshotsPerCycle, snapshots);
        Assert.Equal(PgWaitSamplingCollector.SamplerSnapshotsPerCycle - 1, sleeps);
        Assert.Equal(PgWaitSamplingCollector.SamplerSnapshotsPerCycle - 1, clears);

        /* Every re-read is preceded by a clear, in that order: pg_stat_activity is cached per transaction on
           first access, and the batch is one implicit transaction, so a snapshot without a clear before it
           would return the previous instant again. */
        var statements = sql.Split(';', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToList();
        for (var i = 1; i < statements.Count; i++)
        {
            if (statements[i].Contains("FROM pg_stat_activity", StringComparison.Ordinal))
            {
                Assert.Contains("pg_stat_clear_snapshot", statements[i - 1], StringComparison.Ordinal);
            }
        }

        /* The period the rows carry is the period the batch sleeps. */
        Assert.Contains($"pg_sleep({PgWaitSamplingCollector.SamplerPeriodMs / 1000.0})", sql, StringComparison.Ordinal);
    }

    /// <summary>The window must stay well inside the 60,000 ms body budget SweepPressureClassifier sums
    /// single-run costs against, and inside the collector's own command timeout with headroom.</summary>
    [Fact]
    public void TheWindowFitsTheBodyBudgetAndTheCommandTimeout()
    {
        var windowSeconds = (PgWaitSamplingCollector.SamplerSnapshotsPerCycle - 1) * PgWaitSamplingCollector.SamplerPeriodMs / 1000.0;
        Assert.InRange(windowSeconds, 1, 30);
        Assert.True(PgWaitSamplingCollector.Instance.CommandTimeoutSecondsOverride is { } t && t >= windowSeconds * 2,
            "the batch's results are buffered until it ends, so the command timeout must cover the whole window with headroom");
        Assert.Equal(5, CollectorScheduleDefaults.All["pg_wait_sampling"].FrequencyMinutes);
    }

    [Fact]
    public void TheSnapshotExcludesItselfItsSiblingsAndTheSharedIdleTypes_AndLabelsCpu()
    {
        var sql = PgWaitSamplingCollector.SamplerSnapshotSql(hasQueryId: true);

        Assert.Contains("a.pid <> pg_backend_pid()", sql, StringComparison.Ordinal);
        Assert.Contains($"'{PgWaitSamplingCollector.ServiceApplicationName}'", sql, StringComparison.Ordinal);
        Assert.Contains($"'{PgWaitSamplingCollector.ServiceRemediationApplicationName}'", sql, StringComparison.Ordinal);

        foreach (var type in PgWaitStatsCollector.IgnoredWaitTypes)
        {
            Assert.Contains($"'{type}'", sql, StringComparison.Ordinal);
        }

        /* Same coalesce-FIRST trap the extension arm's query documents: a NULL type is CPU and must survive
           the NOT IN. */
        Assert.Contains("coalesce(a.wait_event_type, 'CPU') NOT IN", sql, StringComparison.Ordinal);
        Assert.Contains("coalesce(a.wait_event, 'Running')", sql, StringComparison.Ordinal);
        /* Only an ACTIVE backend with no wait is on CPU; an idle one with no wait is nothing. */
        Assert.Contains("a.state = 'active'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryIdIsSelectedOnlyWhereItExists()
    {
        Assert.Contains("a.query_id", PgWaitSamplingCollector.SamplerSnapshotSql(hasQueryId: true), StringComparison.Ordinal);
        Assert.DoesNotContain("a.query_id", PgWaitSamplingCollector.SamplerSnapshotSql(hasQueryId: false), StringComparison.Ordinal);

        Assert.DoesNotContain("a.query_id", PgWaitSamplingCollector.Instance.BuildQuery(MakeContext(false, major: 13)).Text, StringComparison.Ordinal);
        Assert.Contains("a.query_id", PgWaitSamplingCollector.Instance.BuildQuery(MakeContext(false, major: 14)).Text, StringComparison.Ordinal);
        /* 0 is "unknown, assume newest" everywhere in the gates. */
        Assert.Contains("a.query_id", PgWaitSamplingCollector.Instance.BuildQuery(MakeContext(false, major: 0)).Text, StringComparison.Ordinal);
    }

    /* ───────────────────────── accumulation ───────────────────────── */

    private static object[] Snap(string type, string evt, long queryId, int pid) => new object[] { type, evt, queryId, pid };
    private static readonly object[][] Sleep = { new object[] { DBNull.Value } };
    private static readonly object[][] Clear = { new object[] { DBNull.Value } };

    /// <summary>
    /// Three snapshots, tallied by (type, event, query_id): a Lock held across all three by one backend, an
    /// IO seen once, CPU twice by two different backends. Sleep and clear result sets are one column and are
    /// drained, not tallied. backend_count is the WINDOW's distinct pids.
    /// </summary>
    [Fact]
    public async Task ASnapshotSequenceIsTalliedByKey_WithWindowDistinctBackends()
    {
        var reader = FakeCollectorDataReader.WithResultSets(
            new[] { Snap("Lock", "relation", 111, 10), Snap("CPU", "Running", 222, 20) },
            Sleep, Clear,
            new[] { Snap("Lock", "relation", 111, 10), Snap("IO", "DataFileRead", 333, 30) },
            Sleep, Clear,
            new[] { Snap("Lock", "relation", 111, 10), Snap("CPU", "Running", 222, 21) });

        var context = MakeContext(hasExtension: false);
        var rows = await PgWaitSamplingCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var lock_ = Assert.Single(rows, r => r.EventType == "Lock");
        Assert.Equal(3, lock_.SampleCount);
        Assert.Equal(1, lock_.BackendCount);
        Assert.Equal(111, lock_.QueryId);
        Assert.Equal(PgWaitSamplingCollector.SamplerPeriodMs, lock_.ProfilePeriodMs);

        var cpu = Assert.Single(rows, r => r.EventType == "CPU");
        Assert.Equal(2, cpu.SampleCount);
        Assert.Equal(2, cpu.BackendCount);

        var io = Assert.Single(rows, r => r.EventType == "IO");
        Assert.Equal(1, io.SampleCount);

        /* V133 (#3691): sampled_ms is the WINDOW's observed time - three four-column snapshots were read, so
           3 x the period - counted by result-set shape like the tally (the sleep and clear sets are not
           snapshots), and the same on every row of the cycle. It is what a rate read divides the delta by
           instead of the interval: here the Lock key waited in 3 of 3,000 observed ms = 100%, which
           "3 samples x 1,000 ms over a 300,000 ms interval" would have called 1%. */
        Assert.All(rows, r => Assert.Equal(3 * PgWaitSamplingCollector.SamplerPeriodMs, r.SampledMs));

        /* Most-sampled first, like the extension arm's ORDER BY. */
        Assert.Equal("Lock", rows[0].EventType);

        Assert.Equal(PgWaitInstrument.ServiceSampled, context.PendingState[PgWaitSamplingCollector.InstrumentStateKey]);
    }

    /// <summary>
    /// The rows are CUMULATIVE: the tally carried in from the previous cycle is added to, every carried key
    /// is re-emitted (so the read's newest-per-key stays current), and the new tally goes back out. This is
    /// the property the read depends on — it differences newest against oldest per key over the window.
    /// </summary>
    [Fact]
    public async Task TheTallyCarriesAcrossCycles_AndEveryCarriedKeyIsReEmitted()
    {
        var first = MakeContext(hasExtension: false);
        await PgWaitSamplingCollector.Instance.ReadAsync(
            FakeCollectorDataReader.WithResultSets(new[] { Snap("Lock", "relation", 111, 10), Snap("IO", "DataFileRead", 0, 30) }),
            first, CancellationToken.None);

        var carried = new Dictionary<string, string> { [PgWaitSamplingCollector.TallyStateKey] = first.PendingState[PgWaitSamplingCollector.TallyStateKey] };
        var second = MakeContext(hasExtension: false, state: carried);
        var rows = await PgWaitSamplingCollector.Instance.ReadAsync(
            FakeCollectorDataReader.WithResultSets(new[] { Snap("Lock", "relation", 111, 12) }),
            second, CancellationToken.None);

        var lock_ = Assert.Single(rows, r => r.EventType == "Lock");
        Assert.Equal(2, lock_.SampleCount);
        Assert.Equal(1, lock_.BackendCount);

        /* Not seen this window, still written with its cumulative count and a window backend count of 0. */
        var io = Assert.Single(rows, r => r.EventType == "IO");
        Assert.Equal(1, io.SampleCount);
        Assert.Equal(0, io.BackendCount);

        /* sampled_ms is THIS cycle's window (one snapshot), not a cumulative across cycles (V133): the first
           cycle observed one snapshot too, and a consumer that wants the two cycles' total sums the two rows'
           values rather than differencing them. A carried key not seen this window still carries the window
           it was re-emitted in - the observation covered it and found nothing. */
        Assert.All(rows, r => Assert.Equal(1 * PgWaitSamplingCollector.SamplerPeriodMs, r.SampledMs));

        var roundTrip = PgWaitSamplingCollector.ParseTally(second.PendingState[PgWaitSamplingCollector.TallyStateKey]);
        Assert.Equal(2, roundTrip[("Lock", "relation", 111)]);
        Assert.Equal(1, roundTrip[("IO", "DataFileRead", 0)]);
    }

    [Fact]
    public async Task AnEmptyWindowOnAnEmptyTallyWritesNothing_AndStillRecordsTheInstrument()
    {
        var context = MakeContext(hasExtension: false);
        var rows = await PgWaitSamplingCollector.Instance.ReadAsync(
            FakeCollectorDataReader.WithResultSets(Array.Empty<object[]>(), Sleep, Clear, Array.Empty<object[]>()),
            context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Equal(PgWaitInstrument.ServiceSampled, context.PendingState[PgWaitSamplingCollector.InstrumentStateKey]);
        Assert.Equal(string.Empty, context.PendingState[PgWaitSamplingCollector.TallyStateKey]);
    }

    [Fact]
    public async Task TheTallyIsCappedByDroppingTheLeastSampled()
    {
        var tally = new Dictionary<(string, string, long), long>();
        for (var i = 0; i < PgWaitSamplingCollector.SamplerTallyCap + 10; i++)
        {
            tally[("Lock", "relation", i)] = i + 1;
        }

        var text = PgWaitSamplingCollector.SerializeTally(tally);
        var parsed = PgWaitSamplingCollector.ParseTally(text);
        Assert.Equal(tally.Count, parsed.Count);

        /* The cap is applied in ReadSamplerAsync; drive it through the read with the oversized tally carried. */
        var context = MakeContext(hasExtension: false, state: new Dictionary<string, string> { [PgWaitSamplingCollector.TallyStateKey] = text });
        var rows = await PgWaitSamplingCollector.Instance.ReadAsync(
            FakeCollectorDataReader.WithResultSets(Array.Empty<object[]>()), context, CancellationToken.None);

        Assert.Equal(PgWaitSamplingCollector.SamplerTallyCap, rows.Count);
        Assert.DoesNotContain(rows, r => r.QueryId < 10);   /* the ten least-sampled went */
        Assert.Contains(rows, r => r.QueryId == PgWaitSamplingCollector.SamplerTallyCap + 9);
    }

    [Fact]
    public void ABadTallyLineIsOneLostKey_NotALostTally()
    {
        var parsed = PgWaitSamplingCollector.ParseTally("Lock\trelation\t1\t5\ngarbage\nIO\tDataFileRead\tnotanumber\t2\nCPU\tRunning\t0\t9\n");
        Assert.Equal(2, parsed.Count);
        Assert.Equal(5, parsed[("Lock", "relation", 1)]);
        Assert.Equal(9, parsed[("CPU", "Running", 0)]);
    }

    /// <summary>The extension arm records ITS instrument too, so a target that gains the extension flips the
    /// disclosure on its next cycle rather than keeping a stale "service_sampled".</summary>
    [Fact]
    public async Task TheExtensionArmRecordsItsInstrument()
    {
        var context = MakeContext(hasExtension: true);
        var rows = await PgWaitSamplingCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(new object[] { "Lock", "relation", 111L, 40L, 10, 2 }),
            context, CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(PgWaitInstrument.ExtensionSampled, context.PendingState[PgWaitSamplingCollector.InstrumentStateKey]);
        /* V133 (#3691): the extension arm has no duty cycle to disclose - the module samples the whole
           interval in-engine - so sampled_ms is NULL, which a reader takes as "period x count over the row's
           whole interval": today's arithmetic, unchanged. NULL rather than the interval length because the
           collector does not know the interval; the read does. */
        Assert.Null(row.SampledMs);
        /* And it CLEARS the sampler's tally rather than leaving the last one to be resumed months later if
           the target ever falls back to the sampler arm (#3645 review) - the host persists only PendingState
           keys, so "leave it alone" would mean "keep it forever". */
        Assert.Equal(string.Empty, context.PendingState[PgWaitSamplingCollector.TallyStateKey]);
    }

    /// <summary>The whole reversion path: a tally carried from the sampler era, an extension-arm cycle, then
    /// the sampler arm again — which must start from zero, not from the carried era.</summary>
    [Fact]
    public async Task AFallbackToTheSamplerAfterTheExtensionArm_StartsFromZero()
    {
        var samplerEra = MakeContext(hasExtension: false);
        await PgWaitSamplingCollector.Instance.ReadAsync(
            FakeCollectorDataReader.WithResultSets(new[] { Snap("Lock", "relation", 111, 10), Snap("Lock", "relation", 111, 11) }),
            samplerEra, CancellationToken.None);
        Assert.Equal(2, PgWaitSamplingCollector.ParseTally(samplerEra.PendingState[PgWaitSamplingCollector.TallyStateKey])[("Lock", "relation", 111)]);

        var extensionEra = MakeContext(hasExtension: true, state: new Dictionary<string, string>(samplerEra.PendingState));
        await PgWaitSamplingCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(new object[] { "Lock", "relation", 111L, 9_999L, 10, 2 }),
            extensionEra, CancellationToken.None);

        var fallback = MakeContext(hasExtension: false, state: new Dictionary<string, string>(extensionEra.PendingState));
        var rows = await PgWaitSamplingCollector.Instance.ReadAsync(
            FakeCollectorDataReader.WithResultSets(new[] { Snap("Lock", "relation", 111, 12) }),
            fallback, CancellationToken.None);

        var lock_ = Assert.Single(rows);
        Assert.Equal(1, lock_.SampleCount);
        Assert.Equal(PgWaitInstrument.ServiceSampled, fallback.PendingState[PgWaitSamplingCollector.InstrumentStateKey]);
    }

    [Fact]
    public void StateKeysDeclareBothPieces_SoTheHostLoadsAndPersistsThem()
    {
        Assert.Equal(
            new[] { PgWaitSamplingCollector.InstrumentStateKey, PgWaitSamplingCollector.TallyStateKey },
            PgWaitSamplingCollector.Instance.StateKeys);
    }

    [Fact]
    public void TheInstrumentVocabularyIsExactlyThreeTokens()
    {
        Assert.True(PgWaitInstrument.IsKnown(PgWaitInstrument.EngineCumulative));
        Assert.True(PgWaitInstrument.IsKnown(PgWaitInstrument.ExtensionSampled));
        Assert.True(PgWaitInstrument.IsKnown(PgWaitInstrument.ServiceSampled));
        Assert.False(PgWaitInstrument.IsKnown("Service_Sampled"));
        Assert.False(PgWaitInstrument.IsKnown(null));
        Assert.Contains("FLOOR", PgWaitInstrument.ServiceSampledCaveat, StringComparison.Ordinal);
    }
}
