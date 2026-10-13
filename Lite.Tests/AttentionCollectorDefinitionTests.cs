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
/// #5640: the shared client-attention collector. Pins the batch text (a missing session THROWs 50000 so the host records
/// SESSION_MISSING, never zero rows and SUCCESS; the counter read and the execution_count gate come before the cast), the
/// counter-delta and restart arithmetic, the count row (only for a delta above zero) and the samples, the shared statement fingerprint and truncation, and the state it stages.
/// </summary>
[Trait("Stage", "Census")]
public sealed class AttentionCollectorDefinitionTests
{
    private static readonly DateTime Now = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);

    private static CollectorContext MakeContext(
        bool isAzureSqlDb = false, IReadOnlyDictionary<string, string>? state = null, string? currentDatabaseName = null, DateTime? watermark = null)
        => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = Now,
            Deltas = null!,
            Target = new CollectorTargetInfo { IsAzureSqlDb = isAzureSqlDb },
            State = state ?? CollectorContext.NoState,
            CurrentDatabaseName = currentDatabaseName,
            Watermark = watermark,
        };

    private static object[] Event(DateTime time, string? text = "SELECT 1", string app = "app")
        => new object[] { time, "db", app, "host", 55, 0, 350L, (object?)text ?? DBNull.Value };

    private static object[][] Trailer(long? executionCount, long? eventCount, bool gated = false)
        => new[] { new object[] { (object?)executionCount ?? DBNull.Value, (object?)eventCount ?? DBNull.Value, gated } };

    private static IReadOnlyDictionary<string, string> Prior(long eventCount, string? database = null)
        => new Dictionary<string, string> { [AttentionCollector.CountKeyFor(database)] = eventCount.ToString() };

    [Fact]
    public void TheDefinition_IsInTheCatalog_ScheduledEveryMinute_ForThirtyDays_AndEnabled()
    {
        Assert.Contains(AttentionCollector.Instance, CollectorCatalog.All);
        Assert.Equal("attention", AttentionCollector.Instance.Name);
        Assert.Equal("attention_events", AttentionCollector.Instance.TargetTable);

        var schedule = CollectorScheduleDefaults.All["attention"];
        Assert.Equal(1, schedule.FrequencyMinutes);
        Assert.Equal(30, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }

    [Fact]
    public void ThePayloadColumns_AreThePlansColumns()
    {
        Assert.Equal(
            new[] { "event_time", "event_kind", "attention_count", "database_name", "client_app_name", "client_hostname", "session_id", "request_id", "interrupt_duration_us", "statement_fingerprint", "statement_text" },
            AttentionCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());
        Assert.Equal("event_time", AttentionCollector.Instance.WatermarkColumn);
        Assert.Equal("database_name", AttentionCollector.Instance.PerDatabaseWatermarkColumn);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheBatch_ThrowsWhenTheSessionIsMissing_BeforeItReadsAnyTarget(bool azure)
    {
        var text = AttentionCollector.BuildText(AttentionCollector.XeSessionName, azure);

        var guard = text.IndexOf("IF NOT EXISTS", StringComparison.Ordinal);
        var firstTargetRead = text.IndexOf("target_name = N'ring_buffer'", StringComparison.Ordinal);
        Assert.True(guard >= 0 && guard < firstTargetRead, "the missing-session guard comes first");
        Assert.Contains("THROW 50000, N'XE session [PerformanceMonitor_Attention] is not running', 1;", text, StringComparison.Ordinal);
        Assert.Contains(azure ? "sys.dm_xe_database_sessions" : "sys.dm_xe_sessions", text, StringComparison.Ordinal);
        Assert.Contains(azure ? "sys.dm_xe_database_session_targets" : "sys.dm_xe_session_targets", text, StringComparison.Ordinal);
        if (azure)
        {
            Assert.DoesNotContain("sys.dm_xe_sessions", text, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheBatch_ReadsTheCounterAndTheExecutionCount_BeforeTheCast_AndGatesTheShredOnBoth()
    {
        var text = AttentionCollector.BuildText(AttentionCollector.XeSessionName, false);

        var counter = text.IndexOf("target_name = N'event_counter'", StringComparison.Ordinal);
        var sum = text.IndexOf("sum(/CounterTarget/Packages/Package/Event[@name=\"attention\"]/@count)", StringComparison.Ordinal);
        var gate = text.IndexOf("SET @shred_needed", StringComparison.Ordinal);
        var cast = text.IndexOf("TRY_CAST(xet.target_data AS xml)", gate, StringComparison.Ordinal);
        var shred = text.IndexOf("CROSS APPLY pma.ring_buffer.nodes", StringComparison.Ordinal);
        Assert.True(counter >= 0 && sum > counter && gate > sum && cast > gate && shred > cast, "counter, then gate, then cast, then shred");

        var gateText = text[gate..cast];
        Assert.Contains("@execution_count <> @last_execution_count", gateText, StringComparison.Ordinal);
        Assert.Contains("@event_count <> @last_event_count", gateText, StringComparison.Ordinal);
        Assert.Contains("@last_event_count IS NULL", gateText, StringComparison.Ordinal);
        /* The cutoff filters on the attribute without shredding every event first (XeTimeFilterSweepTests), strictly after it. */
        Assert.Contains("evt.exist('@timestamp[. > sql:variable(\"@cutoff_time\")]') = 1", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ABadSessionNameCannotBreakOutOfTheLiteral()
    {
        Assert.Contains("N'a''b'", AttentionCollector.BuildText("a'b", false), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildQuery_PassesTheStoredPriors_PerServer_AndPerDatabase_AndTheWatermarkAsTheCutoff()
    {
        var serverState = new Dictionary<string, string>
        {
            [XeShredGate.StateKey] = "9",
            [AttentionCollector.CountStateKey] = "40",
        };
        var watermark = Now.AddMinutes(-3);
        var plan = AttentionCollector.Instance.BuildQuery(MakeContext(state: serverState, watermark: watermark));
        Assert.Equal(9L, plan.Parameters.Single(p => p.Name == "@last_execution_count").Value);
        Assert.Equal(40L, plan.Parameters.Single(p => p.Name == "@last_event_count").Value);
        Assert.Equal(watermark, plan.Parameters.Single(p => p.Name == "@cutoff_time").Value);

        var dbState = new Dictionary<string, string> { ["xe_execution_count:ringdb"] = "5", ["attention_event_count:ringdb"] = "7" };
        var dbPlan = AttentionCollector.Instance.BuildQuery(MakeContext(isAzureSqlDb: true, state: dbState, currentDatabaseName: "ringdb"));
        Assert.Equal(5L, dbPlan.Parameters.Single(p => p.Name == "@last_execution_count").Value);
        Assert.Equal(7L, dbPlan.Parameters.Single(p => p.Name == "@last_event_count").Value);

        var first = AttentionCollector.Instance.BuildQuery(MakeContext());
        Assert.Null(first.Parameters.Single(p => p.Name == "@last_event_count").Value);
        Assert.Equal(Now.AddMinutes(-10), first.Parameters.Single(p => p.Name == "@cutoff_time").Value);
        Assert.Contains(AttentionCollector.CountStateKey, AttentionCollector.Instance.StateKeys);
        Assert.Contains(XeShredGate.StateKey, AttentionCollector.Instance.StateKeys);
    }

    [Theory]
    [InlineData(10L, 4L, 6L)]
    [InlineData(4L, 4L, 0L)]
    [InlineData(3L, 50L, 3L)]
    [InlineData(0L, 50L, 0L)]
    public void CountDelta_IsTheDifference_AndTheWholeCurrentCountWhenTheSessionRestarted(long current, long prior, long expected)
    {
        Assert.Equal(expected, AttentionCollector.CountDelta(current, prior));
    }

    [Fact]
    public void CountDelta_IsUnknownOnTheFirstRunAndWhenTheCounterIsUnreadable()
    {
        Assert.Null(AttentionCollector.CountDelta(10, null));
        Assert.Null(AttentionCollector.CountDelta(null, 10));
    }

    [Fact]
    public async Task TheStoredFingerprint_IsTheSharedQueryHashIdentityFingerprint_NotACollectorOwnHash()
    {
        /* #5640: the fingerprint is a stored grouping key, and the long-query pileup detector groups by the same definition
           (QueryHashIdentity.StatementFingerprint: "text:" plus 16 hex, lower-cased and whitespace-collapsed). One text in two
           spellings must land on the shared helper's output, and a different text on a different one. */
        var context = MakeContext(state: Prior(100));
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { Event(Now.AddSeconds(-30), "SELECT 1"), Event(Now.AddSeconds(-20), "select   1\r\n"), Event(Now.AddSeconds(-10), "SELECT 2") },
            Trailer(77, 100));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var samples = rows.Where(r => r.EventKind == AttentionCollector.KindSample).ToArray();
        Assert.Equal(3, samples.Length);
        Assert.Matches(new Regex("^text:[0-9a-f]{16}$"), samples[0].StatementFingerprint);
        Assert.Equal(PerformanceMonitor.Common.QueryHashIdentity.StatementFingerprint("SELECT 1"), samples[0].StatementFingerprint);
        Assert.Equal(samples[0].StatementFingerprint, samples[1].StatementFingerprint);
        Assert.Equal(PerformanceMonitor.Common.QueryHashIdentity.StatementFingerprint("SELECT 2"), samples[2].StatementFingerprint);
        Assert.NotEqual(samples[0].StatementFingerprint, samples[2].StatementFingerprint);
    }

    [Fact]
    public async Task TheStoredFingerprint_IsNullForNoTextAndForTheWithheldMarker()
    {
        var context = MakeContext(state: Prior(100));
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { Event(Now.AddSeconds(-30), null), Event(Now.AddSeconds(-20), PerformanceMonitor.Common.SensitiveStatements.PlaceholderText) },
            Trailer(77, 100));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.All(rows.Where(r => r.EventKind == AttentionCollector.KindSample), r => Assert.Null(r.StatementFingerprint));
        Assert.Null(PerformanceMonitor.Common.QueryHashIdentity.StatementFingerprint(PerformanceMonitor.Common.SensitiveStatements.PlaceholderText));
    }

    [Fact]
    public void Truncate_CutsAtFourThousandCharacters_AndLeavesShorterTextTheSameInstance()
    {
        var shortText = "SELECT 1";
        Assert.Same(shortText, AttentionCollector.Truncate(shortText));
        Assert.Equal(4000, AttentionCollector.Truncate(new string('x', 4000))!.Length);
        Assert.Equal(4000, AttentionCollector.Truncate(new string('x', 9000))!.Length);
        Assert.Null(AttentionCollector.Truncate(null));
    }

    [Fact]
    public async Task ReadAsync_WithAPrior_WritesOneSamplePerEvent_AndACountRowWithTheCounterDelta()
    {
        var context = MakeContext(state: Prior(100));
        var t1 = Now.AddSeconds(-30);
        var t2 = Now.AddSeconds(-10);
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { Event(t1), Event(t2, new string('y', 5000)) },
            Trailer(77, 107));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(3, rows.Count);
        Assert.All(rows.Take(2), r => Assert.Equal(AttentionCollector.KindSample, r.EventKind));
        Assert.All(rows.Take(2), r => Assert.Equal(0, r.AttentionCount));
        Assert.Equal(4000, rows[1].StatementText!.Length);
        Assert.NotNull(rows[1].StatementFingerprint);

        /* The count is the counter's delta and nothing else: two samples neither reduce it nor add to it. */
        var count = Assert.Single(rows, r => r.EventKind == AttentionCollector.KindCount);
        Assert.Equal(7, count.AttentionCount);
        Assert.Null(count.EventTime);
        Assert.Equal(7, rows.Sum(r => r.AttentionCount));

        Assert.Equal("107", context.StagedItemState[AttentionCollector.CountStateKey]);
        Assert.Equal("77", context.StagedItemState[XeShredGate.StateKey]);
    }

    [Fact]
    public async Task ReadAsync_WhenTheCounterFell_TheSessionRestarted_AndTheCountRowIsTheCurrentCount()
    {
        var context = MakeContext(state: Prior(500));
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { Event(Now.AddSeconds(-5)), Event(Now.AddSeconds(-4)), Event(Now.AddSeconds(-3)) },
            Trailer(2, 3));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(3, rows.Count(r => r.EventKind == AttentionCollector.KindSample));
        Assert.Equal(3, Assert.Single(rows, r => r.EventKind == AttentionCollector.KindCount).AttentionCount);
        Assert.Equal("3", context.StagedItemState[AttentionCollector.CountStateKey]);
    }

    [Fact]
    public async Task ReadAsync_OnTheFirstRun_WritesSamplesButNoCountRow_AndOnlyStagesThePrior()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(new[] { Event(Now.AddSeconds(-5)) }, Trailer(3, 900));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Equal(AttentionCollector.KindSample, rows[0].EventKind);
        Assert.Equal("900", context.StagedItemState[AttentionCollector.CountStateKey]);
    }

    [Fact]
    public async Task TheCountRow_HasNoTimeOfItsOwn_SoItIsStampedWithTheRunsCollectionTime_NeverTheWatermark()
    {
        var context = MakeContext(state: Prior(10), watermark: Now.AddDays(-3));
        using var reader = FakeCollectorDataReader.WithResultSets(Array.Empty<object[]>(), Trailer(4, 25));
        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var count = Assert.Single(rows);
        Assert.Equal(AttentionCollector.KindCount, count.EventKind);
        Assert.Equal(15, count.AttentionCount);
        Assert.Null(count.EventTime);

        /* collection_time is not a payload column: the host stamps every row of the run with context.CollectionTime, and the
           count row's payload carries no time that could stand in for it (event_time is NULL, so the watermark never moves). */
        var writer = new RecordingCollectorRowWriter();
        AttentionCollector.Instance.WritePayload(count, writer, context);
        Assert.Null(writer.Values[0]);
        Assert.Equal(AttentionCollector.KindCount, writer.Values[1]);
        Assert.Equal(15L, writer.Values[2]);
        Assert.DoesNotContain(writer.Values, v => v is DateTime);
        Assert.Equal(Now, context.CollectionTime);
    }

    /* Several runs against one simulated store: the ring buffer holds whatever has been DISPATCHED to it, the counter holds
       whatever the session has COUNTED (they differ by up to 5 s of dispatch latency), the stored samples dedupe by the
       event_time watermark exactly as the host's MAX(event_time) read does, and the staged state lands after each run. */
    private sealed class Sim
    {
        public Dictionary<string, string> State { get; } = new();
        public List<AttentionCollector.Row> Store { get; } = new();

        public DateTime? Watermark => Store.Max(r => r.EventTime);

        public async Task RunAsync(DateTime[] ringBuffer, long? counter)
        {
            var watermark = Watermark;
            var context = MakeContext(state: new Dictionary<string, string>(State), watermark: watermark);
            var visible = ringBuffer.Where(t => watermark is not { } w || t > w).OrderBy(t => t).Select(t => Event(t)).ToArray();
            using var reader = FakeCollectorDataReader.WithResultSets(visible, Trailer(1, counter));
            var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
            Store.AddRange(rows);
            foreach (var kv in context.StagedItemState)
            {
                State[kv.Key] = kv.Value;
            }
        }

        public long CountedTotal => Store.Where(r => r.EventKind == AttentionCollector.KindCount).Sum(r => r.AttentionCount);
        public int CountRows => Store.Count(r => r.EventKind == AttentionCollector.KindCount);
        public int SampleCount => Store.Count(r => r.EventKind == AttentionCollector.KindSample);
        public long SumOfAllCounts => Store.Sum(r => r.AttentionCount);
    }

    [Fact]
    public async Task TwoRuns_AnEventTheCounterHasAndTheRingBufferDoesNot_IsCountedOnce_AndSampledOnceLater()
    {
        var sim = new Sim();
        await sim.RunAsync(Array.Empty<DateTime>(), 100);                       // first run: only stages 100
        var late = new[] { Now.AddSeconds(-62), Now.AddSeconds(-61), Now.AddSeconds(-60) };

        /* Run N: the counter already counts the 3 attentions, the ring buffer has not been dispatched them yet. */
        await sim.RunAsync(Array.Empty<DateTime>(), 103);
        Assert.Equal(3, sim.CountedTotal);
        Assert.Equal(0, sim.SampleCount);

        /* Run N+1: the ring buffer has them now, the counter does not move. They are samples, and the total stays 3. */
        await sim.RunAsync(late, 103);
        Assert.Equal(3, sim.CountedTotal);
        Assert.Equal(3, sim.SampleCount);
        Assert.Equal(3, sim.SumOfAllCounts);

        /* Run N+2: the same events are still in the ring buffer; the watermark keeps them from being sampled twice. */
        await sim.RunAsync(late, 103);
        Assert.Equal(3, sim.CountedTotal);
        Assert.Equal(3, sim.SampleCount);
    }

    [Fact]
    public async Task AnEventBetweenTheCounterReadAndTheRingBufferRead_IsCountedOnce()
    {
        var sim = new Sim();
        await sim.RunAsync(Array.Empty<DateTime>(), 50);

        /* Run N: the counter was read first (50), the event fired, then the ring buffer read caught it: a sample, delta 0. */
        var between = new[] { Now.AddSeconds(-61) };
        await sim.RunAsync(between, 50);
        Assert.Equal(0, sim.CountedTotal);
        Assert.Equal(1, sim.SampleCount);

        /* Run N+1: the counter shows it. The watermark keeps the sample from being written again, and the count is 1, not 2. */
        await sim.RunAsync(between, 51);
        Assert.Equal(1, sim.CountedTotal);
        Assert.Equal(1, sim.SampleCount);
        Assert.Equal(1, sim.SumOfAllCounts);
    }

    [Fact]
    public async Task AnUnreadableCounterInBetween_WritesNoCountRow_AndTheNextDeltaSpansBothRuns_CountedOnce()
    {
        var sim = new Sim();
        await sim.RunAsync(Array.Empty<DateTime>(), 10);

        var first = Now.AddSeconds(-130);
        var second = Now.AddSeconds(-70);
        var third = Now.AddSeconds(-10);
        await sim.RunAsync(new[] { first }, 11);
        Assert.Equal(1, sim.CountedTotal);
        Assert.Equal(1, sim.CountRows);

        await sim.RunAsync(new[] { first, second }, null);
        Assert.Equal(1, sim.CountRows);                                          // no row for the unreadable run
        Assert.Equal("11", sim.State[AttentionCollector.CountStateKey]);         // the prior is kept, not guessed over

        await sim.RunAsync(new[] { first, second, third }, 13);
        Assert.Equal(3, sim.CountedTotal);                                       // 11 -> 13 is 2, spanning both runs
        Assert.Equal(3, sim.SampleCount);
        Assert.Equal(3, sim.SumOfAllCounts);
    }

    [Fact]
    public async Task QuietRuns_WriteNoCountRows_AndAReset_OrAnAttention_WritesItsRow()
    {
        var sim = new Sim();
        await sim.RunAsync(Array.Empty<DateTime>(), 40);                         // first run: staging only
        Assert.Equal(0, sim.CountRows);

        await sim.RunAsync(Array.Empty<DateTime>(), 40);
        await sim.RunAsync(Array.Empty<DateTime>(), 40);
        Assert.Equal(0, sim.CountRows);                                          // zero deltas write nothing...
        Assert.Equal("40", sim.State[AttentionCollector.CountStateKey]);         // ...and the prior is still staged

        await sim.RunAsync(Array.Empty<DateTime>(), 42);
        Assert.Equal(1, sim.CountRows);
        Assert.Equal(2, sim.CountedTotal);

        await sim.RunAsync(Array.Empty<DateTime>(), 42);
        Assert.Equal(1, sim.CountRows);

        await sim.RunAsync(Array.Empty<DateTime>(), 3);                          // a session restart: the whole current count
        Assert.Equal(2, sim.CountRows);
        Assert.Equal(5, sim.CountedTotal);
    }

    [Fact]
    public async Task ACounterReset_CountsTheCurrentValueAsTheDelta_ThenResumesDifferencing()
    {
        var sim = new Sim();
        await sim.RunAsync(Array.Empty<DateTime>(), 500);
        await sim.RunAsync(Array.Empty<DateTime>(), 505);
        Assert.Equal(5, sim.CountedTotal);

        await sim.RunAsync(Array.Empty<DateTime>(), 2);                          // the session restarted
        Assert.Equal(7, sim.CountedTotal);

        await sim.RunAsync(Array.Empty<DateTime>(), 6);
        Assert.Equal(11, sim.CountedTotal);
    }

    [Fact]
    public async Task TheFirstRun_StagesThePrior_WritesNoCountRow_AndTheSecondRunCountsFromIt()
    {
        var sim = new Sim();
        await sim.RunAsync(Array.Empty<DateTime>(), 40);
        Assert.Empty(sim.Store);
        Assert.Equal("40", sim.State[AttentionCollector.CountStateKey]);

        await sim.RunAsync(Array.Empty<DateTime>(), 41);
        Assert.Equal(1, sim.CountedTotal);
    }

    [Fact]
    public async Task ReadAsync_QuietCycle_WritesNoCountRow_StagesTheCounts_AndMeasuresGated()
    {
        var context = MakeContext(state: Prior(25));
        using var reader = FakeCollectorDataReader.WithResultSets(Array.Empty<object[]>(), Trailer(4, 25, gated: true));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        /* A delta of zero is a quiet run: no row (#5640), but the counter read is still staged as the next run's prior. */
        Assert.Empty(rows);
        Assert.Equal("25", context.StagedItemState[AttentionCollector.CountStateKey]);
        Assert.Contains(context.Measurements, m => m.Label == AttentionCollector.ShredGatedMeasurement && m.Value == 1);
    }

    [Fact]
    public async Task ReadAsync_AnUnreadableCounter_StagesNothing_SoAPriorIsNeverOverwrittenWithAGuess()
    {
        var context = MakeContext(state: Prior(25));
        using var reader = FakeCollectorDataReader.WithResultSets(Array.Empty<object[]>(), Trailer(null, null));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Empty(context.StagedItemState);
    }

    [Fact]
    public async Task ReadAsync_PerDatabase_StagesUnderTheCompoundKeys_AndStampsTheCaptureDatabase()
    {
        var context = MakeContext(isAzureSqlDb: true, state: Prior(1, "ringdb"), currentDatabaseName: "ringdb");
        using var reader = FakeCollectorDataReader.WithResultSets(new[] { Event(Now.AddSeconds(-5)) }, Trailer(8, 2));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal("ringdb", r.DatabaseName));
        Assert.Equal("2", context.StagedItemState["attention_event_count:ringdb"]);
        Assert.Equal("8", context.StagedItemState["xe_execution_count:ringdb"]);
    }
}
