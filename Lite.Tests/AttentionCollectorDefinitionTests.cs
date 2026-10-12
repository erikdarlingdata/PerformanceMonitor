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
/// counter-delta and restart arithmetic, the overflow row, the statement fingerprint and truncation, and the state it stages.
/// </summary>
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
        Assert.Contains("> @cutoff_time", text, StringComparison.Ordinal);
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
        Assert.Equal(0, AttentionCollector.OverflowCount(null, 3));
        Assert.Equal(0, AttentionCollector.OverflowCount(5, 5));
        Assert.Equal(0, AttentionCollector.OverflowCount(5, 7));
        Assert.Equal(2, AttentionCollector.OverflowCount(5, 3));
    }

    [Fact]
    public void Fingerprint_Is40LowerHexCharacters_Stable_AndNullForNoTextOrWithheldText()
    {
        var a = AttentionCollector.Fingerprint("SELECT 1");
        Assert.Matches(new Regex("^[0-9a-f]{40}$"), a);
        Assert.Equal(a, AttentionCollector.Fingerprint("SELECT 1"));
        Assert.NotEqual(a, AttentionCollector.Fingerprint("SELECT 2"));
        Assert.Null(AttentionCollector.Fingerprint(null));
        Assert.Null(AttentionCollector.Fingerprint(""));
        Assert.Null(AttentionCollector.Fingerprint(PerformanceMonitor.Common.SensitiveStatements.PlaceholderText));
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
    public async Task ReadAsync_WithAPrior_WritesOneRowPerEvent_AndAnOverflowRowForTheEventsTheRingBufferLost()
    {
        var context = MakeContext(state: Prior(100));
        var t1 = Now.AddSeconds(-30);
        var t2 = Now.AddSeconds(-10);
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { Event(t1), Event(t2, new string('y', 5000)) },
            Trailer(77, 107));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(3, rows.Count);
        Assert.All(rows.Take(2), r => Assert.Equal(AttentionCollector.KindAttention, r.EventKind));
        Assert.All(rows.Take(2), r => Assert.Equal(1, r.AttentionCount));
        Assert.Equal(4000, rows[1].StatementText!.Length);
        Assert.NotNull(rows[1].StatementFingerprint);
        var overflow = rows[2];
        Assert.Equal(AttentionCollector.KindOverflow, overflow.EventKind);
        Assert.Equal(5, overflow.AttentionCount);
        Assert.Equal(t2, overflow.EventTime);
        Assert.Equal(7, rows.Sum(r => r.AttentionCount));

        Assert.Equal("107", context.StagedItemState[AttentionCollector.CountStateKey]);
        Assert.Equal("77", context.StagedItemState[XeShredGate.StateKey]);
    }

    [Fact]
    public async Task ReadAsync_WhenTheCounterFell_TheSessionRestarted_AndTheDeltaIsTheCurrentCount()
    {
        var context = MakeContext(state: Prior(500));
        using var reader = FakeCollectorDataReader.WithResultSets(
            new[] { Event(Now.AddSeconds(-5)), Event(Now.AddSeconds(-4)), Event(Now.AddSeconds(-3)) },
            Trailer(2, 3));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(3, rows.Count);
        Assert.DoesNotContain(rows, r => r.EventKind == AttentionCollector.KindOverflow);
        Assert.Equal("3", context.StagedItemState[AttentionCollector.CountStateKey]);
    }

    [Fact]
    public async Task ReadAsync_OnTheFirstRun_WritesNoOverflowRow_EvenWhenTheCounterIsFarAboveTheKeptRows()
    {
        var context = MakeContext();
        using var reader = FakeCollectorDataReader.WithResultSets(new[] { Event(Now.AddSeconds(-5)) }, Trailer(3, 900));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Single(rows);
        Assert.Equal(AttentionCollector.KindAttention, rows[0].EventKind);
        Assert.Equal("900", context.StagedItemState[AttentionCollector.CountStateKey]);
    }

    [Fact]
    public async Task ReadAsync_OverflowWithNoDetail_SitsAtTheWatermark_NotAtTheReadTime()
    {
        var watermark = Now.AddMinutes(-2);
        var context = MakeContext(state: Prior(10), watermark: watermark);
        using var reader = FakeCollectorDataReader.WithResultSets(Array.Empty<object[]>(), Trailer(4, 25));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var overflow = Assert.Single(rows);
        Assert.Equal(15, overflow.AttentionCount);
        Assert.Equal(watermark, overflow.EventTime);
    }

    [Fact]
    public async Task ReadAsync_QuietCycle_WritesNothing_StagesTheCounts_AndMeasuresGated()
    {
        var context = MakeContext(state: Prior(25));
        using var reader = FakeCollectorDataReader.WithResultSets(Array.Empty<object[]>(), Trailer(4, 25, gated: true));

        var rows = await AttentionCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

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

        Assert.Equal("ringdb", Assert.Single(rows).DatabaseName);
        Assert.Equal("2", context.StagedItemState["attention_event_count:ringdb"]);
        Assert.Equal("8", context.StagedItemState["xe_execution_count:ringdb"]);
    }
}
