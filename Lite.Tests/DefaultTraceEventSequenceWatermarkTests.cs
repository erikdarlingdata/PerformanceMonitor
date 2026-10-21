/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5644 W6: the default trace watermark rides on the trace's <c>EventSequence</c>, not only on
/// <c>StartTime</c>. A grow/shrink stall carries the time it BEGAN but reaches the trace file when it ENDS,
/// after a poll has stored later-starting events and moved the StartTime watermark past it; "EventSequence
/// greater than the last one stored" reads it whenever it is written, and a trace restart (sequence back to 1)
/// is told apart by the trace's identity, <c>sys.traces.start_time</c>.
///
/// <para>The SQL cannot run here, so <see cref="TraceHost"/> is a model of the monitored server's default trace
/// and of the host's store and collector state. It evaluates the query's WHERE in C# from the parameters the
/// REAL <see cref="DefaultTraceEventsCollector.BuildQuery"/> binds, feeds the REAL
/// <see cref="DefaultTraceEventsCollector.ReadAsync"/> a two-result-set reader, stores what it keeps and
/// persists <c>PendingState</c> the way both hosts do (Lite: DuckDB <c>collector_state</c>; Darling: PG
/// <c>collect.collector_state</c>, through the same <c>StateKeys</c>/<c>PendingState</c> plumbing). The text
/// pins at the end hold the SQL to the rule the model evaluates. The same file runs in Lite.Tests and, linked,
/// in Darling.Tests: the collector is one shared definition.</para>
/// </summary>
public sealed class DefaultTraceEventSequenceWatermarkTests
{
    private static readonly DateTime s_trace1Start = new(2026, 10, 12, 11, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime s_noon = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified);
    private const string File5 = @"S:\MSSQL\Log\log_5.trc";
    private const string File6 = @"S:\MSSQL\Log\log_6.trc";
    private const string File7 = @"S:\MSSQL\Log\log_7.trc";
    private const string Grow = "Log File Auto Grow";

    private sealed record TraceEvent(string File, long Sequence, DateTime Start, string Name, long DurationUs);

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }

    /// <summary>The monitored server's default trace plus the host's store and persisted collector state.</summary>
    private sealed class TraceHost
    {
        public DateTime TraceStart = s_trace1Start;
        public string CurrentFile = File5;
        public long NextSequence = 1;

        /// <summary>Every event in the rollover set, in file order, older traces' files included.</summary>
        public List<TraceEvent> Written = new();

        /// <summary>Rows the host stored: (EventSequence, StartTime, name).</summary>
        public List<(long Sequence, DateTime Start, string Name)> Store = new();

        public Dictionary<string, string> State = new(StringComparer.Ordinal);

        public DateTime? Watermark => Store.Count == 0 ? null : Store.Max(r => r.Start);

        public TraceEvent Write(DateTime start, string name = "ErrorLog", long durationUs = 0)
        {
            var ev = new TraceEvent(CurrentFile, NextSequence++, start, name, durationUs);
            Written.Add(ev);
            return ev;
        }

        public void Rollover(string newFile) => CurrentFile = newFile;

        public void RestartTrace(DateTime newStart, string newFile)
        {
            TraceStart = newStart;
            CurrentFile = newFile;
            NextSequence = 1;
        }

        public TraceHost Clone() => new()
        {
            TraceStart = TraceStart,
            CurrentFile = CurrentFile,
            NextSequence = NextSequence,
            Written = new List<TraceEvent>(Written),
            Store = new List<(long, DateTime, string)>(Store),
            State = new Dictionary<string, string>(State, StringComparer.Ordinal),
        };

        public CollectorContext NewContext() => new()
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc),
            Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo(),
            Watermark = Watermark,
            HasCollectedBefore = Store.Count > 0,
            State = new Dictionary<string, string>(State, StringComparer.Ordinal),
        };

        /// <summary>One collection cycle. <paramref name="saveState"/> false models a state save that failed
        /// after the rows were written. Returns the rows the collector kept (and stored).</summary>
        public async Task<List<DefaultTraceEventsCollector.Row>> PollAsync(bool saveState = true)
        {
            var definition = DefaultTraceEventsCollector.Instance;
            var context = NewContext();
            var query = definition.BuildQuery(context);
            var p = query.Parameters.ToDictionary(x => x.Name, x => x.Value, StringComparer.Ordinal);

            var cutoff = (DateTime?)p["@cutoff_time"];
            var lastTraceStart = (DateTime?)p["@last_trace_start"];
            var lastSequence = (long?)p["@last_event_sequence"];
            var lastPath = (string?)p["@last_trace_path"];
            bool? continues = lastTraceStart is null ? null : lastTraceStart == TraceStart;
            bool singleFile = lastPath == CurrentFile && continues == true;

            /* The query's FROM/WHERE: the file set (current file when steady, else the whole rollover set),
               the curated filter, and  StartTime >= cutoff  OR  (running trace AND (sequence rule OR new trace)). */
            var inSet = singleFile ? Written.Where(e => e.File == CurrentFile) : Written;
            var read = inSet
                .Where(e => e.Name == "ErrorLog" || e.DurationUs > 1_000_000)
                .Where(e => (cutoff is { } c && e.Start >= c)
                    || (e.Start >= TraceStart
                        && ((continues == true && lastSequence is { } l && e.Sequence > l) || continues == false)))
                .ToList();                                          /* file order: the query has no ORDER BY (#6304) */

            using var reader = BuildReader(read, CurrentFile, TraceStart);
            var kept = await definition.ReadAsync(reader, context, CancellationToken.None);
            foreach (var row in kept)
            {
                Store.Add((row.EventSequence!.Value, row.EventTime!.Value, row.EventName!));
            }

            if (saveState)
            {
                foreach (var pair in context.PendingState)
                {
                    State[pair.Key] = pair.Value;
                }
            }

            return kept;
        }

        public (long? Sequence, long? StartTicks) RecordedTrace()
        {
            var text = State[DefaultTraceEventsCollector.TraceSequenceStateKey];
            var parts = text.Split(':');
            return (parts[1].Length == 0 ? null : long.Parse(parts[1], CultureInfo.InvariantCulture),
                long.Parse(parts[0], CultureInfo.InvariantCulture));
        }

        /// <summary>Every curated event written so far is in the store, and nothing is in it twice unless
        /// <paramref name="allowRepeats"/> (a failed state save repeats rows, which the readers collapse).</summary>
        public void AssertNothingLost(bool allowRepeats = false)
        {
            var stored = Store.Select(r => (r.Sequence, r.Start)).ToHashSet();
            foreach (var e in Written.Where(e => e.Name == "ErrorLog" || e.DurationUs > 1_000_000))
            {
                Assert.True(stored.Contains((e.Sequence, e.Start)), $"lost {e.Name} seq {e.Sequence} start {e.Start:HH:mm:ss}");
            }

            if (!allowRepeats)
            {
                Assert.Equal(stored.Count, Store.Count);
            }
        }
    }

    /// <summary>Builds the two-result-set batch the real query returns: the payload, then (path, trace start).</summary>
    private static System.Data.Common.DbDataReader BuildReader(List<TraceEvent> rows, string currentFile, DateTime traceStart)
    {
        var set = new DataSet();
        var payload = new DataTable("payload");
        var types = new[]
        {
            typeof(DateTime), typeof(string), typeof(int), typeof(int), typeof(string), typeof(int),
            typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(long),
            typeof(long), typeof(string), typeof(string), typeof(int), typeof(int), typeof(int),
            typeof(long), typeof(long), typeof(DateTime),
        };
        for (int i = 0; i < types.Length; i++)
        {
            payload.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), types[i]);
        }

        foreach (var e in rows)
        {
            var values = new object[types.Length];
            Array.Fill(values, DBNull.Value);
            values[0] = e.Start;
            values[1] = e.Name;
            values[18] = e.Sequence;
            values[19] = e.DurationUs;
            payload.Rows.Add(values);
        }

        var tail = new DataTable("tail");
        tail.Columns.Add("current_trace_path", typeof(string));
        tail.Columns.Add("current_trace_start", typeof(DateTime));
        tail.Rows.Add(currentFile, traceStart);

        set.Tables.Add(payload);
        set.Tables.Add(tail);
        return set.CreateDataReader();
    }

    /* ---- #6304: the query no longer sorts on the server (the Sort's memory grant timed out on small servers) ---- */

    [Fact]
    public async Task ReadAsync_ReturnsNewestFirst_ThoughTheServerSendsFileOrder()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddMinutes(-30));
        host.Write(s_noon.AddSeconds(3));
        host.Write(s_noon.AddMinutes(-10));        /* written last, started before the one above it */

        var kept = await host.PollAsync();

        Assert.Equal(
            new DateTime?[] { s_noon.AddSeconds(3), s_noon.AddMinutes(-10), s_noon.AddMinutes(-30) },
            kept.Select(r => r.EventTime).ToArray());
    }

    /* ---- the defect: a stall written after the poll that moved the watermark past its StartTime ---- */

    [Fact]
    public async Task LateWrittenAutogrow_IsStored_WhereTheStartTimeRuleAloneLosesIt()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddMinutes(-30));
        host.Write(s_noon.AddSeconds(3));                                /* ErrorLog at 12:00:03 */
        Assert.Equal(2, (await host.PollAsync()).Count);                 /* poll at 12:00:05 */
        Assert.Equal(s_noon.AddSeconds(3), host.Watermark);

        /* The log autogrow began 12:00:00 and ran 8 s: the trace writes it at 12:00:08, with a StartTime
           BELOW the watermark. */
        var grow = host.Write(s_noon, Grow, durationUs: 8_000_000);

        var control = host.Clone();
        control.State.Remove(DefaultTraceEventsCollector.TraceSequenceStateKey);
        Assert.Empty(await control.PollAsync());                         /* the old rule: never read */

        var kept = await host.PollAsync();                               /* poll at 12:00:10 */
        var row = Assert.Single(kept);
        Assert.Equal(grow.Sequence, row.EventSequence);
        Assert.Equal(Grow, row.EventName);
        host.AssertNothingLost();
    }

    [Fact]
    public async Task ReRead_StoresNothingTwice_AcrossPollsAndARollover()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(3));
        await host.PollAsync();
        host.Write(s_noon, Grow, 8_000_000);
        await host.PollAsync();
        int stored = host.Store.Count;

        for (int i = 0; i < 3; i++)
        {
            Assert.Empty(await host.PollAsync());
        }

        host.Rollover(File6);                                             /* path moved: the whole set is read */
        Assert.Empty(await host.PollAsync());
        Assert.Empty(await host.PollAsync());

        Assert.Equal(stored, host.Store.Count);
        host.AssertNothingLost();
    }

    /* ---- rollover: one trace, several files, sequences keep increasing ---- */

    [Fact]
    public async Task Rollover_KeepsReading_AndALateStallInTheNewFileIsStillFound()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(1));
        host.Write(s_noon.AddSeconds(2));
        await host.PollAsync();
        Assert.Equal(2L, host.RecordedTrace().Sequence);

        host.Rollover(File6);
        host.Write(s_noon.AddSeconds(6));
        host.Write(s_noon.AddSeconds(1.5), Grow, 4_000_000);              /* began before the watermark */
        var kept = await host.PollAsync();
        Assert.Equal(2, kept.Count);
        Assert.Equal(4L, host.RecordedTrace().Sequence);                   /* continues across files, no reset */

        /* Steady again on file 6: the current file alone, and a stall still found in it. */
        host.Write(s_noon.AddSeconds(5), Grow, 3_000_000);
        Assert.Single(await host.PollAsync());
        host.AssertNothingLost();
    }

    /* ---- restart: the sequence goes back to 1 under a new trace identity ---- */

    [Fact]
    public async Task TraceRestart_StoresTheNewTrace_AndNeverTheOldTracesRowsTwice()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(1));
        host.Write(s_noon.AddSeconds(3));
        host.Write(s_noon.AddSeconds(4));
        await host.PollAsync();
        Assert.Equal(3L, host.RecordedTrace().Sequence);

        /* Written by the old trace after the last poll, just before the server went down: sequence 4. */
        var lastOfOld = host.Write(s_noon.AddSeconds(20));

        var newStart = s_noon.AddMinutes(30);
        host.RestartTrace(newStart, File6);
        host.Write(newStart.AddSeconds(5));                                /* new trace, sequence 1 */
        host.Write(newStart.AddSeconds(6));                                /* sequence 2 */
        int before = host.Store.Count;

        var kept = await host.PollAsync();
        Assert.Equal(3, kept.Count);                                       /* old seq 4 once + new 1 and 2 */
        Assert.Contains(kept, r => r.EventSequence == lastOfOld.Sequence && r.EventTime == lastOfOld.Start);
        Assert.Equal(before + 3, host.Store.Count);

        var (sequence, ticks) = host.RecordedTrace();
        Assert.Equal(newStart.Ticks, ticks);
        Assert.Equal(2, sequence);                                         /* the old trace's 4 does not count */

        /* A stall of the new trace written late: found, because the bound is the new trace's 2 and not the old 4. */
        host.Write(newStart.AddSeconds(1), Grow, 6_000_000);
        var late = Assert.Single(await host.PollAsync());
        Assert.Equal(3, late.EventSequence);
        host.AssertNothingLost();
    }

    [Fact]
    public async Task TraceRestart_WithNoCuratedEventYet_RecordsSequenceZero_AndTheFirstEventsAreRead()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(3));
        await host.PollAsync();

        var newStart = s_noon.AddHours(1);
        host.RestartTrace(newStart, File6);
        Assert.Empty(await host.PollAsync());                               /* quiet new trace */
        Assert.Equal(0L, host.RecordedTrace().Sequence);
        Assert.Equal(newStart.Ticks, host.RecordedTrace().StartTicks);

        host.Write(newStart.AddSeconds(2));
        host.Write(newStart, Grow, 5_000_000);
        Assert.Equal(2, (await host.PollAsync()).Count);
        host.AssertNothingLost();
    }

    [Fact]
    public async Task TraceRestart_AndASecondRestart_EachReadFromTheirFirstEvent()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(3));
        await host.PollAsync();

        host.RestartTrace(s_noon.AddHours(1), File6);
        host.Write(s_noon.AddHours(1).AddSeconds(2));
        Assert.Single(await host.PollAsync());

        host.RestartTrace(s_noon.AddHours(2), File7);
        host.Write(s_noon.AddHours(2).AddSeconds(2));
        host.Write(s_noon.AddHours(2).AddSeconds(3));
        Assert.Equal(2, (await host.PollAsync()).Count);
        Assert.Equal(2L, host.RecordedTrace().Sequence);
        host.AssertNothingLost();
    }

    /* ---- upgrade: no stored sequence yet ---- */

    [Fact]
    public async Task Upgrade_NoSequenceState_FallsBackToTheStartTimeRule_ThenRecordsTheSequence()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(1));
        host.Write(s_noon.AddSeconds(3));
        await host.PollAsync();
        host.State.Remove(DefaultTraceEventsCollector.TraceSequenceStateKey);   /* state as the previous build left it */

        var between = host.Write(s_noon.AddSeconds(5));                     /* written before the upgrade poll */
        var kept = await host.PollAsync();
        var row = Assert.Single(kept);                                      /* stored once, the newest tick not again */
        Assert.Equal(between.Sequence, row.EventSequence);
        Assert.Equal(3L, host.RecordedTrace().Sequence);

        /* From the next poll the late stall is found. */
        host.Write(s_noon.AddSeconds(2), Grow, 9_000_000);
        Assert.Single(await host.PollAsync());
        host.AssertNothingLost();
    }

    [Fact]
    public async Task Upgrade_NoStateAtAll_RepeatsOnlyTheNewestTick_WhichTheReadersCollapse()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(1));
        host.Write(s_noon.AddSeconds(3));
        await host.PollAsync();
        host.State.Clear();                                                  /* state reset or lost */

        var kept = await host.PollAsync();
        Assert.All(kept, r => Assert.Equal(s_noon.AddSeconds(3), r.EventTime));
        Assert.Single(kept);
        Assert.True(host.State.ContainsKey(DefaultTraceEventsCollector.TraceSequenceStateKey));
        host.AssertNothingLost(allowRepeats: true);
    }

    [Fact]
    public async Task Upgrade_AQuietFirstPoll_RecordsTheIdentityAlone_SoTheSteadyStateFileReadIsKept()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(3));
        await host.PollAsync();
        host.State.Remove(DefaultTraceEventsCollector.TraceSequenceStateKey);
        host.Written.Clear();                                               /* nothing curated in the file set */

        await host.PollAsync();
        var (sequence, ticks) = host.RecordedTrace();
        Assert.Null(sequence);                                              /* identity, no sequence yet */
        Assert.Equal(host.TraceStart.Ticks, ticks);

        /* Identity known => steady state reads the current file alone again (#1962), sequence rule still off. */
        var query = DefaultTraceEventsCollector.Instance.BuildQuery(host.NewContext());
        Assert.Equal(host.TraceStart, (DateTime?)query.Parameters.Single(x => x.Name == "@last_trace_start").Value);
        Assert.Null(query.Parameters.Single(x => x.Name == "@last_event_sequence").Value);

        host.Write(s_noon.AddSeconds(7));
        Assert.Single(await host.PollAsync());
        Assert.Equal(host.Written[0].Sequence, host.RecordedTrace().Sequence);
    }

    /* ---- failure: state is saved only after a good write; a failed save repeats at worst ---- */

    [Fact]
    public async Task FailedStateSave_RepeatsAtWorst_AndNeverLosesAnEvent()
    {
        var host = new TraceHost();
        host.Write(s_noon.AddSeconds(1));
        await host.PollAsync();

        host.Write(s_noon.AddSeconds(3));
        host.Write(s_noon, Grow, 8_000_000);
        Assert.Equal(2, (await host.PollAsync(saveState: false)).Count);     /* rows written, state not saved */

        var again = await host.PollAsync();                                   /* stale state: a repeat */
        Assert.True(again.Count <= 2);
        host.AssertNothingLost(allowRepeats: true);

        int settled = host.Store.Count;
        Assert.Empty(await host.PollAsync());                                 /* the saved state ends the repeat */
        Assert.Equal(settled, host.Store.Count);
    }

    /* ---- the stored text ---- */

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("garbage")]
    [InlineData("123")]
    [InlineData(":5")]
    [InlineData("-5:5")]
    [InlineData("639000000000000000:-3")]
    [InlineData("639000000000000000:abc")]
    [InlineData("99999999999999999999999:5")]
    public void MalformedOrAbsentState_BindsNoIdentityAndNoSequence(string? state)
    {
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = DateTime.UtcNow,
            Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo(),
            State = state is null
                ? CollectorContext.NoState
                : new Dictionary<string, string>(StringComparer.Ordinal) { [DefaultTraceEventsCollector.TraceSequenceStateKey] = state },
        };

        var parameters = DefaultTraceEventsCollector.Instance.BuildQuery(context).Parameters;
        Assert.Null(parameters.Single(x => x.Name == "@last_trace_start").Value);
        Assert.Null(parameters.Single(x => x.Name == "@last_event_sequence").Value);
    }

    [Fact]
    public void WellFormedState_BindsTheIdentityAndTheSequence_AtTheRightTypes()
    {
        var start = new DateTime(2026, 10, 10, 5, 27, 27, 487, DateTimeKind.Unspecified);
        var context = new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = DateTime.UtcNow,
            Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo(),
            State = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                [DefaultTraceEventsCollector.TraceSequenceStateKey] = start.Ticks.ToString(CultureInfo.InvariantCulture) + ":79116",
            },
        };

        var parameters = DefaultTraceEventsCollector.Instance.BuildQuery(context).Parameters;
        var traceStart = parameters.Single(x => x.Name == "@last_trace_start");
        var sequence = parameters.Single(x => x.Name == "@last_event_sequence");
        Assert.Equal(start, traceStart.Value);
        Assert.Equal(CollectorParameterType.DateTime2, traceStart.Type);
        Assert.Equal(79116L, sequence.Value);
        Assert.Equal(CollectorParameterType.BigInt, sequence.Type);
    }

    [Fact]
    public void StateKeys_DeclareTheTraceSequence_SoBothHostsLoadAndSaveIt()
    {
        Assert.Equal("trace_event_sequence", DefaultTraceEventsCollector.TraceSequenceStateKey);
        Assert.Contains(DefaultTraceEventsCollector.TraceSequenceStateKey, DefaultTraceEventsCollector.Instance.StateKeys);
    }

    [Fact]
    public void QueryText_HoldsTheRuleTheModelEvaluates()
    {
        var text = DefaultTraceEventsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 1,
            ServerName = "s",
            CollectionTime = DateTime.UtcNow,
            Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo(),
        }).Text.Replace("\r\n", "\n", StringComparison.Ordinal);

        /* The identity of the running trace, captured with the path in the same read. */
        Assert.Contains("@current_trace_start = t.start_time", text, StringComparison.Ordinal);
        Assert.Contains("WHEN CONVERT(datetime, @last_trace_start) = @current_trace_start\n        THEN 1", text, StringComparison.Ordinal);

        /* The StartTime floor, OR the sequence rule restricted to the running trace; a new trace from its start. */
        Assert.Contains("    ft.StartTime >= CONVERT(datetime, COALESCE(@cutoff_time", text, StringComparison.Ordinal);
        Assert.Contains("ft.StartTime >= @current_trace_start", text, StringComparison.Ordinal);
        Assert.Contains("(@trace_continues = 1 AND ft.EventSequence > @last_event_sequence)", text, StringComparison.Ordinal);
        Assert.Contains("OR @trace_continues = 0", text, StringComparison.Ordinal);

        /* Only the same trace may use the one-file steady-state read: an unknown or changed identity reads the set. */
        Assert.Equal(2, CountOf(text, "AND  @trace_continues = 1\n"));

        /* The trailing result set carries the identity next to the path. */
        Assert.Contains("current_trace_path = @current_trace_path,\n    current_trace_start = @current_trace_start;", text, StringComparison.Ordinal);

        /* Unchanged by this fix: the Commit-only DDL rule, the curated list and the 1 s stall filter. */
        Assert.Contains("AND ISNULL(ft.EventSubClass, 1) = 1", text, StringComparison.Ordinal);
        Assert.Contains("ISNULL(ft.Duration, 0) > 1000000", text, StringComparison.Ordinal);
        Assert.Contains("te.name LIKE N'%Log File Auto Grow%'", text, StringComparison.Ordinal);
    }

    private static int CountOf(string text, string needle)
    {
        int count = 0;
        for (int at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0;
             at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
