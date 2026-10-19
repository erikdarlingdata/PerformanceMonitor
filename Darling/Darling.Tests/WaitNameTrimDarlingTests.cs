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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Darling's side of the wait-name trim. Four SQL Server wait names (SQP_STATS_REPORTING, EDC_DOPP_LOCK,
/// EDC_DOPP_BACKGROUND, EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND) come back from the DMV with a trailing space,
/// and every comparison the product makes is exact. The shared readers trim the name before they match, store
/// or key it; Lite.Tests' <c>WaitNameTrimTests</c> drives each reader, and these tests pin what is Darling's own:
/// the ignore set the service hands the collectors, its delta calculator's rule for the new trimmed key, and the
/// system_health shred its viewer and MCP host share.
/// </summary>
public sealed class WaitNameTrimDarlingTests
{
    private const int ServerId = 7;
    /* A spaced name the shared ignore list names: dropped at collection, arriving with its trailing space. */
    private const string SpacedTimer = "SQP_STATS_REPORTING ";

    /* A spaced name nothing ignores, so it is stored and keyed: the subject of the delta-baseline pin. */
    private const string SpacedName = "EDC_DOPP_LOCK ";
    private const string TrimmedName = "EDC_DOPP_LOCK";

    /* wait_stats payload positions (WaitStatsCollector.PayloadColumns order). */
    private const int DeltaTasksColumn = 4;
    private const int DeltaTimeColumn = 5;
    private const int DeltaSignalColumn = 6;
    private const int IntervalColumn = 7;

    private static DateTime T0 => new(2026, 9, 1, 12, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheSharedIgnoreList_NamesTheHyperscaleTimers_AndLeavesRemoteBlockIoVisible()
    {
        Assert.Contains("RBIO_COMM_RETRY", IgnoredWaitDefaults.All);
        Assert.Contains("SQP_STATS_REPORTING", IgnoredWaitDefaults.All);

        /* Real remote I/O: the same timer shape in the measurement, but an operator needs to see it. */
        Assert.DoesNotContain("REMOTE_BLOCK_IO", IgnoredWaitDefaults.All);

        /* An entry is a clean name, because the readers trim before they match. */
        Assert.All(IgnoredWaitDefaults.All, name => Assert.Equal(name.Trim(), name));
    }

    /// <summary>
    /// The service hands every wait-reading context the shared list and no other: the main collection run and the
    /// live fetch both bind <c>IgnoredWaitDefaults.All</c>, so the two new entries are what Darling collects by.
    /// </summary>
    [Fact]
    public void TheServiceRunner_BindsTheSharedIgnoreList_AndNoOtherSource()
    {
        var source = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

        var bindings = Regex.Matches(source, @"IgnoredWaitTypes\s*=\s*([^,;\r\n]+)")
            .Select(m => m.Groups[1].Value.Trim())
            .ToArray();

        Assert.True(bindings.Length >= 2, "the collection run and the live fetch each set the list");
        Assert.All(bindings, binding => Assert.Equal("IgnoredWaitDefaults.All", binding));
    }

    [Fact]
    public async Task WaitStats_WithDarlingsIgnoreSet_DropsTheTimers_AndStillCollectsRemoteBlockIo()
    {
        using var reader = WaitStatsReader(
            ("RBIO_COMM_RETRY", 735, 11_025_000, 0),
            (SpacedTimer, 37, 11_100_000, 0),
            ("REMOTE_BLOCK_IO", 12, 480, 3),
            ("PAGEIOLATCH_SH", 7, 300, 20));

        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, ContextAt(T0, new CollectorDeltaCalculator()), CancellationToken.None);

        Assert.Equal(new[] { "REMOTE_BLOCK_IO", "PAGEIOLATCH_SH" }, rows.Select(r => r.WaitType).ToArray());
    }

    [Fact]
    public async Task WaitingTasks_WithDarlingsIgnoreSet_DropsTheTimers_AndStillCollectsRemoteBlockIo()
    {
        var table = new DataTable("waiting_tasks");
        table.Columns.Add("session_id", typeof(short));
        table.Columns.Add("wait_type", typeof(string));
        table.Columns.Add("wait_duration_ms", typeof(long));
        table.Columns.Add("blocking_session_id", typeof(short));
        table.Columns.Add("database_name", typeof(string));
        table.Rows.Add((short)61, SpacedTimer, 240_000L, DBNull.Value, DBNull.Value);
        table.Rows.Add((short)62, "RBIO_COMM_RETRY", 900L, DBNull.Value, DBNull.Value);
        table.Rows.Add((short)63, "REMOTE_BLOCK_IO", 15L, DBNull.Value, "SalesDb");
        table.Rows.Add((short)64, SpacedName, 1_000L, DBNull.Value, "SalesDb");

        using var reader = table.CreateDataReader();
        var rows = await WaitingTasksCollector.Instance.ReadAsync(reader, ContextAt(T0, new CollectorDeltaCalculator()), CancellationToken.None);

        Assert.Equal(new[] { "REMOTE_BLOCK_IO", TrimmedName }, rows.Select(r => r.WaitType).ToArray());
    }

    /// <summary>
    /// The trimmed name is a NEW key for the delta calculator, so after an upgrade the first sample under it has no
    /// baseline and BECOMES the baseline: delta 0 and interval 0, the pair that means "not knowable" rather than
    /// "idle". The failure this guards against is the whole cumulative counter landing as one interval's delta.
    ///
    /// <para>Run on Darling's own calculator, with the pre-upgrade baseline under the spaced key that the store seed
    /// leaves (the seed reads <c>wait_type</c> as stored and keys on it unchanged). A clean-named wait seeded the
    /// same way is the control: it yields a real delta on the same pass, so the zero under the trimmed key is the
    /// new-key rule and not a missing seed.</para>
    /// </summary>
    [Fact]
    public async Task FirstSampleUnderTheTrimmedKey_IsABaseline_NotTheRunningTotal()
    {
        var deltas = new DarlingDeltaCalculator();

        Baseline(deltas, SpacedName, tasks: 100, timeMs: 30_000_000, signalMs: 0);
        Baseline(deltas, "PAGEIOLATCH_SH", tasks: 50, timeMs: 4_000, signalMs: 100);

        /* The first pass after the upgrade. The DMV still returns the spaced name. */
        var pass1 = await RunPassAsync(
            deltas,
            T0.AddSeconds(120),
            (SpacedName, 101, 30_300_000, 0),
            ("PAGEIOLATCH_SH", 55, 4_500, 120));

        Assert.True(pass1.ContainsKey(TrimmedName), "the row is stored under the trimmed name");
        Assert.False(pass1.ContainsKey(SpacedName), "nothing is stored under the spaced name any more");

        /* A baseline: no delta, and the interval says "not knowable". 30,300,000 must not appear as a delta. */
        var baseline = pass1[TrimmedName];
        Assert.Equal(0L, baseline[DeltaTasksColumn]);
        Assert.Equal(0L, baseline[DeltaTimeColumn]);
        Assert.Equal(0L, baseline[DeltaSignalColumn]);
        Assert.Equal(0, baseline[IntervalColumn]);

        /* The control: a real delta over the real interval. */
        var control = pass1["PAGEIOLATCH_SH"];
        Assert.Equal(500L, control[DeltaTimeColumn]);
        Assert.Equal(120, control[IntervalColumn]);

        /* The next pass is an ordinary one, under the trimmed key. */
        var pass2 = await RunPassAsync(
            deltas,
            T0.AddSeconds(240),
            (SpacedName, 102, 30_420_000, 0),
            ("PAGEIOLATCH_SH", 56, 4_600, 125));

        var second = pass2[TrimmedName];
        Assert.Equal(1L, second[DeltaTasksColumn]);
        Assert.Equal(120_000L, second[DeltaTimeColumn]);
        Assert.Equal(120, second[IntervalColumn]);
    }

    [Fact]
    public void SignificantWait_TrimsATrailingSpaceInTheWaitTypeText()
    {
        var record = SystemHealthParser.ParseSignificantWait(WaitInfoEvent("SQP_STATS_REPORTING "));

        Assert.NotNull(record);
        Assert.Equal("SQP_STATS_REPORTING", record!.WaitType);
    }

    /// <summary>
    /// The significance gate matches the wait type against an exact-match ignore set, so a spaced name has to
    /// reach it trimmed: WAITFOR is on that list, and a WAITFOR that arrived with a trailing space would
    /// otherwise read as a significant wait.
    /// </summary>
    [Fact]
    public void SignificantWait_ASpacedNameStillMatchesTheGatesIgnoreList()
    {
        var record = SystemHealthParser.ParseSignificantWait(WaitInfoEvent("WAITFOR "));

        Assert.NotNull(record);
        Assert.False(SystemHealthSignificance.IsSignificant(record!));
    }

    private static string WaitInfoEvent(string waitTypeText) =>
        "<event name=\"wait_info\" package=\"sqlserver\" timestamp=\"2026-07-05T12:04:30.900Z\">" +
        "<data name=\"wait_type\"><type name=\"wait_types\" package=\"sqlserver\" /><value>66</value><text>" + waitTypeText + "</text></data>" +
        "<data name=\"duration\"><value>1500</value></data>" +
        "<data name=\"signal_duration\"><value>12</value></data>" +
        "<action name=\"session_id\" package=\"sqlserver\"><value>57</value></action>" +
        "<action name=\"sql_text\" package=\"sqlserver\"><value>SELECT 1;</value></action>" +
        "</event>";

    /// <summary>
    /// What a store seed leaves in the calculator for one wait: the last pre-upgrade counters at <see cref="T0"/>.
    /// (A key with no history returns 0 and stores the value as its baseline, which is exactly what the seed does.)
    /// </summary>
    private static void Baseline(ICollectorDeltaCalculator deltas, string key, long tasks, long timeMs, long signalMs)
    {
        deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_tasks", key, tasks, out _, collectionTime: T0);
        deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_time", key, timeMs, out _, collectionTime: T0);
        deltas.CalculateDeltaWithInterval(ServerId, "wait_stats_signal", key, signalMs, out _, collectionTime: T0);
    }

    private static async Task<Dictionary<string, object?[]>> RunPassAsync(
        ICollectorDeltaCalculator deltas, DateTime collectionTime, params (string WaitType, long Tasks, long TimeMs, long SignalMs)[] dmvRows)
    {
        var context = ContextAt(collectionTime, deltas);

        using var reader = WaitStatsReader(dmvRows);
        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var stored = new Dictionary<string, object?[]>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            var writer = new RecordingCollectorRowWriter();
            WaitStatsCollector.Instance.WritePayload(row, writer, context);
            stored[(string)writer.Values[0]!] = writer.Values.ToArray();
        }

        return stored;
    }

    private static CollectorContext ContextAt(DateTime collectionTime, ICollectorDeltaCalculator deltas) => new()
    {
        ServerId = ServerId,
        ServerName = "wait-name-trim",
        CollectionTime = collectionTime,
        Deltas = deltas,
        /* The same set the service hands the collectors. */
        IgnoredWaitTypes = IgnoredWaitDefaults.All,
    };

    private static DataTableReader WaitStatsReader(params (string WaitType, long Tasks, long TimeMs, long SignalMs)[] rows)
    {
        var table = new DataTable("wait_stats");
        table.Columns.Add("wait_type", typeof(string));
        table.Columns.Add("waiting_tasks_count", typeof(long));
        table.Columns.Add("wait_time_ms", typeof(long));
        table.Columns.Add("signal_wait_time_ms", typeof(long));
        foreach (var row in rows)
        {
            table.Rows.Add(row.WaitType, row.Tasks, row.TimeMs, row.SignalMs);
        }

        return table.CreateDataReader();
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    /// <summary>Captures the values a collector definition writes for one row, in payload order.</summary>
    private sealed class RecordingCollectorRowWriter : ICollectorRowWriter
    {
        public List<object?> Values { get; } = new();

        public ICollectorRowWriter Value(string? value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(long value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(long? value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(int value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(int? value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(short value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(short? value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(double value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(double? value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(decimal value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(decimal? value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(bool value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(bool? value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(DateTime value) { Values.Add(value); return this; }
        public ICollectorRowWriter Value(DateTime? value) { Values.Add(value); return this; }
        public ICollectorRowWriter NullValue() { Values.Add(null); return this; }
    }
}
