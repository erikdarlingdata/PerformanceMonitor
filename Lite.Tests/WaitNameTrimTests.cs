/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Four SQL Server wait names come back from the DMV with a trailing space (SQP_STATS_REPORTING,
/// EDC_DOPP_LOCK, EDC_DOPP_BACKGROUND, EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND). Every comparison the product
/// makes is exact, so before the trim an ignore entry named <c>SQP_STATS_REPORTING</c> could never match the
/// stored <c>SQP_STATS_REPORTING </c>. These tests drive each shared reader with the spaced name the server
/// returns and pin that the name is matched, stored and keyed trimmed.
///
/// <para>The same readers serve both apps, so the trim is pinned once here and the Darling side pins its own
/// ignore set, delta calculator and parser in <c>WaitNameTrimDarlingTests</c>.</para>
/// </summary>
public sealed class WaitNameTrimTests
{
    [Theory]
    [InlineData("SQP_STATS_REPORTING ", "SQP_STATS_REPORTING")]
    [InlineData("EDC_DOPP_LOCK ", "EDC_DOPP_LOCK")]
    [InlineData("EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND \t\r\n", "EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND")]
    [InlineData("PAGEIOLATCH_SH", "PAGEIOLATCH_SH")]
    [InlineData("", "")]
    public void Trim_RemovesTrailingWhitespace(string raw, string expected)
        => Assert.Equal(expected, WaitTypeName.Trim(raw));

    [Fact]
    public void Trim_TouchesNothingButTheTail_AndPassesNullThrough()
    {
        Assert.Equal(" LEADING_AND INNER", WaitTypeName.Trim(" LEADING_AND INNER  "));
        Assert.Null(WaitTypeName.Trim(null));
    }

    [Fact]
    public async Task WaitStats_ReadAsync_StoresTheTrimmedName_AndKeysTheDeltaOnIt()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND ", 4L, 900_000L, 0L },
            new object[] { "EDC_DOPP_LOCK ", 2L, 500L, 10L });
        var deltas = new RecordingCollectorDeltaCalculator();
        var context = CollectorTestContext.Make(deltas);

        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(
            new[] { "EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND", "EDC_DOPP_LOCK" },
            rows.Select(r => r.WaitType).ToArray());

        var writer = new RecordingCollectorRowWriter();
        WaitStatsCollector.Instance.WritePayload(rows[0], writer, context);

        /* The stored value and all three delta keys read the trimmed string. */
        Assert.Equal("EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND", writer.Values[0]);
        Assert.Equal(3, deltas.Calls.Count);
        Assert.All(deltas.Calls, call => Assert.Equal("EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND", call.Key));
    }

    [Fact]
    public async Task WaitStats_ReadAsync_ASpacedNameMatchesItsIgnoreEntry()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "SQP_STATS_REPORTING ", 37L, 11_100_000L, 0L },
            new object[] { "PAGEIOLATCH_SH", 7L, 300L, 20L });
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator(), ignored: new[] { "SQP_STATS_REPORTING" });

        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal("PAGEIOLATCH_SH", Assert.Single(rows).WaitType);
    }

    /// <summary>
    /// The shipped lists, through the real reader: the two Hyperscale timers are dropped (one of them arriving
    /// with its trailing space), REMOTE_BLOCK_IO is still collected. The shared constant is what Darling runs;
    /// the bundled json is what a fresh Lite install runs.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WaitStats_ReadAsync_WithTheShippedList_DropsTheHyperscaleTimers_AndStillCollectsRemoteBlockIo(bool useBundledJson)
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "RBIO_COMM_RETRY", 735L, 11_025_000L, 0L },
            new object[] { "SQP_STATS_REPORTING ", 37L, 11_100_000L, 0L },
            new object[] { "REMOTE_BLOCK_IO", 12L, 480L, 3L },
            new object[] { "PAGEIOLATCH_SH", 7L, 300L, 20L });
        var context = CollectorTestContext.Make(
            new RecordingCollectorDeltaCalculator(),
            ignored: useBundledJson ? LoadBundledIgnoreList() : IgnoredWaitDefaults.All);

        var rows = await WaitStatsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(new[] { "REMOTE_BLOCK_IO", "PAGEIOLATCH_SH" }, rows.Select(r => r.WaitType).ToArray());
    }

    [Fact]
    public async Task WaitingTasks_ReadAsync_StoresTheTrimmedName()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { (short)64, "EDC_DOPP_LOCK ", 1_000L, DBNull.Value, "SalesDb" });
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        var rows = await WaitingTasksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        WaitingTasksCollector.Instance.WritePayload(Assert.Single(rows), writer, context);
        Assert.Equal("EDC_DOPP_LOCK", writer.Values[1]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WaitingTasks_ReadAsync_WithTheShippedList_DropsTheHyperscaleTimers_AndStillCollectsRemoteBlockIo(bool useBundledJson)
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { (short)61, "SQP_STATS_REPORTING ", 240_000L, DBNull.Value, DBNull.Value },
            new object[] { (short)62, "RBIO_COMM_RETRY", 900L, DBNull.Value, DBNull.Value },
            new object[] { (short)63, "REMOTE_BLOCK_IO", 15L, DBNull.Value, "SalesDb" },
            new object[] { (short)64, "EDC_DOPP_LOCK ", 1_000L, DBNull.Value, "SalesDb" });
        var context = CollectorTestContext.Make(
            new RecordingCollectorDeltaCalculator(),
            ignored: useBundledJson ? LoadBundledIgnoreList() : IgnoredWaitDefaults.All);

        var rows = await WaitingTasksCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        Assert.Equal(new[] { "REMOTE_BLOCK_IO", "EDC_DOPP_LOCK" }, rows.Select(r => r.WaitType).ToArray());
    }

    /// <summary>
    /// A running request's wait_type is stored in query_snapshots, and Darling's live Active Queries fetch reads
    /// the same definition, so both surfaces carry the trimmed name.
    /// </summary>
    [Fact]
    public async Task QuerySnapshots_ReadAsync_StoresTheTrimmedWaitType()
    {
        var row = new object[35];
        Array.Fill(row, DBNull.Value);
        row[0] = 55;                                                   /* session_id */
        row[8] = "EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND ";          /* wait_type */
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());

        using var reader = new FakeCollectorDataReader(row);
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        QuerySnapshotsCollector.Instance.WritePayload(Assert.Single(rows), writer, context);
        Assert.Equal("EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND", writer.Values[8]);
    }

    /// <summary>
    /// Lite's Live Snapshot button runs the same query but reads its rows itself, for a grid row that is never
    /// stored. It shows the same trimmed name, as Darling's live fetch does through the collector.
    /// </summary>
    [Fact]
    public void LiveSnapshot_ReadRow_ShowsTheTrimmedWaitType()
    {
        var row = new object[35];
        Array.Fill(row, DBNull.Value);
        row[0] = 55;                                                   /* session_id */
        row[8] = "EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND ";          /* wait_type */

        using var reader = new FakeCollectorDataReader(row);
        Assert.True(reader.Read());

        var live = ServerTab.ReadLiveSnapshotRow(reader, new DateTime(2026, 9, 30, 12, 0, 0));
        Assert.Equal("EXTERNAL_GOVERNANCE_ATTR_SYNC_BACKGROUND", live.WaitType);
    }

    private static HashSet<string> LoadBundledIgnoreList()
    {
        var dir = AppContext.BaseDirectory;
        string? path = null;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, "Lite", "config", "ignored_wait_types.json");
            if (File.Exists(candidate))
            {
                path = candidate;
                break;
            }
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(path);
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        return doc.RootElement.GetProperty("ignored_waits")
            .EnumerateArray()
            .Select(e => e.GetString()!)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
    }
}

/// <summary>
/// The trimmed name is a NEW key for the delta calculator, so after an upgrade the first sample under it has no
/// baseline, and the rule for a key without one is that the sample BECOMES the baseline: delta 0 and interval 0
/// (the pair that means "not knowable", not "idle"). The failure this guards against is the whole cumulative
/// counter landing as one interval's delta.
///
/// <para>Run against a real DuckDB and Lite's real <see cref="DeltaCalculator"/>, seeded the way a restart seeds
/// it: from the last pre-upgrade collection, whose wait_type is stored with the trailing space. A clean-named wait
/// in the same collection is the control: it seeds and produces a real delta on the same pass, so the zero under
/// the trimmed key is the new-key rule and not a seed that failed to load.</para>
/// </summary>
public sealed class WaitNameTrimBaselineLiteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    /// <summary>Distinctive fake id; a real server_id is a storage-name hash, never this.</summary>
    private const int ServerId = -717171;

    private const string SpacedName = "SQP_STATS_REPORTING ";
    private const string TrimmedName = "SQP_STATS_REPORTING";

    /* wait_stats payload positions (WaitStatsCollector.PayloadColumns order). */
    private const int DeltaTasksColumn = 4;
    private const int DeltaTimeColumn = 5;
    private const int DeltaSignalColumn = 6;
    private const int IntervalColumn = 7;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public WaitNameTrimBaselineLiteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task FirstSampleUnderTheTrimmedKey_IsABaseline_NotTheRunningTotal()
    {
        var now = Truncate(DateTime.UtcNow);
        var lastBeforeUpgrade = now.AddSeconds(-120);

        /* A pre-upgrade store: the name exactly as the DMV returned it, plus a clean-named control. */
        await InsertWaitStatsAsync(lastBeforeUpgrade, SpacedName, waitingTasks: 100, waitTimeMs: 30_000_000, signalWaitTimeMs: 0);
        await InsertWaitStatsAsync(lastBeforeUpgrade, "PAGEIOLATCH_SH", waitingTasks: 50, waitTimeMs: 4_000, signalWaitTimeMs: 100);

        var deltas = new DeltaCalculator(NullLogger.Instance);
        await deltas.SeedFromDatabaseAsync(_duckDb);

        /* The first pass after the upgrade. The DMV still returns the spaced name. */
        var pass1 = await RunPassAsync(
            deltas,
            now,
            new object[] { SpacedName, 101L, 30_300_000L, 0L },
            new object[] { "PAGEIOLATCH_SH", 55L, 4_500L, 120L });

        Assert.True(pass1.ContainsKey(TrimmedName), "the row is stored under the trimmed name");
        Assert.False(pass1.ContainsKey(SpacedName), "nothing is stored under the spaced name any more");

        /* A baseline: no delta, and the interval says "not knowable". 30,300,000 must not appear as a delta. */
        var baseline = pass1[TrimmedName];
        Assert.Equal(0L, baseline[DeltaTasksColumn]);
        Assert.Equal(0L, baseline[DeltaTimeColumn]);
        Assert.Equal(0L, baseline[DeltaSignalColumn]);
        Assert.Equal(0, baseline[IntervalColumn]);

        /* The control seeded from the same collection and produced a real delta over the real interval. */
        var control = pass1["PAGEIOLATCH_SH"];
        Assert.Equal(500L, control[DeltaTimeColumn]);
        Assert.Equal(120, control[IntervalColumn]);

        /* The next pass is an ordinary one: a real delta over the real interval, under the trimmed key. */
        var pass2 = await RunPassAsync(
            deltas,
            now.AddSeconds(120),
            new object[] { SpacedName, 102L, 30_420_000L, 0L },
            new object[] { "PAGEIOLATCH_SH", 56L, 4_600L, 125L });

        var second = pass2[TrimmedName];
        Assert.Equal(1L, second[DeltaTasksColumn]);
        Assert.Equal(120_000L, second[DeltaTimeColumn]);
        Assert.Equal(120, second[IntervalColumn]);
    }

    private static async Task<Dictionary<string, object?[]>> RunPassAsync(
        ICollectorDeltaCalculator deltas, DateTime collectionTime, params object[][] dmvRows)
    {
        /* No ignore set: an existing install keeps its own per-user ignored_wait_types.json, which predates the
           new entries, so SQP_STATS_REPORTING is still collected there and its key still changes. */
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "wait-name-trim",
            CollectionTime = collectionTime,
            Deltas = deltas,
        };

        using var reader = new FakeCollectorDataReader(dmvRows);
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

    /* Naive-UTC storage by convention across the product; seconds resolution matches the collectors. */
    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task InsertWaitStatsAsync(
        DateTime collectionTimeUtc, string waitType, long waitingTasks, long waitTimeMs, long signalWaitTimeMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "wait-name-trim" });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitingTasks });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitTimeMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = signalWaitTimeMs });
        await cmd.ExecuteNonQueryAsync();
    }
}
