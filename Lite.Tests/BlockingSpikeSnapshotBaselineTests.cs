using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7588: the blocking spike counts its window as blocked process reports, or as DMV snapshot rows when the window holds no
/// report (RDS, Azure). The history it is compared with has to be in the same unit: a snapshot window is judged against
/// snapshot history, a report window against report history. Runs the real detector and baseline provider over DuckDB.
/// </summary>
[Collection(SeparatelyMonitoredProviderCollection.Name)]
public class BlockingSpikeSnapshotBaselineTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -758802;

    /* Rows per collection, four collections an hour. */
    private const int RowsPerCollection = 3;
    private const int RowsPerHour = RowsPerCollection * 4;

    private static readonly DateTime Today = DateTime.UtcNow.Date;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _conn;
    private long _nextId = -758_802_000_000;

    public BlockingSpikeSnapshotBaselineTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _conn?.Dispose();

    private async Task ExecAsync(string sql, params object?[] args)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _conn ??= _duckDb.CreateConnection();
        if (_conn.State != System.Data.ConnectionState.Open) await _conn.OpenAsync();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var arg in args)
            cmd.Parameters.Add(new DuckDBParameter { Value = arg ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Rows of one kind, <paramref name="perCollection"/> per quarter hour over [from, to), plus one SUCCESS run of that
    /// kind's collector per quarter hour in the collection log.</summary>
    private async Task SeedAsync(bool reports, DateTime from, DateTime to, int perCollection)
    {
        _nextId -= 1_000_000;
        if (reports)
            await ExecAsync(
                @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, database_name, wait_time_ms, blocked_spid, blocking_spid)
                  SELECT $1 - CAST(row_number() OVER () AS BIGINT), t, t, $2, 'TestServer', 'appdb', 5000, 100 + j, 60
                  FROM generate_series($3::TIMESTAMP, $4::TIMESTAMP - INTERVAL 1 SECOND, INTERVAL 15 MINUTE) AS g(t)
                  CROSS JOIN generate_series(1, $5) AS k(j)",
                _nextId, ServerId, from, to, perCollection);
        else
            await ExecAsync(
                @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, database_name, monitor_loop, blocked_spid, blocking_spid, wait_time_ms)
                  SELECT $1 - CAST(row_number() OVER () AS BIGINT), t, $2, 'TestServer', 'appdb', 1, 100 + j, 60, 5000
                  FROM generate_series($3::TIMESTAMP, $4::TIMESTAMP - INTERVAL 1 SECOND, INTERVAL 15 MINUTE) AS g(t)
                  CROSS JOIN generate_series(1, $5) AS k(j)",
                _nextId, ServerId, from, to, perCollection);
        _nextId -= 1_000_000;
        await ExecAsync(
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected, error_message)
              SELECT $1 - CAST(row_number() OVER () AS BIGINT), $2, 'TestServer', $3, t, 'SUCCESS', 1, NULL
              FROM generate_series($4::TIMESTAMP, $5::TIMESTAMP - INTERVAL 1 SECOND, INTERVAL 15 MINUTE) AS g(t)",
            _nextId, ServerId, reports ? "blocked_process_report" : "dmv_blocking_snapshot", from, to);
    }

    /// <summary>Self-wait snapshot rows (a session that is its own blocker: blocked_spid = blocking_spid), <paramref name="perCollection"/>
    /// per quarter hour over [from, to). No collection log runs: the real rows seeded beside them already cover the hours.</summary>
    private async Task SeedSelfWaitSnapshotsAsync(DateTime from, DateTime to, int perCollection)
    {
        _nextId -= 1_000_000;
        await ExecAsync(
            @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, database_name, monitor_loop, blocked_spid, blocking_spid, wait_time_ms)
              SELECT $1 - CAST(row_number() OVER () AS BIGINT), t, $2, 'TestServer', 'appdb', 1, 200 + j, 200 + j, 5000
              FROM generate_series($3::TIMESTAMP, $4::TIMESTAMP - INTERVAL 1 SECOND, INTERVAL 15 MINUTE) AS g(t)
              CROSS JOIN generate_series(1, $5) AS k(j)",
            _nextId, ServerId, from, to, perCollection);
    }

    private static DateTime WindowStart => Today.AddDays(-1).AddHours(12);

    private Task SeedWindowAsync(bool reports, int perCollection) => SeedAsync(reports, WindowStart, WindowStart.AddHours(4), perCollection);

    private Task SeedHistoryAsync(bool reports, int days, int perCollection) => SeedAsync(reports, Today.AddDays(-1 - days), Today.AddDays(-1), perCollection);

    /// <summary>Runs the real detector over a four hour window yesterday afternoon. History is the days before it.</summary>
    private async Task<Fact?> SpikeAsync()
    {
        /* The detector skips every family on a server with no CPU rows in the days before the window. */
        await ExecAsync(
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time,
                sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, 'TestServer', $2, 10, 2)",
            _nextId--, Today.AddDays(-3), ServerId);
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = "TestServer",
            TimeRangeStart = WindowStart,
            TimeRangeEnd = WindowStart.AddHours(4),
        };
        var anomalies = await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(context);
        return anomalies.SingleOrDefault(a => a.Key == "ANOMALY_BLOCKING_SPIKE");
    }

    /* A snapshot-only server whose window runs at its usual snapshot rate is not a spike. Before #7588 the window's snapshot rows
       were divided by an empty report history, which is untrusted, so the count alone fired. */
    [Fact]
    public async Task SnapshotWindow_AtItsOwnUsualRate_IsNotASpike()
    {
        await SeedHistoryAsync(reports: false, days: 12, RowsPerCollection);
        await SeedWindowAsync(reports: false, RowsPerCollection);

        Assert.Null(await SpikeAsync());
    }

    /* The same history with a window at five times the rate: a real spike, judged as a ratio of snapshot rows to snapshot rows,
       and stamped as the snapshot arm. */
    [Fact]
    public async Task SnapshotWindow_AtFiveTimesItsHistory_SpikesAsAFiveTimesRatio()
    {
        await SeedHistoryAsync(reports: false, days: 12, RowsPerCollection);
        await SeedWindowAsync(reports: false, RowsPerCollection * 5);

        var spike = await SpikeAsync();
        Assert.NotNull(spike);
        Assert.Equal(1.0, spike!.Metadata[CountFamilyMetadata.FromDmvSnapshotsKey]);
        Assert.Equal(0.0, spike.Metadata.GetValueOrDefault("is_new"));
        Assert.InRange(spike.Metadata["ratio"], 4.5, 5.5);
        Assert.Equal(RowsPerHour, spike.Metadata["baseline_rate"], 1.0);
    }

    /* A server with blocked process reports keeps its report ratio exactly, whatever the snapshot history holds: here a snapshot
       history a hundred times the report history sits beside it. */
    [Fact]
    public async Task ReportWindow_KeepsItsReportRatio_WhateverTheSnapshotHistoryHolds()
    {
        await SeedHistoryAsync(reports: true, days: 12, RowsPerCollection);
        await SeedHistoryAsync(reports: false, days: 12, RowsPerCollection * 100);
        await SeedWindowAsync(reports: true, RowsPerCollection * 5);

        var spike = await SpikeAsync();
        Assert.NotNull(spike);
        Assert.False(spike!.Metadata.ContainsKey(CountFamilyMetadata.FromDmvSnapshotsKey));
        Assert.InRange(spike.Metadata["ratio"], 4.5, 5.5);
        Assert.Equal(RowsPerHour, spike.Metadata["baseline_rate"], 1.0);
    }

    /* A snapshot window with only a day of snapshot history has no trustworthy baseline: the finding is a first occurrence on the
       count and claims no ratio, rather than a ratio against a thin history. */
    [Fact]
    public async Task SnapshotWindow_WithAShortSnapshotHistory_ClaimsNoRatio()
    {
        await SeedHistoryAsync(reports: false, days: 1, RowsPerCollection);
        await SeedWindowAsync(reports: false, RowsPerCollection * 5);

        var spike = await SpikeAsync();
        Assert.NotNull(spike);
        Assert.Equal(1.0, spike!.Metadata[CountFamilyMetadata.FromDmvSnapshotsKey]);
        Assert.Equal(1.0, spike.Metadata["is_new"]);
    }

    /* #7588 review: the snapshot history leaves out self-wait rows, the same filter the window count uses (BlockingWaits.NotSelfWaitSql).
       Here fifty self-wait rows per collection sit in the history beside the real ones. Counted, they would make the usual rate
       about fifty times higher and hide a real five times window; left out, the baseline stays at the real rate. */
    [Fact]
    public async Task SnapshotHistory_LeavesOutSelfWaitRows()
    {
        await SeedHistoryAsync(reports: false, days: 12, RowsPerCollection);
        await SeedSelfWaitSnapshotsAsync(Today.AddDays(-13), Today.AddDays(-1), 50);
        await SeedWindowAsync(reports: false, RowsPerCollection * 5);

        var spike = await SpikeAsync();
        Assert.NotNull(spike);
        Assert.Equal(RowsPerHour, spike!.Metadata["baseline_rate"], 1.0);
        Assert.InRange(spike.Metadata["ratio"], 4.5, 5.5);
    }
}
