/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3999: Lite's trace-flag read answers for the collector's newest SUCCESSFUL run, the twin of Darling's
/// <c>TraceFlags_AnswerForTheNewestSuccessfulRun_AgainstDevPostgres</c>. A capture that finds every flag off
/// writes ZERO rows (<c>DBCC TRACESTATUS(-1)</c> lists only flags that are on), so the newest trace_flags row
/// can outlive the state it describes. The read decides on the newest SUCCESS's <c>rows_collected</c>, never on
/// timestamps. Lite stamps collection_log when a run STARTS and Darling when it ENDS, so no timestamp
/// comparison means the same thing on both; seeded here in Lite's order, log stamp first.
/// </summary>
public sealed class TraceFlagsLatestRunTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 3999;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public TraceFlagsLatestRunTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task TheReadAnswersForTheNewestSuccessfulRun()
    {
        var service = new LocalDataService(_duckDb);
        var day1 = new DateTime(2026, 9, 20, 6, 0, 0, DateTimeKind.Utc);

        async Task<int[]> FlagsAsync() =>
            (await service.GetLatestTraceFlagsAsync(ServerId)).Select(r => r.TraceFlag).ToArray();

        /* 1. An ordinary run: logged at its start, capture 1 s later with 1117 on. */
        await SeedRunAsync(day1, "SUCCESS", rowsCollected: 1);
        await SeedTraceFlagAsync(day1.AddSeconds(1), 1117);
        Assert.Equal(new[] { 1117 }, await FlagsAsync());

        /* 2. The next day every flag is off: a SUCCESS that wrote nothing. The stale 1117 row must not read. */
        await SeedRunAsync(day1.AddDays(1), "SUCCESS", rowsCollected: 0);
        Assert.Empty(await FlagsAsync());

        /* 3. A failed run after that proves nothing about the flags, so the answer stays "none". */
        await SeedRunAsync(day1.AddDays(1).AddHours(1), "ERROR", rowsCollected: 0);
        Assert.Empty(await FlagsAsync());

        /* 4. The day after, 4199 is on: a new run with its capture reads again, and only 4199. */
        await SeedRunAsync(day1.AddDays(2), "SUCCESS", rowsCollected: 1);
        await SeedTraceFlagAsync(day1.AddDays(2).AddSeconds(1), 4199);
        Assert.Equal(new[] { 4199 }, await FlagsAsync());
    }

    [Fact]
    public async Task NoRunRecord_KeepsTheNewestCapture()
    {
        /* Nothing in collection_log (retention aged it out, or nothing has run through the log yet): the read
           keeps the newest capture rather than suppressing a row it cannot corroborate. */
        var service = new LocalDataService(_duckDb);
        await SeedTraceFlagAsync(new DateTime(2026, 9, 20, 6, 0, 1, DateTimeKind.Utc), 1117);

        Assert.Equal(new[] { 1117 }, (await service.GetLatestTraceFlagsAsync(ServerId)).Select(r => r.TraceFlag).ToArray());
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedRunAsync(DateTime startedUtc, string status, int rowsCollected)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1, $2, $3, 'trace_flags', $4, 900, $5, NULL, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = startedUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = rowsCollected });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedTraceFlagAsync(DateTime captureTimeUtc, int traceFlag)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, true, true, false)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = captureTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = traceFlag });
        await cmd.ExecuteNonQueryAsync();
    }
}
