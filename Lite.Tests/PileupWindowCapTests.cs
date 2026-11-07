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
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8157, the tripwire in Lite's reader: the pileup window read keeps a row cap against a runaway session count, and
/// when the cap fills before the read reaches the baseline floor the finding would state, the sweep evaluates nothing
/// and says so in the log once, instead of handing the detector a baseline shorter than the one its finding describes.
/// The floor is strict: a cap that fills with its oldest row exactly on the floor instant has cut a snapshot the
/// detector's baseline keeps, so that read is skipped too. A cap that fills past the floor changes nothing.
///
/// <para>The cap is lowered through the reader's seam so the seed stays small: 56 one-minute snapshots of 120 user
/// sessions on one statement, 6,720 rows, at a fixed instant. A cap of 3,000 rows is 25 snapshots, 20 minutes short
/// of the 45-minute floor; 5,520 is 46 snapshots, the oldest exactly on it; 6,000 is 50, four minutes past it.</para>
/// </summary>
public sealed class PileupWindowCapTests : IClassFixture<SharedDuckDbFixture>
{
    private const string ServerName = "pileup-window-cap";
    private const int ServerId = 8157;
    private const int Snapshots = 56;
    private const int SessionsPerSnapshot = 120;

    /* The newest snapshot; the floor the sweep passes is the lookback plus the staleness margin behind it. */
    private static readonly DateTime Latest = new(2026, 9, 15, 20, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Floor = Latest.AddMinutes(
        -(SameStatementPileupDetector.BaselineLookbackMinutes + SameStatementPileupDetector.StaleSnapshotCutoffMinutes));

    private readonly DuckDbInitializer _duckDb;

    public PileupWindowCapTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ACapThatFillsShortOfTheBaselineFloor_SkipsTheSweepWithOneWarning()
    {
        await SeedAsync();

        /* 3,000 rows is 25 snapshots: the cap fills 20 minutes short of the 45-minute floor. */
        var log = new CapturingLogger();
        var rows = await new PileupSnapshotReader(_duckDb, log, rowCap: 3_000).ReadWindowAsync(ServerId, Floor, CancellationToken.None);

        Assert.Empty(rows);
        var warning = Assert.Single(log.Lines, line => line.Level == LogLevel.Warning);
        Assert.Contains("3000", warning.Message, StringComparison.Ordinal);
        Assert.Contains("skipped", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACapThatFillsWithTheOldestRowExactlyAtTheFloor_SkipsTheSweep()
    {
        await SeedAsync();

        /* 5,520 rows is 46 snapshots: the newest and the 45 behind it, so the cap fills with its oldest row on the floor
           instant itself. The detector's baseline keeps that instant, so the read has to reach past it to stand. */
        var log = new CapturingLogger();
        var cap = (SameStatementPileupDetector.BaselineLookbackMinutes + 1) * SessionsPerSnapshot;
        var rows = await new PileupSnapshotReader(_duckDb, log, rowCap: cap).ReadWindowAsync(ServerId, Floor, CancellationToken.None);

        Assert.Empty(rows);
        Assert.Single(log.Lines, line => line.Level == LogLevel.Warning);
    }

    [Fact]
    public async Task ACapThatFillsPastTheFloor_HandsTheRowsOn_AndTheDefaultCapIsAboveThisSeed()
    {
        await SeedAsync();

        /* 6,000 rows is 50 snapshots: the cap fills with the floor already four minutes behind, so the rows stand. */
        var log = new CapturingLogger();
        var rows = await new PileupSnapshotReader(_duckDb, log, rowCap: 6_000).ReadWindowAsync(ServerId, Floor, CancellationToken.None);

        Assert.Equal(6_000, rows.Count);
        var newest = rows.Max(r => r.CollectionTime);
        Assert.True(rows.Min(r => r.CollectionTime) < newest.AddMinutes(-SameStatementPileupDetector.BaselineLookbackMinutes));
        Assert.DoesNotContain(log.Lines, line => line.Level == LogLevel.Warning);

        /* The default cap is a tripwire, not a working bound: every seeded row comes back. */
        var all = await new PileupSnapshotReader(_duckDb, log).ReadWindowAsync(ServerId, Floor, CancellationToken.None);
        Assert.Equal(Snapshots * SessionsPerSnapshot, all.Count);
        Assert.DoesNotContain(log.Lines, line => line.Level == LogLevel.Warning);
    }

    private async Task SeedAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
INSERT INTO query_snapshots
    (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status,
     blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, logical_reads, reads, program_name, request_id, query_hash)
SELECT -1 - (m.range * {SessionsPerSnapshot} + s.range), $1::TIMESTAMP - to_minutes(m.range::INTEGER), $2, $3, 100 + s.range, 'AppDb',
       'SELECT payload FROM orders WHERE id = 1', 'running', 0, NULL, 0, 10, 100, 20, 0, 'AppServer', 0, '0xABC'
FROM range({Snapshots}) AS m CROSS JOIN range({SessionsPerSnapshot}) AS s";
        cmd.Parameters.Add(new DuckDBParameter { Value = Latest });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        await cmd.ExecuteNonQueryAsync();
    }

    private sealed class CapturingLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Message)> _lines = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Lines
        {
            get
            {
                lock (_lines)
                {
                    return _lines.ToList();
                }
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Add((logLevel, formatter(state, exception)));
            }
        }
    }
}
