/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5820: the DuckDB behavior the Lite investigation store (L1b) is built on, proven on the real investigation tables
/// in a throwaway database before any store statement is written. Each test names the store statement that depends on
/// it. In the timing collection because the lock test holds the process-wide write lock for the store's five-second
/// budget, and that collection runs alone after the parallel classes, so the hold blocks no neighbor.
/// </summary>
[Collection("timing")]
public sealed class InvestigationDuckDbSpikeTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public InvestigationDuckDbSpikeTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "investigations.duckdb");

        using var connection = Open();
        foreach (var statement in Schema.InvestigationTableStatements())
        {
            Execute(connection, statement);
        }
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch (IOException)
        {
            /* A file still mapped by DuckDB on a slow release; the temp folder is the OS's to clean. */
        }
    }

    [Fact]
    public void JournalOnceKey_ManyNullKeysInsert_AndARepeatedKeyIsSkipped()
    {
        // L1b's journal INSERT (the twin of Darling's JournalSql) writes ON CONFLICT (once_key) DO NOTHING.
        using var connection = Open();

        for (var id = 1; id <= 5; id++)
        {
            Assert.Equal(1, InsertJournal(connection, id, onceKey: null));
        }

        Assert.Equal(1, InsertJournal(connection, 6, "limit_reached:7:2026-10-15T10"));
        Assert.Equal(0, InsertJournal(connection, 7, "limit_reached:7:2026-10-15T10"));
        Assert.Equal(1, InsertJournal(connection, 8, "limit_reached:7:2026-10-15T11"));

        Assert.Equal(5L, Scalar(connection, "SELECT count(*) FROM investigation_journal WHERE once_key IS NULL"));
        Assert.Equal(1L, Scalar(connection, "SELECT count(*) FROM investigation_journal WHERE once_key = 'limit_reached:7:2026-10-15T10'"));
        Assert.Equal(0L, Scalar(connection, "SELECT count(*) FROM investigation_journal WHERE journal_id = 7"));
    }

    [Fact]
    public void JournalOnceKey_TwoWriteLockHoldersOnTheirOwnConnections_RecordTheKeyOnce()
    {
        // L1b's limit and refusal journal rows: two alert paths, each with its own connection, under the write lock.
        using var initializer = new DuckDbInitializer(_dbPath);
        var results = new int[2];
        var errors = new Exception?[2];
        var start = new ManualResetEventSlim();

        var threads = Enumerable.Range(0, 2).Select(i => new Thread(() =>
        {
            try
            {
                start.Wait();
                using var writeLock = initializer.AcquireWriteLock(LocalDataService.DefaultWriteLockBudget);
                using var connection = Open();
                var nextId = Scalar(connection, "SELECT COALESCE(MAX(journal_id), 0) + 1 FROM investigation_journal");
                results[i] = InsertJournal(connection, nextId, "refused:request:lease_held");
            }
            catch (Exception ex)
            {
                errors[i] = ex;
            }
        }) { IsBackground = true, Name = $"journal writer {i}" }).ToList();

        threads.ForEach(t => t.Start());
        start.Set();
        threads.ForEach(t => Assert.True(t.Join(TimeSpan.FromSeconds(30)), "a journal writer never finished"));

        Assert.All(errors, e => Assert.Null(e));
        Assert.Equal(1, results.Sum());
        using var check = Open();
        Assert.Equal(1L, Scalar(check, "SELECT count(*) FROM investigation_journal WHERE once_key = 'refused:request:lease_held'"));
    }

    [Fact]
    public void RequestOrder_ByOpenedAtDescThenRequestIdText_IsStableAndMatchesDarlingsTieRule()
    {
        // L1b's list and reports reads: ORDER BY opened_at DESC, request_id::VARCHAR (Darling's tie rule).
        using var connection = Open();
        var openedAt = new DateTime(2026, 10, 15, 9, 30, 0, DateTimeKind.Utc);
        var tied = new[]
        {
            Guid.Parse("f0000000-0000-0000-0000-000000000001"),
            Guid.Parse("00000000-0000-0000-0000-00000000000a"),
            Guid.Parse("8fffffff-ffff-ffff-ffff-ffffffffffff"),
            Guid.Parse("7fffffff-ffff-ffff-ffff-ffffffffffff"),
            Guid.Parse("a0000000-0000-0000-0000-000000000000"),
            Guid.Parse("00000000-0000-0000-0000-000000000009"),
        };
        var newer = Guid.Parse("ffffffff-ffff-ffff-ffff-fffffffffffe");

        foreach (var id in tied)
        {
            InsertRequest(connection, id, openedAt);
        }

        InsertRequest(connection, newer, openedAt.AddMinutes(1));

        /* PostgreSQL's uuid::text is the lowercase 36-character form, which is Guid.ToString("D"); its hex digits and
           fixed hyphens sort the same under any collation, so ordinal order of that text is Darling's order. */
        var expected = new[] { newer }
            .Concat(tied.OrderBy(g => g.ToString("D"), StringComparer.Ordinal))
            .ToList();

        const string sql = "SELECT request_id FROM investigation_requests ORDER BY opened_at DESC, request_id::VARCHAR";
        var first = ReadGuids(connection, sql);
        var second = ReadGuids(connection, sql);

        Assert.Equal(expected, first);
        Assert.Equal(first, second);

        var texts = ReadStrings(connection, "SELECT request_id::VARCHAR FROM investigation_requests");
        Assert.All(texts, t => Assert.Contains(t, tied.Append(newer).Select(g => g.ToString("D"))));
    }

    [Fact]
    public void RequestId_AGuidBoundIntoTheUuidColumn_ReadsBackUnchanged()
    {
        // L1b's every request_id and claim_id bind and read (OpenOrJoin, Claim, Submit, the list and reports reads).
        using var connection = Open();
        var ids = new[]
        {
            Guid.NewGuid(),
            Guid.Parse("00000000-0000-0000-0000-000000000001"),
            Guid.Parse("7fffffff-ffff-ffff-ffff-ffffffffffff"),
            Guid.Parse("80000000-0000-0000-0000-000000000000"),
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        };
        var openedAt = new DateTime(2026, 10, 15, 9, 0, 0, DateTimeKind.Utc);

        foreach (var id in ids)
        {
            InsertRequest(connection, id, openedAt, claimId: id);
        }

        foreach (var id in ids)
        {
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT request_id, claim_id FROM investigation_requests WHERE request_id = $1";
            command.Parameters.Add(new DuckDBParameter { Value = id });
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read(), $"no row found by the bound Guid {id}");
            Assert.Equal(id, reader.GetGuid(0));
            Assert.Equal(id, reader.GetGuid(1));
            Assert.False(reader.Read());
        }
    }

    [Fact]
    public void Timestamp_MicrosecondValue_RoundTrips_AndATickRemainderIsTruncated()
    {
        // L1b's bound times (alert time, window_until, lease_until, expires_at): truncated to microseconds, read with SpecifyKind(Utc).
        using var connection = Open();
        var micro = new DateTime(2026, 10, 15, 9, 41, 7, DateTimeKind.Utc).AddTicks(1_234_560);
        var withTicks = micro.AddTicks(7);
        var id = Guid.NewGuid();
        var other = Guid.NewGuid();

        InsertRequest(connection, id, micro);
        InsertRequest(connection, other, withTicks);

        var readBack = ReadOpenedAt(connection, id);
        Assert.Equal(micro, DateTime.SpecifyKind(readBack, DateTimeKind.Utc));
        Assert.Equal(DateTimeKind.Utc, DateTime.SpecifyKind(readBack, DateTimeKind.Utc).Kind);

        /* DuckDB keeps microseconds: the 700 ns remainder is dropped, so a store comparing a bound time with a read one
           truncates first. */
        Assert.Equal(micro, DateTime.SpecifyKind(ReadOpenedAt(connection, other), DateTimeKind.Utc));
    }

    [Fact]
    public void WriteLock_WithTheFiveSecondBudget_GivesUpWithTheLockTimeout_InsteadOfWaiting()
    {
        // L1b's every write: DuckDbInitializer.AcquireWriteLock(LocalDataService.DefaultWriteLockBudget), never the no-timeout form.
        using var initializer = new DuckDbInitializer(_dbPath);
        var budget = LocalDataService.DefaultWriteLockBudget;
        var acquired = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        var holder = new Thread(() =>
        {
            using (initializer.AcquireWriteLock(TimeSpan.FromSeconds(30)))
            {
                acquired.Set();
                release.Wait(budget * 4);
            }
        }) { IsBackground = true, Name = "InvestigationDuckDbSpikeTests holder" };
        holder.Start();

        try
        {
            Assert.True(acquired.Wait(TimeSpan.FromSeconds(10)), "the holder never got the write lock");

            var waited = Stopwatch.StartNew();
            var ex = OwnThread.Run<Exception?>(() =>
            {
                try
                {
                    using (initializer.AcquireWriteLock(budget))
                    {
                    }

                    return null;
                }
                catch (Exception caught)
                {
                    return caught;
                }
            });
            waited.Stop();

            Assert.IsType<StoreLockTimeoutException>(ex);
            Assert.True(waited.Elapsed >= budget - TimeSpan.FromMilliseconds(50),
                $"the caller gave up after {waited.Elapsed.TotalMilliseconds:F0} ms, before the budget");
            Assert.True(waited.Elapsed < budget * 3,
                $"the caller waited {waited.Elapsed.TotalMilliseconds:F0} ms, past the budget, for a holder that never let go");
        }
        finally
        {
            release.Set();
            Assert.True(holder.Join(TimeSpan.FromSeconds(30)), "the holder never released the write lock");
        }
    }

    private DuckDBConnection Open()
    {
        var connection = new DuckDBConnection($"Data Source={_dbPath}");
        connection.Open();
        return connection;
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static long Scalar(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(command.ExecuteScalar());
    }

    private static int InsertJournal(DuckDBConnection connection, long journalId, string? onceKey)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO investigation_journal (journal_id, event_time, request_id, route_id, server_id, event, actor, reason, detail, once_key)
VALUES ($1, $2, NULL, 7, 42, 'limit_reached', 'system', NULL, NULL, $3)
ON CONFLICT (once_key) DO NOTHING";
        command.Parameters.Add(new DuckDBParameter { Value = journalId });
        command.Parameters.Add(new DuckDBParameter { Value = new DateTime(2026, 10, 15, 10, 0, 0) });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)onceKey ?? DBNull.Value });
        return command.ExecuteNonQuery();
    }

    private static void InsertRequest(DuckDBConnection connection, Guid requestId, DateTime openedAt, Guid? claimId = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO investigation_requests
    (request_id, route_id, server_id, server_name, family, first_metric, first_alert_at, last_alert_at, window_until,
     opened_at, expires_at, claim_id)
VALUES ($1, 7, 42, 'srv', 'blocking', 'Blocking', $2, $2, $2 + INTERVAL 30 MINUTE, $2, $2 + INTERVAL 240 MINUTE, $3)";
        command.Parameters.Add(new DuckDBParameter { Value = requestId });
        command.Parameters.Add(new DuckDBParameter { Value = openedAt });
        command.Parameters.Add(new DuckDBParameter { Value = claimId.HasValue ? claimId.Value : DBNull.Value });
        command.ExecuteNonQuery();
    }

    private static DateTime ReadOpenedAt(DuckDBConnection connection, Guid requestId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT opened_at FROM investigation_requests WHERE request_id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = requestId });
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        return reader.GetDateTime(0);
    }

    private static List<Guid> ReadGuids(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var rows = new List<Guid>();
        while (reader.Read())
        {
            rows.Add(reader.GetGuid(0));
        }

        return rows;
    }

    private static List<string> ReadStrings(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var rows = new List<string>();
        while (reader.Read())
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }
}
