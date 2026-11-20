/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and takes locks only there, so the
   class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9102: the retention pass's <c>drop_chunks</c> against a reader of the same hypertable, on a real TimescaleDB.
///
/// <para>A read through the hypertable takes AccessShare on its chunks; <c>drop_chunks</c> takes AccessExclusive on each
/// expired chunk in turn and keeps the ones it has until it commits. A reader that holds one expired chunk and asks for
/// another one closes a cycle, and PostgreSQL ends it by cancelling one of the two sessions.</para>
///
/// <para>The reader here is a second connection that locks chunks by name, so each fact decides the order of the lock
/// requests and nothing depends on when a statement happens to run. <c>server_config</c> is the table: three captures,
/// 45 and 40 days old (two chunks past its 30-day horizon) and one a day old.</para>
/// </summary>
public sealed class RetentionDropChunksGivesWayLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Table = "server_config";
    private const int TestServerId = 910200;

    /// <summary>How long a fact waits for a pass that should return in seconds before it calls the pass stuck.</summary>
    private static readonly TimeSpan PassDeadline = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task AReaderHoldsAnExpiredChunk_ThePassGivesWayAndReturns_DeletesNoRow_AndTheNextPassDropsTheChunks()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention give-way live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var timescale = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipWhen(!timescale, "The retention give-way live pin needs TimescaleDB: drop_chunks is the statement under test.");

        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            var (older, newer) = await SeedAsync(connection, ct);

            /* The reader: an open transaction holding AccessShare on the newer expired chunk. */
            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await ExecAsync(holder, "BEGIN", ct);
            await ExecAsync(holder, "LOCK TABLE " + newer + " IN ACCESS SHARE MODE", ct);

            var log = new HookLogger();
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var pass = Task.Run(() => DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, log, ct), ct);
            var returned = await Task.WhenAny(pass, Task.Delay(PassDeadline, ct)) == pass;
            if (!returned)
            {
                /* The pass is waiting on the reader. Let the reader go so the pass can end, then fail below. */
                await ExecAsync(holder, "ROLLBACK", ct);
            }

            await pass;
            Assert.True(returned,
                $"the retention pass was still waiting on the reader's chunk lock after {PassDeadline.TotalSeconds:F0} s; it must give way and return. {log.For(Table)}");

            /* The reader still holds its lock. Nothing was dropped, and no DELETE took the rows instead. */
            Assert.Equal("1,40,45", await AgesAsync(connection, ct));
            Assert.Equal(3L, await ChunkCountAsync(connection, ct));
            Assert.True(await RelationExistsAsync(connection, older, ct), "the older expired chunk was dropped while the reader held the newer one");

            /* Each try that gave way is said at Information. Leaving the table for the next pass is said once at Warning:
               a reader that holds its lock on every pass stops this table's retention, and that must show above Information. */
            Assert.Contains(log.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal) && Names(l, Table));
            var leftForNextPass = log.Problems(Table);
            Assert.True(leftForNextPass.Count == 1 && leftForNextPass[0].StartsWith("Warning:", StringComparison.Ordinal),
                $"the pass must say once, at Warning, that it left the table's expired chunks for the next pass. {log.For(Table)}");

            /* The reader lets go, and the next pass drops both expired chunks and has nothing to warn about. */
            await ExecAsync(holder, "ROLLBACK", ct);
            var nextLog = new HookLogger();
            var next = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, nextLog, ct);

            Assert.Equal("1", await AgesAsync(connection, ct));
            Assert.Equal(1L, await ChunkCountAsync(connection, ct));
            Assert.True(next.ChunksDropped >= 2, $"the next pass dropped {next.ChunksDropped} chunk(s). {nextLog.For(Table)}");
            Assert.Empty(nextLog.Problems(Table));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AReaderAsksForTheChunkThePassAlreadyHolds_NeitherSessionIsADeadlockVictim_AndThePassDropsTheChunksOnceTheReaderIsDone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention lock-cycle live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var timescale = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipWhen(!timescale, "The retention lock-cycle live pin needs TimescaleDB: drop_chunks is the statement under test.");

        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            var (older, newer) = await SeedAsync(connection, ct);

            /* The reader holds the NEWER expired chunk. drop_chunks takes the older one first, then waits for this one. */
            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await ExecAsync(holder, "BEGIN", ct);
            await ExecAsync(holder, "LOCK TABLE " + newer + " IN ACCESS SHARE MODE", ct);

            /* The reader's second request, for the chunk the pass already holds: the request that closes the cycle. */
            await using var secondLock = new NpgsqlCommand("LOCK TABLE " + older + " IN ACCESS SHARE MODE", holder) { CommandTimeout = 120 };
            await using var release = new NpgsqlCommand("ROLLBACK", holder) { CommandTimeout = 120 };

            var gate = new object();
            var released = false;
            Task? secondRequest = null;
            Exception? readerFailure = null;

            /* The reader is done when the pass first says anything about this table's drop_chunks: it finishes the request
               it has in flight, then ends its transaction. Run inside the pass's own log call, so the pass goes on only
               after the reader's locks are gone. */
            void ReleaseReader()
            {
                Task? inFlight;
                lock (gate)
                {
                    if (released)
                    {
                        return;
                    }

                    released = true;
                    inFlight = secondRequest;
                }

                try
                {
                    inFlight?.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    readerFailure = ex;
                }

                release.ExecuteNonQuery();
            }

            var log = new HookLogger(line =>
            {
                if (Names(line, Table))
                {
                    ReleaseReader();
                }
            });

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var pass = Task.Run(() => DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, log, ct), ct);

            /* Wait for the state, not for a time: the pass's session is queued for AccessExclusive on the reader's chunk. */
            var pollDeadline = Task.Delay(PassDeadline, ct);
            var queued = false;
            while (!queued && !pass.IsCompleted && !Volatile.Read(ref released) && !pollDeadline.IsCompleted)
            {
                queued = (bool)(await ScalarAsync(connection,
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'relation' AND relation = '" + newer + "'::regclass "
                    + "AND mode = 'AccessExclusiveLock' AND NOT granted)", ct))!;
                if (!queued)
                {
                    await Task.Delay(10, ct);
                }
            }

            if (queued)
            {
                lock (gate)
                {
                    if (!released)
                    {
                        secondRequest = secondLock.ExecuteNonQueryAsync(ct);
                    }
                }
            }

            var returned = await Task.WhenAny(pass, Task.Delay(PassDeadline, ct)) == pass;
            if (!returned)
            {
                ReleaseReader();
            }

            var summary = await pass;

            /* A pass that never spoke about the table left the reader's transaction open: end it here. */
            ReleaseReader();

            Assert.True(returned, $"the retention pass had not returned {PassDeadline.TotalSeconds:F0} s after the reader's second request. {log.For(Table)}");
            Assert.True(queued || log.Lines.Any(l => Names(l, Table)),
                $"the pass never queued behind the reader's chunk lock, so the fact did not reach its subject. {log.For(Table)}");

            /* Neither side was cancelled: the reader got the chunk it asked for, and the pass logged no failed drop. */
            Assert.True(readerFailure is null, "the reader's lock request failed: " + readerFailure?.Message);
            Assert.Empty(log.Problems(Table));

            /* The pass dropped both expired chunks itself once the reader was done. */
            Assert.Equal("1", await AgesAsync(connection, ct));
            Assert.Equal(1L, await ChunkCountAsync(connection, ct));
            Assert.True(summary.ChunksDropped >= 2, $"the pass dropped {summary.ChunksDropped} chunk(s). {log.For(Table)}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The store as a started service leaves it (every catalog table a hypertable, no background job running), with three
    /// <c>server_config</c> captures: 45 and 40 days old, each alone in a chunk past the 30-day horizon, and one a day old.
    /// Returns the two expired chunks' qualified names, older first.
    /// </summary>
    private static async Task<(string Older, string Newer)> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        Assert.Equal(TimescaleSupport.ConvertedTables.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));

        /* Noon, so a capture sits mid-chunk and the purge's own clock, read a little later, cannot move it across a horizon. */
        var noon = LiveClock.Now().Date.AddHours(12);
        foreach (var days in new[] { 45, 40, 1 })
        {
            await using var insert = new NpgsqlCommand(
                "INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced) "
                + "VALUES ($1, $2, $3, 'retention-give-way', 'max degree of parallelism', 4, 4, true, true)", connection) { CommandTimeout = 120 };
            insert.Parameters.AddWithValue(9102000L + days);
            insert.Parameters.AddWithValue(noon.AddDays(-days));
            insert.Parameters.AddWithValue(TestServerId);
            await insert.ExecuteNonQueryAsync(ct);
        }

        var chunks = new List<string>();
        await using (var command = new NpgsqlCommand(
            "SELECT format('%I.%I', chunk_schema, chunk_name) FROM timescaledb_information.chunks "
            + "WHERE hypertable_schema = 'collect' AND hypertable_name = '" + Table + "' ORDER BY range_start", connection) { CommandTimeout = 120 })
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        Assert.Equal(3, chunks.Count);
        return (chunks[0], chunks[1]);
    }

    /// <summary>The surviving captures' ages in whole days, oldest last: <c>1,40,45</c> before any drop.</summary>
    private static async Task<string?> AgesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT string_agg(d::text, ',' ORDER BY d) FROM (SELECT ($1::date - capture_time::date) AS d FROM server_config WHERE server_id = $2) AS x",
            connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(LiveClock.Now().Date);
        command.Parameters.AddWithValue(TestServerId);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    private static async Task<long> ChunkCountAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (long)(await ScalarAsync(connection,
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = '" + Table + "'", ct))!;

    private static async Task<bool> RelationExistsAsync(NpgsqlConnection connection, string qualifiedName, CancellationToken ct) =>
        (bool)(await ScalarAsync(connection, "SELECT to_regclass('" + qualifiedName + "') IS NOT NULL", ct))!;

    /// <summary>Whether a log line is about this table's <c>drop_chunks</c>: the table's whole name, not a longer name that ends in it.</summary>
    private static bool Names(string line, string table) =>
        line.Contains("drop_chunks", StringComparison.Ordinal)
        && Regex.IsMatch(line, "(?<![A-Za-z0-9_])" + Regex.Escape(table) + "(?![A-Za-z0-9_])", RegexOptions.CultureInvariant);

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>
    /// Keeps every line as <c>Level: message</c> and hands each one to <paramref name="onLine"/> inside the caller's log
    /// call, so a fact can act at the exact point the pass reports something and before the pass goes on.
    /// </summary>
    private sealed class HookLogger(Action<string>? onLine = null) : ILogger
    {
        private readonly List<string> _lines = new();

        public IReadOnlyList<string> Lines
        {
            get
            {
                lock (_lines)
                {
                    return _lines.ToArray();
                }
            }
        }

        /// <summary>The Warning, Error and Critical lines about this table's <c>drop_chunks</c>.</summary>
        public IReadOnlyList<string> Problems(string table) =>
            Lines.Where(l => Names(l, table)
                && (l.StartsWith("Warning:", StringComparison.Ordinal) || l.StartsWith("Error:", StringComparison.Ordinal) || l.StartsWith("Critical:", StringComparison.Ordinal)))
            .ToArray();

        /// <summary>Every line that names the table, for an assertion message.</summary>
        public string For(string table)
        {
            var lines = Lines.Where(l => l.Contains(table, StringComparison.Ordinal)).ToArray();
            return lines.Length == 0 ? "(the pass logged nothing about " + table + ")" : string.Join(" | ", lines);
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var line = string.Create(CultureInfo.InvariantCulture, $"{logLevel}: {formatter(state, exception)}");
            lock (_lines)
            {
                _lines.Add(line);
            }

            onLine?.Invoke(line);
        }
    }
}
