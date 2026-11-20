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
/// requests and nothing depends on when a statement happens to run. Each fact puts three rows in its table: two past the
/// table's horizon, each alone in a chunk, and one a day old. <c>server_config</c> stands for the collector catalog's tables;
/// <c>collection_log</c> is purged by its own call, outside that catalog.</para>
/// </summary>
public sealed class RetentionDropChunksGivesWayLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string Table = "server_config";
    private const int TestServerId = 910200;

    /// <summary>How long a fact waits for a pass that should return in seconds before it calls the pass stuck.</summary>
    private static readonly TimeSpan PassDeadline = TimeSpan.FromSeconds(60);

    /// <summary>A hypertable the pass purges, and the rows a fact puts in it: two past its horizon and one a day old.</summary>
    private sealed record Subject(string Table, string TimeColumn, int OlderDays, int NewerDays, string InsertSql)
    {
        /// <summary>The three rows' ages in whole days as <see cref="AgesAsync"/> reads them back, before any drop.</summary>
        public string AllAges => string.Create(CultureInfo.InvariantCulture, $"1,{NewerDays},{OlderDays}");
    }

    /// <summary>45 and 40 days old against <c>server_config</c>'s 30-day horizon.</summary>
    private static readonly Subject s_serverConfig = new(Table, "capture_time", 45, 40,
        "INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced) "
        + "VALUES ($1, $2, $3, 'retention-give-way', 'max degree of parallelism', 4, 4, true, true)");

    /// <summary>30 and 10 days past <c>collection_log</c>'s own horizon.</summary>
    private static readonly Subject s_collectionLog = new("collection_log", "collection_time",
        DarlingRetention.CollectionLogRetentionDays + 30, DarlingRetention.CollectionLogRetentionDays + 10,
        "INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status) "
        + "VALUES ($1, $2, $3, 'retention-give-way', 'retention_give_way', 'SUCCESS')");

    [Fact]
    public Task AReaderHoldsAnExpiredChunk_ThePassGivesWayAndReturns_DeletesNoRow_AndTheNextPassDropsTheChunks() =>
        AReaderHoldsAnExpiredChunkForTheWholePassAsync(s_serverConfig);

    [Fact]
    public Task AReaderHoldsAnExpiredCollectionLogChunk_ThePassGivesWayAndReturns_DeletesNoRow_AndTheNextPassDropsTheChunks() =>
        AReaderHoldsAnExpiredChunkForTheWholePassAsync(s_collectionLog);

    private static async Task AReaderHoldsAnExpiredChunkForTheWholePassAsync(Subject subject)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the retention give-way live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var timescale = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        Assert.SkipWhen(!timescale, "The retention give-way live pin needs TimescaleDB: drop_chunks is the statement under test.");

        var table = subject.Table;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            var (anchor, older, newer) = await SeedAsync(connection, subject, ct);

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
                $"the retention pass was still waiting on the reader's chunk lock after {PassDeadline.TotalSeconds:F0} s; it must give way and return. {log.For(table)}");

            /* The reader still holds its lock. Nothing was dropped, and no DELETE took the rows instead. */
            Assert.Equal(subject.AllAges, await AgesAsync(connection, subject, anchor, ct));
            Assert.True(await RelationExistsAsync(connection, older, ct), "the older expired chunk was dropped while the reader held the newer one");
            Assert.True(await RelationExistsAsync(connection, newer, ct), "the chunk the reader holds was dropped");

            /* Each try that gave way is said at Information. Leaving the table for the next pass is said once at Warning:
               a reader that holds its lock on every pass stops this table's retention, and that must show above Information. */
            Assert.Contains(log.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal) && Names(l, table));
            var leftForNextPass = log.Problems(table);
            Assert.True(leftForNextPass.Count == 1 && leftForNextPass[0].StartsWith("Warning:", StringComparison.Ordinal),
                $"the pass must say once, at Warning, that it left the table's expired chunks for the next pass. {log.For(table)}");

            /* The reader lets go, and the next pass drops both expired chunks and has nothing to warn about. */
            await ExecAsync(holder, "ROLLBACK", ct);
            var nextLog = new HookLogger();
            var next = await DarlingRetention.PurgeAsync(postgres, timescaleAvailable: true, nextLog, ct);

            Assert.Equal("1", await AgesAsync(connection, subject, anchor, ct));
            Assert.False(await RelationExistsAsync(connection, older, ct), "the next pass left the older expired chunk");
            Assert.False(await RelationExistsAsync(connection, newer, ct), "the next pass left the newer expired chunk");
            Assert.True(next.ChunksDropped >= 2, $"the next pass dropped {next.ChunksDropped} chunk(s). {nextLog.For(table)}");
            Assert.Empty(nextLog.Problems(table));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// A lock cycle between the pass and a reader needs the pass to hold one expired chunk while it waits for another: the
    /// reader then asks for the held one, and whichever of the two began waiting first is cancelled once it has waited
    /// <c>deadlock_timeout</c>. Which one began first is a matter of timing, so this fact watches for the state itself.
    /// </summary>
    [Fact]
    public async Task AReaderHoldsAnExpiredChunk_ThePassNeverWaitsForOneChunkLockWhileItHoldsAnother_AndDropsTheChunksOnceTheReaderIsDone()
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
            var (anchor, older, newer) = await SeedAsync(connection, s_serverConfig, ct);

            /* The reader holds the NEWER expired chunk; drop_chunks reaches the older one first. */
            await using var holder = new NpgsqlConnection(scratch.ConnectionString);
            await holder.OpenAsync(ct);
            await ExecAsync(holder, "BEGIN", ct);
            await ExecAsync(holder, "LOCK TABLE " + newer + " IN ACCESS SHARE MODE", ct);
            await using var release = new NpgsqlCommand("ROLLBACK", holder) { CommandTimeout = 120 };

            var gate = new object();
            var released = false;

            /* The reader is done when the pass first says anything about this table's drop_chunks. Run inside the pass's own
               log call, so the pass goes on only after the reader's lock is gone. */
            void ReleaseReader()
            {
                lock (gate)
                {
                    if (!released)
                    {
                        released = true;
                        release.ExecuteNonQuery();
                    }
                }
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

            /* Read the lock table until the pass is over: is any session waiting for the newer chunk while it holds
               AccessExclusive on the older one? to_regclass, because both chunks are gone once the pass has dropped them. */
            var deadline = Task.Delay(PassDeadline, ct);
            var heldOneWhileWaitingForAnother = false;
            while (!heldOneWhileWaitingForAnother && !pass.IsCompleted && !deadline.IsCompleted)
            {
                heldOneWhileWaitingForAnother = (bool)(await ScalarAsync(connection,
                    "SELECT EXISTS (SELECT 1 FROM pg_locks AS waiting JOIN pg_locks AS held ON held.pid = waiting.pid "
                    + "WHERE waiting.locktype = 'relation' AND NOT waiting.granted AND waiting.relation = to_regclass('" + newer + "') "
                    + "AND held.locktype = 'relation' AND held.granted AND held.mode = 'AccessExclusiveLock' AND held.relation = to_regclass('" + older + "'))", ct))!;
                if (!heldOneWhileWaitingForAnother)
                {
                    await Task.Delay(10, ct);
                }
            }

            var returned = await Task.WhenAny(pass, deadline) == pass;
            if (!returned)
            {
                ReleaseReader();
            }

            var summary = await pass;

            /* A pass that never spoke about the table left the reader's transaction open: end it here. */
            ReleaseReader();

            Assert.True(returned, $"the retention pass had not returned after {PassDeadline.TotalSeconds:F0} s. {log.For(Table)}");
            Assert.False(heldOneWhileWaitingForAnother,
                "the pass waited for the reader's chunk while it held AccessExclusive on the older one: a reader that asks for the older chunk now closes a lock cycle. "
                + log.For(Table));

            /* The pass met the reader's lock, said so without a warning, and dropped both chunks itself once the reader was done. */
            Assert.Contains(log.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal) && Names(l, Table));
            Assert.Empty(log.Problems(Table));
            Assert.Equal("1", await AgesAsync(connection, s_serverConfig, anchor, ct));
            Assert.False(await RelationExistsAsync(connection, older, ct), "the pass left the older expired chunk");
            Assert.False(await RelationExistsAsync(connection, newer, ct), "the pass left the newer expired chunk");
            Assert.True(summary.ChunksDropped >= 2, $"the pass dropped {summary.ChunksDropped} chunk(s). {log.For(Table)}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The store as a started service leaves it (every catalog table a hypertable, no background job running), with the
    /// subject's three rows in it. Returns the day the rows were placed from and the two expired chunks' qualified names,
    /// older first.
    /// </summary>
    private static async Task<(DateTime Anchor, string Older, string Newer)> SeedAsync(NpgsqlConnection connection, Subject subject, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        Assert.Equal(TimescaleSupport.ConvertedTables.Count, await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct));

        /* One reading of the clock for the whole fact: the rows are placed from it and their ages are read back against
           it, so a fact that runs across midnight still reads the ages it wrote. Noon, so a row sits mid-chunk and the
           purge's own clock, read a little later, cannot move it across a horizon. */
        var anchor = LiveClock.Now().Date;
        var noon = anchor.AddHours(12);
        foreach (var days in new[] { subject.OlderDays, subject.NewerDays, 1 })
        {
            await using var insert = new NpgsqlCommand(subject.InsertSql, connection) { CommandTimeout = 120 };
            insert.Parameters.AddWithValue(9102000L + days);
            insert.Parameters.AddWithValue(noon.AddDays(-days));
            insert.Parameters.AddWithValue(TestServerId);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* The chunk each row landed in, oldest row first. */
        var chunks = new List<string>();
        await using (var command = new NpgsqlCommand(
            "SELECT format('%I.%I', n.nspname, c.relname) FROM " + subject.Table + " AS t "
            + "JOIN pg_class AS c ON c.oid = t.tableoid JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            + "WHERE t.server_id = $1 ORDER BY t." + subject.TimeColumn, connection) { CommandTimeout = 120 })
        {
            command.Parameters.AddWithValue(TestServerId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        Assert.Equal(3, chunks.Distinct(StringComparer.Ordinal).Count());
        return (anchor, chunks[0], chunks[1]);
    }

    /// <summary>The surviving rows' ages in whole days against the day they were placed from, oldest last.</summary>
    private static async Task<string?> AgesAsync(NpgsqlConnection connection, Subject subject, DateTime anchor, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT string_agg(d::text, ',' ORDER BY d) FROM (SELECT ($1::date - " + subject.TimeColumn + "::date) AS d FROM " + subject.Table + " WHERE server_id = $2) AS x",
            connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(anchor);
        command.Parameters.AddWithValue(TestServerId);
        return await command.ExecuteScalarAsync(ct) as string;
    }

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
