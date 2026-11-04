/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the monitored server is a cluster of this class's own, and the store is a scratch database made through ScratchPostgres. */

/// <summary>
/// What the table bloat and index usage reads say after PostgreSQL loses its statistics at crash recovery (#7456).
///
/// <para>The monitored server is a cluster of this class's own, booted from the runtime <c>DARLING_TEST_PGRUNTIME</c> names and
/// stopped with a real <c>pg_ctl stop -m immediate</c>, because the defect is a fact about what PostgreSQL does at the next start:
/// <c>last_analyze</c>, <c>n_mod_since_analyze</c> and <c>idx_scan</c> all go back to empty while <c>pg_stats</c>, <c>reltuples</c>
/// and the database's <c>stats_reset</c> do not change. The collectors' own queries read the cluster before and after, so a change
/// to what they select is exercised rather than assumed.</para>
/// </summary>
public sealed class PgStatsLostAtRecoveryLiveTests
{
    private const int ServerId = -745601;
    private const string ServerName = "stats-lost-e2e";

    private sealed class TargetCluster : IAsyncDisposable
    {
        private readonly string _root;
        private readonly string _bin;

        private TargetCluster(string root, string bin, int port)
        {
            _root = root;
            _bin = bin;
            Port = port;
        }

        public int Port { get; }

        private string Data => Path.Combine(_root, "data");

        public string ConnectionString => new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1", Port = Port, Username = "darling", Database = "postgres", Pooling = false,
        }.ConnectionString;

        public static async Task<TargetCluster?> StartAsync(CancellationToken ct)
        {
            var runtime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
            if (string.IsNullOrWhiteSpace(runtime) || !File.Exists(Path.Combine(runtime, "pgsql", "bin", "initdb.exe")))
                return null;

            var bin = Path.Combine(runtime, "pgsql", "bin");
            var root = Path.Combine(Path.GetTempPath(), "pmsl-" + Guid.NewGuid().ToString("N")[..12]);
            Directory.CreateDirectory(root);
            int port;
            using (var listener = new TcpListener(IPAddress.Loopback, 0))
            {
                listener.Start();
                port = ((IPEndPoint)listener.LocalEndpoint).Port;
            }

            var cluster = new TargetCluster(root, bin, port);
            try
            {
                await cluster.RunAsync("initdb.exe", $"-D \"{cluster.Data}\" -U darling -A trust --encoding=UTF8 --no-instructions", ct);
                /* autovacuum off: an autoanalyze landing between the restart and the read would write the very record the test needs to be missing. */
                await File.AppendAllTextAsync(Path.Combine(cluster.Data, "postgresql.conf"),
                    $"\nport = {port}\nlisten_addresses = '127.0.0.1'\nmax_connections = 40\nfsync = off\nfull_page_writes = off\nautovacuum = off\ntimezone = 'UTC'\nlog_timezone = 'UTC'\n", ct);
                await cluster.StartServerAsync(ct);
                return cluster;
            }
            catch
            {
                await cluster.DisposeAsync();
                throw;
            }
        }

        /// <summary>Not through RunToolAsync: the server inherits a redirected output pipe, which then never closes. Its output goes to the log.</summary>
        private async Task StartServerAsync(CancellationToken ct)
        {
            using var start = Process.Start(new ProcessStartInfo(Path.Combine(_bin, "pg_ctl.exe"), $"start -D \"{Data}\" -l \"{Path.Combine(_root, "server.log")}\" -w -t 90")
            {
                UseShellExecute = false,
                CreateNoWindow = true,
            })!;
            await start.WaitForExitAsync(ct);
            if (start.ExitCode != 0)
                throw new InvalidOperationException($"pg_ctl start exited {start.ExitCode}: {await File.ReadAllTextAsync(Path.Combine(_root, "server.log"), ct)}");
        }

        /// <summary>A crash, as far as the statistics system can tell: no shutdown checkpoint, so the next start runs recovery and discards every counter.</summary>
        public async Task CrashAndRestartAsync(CancellationToken ct)
        {
            await RunAsync("pg_ctl.exe", $"stop -D \"{Data}\" -m immediate -w -t 90", ct);
            await StartServerAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            if (File.Exists(Path.Combine(Data, "postmaster.pid")))
                await RunAsync("pg_ctl.exe", $"stop -D \"{Data}\" -m immediate -w -t 90", CancellationToken.None, throwOnError: false);
            DarlingManagedPostgresTests.TryDeleteRecursive(_root);
        }

        private async Task RunAsync(string tool, string arguments, CancellationToken ct, bool throwOnError = true)
        {
            var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(Path.Combine(_bin, tool), arguments, TimeSpan.FromSeconds(120), ct);
            if (exitCode != 0 && throwOnError)
                throw new InvalidOperationException($"{tool} {arguments} exited {exitCode}: {output}");
        }
    }

    private const string SetupSql = """
        CREATE TABLE widget (id int PRIMARY KEY, grp int NOT NULL, v text NOT NULL);
        INSERT INTO widget SELECT g, g % 500, repeat('x', 100) FROM generate_series(1, 30000) AS g;
        CREATE INDEX ix_widget_grp ON widget (grp);
        ANALYZE widget;
        """;

    /// <summary>An index scan on <c>ix_widget_grp</c>, then a flush, so the counter is in shared statistics before anything reads or crashes.</summary>
    private static async Task ScanTheIndexAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "SET enable_seqscan = off; SET enable_bitmapscan = off; SELECT count(*) FROM widget WHERE grp = 7", ct);
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One <c>widget</c> row as the table bloat collector's own query reports it.</summary>
    private static async Task<DarlingPgTableBloatReader.PgTableBloatRow> ReadBloatAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(PgTableBloatStatsCollector.QueryText, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetString(1) != "widget")
                continue;
            return new DarlingPgTableBloatReader.PgTableBloatRow(
                "postgres", reader.GetString(0), reader.GetString(1), DateTime.UtcNow,
                reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5),
                reader.GetInt64(6), reader.GetInt64(7), reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                (double)reader.GetDecimal(10), reader.GetInt64(11), reader.GetInt32(12),
                reader.GetInt64(13), reader.GetDecimal(14), reader.GetBoolean(15), reader.GetInt32(16), reader.GetBoolean(17),
                reader.GetInt64(2), reader.GetInt64(13), DateTime.UtcNow.AddDays(-1), 2);
        }

        throw new InvalidOperationException("The table bloat collector's query returned no row for widget.");
    }

    [Fact]
    public async Task ABloatEstimateIsWithheldAfterCrashRecovery_AndPublishesAgainOnceTheTableIsAnalyzed()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var target = await TargetCluster.StartAsync(ct);
        Assert.SkipWhen(target is null, "Set DARLING_TEST_PGRUNTIME (a runtime folder holding pgsql\\bin\\initdb.exe) to run the crash-recovery live test.");

        await using var connection = new NpgsqlConnection(target!.ConnectionString);
        await connection.OpenAsync(ct);
        await ExecAsync(connection, SetupSql, ct);

        /* Before the crash: the analyze is recorded. (Whether the estimate publishes here depends on when the insert's counters flush, so it is not asserted.) */
        var before = await ReadBloatAsync(target.ConnectionString, ct);
        Assert.NotNull(before.LastAnalyzed);
        Assert.False(before.EstimateUnavailable);

        await target.CrashAndRestartAsync(ct);

        /* After: PostgreSQL still holds the column statistics and the row estimate, and has lost the record of when they were taken. */
        var after = await ReadBloatAsync(target.ConnectionString, ct);
        Assert.Null(after.LastAnalyzed);
        Assert.Equal(0, after.ModsSinceAnalyze);
        Assert.False(after.EstimateUnavailable);
        Assert.True(after.LiveTuples >= 0, $"reltuples should survive recovery, was {after.LiveTuples}");

        var reason = DarlingMcpPgTableBloatTools.EstimateSuppressionReason(after);
        Assert.NotNull(reason);
        Assert.True(DarlingPgTableBloatReader.EstimateIsUnpublishable(after));
        Assert.Contains("no record of when it was last analyzed", reason, StringComparison.Ordinal);
        Assert.Contains("ANALYZE", reason, StringComparison.Ordinal);
        /* Its own reason: not the never-analyzed one, not the stale one, and no claim that a crash happened (pg_stat_reset() leaves the same trace). */
        Assert.DoesNotContain("never been analyzed", reason, StringComparison.Ordinal);
        Assert.DoesNotContain("STALE", reason, StringComparison.Ordinal);
        Assert.DoesNotContain("crashed", reason, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("Unknown", DarlingMcpPgTableBloatTools.BloatSeverity(after));

        /* Analyzed after the restart: the record exists again and the estimate publishes. */
        await using var again = new NpgsqlConnection(target.ConnectionString);
        await again.OpenAsync(ct);
        await ExecAsync(again, "ANALYZE widget", ct);
        await ExecAsync(again, "SELECT pg_stat_force_next_flush()", ct);
        var analyzed = await ReadBloatAsync(target.ConnectionString, ct);
        Assert.NotNull(analyzed.LastAnalyzed);
        Assert.Null(DarlingMcpPgTableBloatTools.EstimateSuppressionReason(analyzed));
    }

    [Fact]
    public async Task IndexUsageDoesNotSayStatisticsWereNeverReset_AfterCrashRecovery()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the crash-recovery live test.");
        await using var target = await TargetCluster.StartAsync(ct);
        Assert.SkipWhen(target is null, "Set DARLING_TEST_PGRUNTIME (a runtime folder holding pgsql\\bin\\initdb.exe) to run the crash-recovery live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(target!.ConnectionString);
            await connection.OpenAsync(ct);
            await ExecAsync(connection, SetupSql, ct);
            await ScanTheIndexAsync(connection, ct);

            var before = await ReadIndexAsync(target.ConnectionString, ct);
            Assert.True(before.Scans > 0, "the scan did not reach the counter");
            Assert.Null(before.StatsReset);

            await target.CrashAndRestartAsync(ct);

            /* PostgreSQL's own answer after recovery: the counter is back at zero and the reset timestamp has not moved. */
            var after = await ReadIndexAsync(target.ConnectionString, ct);
            Assert.Equal(0, after.Scans);
            Assert.Null(after.StatsReset);

            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingMcpTestData.RegisterServerAsync(store, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(store, ct, "UPDATE servers SET created_date = $2 WHERE server_id = $1", ServerId, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-30)));

            /* One sample before the crash and one after: the counter fell, the timestamp did not move. */
            await SeedAsync(store, DateTime.UtcNow.AddHours(-3), before, ct);
            await SeedAsync(store, DateTime.UtcNow.AddHours(-2), after, ct);

            var rows = await DarlingPgIndexUsageReader.GetPgIndexUsageAsync(
                dataSource, ServerId, DateTime.UtcNow.AddHours(-24), DateTime.UtcNow, 10, ct);
            var row = Assert.Single(rows);
            Assert.Null(row.StatsReset);
            Assert.True(row.StatsWereResetInWindow, "the counter fell between two samples, so the reset flag must be set");

            using var json = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, 10, null, null, ct));
            var root = json.RootElement;
            Assert.True(root.GetProperty("statistics_were_reset_in_window").GetBoolean());
            var index = Assert.Single(root.GetProperty("indexes").EnumerateArray());
            var note = index.GetProperty("stats_reset_note").GetString()!;
            Assert.DoesNotContain("never been reset", note, StringComparison.Ordinal);
            Assert.Contains("fell between two samples", note, StringComparison.Ordinal);
            Assert.Contains("postmaster", note, StringComparison.Ordinal);

            /* A zero after the loss is not a claim of disuse: the index is not counted among the unscanned, and its finding says why. */
            Assert.Equal(0, root.GetProperty("unscanned_without_a_structural_blocker").GetInt32());
            Assert.Contains("not evidence of disuse", index.GetProperty("droppability_finding").GetString()!, StringComparison.Ordinal);

            /* No fall in the series: the note still does not call the statistics never reset. */
            Assert.DoesNotContain("never been reset", DarlingMcpPgIndexUsageTools.StatsResetNote(row with { StatsWereResetInWindow = false }), StringComparison.Ordinal);
            Assert.Contains("postmaster", DarlingMcpPgIndexUsageTools.StatsResetNote(row with { StatsWereResetInWindow = false }), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private sealed record IndexSample(long Scans, long IndexBytes, long TableBytes, DateTime? StatsReset);

    private static async Task<IndexSample> ReadIndexAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(PgIndexUsageStatsCollector.BuildQueryText(17), connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetString(2) != "ix_widget_grp")
                continue;
            return new IndexSample(reader.GetInt64(3), reader.GetInt64(8), reader.GetInt64(9), reader.IsDBNull(22) ? null : reader.GetDateTime(22));
        }

        throw new InvalidOperationException("The index usage collector's query returned no row for ix_widget_grp.");
    }

    private static Task SeedAsync(NpgsqlConnection store, DateTime at, IndexSample sample, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(store, ct,
            @"INSERT INTO pg_index_usage_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, index_name, index_scans,
     tuples_read, tuples_fetched, blocks_read, blocks_hit, index_bytes, table_bytes, is_unique, is_primary_key, is_valid, is_ready,
     is_replica_identity, is_partial, is_expression, supports_constraint, index_method, column_count, index_definition, stats_reset)
VALUES ($1,$2,$3,$4,'postgres','public','widget','ix_widget_grp',$5, 0,0,0,0, $6, $7, false,false,true,true, false,false,false,false, 'btree', 1,
        'CREATE INDEX ix_widget_grp ON public.widget USING btree (grp)', $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, sample.Scans, sample.IndexBytes, sample.TableBytes,
            sample.StatsReset is { } reset ? DarlingMcpTestData.Naive(reset) : null);
}
