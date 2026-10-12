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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The raw procedure duration trend reads the per-collection totals (#5638, V176) for the collections at or after a server's floor
/// (<c>MIN(collection_time)</c> of <c>collect.procedure_stats_totals</c>) and the <c>procedure_stats</c> rows below it. The ORACLE in
/// every comparison is the statement as it ran before the totals (the stored CTE summing every row of every collection, kept below as
/// <see cref="OldStoredBlock"/> and spliced into the new text), run on the same store. The new statement must return, column by column
/// and type by type, the buckets the old one returns, for the unfiltered and the database-filtered shapes, at the 1 and 60 minute
/// widths, for a window after the floor, one that straddles it, one before it, and for servers with no totals at all.
///
/// <para>The data: server 7 has 24 five-minute collections (10:00 to 11:55). The first twelve are rows only, written directly (a store
/// upgraded to V176 mid-stream); the rest go through the product's own <c>WriteBatchAsync</c>, so their totals are the real writer's.
/// They include a restart collection (interval 0 everywhere, before and after the floor), a collection whose intervals are all NULL, a
/// collection with a NULL database_name, a collection whose deltas are all NULL, a collection that stored one database only, and
/// collections that stored nothing (idle runs in <c>collection_log</c>, before and after the floor). Server 8 carries the same rows
/// and no totals at all (the floor is NULL: every collection reads the rows).</para>
///
/// <para><b>#1776 own-store:</b> the test creates and drops its own scratch database through <see cref="ScratchPostgres"/>, so it is
/// not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class ProcedureTrendTotalsStitchLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the procedure trend totals stitch's live equivalence pins (each mints its own scratch database).";

    private static readonly DateTime T0 = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Unspecified);

    private const int FloorIndex = 12;

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static DateTime At(int index) => T0.AddMinutes(5 * index);

    /// <summary>The <c>stored</c> CTE as it ran before #5638: every row of every collection, summed per collection.</summary>
    private static string OldStoredBlock(bool withDatabaseFilter)
    {
        const string match = "$4::text[] IS NULL OR database_name = ANY($4)";
        var maxGap = CollectorDeltaCalculator.DefaultMaxGapSeconds;
        var sums = withDatabaseFilter
            ? $"COALESCE(SUM(delta_elapsed_time) FILTER (WHERE {match}), 0) / 1000.0 AS total_elapsed_ms,\n"
              + $"        COALESCE(SUM(delta_execution_count) FILTER (WHERE {match}), 0) AS total_executions,\n"
              + $"        COUNT(*) FILTER (WHERE {match}) AS matched_rows,"
            : "SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,\n"
              + "        SUM(delta_execution_count) AS total_executions,";
        return $"""
        stored AS
        (
            SELECT
                collection_time,
                {sums}
                MAX(sample_interval_seconds) AS max_interval_seconds
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $2 - INTERVAL '{maxGap} seconds'
            AND   collection_time <= $3
            GROUP BY collection_time
        ),

        """;
    }

    /// <summary>The new statement with its stitched <c>totals_floor</c> + <c>stored</c> pair replaced by the pre-change <c>stored</c>.</summary>
    private static string OldSql(bool withDatabaseFilter)
    {
        var text = DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", withDatabaseFilter);
        var start = text.IndexOf("totals_floor AS", StringComparison.Ordinal);
        var end = text.IndexOf("runs AS", StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "the stitched pair is the text between totals_floor and runs");
        return text[..start] + OldStoredBlock(withDatabaseFilter).Replace("\r\n", "\n") + text[end..];
    }

    /// <summary>The stitched text names the totals table, takes the floor per server, and keeps the rows arm below it.</summary>
    [Fact]
    public void TheStatementReadsTheTotalsAtOrAfterTheFloorAndTheRowsBelowIt()
    {
        foreach (var filtered in new[] { false, true })
        {
            var text = System.Text.RegularExpressions.Regex.Replace(DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", filtered), @"\s+", " ");
            Assert.Contains("FROM collect.procedure_stats_totals WHERE server_id = $1", text, StringComparison.Ordinal);
            Assert.Contains("COALESCE(MIN(collection_time), 'infinity'::timestamp) AS floor_time", text, StringComparison.Ordinal);
            Assert.Contains("AND collection_time < (SELECT floor_time FROM totals_floor)", text, StringComparison.Ordinal);
            Assert.Contains("MAX(max_sample_interval_seconds) AS max_interval_seconds", text, StringComparison.Ordinal);
            Assert.Contains("SUM(total_elapsed_time)", text, StringComparison.Ordinal);
            Assert.Equal(filtered, text.Contains("COALESCE(SUM(row_count) FILTER (WHERE $4::text[] IS NULL OR database_name = ANY($4)), 0)::bigint AS matched_rows", StringComparison.Ordinal));

            /* The pre-change oracle really is a different text, so the equivalence below compares two statements. */
            Assert.NotEqual(DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", filtered), OldSql(filtered));
            Assert.DoesNotContain("procedure_stats_totals", OldSql(filtered), StringComparison.Ordinal);
        }
    }

    internal sealed record Spec(string? Database, string Procedure, long? Elapsed, long? Executions, int? Interval);

    /// <summary>The rows of collection <paramref name="index"/>, their final shape (NULLs included). An empty list stores nothing.</summary>
    private static List<Spec> Specs(int index)
    {
        if (index is 9 or 19)
        {
            return new List<Spec>();
        }

        var specs = new List<Spec>();
        var databases = index == 21 ? new[] { "B" } : index == 22 ? new[] { "A" } : new[] { "A", "B", "C" };
        for (var d = 0; d < databases.Length; d++)
        {
            var database = databases[d];
            for (var p = 1; p <= 3; p++)
            {
                long elapsed = 1000L * (index + 1) + p * 7 + d * 13;
                long executions = 1 + (index % 4) + p;
                int? interval = (index + p) % 5 == 0 ? 120 : 300;
                if (index is 6 or 15)
                {
                    elapsed = 0;
                    executions = 0;
                    interval = 0;
                }
                else if (index is 8 or 17)
                {
                    interval = null;
                }

                string? name = database;
                if (index == 20 && database == "C")
                {
                    name = null;
                }

                long? e = elapsed;
                long? x = executions;
                if (index == 22)
                {
                    e = null;
                    x = null;
                }

                specs.Add(new Spec(name, $"{database}_p{p}", e, x, interval));
            }
        }

        return specs;
    }

    private static readonly int[] CollectionIndexes = BuildIndexes();

    private static int[] BuildIndexes()
    {
        var all = new int[24];
        for (var i = 0; i < all.Length; i++)
        {
            all[i] = i;
        }

        return all;
    }

    private sealed class Rig : IAsyncDisposable
    {
        private int _directId;
        private int _logId;

        public required ScratchPostgres Scratch { get; init; }

        public required NpgsqlDataSource DataSource { get; init; }

        public required NpgsqlConnection Connection { get; init; }

        public required DarlingCollectorRunner Runner { get; init; }

        public required CollectorDeltaCalculator Deltas { get; init; }

        public required ServerRuntime Server { get; init; }

        public static async Task<Rig> OpenAsync(int serverId, CancellationToken ct)
        {
            var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var deltas = new CollectorDeltaCalculator();
            var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            return new Rig
            {
                Scratch = scratch,
                DataSource = dataSource,
                Connection = connection,
                Deltas = deltas,
                Runner = new DarlingCollectorRunner(dataSource, deltas),
                Server = new ServerRuntime
                {
                    Config = new MonitoredServer { Name = "Stitch\\INST", Host = "Stitch\\INST" },
                    ConnectionString = "Server=Stitch",
                    Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
                    StorageName = "Stitch\\INST",
                    ServerId = serverId,
                    EngineEdition = 3,
                },
            };
        }

        /// <summary>Sets a stored row to the spec's final shape (a NULL database, interval or deltas), the shape rows written before those columns carried.</summary>
        public async Task ReshapeAsync(int serverId, DateTime at, Spec spec, CancellationToken ct)
        {
            await using var update = new NpgsqlCommand(
                "UPDATE collect.procedure_stats SET database_name = $1, delta_elapsed_time = $2, delta_execution_count = $3, sample_interval_seconds = $4 "
                + "WHERE server_id = $5 AND collection_time = $6 AND object_name = $7",
                Connection);
            update.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)spec.Database ?? DBNull.Value);
            update.Parameters.AddWithValue(NpgsqlDbType.Bigint, (object?)spec.Elapsed ?? DBNull.Value);
            update.Parameters.AddWithValue(NpgsqlDbType.Bigint, (object?)spec.Executions ?? DBNull.Value);
            update.Parameters.AddWithValue(NpgsqlDbType.Integer, (object?)spec.Interval ?? DBNull.Value);
            update.Parameters.AddWithValue(serverId);
            update.Parameters.AddWithValue(NpgsqlDbType.Timestamp, at);
            update.Parameters.AddWithValue(spec.Procedure);
            Assert.Equal(1, await update.ExecuteNonQueryAsync(ct));
        }

        /// <summary>One row stored directly (a store with no totals writer yet), NULLs as given. Ids are negative so they never meet the writer's.</summary>
        public async Task DirectRowAsync(int serverId, DateTime at, Spec spec, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, 'stitch', $4, 'dbo', $5, $5, 1, $6, $7, $8)", Connection);
            insert.Parameters.AddWithValue(--_directId);
            insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, at);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)spec.Database ?? DBNull.Value);
            insert.Parameters.AddWithValue(spec.Procedure);
            insert.Parameters.AddWithValue(NpgsqlDbType.Bigint, (object?)spec.Elapsed ?? DBNull.Value);
            insert.Parameters.AddWithValue(NpgsqlDbType.Bigint, (object?)spec.Executions ?? DBNull.Value);
            insert.Parameters.AddWithValue(NpgsqlDbType.Integer, (object?)spec.Interval ?? DBNull.Value);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async Task RunAsync(int serverId, DateTime loggedAt, long durationMs, string status, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1, $2, 'stitch', 'procedure_stats', $3, $4, $5, 0)", Connection);
            insert.Parameters.AddWithValue(++_logId);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, loggedAt);
            insert.Parameters.AddWithValue(durationMs);
            insert.Parameters.AddWithValue(status);
            await insert.ExecuteNonQueryAsync(ct);
        }

        /// <summary>One batch through the product's <c>WriteBatchAsync</c>: the rows and their totals, in the COPY's own transaction.</summary>
        public async Task<int> WriteAsync(DateTime collectionTime, List<ProcedureStatsCollector.Row> rows, CancellationToken ct)
        {
            var context = new CollectorContext
            {
                ServerId = Server.ServerId,
                ServerName = Server.StorageName,
                CollectionTime = collectionTime,
                Deltas = Deltas,
                Target = Server.Target,
            };

            var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(ProcedureStatsCollector.Row)).Invoke(
                Runner,
                new object?[] { Connection, ProcedureStatsCollector.Instance, rows, Server, collectionTime, context, ct })!;
            await task;
            return (int)task.GetType().GetProperty("Result")!.GetValue(task)!;
        }

        /// <summary>Recomputes one collection's totals from its rows with the product's own upsert, after the rows were reshaped to a NULL case.</summary>
        public async Task RetotalAsync(int serverId, DateTime at, int rows, CancellationToken ct)
        {
            await using (var delete = new NpgsqlCommand("DELETE FROM collect.procedure_stats_totals WHERE server_id = $1 AND collection_time = $2", Connection))
            {
                delete.Parameters.AddWithValue(serverId);
                delete.Parameters.AddWithValue(NpgsqlDbType.Timestamp, at);
                await delete.ExecuteNonQueryAsync(ct);
            }

            await using var tx = await Connection.BeginTransactionAsync(ct);
            await ProcedureStatsTotalsWriter.UpsertBatchAsync(Connection, tx, serverId, at, rows, 30, null, ct);
            await tx.CommitAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await Connection.DisposeAsync();
            await DataSource.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>A row for the real writer. The placeholders (database, deltas, interval) are reshaped to the spec's NULLs afterwards.</summary>
    private static ProcedureStatsCollector.Row WriterRow(Spec spec) => new(
        spec.Database ?? "C", "dbo", spec.Procedure, "P ", T0, T0, spec.Executions ?? 0, spec.Elapsed ?? 0, spec.Elapsed ?? 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0,
        "0x" + spec.Procedure, "0xP" + spec.Procedure, null, null)
    {
        Precomputed = new ProcedureStatsCollector.ReadTimeDeltas(spec.Executions ?? 0, spec.Elapsed ?? 0, spec.Elapsed ?? 0, 1, 1, 1, 0, spec.Interval ?? 300),
    };

    /// <summary>Loads server 7 (rows below the floor, rows and real totals from it) and server 8 (rows only), and both servers' collection_log runs.</summary>
    private static async Task LoadAsync(Rig rig, int totalsServer, int rowsOnlyServer, CancellationToken ct)
    {
        foreach (var i in CollectionIndexes)
        {
            var specs = Specs(i);

            foreach (var spec in specs)
            {
                await rig.DirectRowAsync(rowsOnlyServer, At(i), spec, ct);
            }

            if (i < FloorIndex)
            {
                foreach (var spec in specs)
                {
                    await rig.DirectRowAsync(totalsServer, At(i), spec, ct);
                }
            }
            else if (specs.Count > 0)
            {
                Assert.Equal(specs.Count, await rig.WriteAsync(At(i), specs.ConvertAll(WriterRow), ct));
                var reshaped = false;
                foreach (var spec in specs)
                {
                    if (spec.Database is null || spec.Interval is null || spec.Elapsed is null)
                    {
                        reshaped = true;
                        await rig.ReshapeAsync(totalsServer, At(i), spec, ct);
                    }
                }

                if (reshaped)
                {
                    await rig.RetotalAsync(totalsServer, At(i), specs.Count, ct);
                }
            }

            /* Every cycle is a SUCCESS run logged 20 s after its stamp; a run that stored nothing is the idle point. */
            foreach (var server in new[] { totalsServer, rowsOnlyServer })
            {
                await rig.RunAsync(server, At(i).AddSeconds(20), 1500, "SUCCESS", ct);
            }
        }

        /* A failed run is no point at all. */
        foreach (var server in new[] { totalsServer, rowsOnlyServer })
        {
            await rig.RunAsync(server, At(24).AddSeconds(20), 900, "FAILED", ct);
        }
    }

    private static async Task<(string Types, List<string> Rows)> ReadAsync(
        NpgsqlDataSource dataSource, string sql, bool filtered, string[]? filter, int serverId, DateTime start, DateTime end, int width, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, start);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end);
        if (filtered)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = (object?)filter ?? DBNull.Value });
        }

        command.Parameters.AddWithValue(NpgsqlDbType.Integer, width);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        var types = new string[reader.FieldCount];
        for (var c = 0; c < types.Length; c++)
        {
            types[c] = reader.GetName(c) + ":" + reader.GetDataTypeName(c);
        }

        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var c = 0; c < cells.Length; c++)
            {
                cells[c] = reader.IsDBNull(c) ? "<null>" : Convert.ToString(reader.GetValue(c), CultureInfo.InvariantCulture) ?? "<null>";
            }

            rows.Add(string.Join("|", cells));
        }

        return (string.Join(",", types), rows);
    }

    [Fact]
    public async Task TheStitchedStatementReturnsTheBucketsTheRowsOnlyStatementReturns()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int totalsServer = -945307;
        const int rowsOnlyServer = -945308;
        const int unknownServer = -945399;

        await using var rig = await Rig.OpenAsync(totalsServer, ct);

        var bodySucceeded = false;
        try
        {
            await LoadAsync(rig, totalsServer, rowsOnlyServer, ct);

            /* The setup is what the test claims: the floor is the first writer collection, nothing is totalled below it, the idle
               collection after it has no totals row, the NULL database is one totals group, and the rows-only server has no totals. */
            await using (var check = rig.DataSource.CreateCommand(
                "SELECT (SELECT MIN(collection_time) FROM collect.procedure_stats_totals WHERE server_id = $1), "
                + "(SELECT COUNT(*) FROM collect.procedure_stats_totals WHERE server_id = $1 AND collection_time < $2), "
                + "(SELECT COUNT(*) FROM collect.procedure_stats_totals WHERE server_id = $1), "
                + "(SELECT COUNT(*) FROM collect.procedure_stats_totals WHERE server_id = $3), "
                + "(SELECT COUNT(*) FROM collect.procedure_stats_totals WHERE server_id = $1 AND collection_time = $4), "
                + "(SELECT COUNT(*) FROM collect.procedure_stats_totals WHERE server_id = $1 AND database_name IS NULL)"))
            {
                check.Parameters.AddWithValue(NpgsqlDbType.Integer, totalsServer);
                check.Parameters.AddWithValue(NpgsqlDbType.Timestamp, At(FloorIndex));
                check.Parameters.AddWithValue(NpgsqlDbType.Integer, rowsOnlyServer);
                check.Parameters.AddWithValue(NpgsqlDbType.Timestamp, At(19));
                await using var reader = await check.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(At(FloorIndex), reader.GetDateTime(0));
                Assert.Equal(0L, reader.GetInt64(1));
                Assert.Equal(29L, reader.GetInt64(2));
                Assert.Equal(0L, reader.GetInt64(3));
                Assert.Equal(0L, reader.GetInt64(4));
                Assert.Equal(1L, reader.GetInt64(5));
            }

            var windows = new (string Name, DateTime Start, DateTime End)[]
            {
                ("after the floor", At(13), At(23)),
                ("straddling the floor", At(6), At(18)),
                ("before the floor", At(0), At(10)),
                ("everything", At(0), At(24)),
            };
            var filters = new (string Name, bool Filtered, string[]? Filter)[]
            {
                ("unfiltered", false, null),
                ("filter NULL", true, null),
                ("filter A", true, new[] { "A" }),
                ("filter B,C", true, new[] { "B", "C" }),
                ("filter with no rows", true, new[] { "NoSuchDb" }),
            };

            var mismatches = new List<string>();
            var compared = 0;
            var nonEmpty = 0;
            var unrated = 0;
            foreach (var server in new[] { totalsServer, rowsOnlyServer, unknownServer })
            {
                foreach (var window in windows)
                {
                    foreach (var filter in filters)
                    {
                        foreach (var width in new[] { 1, 60 })
                        {
                            var expected = await ReadAsync(rig.DataSource, OldSql(filter.Filtered), filter.Filtered, filter.Filter, server, window.Start, window.End, width, ct);
                            var actual = await ReadAsync(
                                rig.DataSource, DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", filter.Filtered), filter.Filtered, filter.Filter,
                                server, window.Start, window.End, width, ct);
                            compared++;
                            if (expected.Rows.Count > 0)
                            {
                                nonEmpty++;
                            }

                            if (expected.Rows.Exists(r => r.Contains("<null>", StringComparison.Ordinal)))
                            {
                                unrated++;
                            }

                            var label = $"server {server}, {window.Name}, {filter.Name}, width {width}";
                            if (expected.Types != actual.Types)
                            {
                                mismatches.Add(label + ": column types " + expected.Types + " vs " + actual.Types);
                            }
                            else if (!expected.Rows.SequenceEqual(actual.Rows))
                            {
                                mismatches.Add(label + ": " + expected.Rows.Count + " old rows vs " + actual.Rows.Count + " new rows; first old " + (expected.Rows.Count > 0 ? expected.Rows[0] : "-") + " first new " + (actual.Rows.Count > 0 ? actual.Rows[0] : "-"));
                            }
                        }
                    }
                }
            }

            Assert.Empty(mismatches);

            /* The comparison is not vacuous: most statements returned buckets, and the restart / NULL-interval collections left unrated points. */
            Assert.Equal(3 * windows.Length * filters.Length * 2, compared);
            /* Two servers with rows x 4 windows x 4 filters that match (the no-rows filter and the unknown server return nothing) x 2 widths. */
            Assert.Equal(64, nonEmpty);
            Assert.True(unrated > 0, "no unrated point was compared");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static List<ProcedureStatsCollector.Row> GapRows(int salt) => new()
    {
        WriterRow(new Spec("A", "p1", 100 + salt, 1 + salt, 300)),
        WriterRow(new Spec("B", "p2", 200 + salt, 2, 120)),
    };

    /// <summary>
    /// Review of #5706, finding 3: a pre-V176 binary (a rollback) wrote collection t2 above the floor, after the server's totals at t1, so t2 has
    /// raw rows and no totals and the stitched trend (rows only below the floor) would drop it. The next batch (t3) totals every collection
    /// strictly between the server's newest earlier totals row and itself, and the trend then equals the rows-only oracle.
    /// </summary>
    [Fact]
    public async Task ACollectionAnOlderBinaryWroteAboveTheFloor_IsTotalledByTheNextBatch_AndTheTrendEqualsTheOldStatement()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945310;

        await using var rig = await Rig.OpenAsync(serverId, ct);

        var bodySucceeded = false;
        try
        {
            for (var i = 0; i < 3; i++)
            {
                await rig.RunAsync(serverId, At(i).AddSeconds(20), 1500, "SUCCESS", ct);
            }

            Assert.Equal(2, await rig.WriteAsync(At(0), GapRows(0), ct));
            Assert.Equal(2, await rig.WriteAsync(At(1), GapRows(10), ct));

            /* The older binary wrote t2: its rows are there, its totals row is not. */
            await using (var old = new NpgsqlCommand("DELETE FROM collect.procedure_stats_totals WHERE server_id = $1 AND collection_time = $2", rig.Connection))
            {
                old.Parameters.AddWithValue(serverId);
                old.Parameters.AddWithValue(NpgsqlDbType.Timestamp, At(1));
                Assert.Equal(2, await old.ExecuteNonQueryAsync(ct));
            }

            /* Not vacuous: before the next batch the stitched trend is missing t2 against the rows-only oracle. */
            var width = 1;
            var windowStart = At(0);
            var windowEnd = At(3);
            var before = await ReadAsync(rig.DataSource, DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", false), false, null, serverId, windowStart, windowEnd, width, ct);
            var oracleBefore = await ReadAsync(rig.DataSource, OldSql(false), false, null, serverId, windowStart, windowEnd, width, ct);
            Assert.NotEqual(oracleBefore.Rows, before.Rows);

            Assert.Equal(2, await rig.WriteAsync(At(2), GapRows(20), ct));
            Assert.Equal(6L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals WHERE server_id = " + serverId.ToString(CultureInfo.InvariantCulture), ct)));

            foreach (var filtered in new[] { false, true })
            {
                foreach (var w in new[] { 1, 60 })
                {
                    var expected = await ReadAsync(rig.DataSource, OldSql(filtered), filtered, null, serverId, windowStart, windowEnd, w, ct);
                    var actual = await ReadAsync(rig.DataSource, DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", filtered), filtered, null, serverId, windowStart, windowEnd, w, ct);
                    Assert.NotEmpty(expected.Rows);
                    Assert.Equal(expected.Types, actual.Types);
                    Assert.Equal(expected.Rows, actual.Rows);
                }
            }

            /* The probe is normally empty and indexed: with a totals row at every collection the range holds no raw row, and the plan reads no
               procedure_stats heap beyond an index range. Functional only (no timing claimed). */
            await using (var set = new NpgsqlCommand("SET enable_seqscan = off", rig.Connection))
            {
                await set.ExecuteNonQueryAsync(ct);
            }

            var plan = new System.Text.StringBuilder();
            await using (var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + ProcedureStatsTotals.GapFillSql, rig.Connection))
            {
                explain.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
                explain.Parameters.AddWithValue(NpgsqlDbType.Timestamp, At(2));
                await using var reader = await explain.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    plan.AppendLine(reader.GetString(0));
                }
            }

            Assert.DoesNotContain("Seq Scan", plan.ToString(), StringComparison.Ordinal);
            Assert.Contains("Index", plan.ToString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Review of #5706, finding 3, the other side: a server with raw rows and no totals row at all needs nothing from the gap fill (its floor is
    /// infinity, the rows arm reads it), so the first batch totals only itself and the trend still equals the rows-only oracle.
    /// </summary>
    [Fact]
    public async Task AServerWithNoTotalsYet_IsNotBackfilledByTheFirstBatch_AndTheTrendStillEqualsTheOldStatement()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -945313;

        await using var rig = await Rig.OpenAsync(serverId, ct);

        var bodySucceeded = false;
        try
        {
            for (var i = 0; i < 3; i++)
            {
                await rig.RunAsync(serverId, At(i).AddSeconds(20), 1500, "SUCCESS", ct);
            }

            /* Rows with no totals at all: what a store holds before the first V176 batch. */
            Assert.Equal(2, await rig.WriteAsync(At(0), GapRows(0), ct));
            await ExecAsync(rig.Connection, "DELETE FROM collect.procedure_stats_totals", ct);
            Assert.Equal(2, await rig.WriteAsync(At(1), GapRows(10), ct));
            Assert.Equal(2L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals", ct)));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(rig.Connection, "SELECT count(*) FROM collect.procedure_stats_totals WHERE collection_time = '" + At(0).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "'", ct)));

            var expected = await ReadAsync(rig.DataSource, OldSql(false), false, null, serverId, At(0), At(3), 1, ct);
            var actual = await ReadAsync(rig.DataSource, DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", false), false, null, serverId, At(0), At(3), 1, ct);
            Assert.NotEmpty(expected.Rows);
            Assert.Equal(expected.Rows, actual.Rows);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(rig.Scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
