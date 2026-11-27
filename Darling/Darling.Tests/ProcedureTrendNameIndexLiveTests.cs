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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the one-procedure duration trend has two raw texts. <see cref="DarlingTrendReader.ProcedureDurationTrendOneProcedureSql"/>
/// keeps the procedure's name inside the aggregates and needs no index; it is the ORACLE in every comparison here.
/// <see cref="DarlingTrendReader.ProcedureDurationTrendOneProcedureByNameSql"/> reads the procedure's rows by name through
/// <see cref="ProcedureStatsNameIndex"/> and joins them to the collections the per-collection totals list. The two must return, column
/// by column and type by type, the same buckets, and the reader must run the by-name text only on a store that has the index.
///
/// <para><b>#1776 own-store:</b> each test creates and drops its own scratch database through <see cref="ScratchPostgres"/>, so the
/// class is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class ProcedureTrendNameIndexLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to run the procedure name index's live pins (each mints its own scratch database).";

    /// <summary>Collection 0. Collection 24 is midnight, so the collections fall in two daily chunks.</summary>
    private static readonly DateTime T0 = new(2026, 1, 5, 22, 0, 0, DateTimeKind.Unspecified);

    private const int CollectionCount = 48;

    private const int MidnightIndex = 24;

    private static DateTime At(int index) => T0.AddMinutes(5 * index);

    /// <summary>The by-name text is asked for one procedure only: the builder refuses it without the procedure filter.</summary>
    [Fact]
    public void TheByNameTextIsAskedForOneProcedureOnly()
    {
        Assert.Throws<ArgumentException>(() =>
            DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", withDatabaseFilter: true, withProcedureFilter: false, procedureRowsByName: true));
    }

    /// <summary>The ten rows a collection stores at most: database, schema, object and handle.</summary>
    private static readonly (string? Database, string? Schema, string Name, string Handle)[] Procedures =
    {
        ("A", "dbo", "usp_one", "0x01"),
        ("A", "dbo", "usp_two", "0x02"),
        /* The same name in another database. */
        ("B", "dbo", "usp_one", "0x03"),
        /* No schema: the full name is the bare object name, for an empty schema and for a NULL one. */
        ("A", "", "usp_bare", "0x04"),
        ("A", null, "usp_nullschema", "0x05"),
        /* Two different procedures whose full name is the same text, a.b.c. */
        ("A", "a", "b.c", "0x06"),
        ("A", "a.b", "c", "0x07"),
        /* One procedure with two cached plans: two rows a collection. */
        ("A", "dbo", "usp_plans", "0x08"),
        ("A", "dbo", "usp_plans", "0x09"),
        /* A row with no database, which only the read of every database matches. */
        (null, "dbo", "usp_one", "0x0A"),
    };

    private sealed record StoredRow(string? Database, string? Schema, string Name, string Handle, long? Elapsed, long? Executions, int? Interval);

    /// <summary>
    /// The rows of collection <paramref name="index"/>. Collections 5, 20 and 33 store nothing (idle runs). 6, 15 and 30 are restart
    /// collections (interval 0 on every row), 8 and 27 have no interval, 10 and 40 have no deltas, 22 stores database B only and 23
    /// database A only. A.dbo.usp_one is absent from every seventh collection (it ran nothing while the others did), and in
    /// collections 4 and 44 its own row is a restart row while the other rows of the collection carry their interval.
    /// </summary>
    private static List<StoredRow> RowsOf(int index)
    {
        var rows = new List<StoredRow>();
        if (index is 5 or 20 or 33)
        {
            return rows;
        }

        for (var p = 0; p < Procedures.Length; p++)
        {
            var (database, schema, name, handle) = Procedures[p];
            if (p == 0 && index % 7 == 3)
            {
                continue;
            }

            if ((index == 22 && database != "B") || (index == 23 && database != "A"))
            {
                continue;
            }

            long? elapsed = 1000L * (index + 1) + (p + 1) * 7;
            long? executions = 1 + (index % 4) + p;
            int? interval = (index + p) % 5 == 0 ? 120 : 300;
            if (index is 6 or 15 or 30 || (index is 4 or 44 && p == 0))
            {
                elapsed = 0;
                executions = 0;
                interval = 0;
            }
            else if (index is 8 or 27)
            {
                interval = null;
            }
            else if (index is 10 or 40)
            {
                elapsed = null;
                executions = null;
            }

            rows.Add(new StoredRow(database, schema, name, handle, elapsed, executions, interval));
        }

        return rows;
    }

    private static long s_collectionId;

    /// <summary>
    /// Stores every collection's rows for <paramref name="serverId"/> and one SUCCESS run a collection, then totals the collections
    /// from <paramref name="floorIndex"/> on with the product's own upsert, in collection order as the collector writes them. A floor
    /// past the last collection leaves the server with rows and no totals.
    /// </summary>
    private static async Task LoadServerAsync(NpgsqlConnection connection, int serverId, int floorIndex, CancellationToken ct)
    {
        var ids = new List<long>();
        var times = new List<DateTime>();
        var databases = new List<string?>();
        var schemas = new List<string?>();
        var names = new List<string>();
        var handles = new List<string>();
        var elapsed = new List<long?>();
        var executions = new List<long?>();
        var intervals = new List<int?>();
        for (var i = 0; i < CollectionCount; i++)
        {
            foreach (var row in RowsOf(i))
            {
                ids.Add(Interlocked.Decrement(ref s_collectionId));
                times.Add(At(i));
                databases.Add(row.Database);
                schemas.Add(row.Schema);
                names.Add(row.Name);
                handles.Add(row.Handle);
                elapsed.Add(row.Elapsed);
                executions.Add(row.Executions);
                intervals.Add(row.Interval);
            }
        }

        await using (var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT r.id, r.at, $1, 'name-index', r.db, r.sch, r.obj, r.handle, r.elapsed / 2, r.elapsed, r.executions, r.secs
FROM unnest($2::bigint[], $3::timestamp[], $4::text[], $5::text[], $6::text[], $7::text[], $8::bigint[], $9::bigint[], $10::integer[])
    AS r(id, at, db, sch, obj, handle, elapsed, executions, secs)", connection))
        {
            insert.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bigint, ids.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Timestamp, times.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, databases.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, schemas.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, names.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, handles.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bigint, elapsed.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bigint, executions.ToArray());
            insert.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, intervals.ToArray());
            Assert.Equal(ids.Count, await insert.ExecuteNonQueryAsync(ct));
        }

        /* Every cycle is a SUCCESS run logged 20 s after its stamp; a run that stored nothing is the idle point. */
        await using (var runs = new NpgsqlCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1::bigint * 1000 - g.i, $1, 'name-index', 'procedure_stats', $2::timestamp + (g.i * 5 || ' minutes')::interval + INTERVAL '20 seconds', 1500, 'SUCCESS', 0
FROM generate_series(0, $3 - 1) AS g(i)", connection))
        {
            runs.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
            runs.Parameters.AddWithValue(NpgsqlDbType.Timestamp, T0);
            runs.Parameters.AddWithValue(NpgsqlDbType.Integer, CollectionCount);
            Assert.Equal(CollectionCount, await runs.ExecuteNonQueryAsync(ct));
        }

        for (var i = floorIndex; i < CollectionCount; i++)
        {
            var stored = RowsOf(i).Count;
            if (stored == 0)
            {
                continue;
            }

            await using var transaction = await connection.BeginTransactionAsync(ct);
            await ProcedureStatsTotalsWriter.UpsertBatchAsync(connection, transaction, serverId, At(i), stored, 30, null, ct);
            await transaction.CommitAsync(ct);
        }
    }

    private static async Task<(string Types, List<string> Rows)> ReadAsync(
        NpgsqlDataSource dataSource, string sql, int serverId, DateTime start, DateTime end, string[]? databases, string procedure, int width, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, start);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = (object?)databases ?? DBNull.Value });
        command.Parameters.AddWithValue(NpgsqlDbType.Text, procedure);
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

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>
    /// Three servers carry the same rows: one totalled from collection 12 (the floor is inside the older chunk), one totalled from
    /// collection 30 (the floor is inside the newer chunk) and one with no totals at all; a fourth server id has no rows. Every
    /// procedure name, database set, window and width is read with both texts three times: with the index, with the index and the
    /// older chunk compressed (a TimescaleDB store), and with the index dropped.
    /// </summary>
    [Fact]
    public async Task TheByNameStatement_ReturnsTheBucketsTheFilterStatementReturns_WithTheIndex_OverACompressedChunk_AndWithoutTheIndex()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int earlyFloorServer = -857001;
        const int lateFloorServer = -857002;
        const int rowsOnlyServer = -857003;
        const int unknownServer = -857099;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            if (scratch.TimescaleEnabled)
            {
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
                await ExecAsync(connection, TimescaleSupport.EnableCompressionSql(ProcedureStatsCollector.Instance), ct);
            }

            /* The start path builds the name index, on empty tables here. */
            await PgTableTuning.ApplyAsync(connection, null, ct);
            await LoadServerAsync(connection, earlyFloorServer, 12, ct);
            await LoadServerAsync(connection, lateFloorServer, 30, ct);
            await LoadServerAsync(connection, rowsOnlyServer, CollectionCount, ct);

            /* The setup is what the test claims: the two floors, and no totals for the third server. */
            await using (var check = new NpgsqlCommand(
                "SELECT (SELECT MIN(collection_time) FROM collect.procedure_stats_totals WHERE server_id = $1), "
                + "(SELECT MIN(collection_time) FROM collect.procedure_stats_totals WHERE server_id = $2), "
                + "(SELECT COUNT(*) FROM collect.procedure_stats_totals WHERE server_id = $3), "
                + "(SELECT COUNT(*) FROM collect.procedure_stats WHERE server_id = $1)", connection))
            {
                check.Parameters.AddWithValue(NpgsqlDbType.Integer, earlyFloorServer);
                check.Parameters.AddWithValue(NpgsqlDbType.Integer, lateFloorServer);
                check.Parameters.AddWithValue(NpgsqlDbType.Integer, rowsOnlyServer);
                await using var reader = await check.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(At(12), reader.GetDateTime(0));
                Assert.Equal(At(30), reader.GetDateTime(1));
                Assert.Equal(0L, reader.GetInt64(2));
                Assert.Equal((long)Enumerable.Range(0, CollectionCount).Sum(static i => RowsOf(i).Count), reader.GetInt64(3));
            }

            var servers = new[] { earlyFloorServer, lateFloorServer, rowsOnlyServer, unknownServer };
            var windows = new (string Name, DateTime Start, DateTime End)[]
            {
                ("everything", At(0), At(CollectionCount)),
                ("the first hour", At(0), At(11)),
                ("across collection 12", At(6), At(18)),
                ("across midnight", At(18), At(32)),
                ("the last hour", At(36), At(CollectionCount)),
            };
            var filters = new (string Name, string[]? Databases)[]
            {
                ("every database", null),
                ("A", new[] { "A" }),
                ("B", new[] { "B" }),
                ("A and B", new[] { "A", "B" }),
                ("a database with no rows", new[] { "NoSuchDb" }),
            };
            var procedures = new[] { "dbo.usp_one", "dbo.usp_two", "usp_bare", "usp_nullschema", "a.b.c", "dbo.usp_plans", "dbo.usp_absent", "DBO.USP_ONE" };
            var widths = new[] { 1, 60 };

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* The oracle's answers, read once: the rows do not change below, only how they are stored and indexed. */
            var expected = new Dictionary<string, (string Types, List<string> Rows)>(StringComparer.Ordinal);
            var nonEmpty = 0;
            var unrated = 0;
            foreach (var server in servers)
            {
                foreach (var window in windows)
                {
                    foreach (var filter in filters)
                    {
                        foreach (var procedure in procedures)
                        {
                            foreach (var width in widths)
                            {
                                var read = await ReadAsync(
                                    dataSource, DarlingTrendReader.ProcedureDurationTrendOneProcedureSql, server, window.Start, window.End, filter.Databases, procedure, width, ct);
                                expected[$"server {server}, {window.Name}, {filter.Name}, {procedure}, width {width}"] = read;
                                nonEmpty += read.Rows.Count > 0 ? 1 : 0;
                                unrated += read.Rows.Exists(static r => r.Contains("<null>", StringComparison.Ordinal)) ? 1 : 0;
                            }
                        }
                    }
                }
            }

            /* The comparison is not vacuous: a read of a name that matched rows returned buckets (the unknown server, the absent names and
               the databases a name is not in return none), and the restart and no-interval collections left unrated points. */
            Assert.Equal(servers.Length * windows.Length * filters.Length * procedures.Length * widths.Length, expected.Count);
            Assert.True(nonEmpty > expected.Count / 4, $"only {nonEmpty} of {expected.Count} oracle reads returned a bucket");
            Assert.True(unrated > 0, "no unrated point was compared");
            /* A name matched only where it should be: both a.b.c procedures answer as one name, and the wrong case answers as no procedure. */
            var everything = $"server {earlyFloorServer}, everything, A, ";
            Assert.NotEqual(expected[everything + "dbo.usp_one, width 60"].Rows, expected[everything + "dbo.usp_two, width 60"].Rows);
            Assert.Equal(expected[everything + "dbo.usp_absent, width 60"].Rows, expected[everything + "DBO.USP_ONE, width 60"].Rows);
            Assert.NotEqual(expected[everything + "dbo.usp_absent, width 60"].Rows, expected[everything + "a.b.c, width 60"].Rows);

            async Task<List<string>> CompareAsync(string phase)
            {
                var mismatches = new List<string>();
                foreach (var server in servers)
                {
                    foreach (var window in windows)
                    {
                        foreach (var filter in filters)
                        {
                            foreach (var procedure in procedures)
                            {
                                foreach (var width in widths)
                                {
                                    var label = $"server {server}, {window.Name}, {filter.Name}, {procedure}, width {width}";
                                    var oracle = expected[label];
                                    var actual = await ReadAsync(
                                        dataSource, DarlingTrendReader.ProcedureDurationTrendOneProcedureByNameSql, server, window.Start, window.End, filter.Databases, procedure, width, ct);
                                    if (oracle.Types != actual.Types)
                                    {
                                        mismatches.Add($"{phase}: {label}: column types {oracle.Types} vs {actual.Types}");
                                    }
                                    else if (!oracle.Rows.SequenceEqual(actual.Rows))
                                    {
                                        var at = Enumerable.Range(0, Math.Min(oracle.Rows.Count, actual.Rows.Count)).FirstOrDefault(i => oracle.Rows[i] != actual.Rows[i], -1);
                                        mismatches.Add(
                                            $"{phase}: {label}: {oracle.Rows.Count} filter rows vs {actual.Rows.Count} by-name rows; first difference "
                                            + (at < 0 ? "is the row count" : oracle.Rows[at] + " vs " + actual.Rows[at]));
                                    }
                                }
                            }
                        }
                    }
                }

                return mismatches;
            }

            const string indexIsValid =
                "SELECT COALESCE((SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('collect." + ProcedureStatsNameIndex.IndexName + "')), FALSE)";
            Assert.True(await ScalarAsync(connection, indexIsValid, ct) is true, "the start path did not leave the name index");
            Assert.Empty(await CompareAsync("with the index"));

            if (scratch.TimescaleEnabled)
            {
                /* The older chunk compressed, the newer one not: the by-name read takes the first through its batches and the second
                   through the index, and the oracle's answer for the same rows still stands. */
                await using (var compress = new NpgsqlCommand(
                    "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('collect.procedure_stats', older_than => $1::timestamp) AS c", connection))
                {
                    compress.Parameters.AddWithValue(NpgsqlDbType.Timestamp, At(MidnightIndex));
                    Assert.Equal(1L, Convert.ToInt64(await compress.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
                }

                Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(
                    connection, "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'procedure_stats' AND NOT is_compressed", ct), CultureInfo.InvariantCulture));
                await ExecAsync(connection, "ANALYZE collect.procedure_stats", ct);
                Assert.Empty(await CompareAsync("with the index, older chunk compressed"));
                var oracleAgain = await ReadAsync(
                    dataSource, DarlingTrendReader.ProcedureDurationTrendOneProcedureSql, earlyFloorServer, At(0), At(CollectionCount), new[] { "A" }, "dbo.usp_one", 60, ct);
                Assert.Equal(expected[everything + "dbo.usp_one, width 60"].Rows, oracleAgain.Rows);
            }

            /* Without the index the by-name text is slower and still right: a data source that saw the index keeps that text. */
            await ExecAsync(connection, "DROP INDEX collect." + ProcedureStatsNameIndex.IndexName, ct);
            Assert.True(await ScalarAsync(connection, indexIsValid, ct) is false, "the name index is still there");
            Assert.Empty(await CompareAsync("without the index"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The <c>trend</c> array of a tool answer, as its own text, after checking the answer came from the raw tier.</summary>
    private static string RawTrendOf(string json)
    {
        var root = JsonDocument.Parse(json).RootElement;
        Assert.Equal("raw", root.GetProperty("source").GetString());
        return root.GetProperty("trend").GetRawText();
    }

    /// <summary>
    /// The store the start path built has the index on the hypertable, the reader asks for the by-name text there and the plan seeks
    /// the index. With the index dropped a new data source is told it is absent and runs the filter text, and the tool's answer is
    /// the same either way. A start that has an earlier index to build on the same table defers this one, and that data source is
    /// still told no; the next start builds it and the same data source, never recreated, switches to the by-name text.
    /// Six servers, three databases of forty procedures and a collection a minute for four hours, so the server's rows in the window
    /// (28,800) outnumber the procedure's (240) and the choice of index shows in the plan.
    /// </summary>
    [Fact]
    public async Task TheReaderRunsTheByNameTextOnlyWhereTheIndexIs_AndTheToolAnswersTheSameWithAndWithoutIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), SkipReason);
        var ct = TestContext.Current.CancellationToken;
        const int serverId = -857010;
        const string serverName = "a8570-name-index";
        const string database = "db1";
        const string procedure = "dbo.usp_p07";
        /* The procedure's elapsed microseconds in every collection (1000 x 7 + 100000 x 1), over the collection's 60 seconds. */
        const double expectedRate = 107_000 / 1000.0 / 60;

        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            if (scratch.TimescaleEnabled)
            {
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            }

            await PgTableTuning.ApplyAsync(connection, null, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);

            /* Present, valid, four keys wide and one of them an expression, on the table as the start path leaves it. */
            Assert.True(await ScalarAsync(
                connection,
                "SELECT indisvalid AND indnkeyatts = 4 AND indexprs IS NOT NULL AND indpred IS NULL FROM pg_index WHERE indexrelid = to_regclass('collect."
                + ProcedureStatsNameIndex.IndexName + "')",
                ct) is true, "the start path did not leave the name index as a valid four-key expression index");

            /* The seed's newest collection and the tool's as_of: the tool refuses an as_of after the real clock. */
            var end = LiveClock.AnchoredEndNotAfterNow();
            await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
SELECT -(m.m * 1000000 + s.s * 10000 + d.d * 100 + p.p)::bigint - 1, $1::timestamp - (m.m || ' minutes')::interval, $2 + s.s, $3, 'db' || d.d, 'dbo',
       'usp_p' || lpad(p.p::text, 2, '0'), '0xH' || d.d || '_' || p.p, 10, 500 * p.p, 1000 * p.p + 100000 * d.d + 7 * s.s, 100, 60
FROM generate_series(0, 239) AS m(m)
CROSS JOIN generate_series(0, 5) AS s(s)
CROSS JOIN generate_series(1, 3) AS d(d)
CROSS JOIN generate_series(1, 40) AS p(p)", connection))
            {
                seed.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end);
                seed.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
                seed.Parameters.AddWithValue(NpgsqlDbType.Text, serverName);
                Assert.Equal(240 * 6 * 3 * 40, await seed.ExecuteNonQueryAsync(ct));
            }

            /* The per-database totals of every collection, as the collector's writer leaves them. */
            await using (var totals = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats_totals
    (server_id, collection_time, database_name, total_elapsed_time, total_execution_count, row_count, max_sample_interval_seconds)
SELECT server_id, collection_time, database_name, SUM(delta_elapsed_time), SUM(delta_execution_count), COUNT(*), MAX(sample_interval_seconds)
FROM collect.procedure_stats
WHERE server_id BETWEEN $1 AND $1 + 5
GROUP BY server_id, collection_time, database_name
ORDER BY server_id, collection_time, database_name", connection))
            {
                totals.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
                Assert.Equal(240 * 6 * 3, await totals.ExecuteNonQueryAsync(ct));
            }

            await ExecAsync(connection, "VACUUM (ANALYZE) collect.procedure_stats", ct);
            await ExecAsync(connection, "VACUUM (ANALYZE) collect.procedure_stats_totals", ct);

            var asOf = DateTime.SpecifyKind(end, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
            Task<string> ToolAsync(NpgsqlDataSource postgres) =>
                DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 4, asOf, null, procedure, database, ct);

            async Task<string> PlanAsync(string sql)
            {
                await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE) " + sql, connection);
                explain.Parameters.AddWithValue(NpgsqlDbType.Integer, serverId);
                explain.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end.AddHours(-4));
                explain.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end);
                explain.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, new[] { database });
                explain.Parameters.AddWithValue(NpgsqlDbType.Text, procedure);
                explain.Parameters.AddWithValue(NpgsqlDbType.Integer, 5);
                var lines = new List<string>();
                await using var reader = await explain.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    lines.Add(reader.GetString(0));
                }

                return string.Join("\n", lines);
            }

            /* With the index: the reader is told it is there, picks the by-name text, and that text seeks the index. */
            await using var sawTheIndex = NpgsqlDataSource.Create(scratch.ConnectionString);
            Assert.True(await ProcedureStatsNameIndex.IsPresentAsync(sawTheIndex, ct));
            Assert.Same(DarlingTrendReader.ProcedureDurationTrendOneProcedureByNameSql, await DarlingTrendReader.OneProcedureRawSqlAsync(sawTheIndex, ct));
            Assert.Contains(ProcedureStatsNameIndex.IndexName, await PlanAsync(DarlingTrendReader.ProcedureDurationTrendOneProcedureByNameSql), StringComparison.Ordinal);

            var withIndex = RawTrendOf(await ToolAsync(sawTheIndex));
            var points = JsonDocument.Parse(withIndex).RootElement.EnumerateArray().ToList();
            Assert.NotEmpty(points);
            Assert.Equal(expectedRate, points[^1].GetProperty("elapsed_ms_per_second").GetDouble(), 6);

            /* Without it: a data source that has not seen the index is told no and runs the filter text, to the same answer. */
            await ExecAsync(connection, "DROP INDEX collect." + ProcedureStatsNameIndex.IndexName, ct);
            await ExecAsync(connection, "ANALYZE collect.procedure_stats", ct);
            await using var sawNoIndex = NpgsqlDataSource.Create(scratch.ConnectionString);
            Assert.False(await ProcedureStatsNameIndex.IsPresentAsync(sawNoIndex, ct));
            Assert.Same(DarlingTrendReader.ProcedureDurationTrendOneProcedureSql, await DarlingTrendReader.OneProcedureRawSqlAsync(sawNoIndex, ct));
            Assert.Equal(withIndex, RawTrendOf(await ToolAsync(sawNoIndex)));

            /* The data source that saw the index keeps the by-name text, and that read is the same answer with the index gone. */
            Assert.Same(DarlingTrendReader.ProcedureDurationTrendOneProcedureByNameSql, await DarlingTrendReader.OneProcedureRawSqlAsync(sawTheIndex, ct));
            Assert.Equal(withIndex, RawTrendOf(await ToolAsync(sawTheIndex)));

            /* A start with rows in the table, on a clock where one build takes the whole build time a start shares out. The restart-row
               index is missing too and is earlier in the list, so this start builds that one and names this one as waiting, and the
               data source that was told no is told no again. */
            await ExecAsync(connection, "DROP INDEX collect." + PgTableTuning.ProcedureStatsRestartRowIndexName, ct);
            var firstStart = new CapturingTestLogger();
            await PgTableTuning.ApplyAsync(connection, firstStart, hourly: false, new TuningStartPasses.BuildClock(secondsPerBuild: 300), 300, ct);
            Assert.Single(firstStart.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
                && l.Contains(ProcedureStatsNameIndex.IndexName, StringComparison.Ordinal)
                && l.Contains("built at the next start", StringComparison.Ordinal));
            Assert.False(await ProcedureStatsNameIndex.IsPresentAsync(sawNoIndex, ct));
            Assert.Same(DarlingTrendReader.ProcedureDurationTrendOneProcedureSql, await DarlingTrendReader.OneProcedureRawSqlAsync(sawNoIndex, ct));

            /* The next start builds it. The statement is a no-op where the index is, and the data source that was told no asks again. */
            var secondStart = new CapturingTestLogger();
            await PgTableTuning.ApplyAsync(connection, secondStart, ct);
            Assert.DoesNotContain(secondStart.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
            await ExecAsync(connection, ProcedureStatsNameIndex.CreateSql, ct);
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(
                connection, "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND tablename = 'procedure_stats' AND indexname = '" + ProcedureStatsNameIndex.IndexName + "'", ct),
                CultureInfo.InvariantCulture));
            Assert.True(await ProcedureStatsNameIndex.IsPresentAsync(sawNoIndex, ct));
            Assert.Same(DarlingTrendReader.ProcedureDurationTrendOneProcedureByNameSql, await DarlingTrendReader.OneProcedureRawSqlAsync(sawNoIndex, ct));
            Assert.Equal(withIndex, RawTrendOf(await ToolAsync(sawNoIndex)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
