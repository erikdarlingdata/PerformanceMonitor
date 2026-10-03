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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4955: the per-server collection-health statements (the MCP tool's and the viewer grid's) used to rank every
/// run of the seven-day window four (MCP) or three (viewer) times with <c>ROW_NUMBER()</c> only to read the
/// NEWEST row of a class, and now read those rows through keyed lookups instead. A source pin cannot tell a
/// lookup that picks the same row as the rank from one that picks a neighbour, so this class runs the pre-change
/// statements (<see cref="CollectionHealthOracleSql"/>, kept verbatim) beside the current ones over one seeded
/// store and requires every column of every row to agree - name, type and value - across the ties and the empty
/// classes where the two shapes could part: equal timestamps with different statuses and different texts, a
/// failure with text older than one without, notes that a run abandoned, a streak with and without a break, a
/// collector that only skipped, a collector with one run, fan-out rows tied on the slowest item, a status the
/// column cannot hold in the real table (a NULL one, seeded into a session-local copy of the view), and rows just
/// before the window.
///
/// <para>Where the oracle's own order is not total (two rows equal on every ORDER BY key and different in a
/// selected column) either row is a right answer, so no such case is seeded.</para>
///
/// <para>Each live test mints its own scratch database, so the rows it seeds and the plans it explains are its own:
/// no other class's rows or chunks can reach them (#4650).</para>
/// </summary>
/* #1776 own-store: each live test mints its own scratch database, so no other class's rows or chunks shape what it
   reads or how its plans come out. */
[Collection("live-postgres")]
public sealed class CollectionHealthKeyedLookupParityTests
{
    private const int ServerId = -4955001;
    private const string ServerName = "keyed-lookup-parity";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly Regex s_fullWindowRank = new(
        @"ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION\s+BY\s+collector_name", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    [Theory]
    [InlineData("mcp")]
    [InlineData("viewer")]
    public void TheStatementDoesNotRankTheWholeWindow(string which)
    {
        var (current, oracle) = which == "mcp"
            ? (DarlingDataReader.CollectionHealthSql, CollectionHealthOracleSql.Mcp)
            : (ViewerDataService.CollectionHealthSql, CollectionHealthOracleSql.Viewer);

        /* The oracle is the shape being left: a rank per newest-row column over the whole window. */
        Assert.Matches(s_fullWindowRank, oracle);

        Assert.DoesNotMatch(s_fullWindowRank, current);
        foreach (var rank in new[] { "note_rank", "error_rank", "slowest_rank", "recency_rank" })
            Assert.DoesNotContain(rank, current, StringComparison.Ordinal);

        /* The newest rows come from lookups keyed on an instant the plain aggregate found, one per class. */
        Assert.Contains("LEFT JOIN LATERAL", current, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheCurrentPlansRankNothingOverTheWindow_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-health plan test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = Now();
            await SeedEdgeCasesAsync(connection, now, ct);
            var windowStart = DarlingMcpTestData.Naive(now.AddDays(-7));

            /* The oracle's plans are the shape this pin is about: one WindowAgg per rank. */
            Assert.True(
                await CountWindowAggsAsync(connection, CollectionHealthOracleSql.Mcp, windowStart, ct) >= 4,
                "the oracle should still show one WindowAgg per rank");
            Assert.True(
                await CountWindowAggsAsync(connection, CollectionHealthOracleSql.Viewer, windowStart, ct) >= 3,
                "the viewer oracle should still show one WindowAgg per rank");

            /* The viewer's statement has no window left at all; the MCP statement keeps ONE, the trailing
               zero-row count, over the rows from the newest streak break on (a lookup, never the window). */
            Assert.Equal(0, await CountWindowAggsAsync(connection, ViewerDataService.CollectionHealthSql, windowStart, ct));
            Assert.Equal(1, await CountWindowAggsAsync(connection, DarlingDataReader.CollectionHealthSql, windowStart, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task BothStatements_ReturnTheOraclesRows_OverEveryEdgeCase_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-health parity test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = Now();
            await SeedEdgeCasesAsync(connection, now, ct);
            var windowStart = DarlingMcpTestData.Naive(now.AddDays(-7));

            /* ── the real store ── */
            var mcpOracle = await ReadAsync(connection, CollectionHealthOracleSql.Mcp, windowStart, ct);
            var mcpCurrent = await ReadAsync(connection, DarlingDataReader.CollectionHealthSql, windowStart, ct);
            Assert.Equal(31, mcpOracle.Columns.Count);
            AssertSameRows("mcp", mcpOracle, mcpCurrent);
            AssertOracleSaw(mcpOracle, now);

            var viewerOracle = await ReadAsync(connection, CollectionHealthOracleSql.Viewer, windowStart, ct);
            var viewerCurrent = await ReadAsync(connection, ViewerDataService.CollectionHealthSql, windowStart, ct);
            Assert.Equal(19, viewerOracle.Columns.Count);
            AssertSameRows("viewer", viewerOracle, viewerCurrent);

            /* ── a session-local copy of the view, for the rows the table's NOT NULL status cannot hold ──
               Both statements name v_collection_log unqualified, and pg_temp is searched first, so a temp
               table of that name (made by CTAS, which keeps no NOT NULL) stands in for the view on this
               connection alone and is gone with it. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "CREATE TEMP TABLE v_collection_log AS SELECT * FROM collect.v_collection_log WHERE server_id = $1", ServerId);
            var nullStatusCasesRan = false;
            try
            {
                await SeedNullStatusCasesAsync(connection, now, ct);

                var mcpOracleNull = await ReadAsync(connection, CollectionHealthOracleSql.Mcp, windowStart, ct);
                var mcpCurrentNull = await ReadAsync(connection, DarlingDataReader.CollectionHealthSql, windowStart, ct);
                AssertSameRows("mcp+null status", mcpOracleNull, mcpCurrentNull);
                var nullTie = mcpOracleNull.Row("null_status_tie");
                Assert.Null(nullTie["current_status"]);
                Assert.Null(nullTie["latest_run_note"]);
                Assert.Equal(3L, nullTie["trailing_zero_row_success_runs"]);

                var viewerOracleNull = await ReadAsync(connection, CollectionHealthOracleSql.Viewer, windowStart, ct);
                var viewerCurrentNull = await ReadAsync(connection, ViewerDataService.CollectionHealthSql, windowStart, ct);
                AssertSameRows("viewer+null status", viewerOracleNull, viewerCurrentNull);
                Assert.Null(viewerOracleNull.Row("null_status_tie")["latest_run_note"]);
                nullStatusCasesRan = true;
            }
            finally
            {
                /* #1902: RunOwnedAsync, not RunAsync - the temp table is session-local, so the drop has to run on
                   this very connection; a fresh one would leave the shadow in place and report success. */
                await LiveStoreCleanup.RunOwnedAsync(nullStatusCasesRan, async () =>
                    await DarlingMcpTestData.ExecAsync(connection, ct, "DROP TABLE IF EXISTS pg_temp.v_collection_log"));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    // ── what the oracle must show for the seeded edges: the seeds prove the cases they name ───────────────────

    private static void AssertOracleSaw(Result mcp, DateTime now)
    {
        /* Two rows at the newest instant, SUCCESS and ERROR: status DESC puts SUCCESS first, so exactly the one
           zero-row success sits ahead of the newest break. */
        var tieStatus = mcp.Row("tie_status");
        Assert.Equal("SUCCESS", tieStatus["current_status"]);
        Assert.Equal(1L, tieStatus["trailing_zero_row_success_runs"]);
        Assert.Equal("tie error", tieStatus["last_error"]);

        /* Two failures at one instant: the greater text wins, and the older instant's lesser text does not. */
        var tieText = mcp.Row("tie_error_text");
        Assert.Equal("err_zzz", tieText["last_error"]);
        Assert.Equal("note_z", tieText["last_note"]);

        /* A failure with text older than one without: the text is the older run's, the time is the newer's. */
        var oldText = mcp.Row("old_text_new_textless");
        Assert.Equal("old text", oldText["last_error"]);
        Assert.Equal(DarlingMcpTestData.Naive(now.AddMinutes(-100)), oldText["last_error_time"]);

        /* A SUCCESS note and a failure with text at one instant: neither class picks the other's row. */
        var sameInstant = mcp.Row("note_vs_error_same_instant");
        Assert.Equal("a note", sameInstant["last_note"]);
        Assert.Equal("an error", sameInstant["last_error"]);

        /* No failure, or failures with no text: no last_error. */
        Assert.Null(mcp.Row("no_failures")["last_error"]);
        Assert.Null(mcp.Row("failures_no_text")["last_error"]);
        Assert.Null(mcp.Row("failures_no_text")["last_note"]);

        /* Streaks: broken by an error, never broken, never a success, a NULL predicate that is no break. */
        Assert.Equal(4L, mcp.Row("streak_with_break")["trailing_zero_row_success_runs"]);
        Assert.Equal(6L, mcp.Row("streak_no_break")["trailing_zero_row_success_runs"]);
        Assert.Equal(0L, mcp.Row("skipped_only")["trailing_zero_row_success_runs"]);
        Assert.Equal(2L, mcp.Row("null_predicate")["trailing_zero_row_success_runs"]);
        Assert.Equal(2L, mcp.Row("notes_and_abandoned")["trailing_zero_row_success_runs"]);
        Assert.Equal(1L, mcp.Row("one_row")["total_runs"]);

        /* Fan-out: the newest of the rows tied on the slowest item, and the NULL one never wins. */
        var fanout = mcp.Row("fanout_ties");
        Assert.Equal("db_new_tied", fanout["slowest_item"]);
        Assert.Equal(6, fanout["fanout_items"]);
        Assert.Equal(5000, fanout["slowest_item_ms"]);
        Assert.Equal(38000, fanout["slowest_run_duration_ms"]);

        /* A dearest run with no duration reports no duration. */
        Assert.Null(mcp.Row("null_duration")["slowest_run_duration_ms"]);
        Assert.Equal(900, mcp.Row("null_duration")["slowest_item_ms"]);

        /* No row ever carried a slowest item: rank 1 falls through to the NEWEST row, whose own columns show. */
        var none = mcp.Row("no_fanout_newest");
        Assert.Null(none["slowest_item_ms"]);
        Assert.Equal(777, none["slowest_run_duration_ms"]);
        Assert.Equal(2, none["fanout_items"]);
        Assert.Equal("x", none["slowest_item"]);

        /* Rows before the window are not in it: the only text on this collector is older than the window. */
        var before = mcp.Row("before_window");
        Assert.Null(before["last_error"]);
        Assert.Null(before["last_note"]);
        Assert.Equal(2L, before["total_runs"]);
    }

    // ── the seeds ─────────────────────────────────────────────────────────────────────────────────────────────

    private static DateTime Now()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMillisecond), DateTimeKind.Utc);
    }

    private static async Task SeedEdgeCasesAsync(NpgsqlConnection c, DateTime now, CancellationToken ct)
    {
        DateTime Ago(double minutes) => now.AddMinutes(-minutes);
        Task Seed(string collector, double minutesAgo, string status, string? message = null,
                  int? durationMs = 100, int? rows = 10, int? fanoutItems = null, string? slowestItem = null,
                  int? slowestItemMs = null) =>
            SeedAsync(c, ct, collector, Ago(minutesAgo), status, message, durationMs, rows, fanoutItems, slowestItem, slowestItemMs);

        /* Two rows at the newest instant with different statuses: current_status takes the greater status, and
           the streak count puts the zero-row SUCCESS ahead of the ERROR that breaks it. */
        await Seed("tie_status", 300, "SUCCESS", rows: 10);
        await Seed("tie_status", 200, "SUCCESS", rows: 0);
        await Seed("tie_status", 100, "SUCCESS", rows: 0);
        await Seed("tie_status", 50, "SUCCESS", rows: 0);
        await Seed("tie_status", 50, "ERROR", "tie error", rows: 0);

        /* Two failures at one instant with different texts, and two notes at one (older) instant. */
        await Seed("tie_error_text", 400, "SUCCESS", "note_a", rows: 0);
        await Seed("tie_error_text", 400, "SUCCESS", "note_z", rows: 0);
        await Seed("tie_error_text", 300, "ERROR", "err_aaa", rows: 0);
        await Seed("tie_error_text", 300, "PERMISSIONS", "err_zzz", rows: 0);
        await Seed("tie_error_text", 100, "SUCCESS", rows: 5);

        /* NULL durations, one of them on the dearest fan-out run. */
        await Seed("null_duration", 200, "SUCCESS", durationMs: null);
        await Seed("null_duration", 150, "SUCCESS", durationMs: null, fanoutItems: 3, slowestItem: "db_a", slowestItemMs: 900);
        await Seed("null_duration", 100, "SUCCESS", durationMs: 40);

        /* No failures at all. */
        await Seed("no_failures", 300, "SUCCESS");
        await Seed("no_failures", 200, "SUCCESS");
        await Seed("no_failures", 100, "SUCCESS");

        /* Failures that carry no text. */
        await Seed("failures_no_text", 200, "ERROR", rows: 0);
        await Seed("failures_no_text", 100, "PERMISSIONS", rows: 0);

        /* A failure with text older than a failure without: the text is the older run's. */
        await Seed("old_text_new_textless", 300, "ERROR", "old text", rows: 0);
        await Seed("old_text_new_textless", 100, "ERROR", rows: 0);
        await Seed("old_text_new_textless", 50, "SUCCESS", rows: 3);

        /* A SUCCESS note and a failure with text at ONE instant, neither the newest run. */
        await Seed("note_vs_error_same_instant", 200, "SUCCESS", "a note", rows: 0);
        await Seed("note_vs_error_same_instant", 200, "ERROR", "an error", rows: 0);
        await Seed("note_vs_error_same_instant", 100, "SUCCESS", rows: 4);

        /* Notes, a run the wall-clock budget abandoned before and after the ABANDONED status existed. */
        var budget = EnumeratedCollectorDriver.WholeCycleBudgetNote(120);
        await Seed("notes_and_abandoned", 400, "SUCCESS", budget, rows: 0);
        await Seed("notes_and_abandoned", 300, EnumeratedCollectorDriver.AbandonedStatus, budget, rows: 0);
        await Seed("notes_and_abandoned", 200, "SUCCESS", EnumeratedCollectorDriver.EmptyEnumerationMessage, rows: 0);
        await Seed("notes_and_abandoned", 100, "SUCCESS", rows: 0);

        /* A streak with a break, one without, and a collector that only ever skipped. */
        await Seed("streak_with_break", 700, "SUCCESS", rows: 10);
        await Seed("streak_with_break", 600, "SUCCESS", rows: 0);
        await Seed("streak_with_break", 500, "SUCCESS", rows: 0);
        await Seed("streak_with_break", 400, "SUCCESS", rows: 0);
        await Seed("streak_with_break", 300, "ERROR", "boom", rows: 0);
        foreach (var minutes in new double[] { 200, 150, 100, 50 })
            await Seed("streak_with_break", minutes, "SUCCESS", rows: 0);
        foreach (var minutes in new double[] { 600, 500, 400, 300, 200, 100 })
            await Seed("streak_no_break", minutes, "SUCCESS", rows: 0);
        foreach (var minutes in new double[] { 300, 200, 100 })
            await Seed("skipped_only", minutes, "SKIPPED", rows: 0);

        /* A row whose streak predicate is NULL (a NULL row count beside the budget note) is no break. */
        await Seed("null_predicate", 300, "SUCCESS", rows: 5);
        await Seed("null_predicate", 200, "SUCCESS", budget, rows: null);
        await Seed("null_predicate", 100, "SUCCESS", rows: 0);

        /* One run only. */
        await Seed("one_row", 100, "SUCCESS", rows: 1);

        /* Three rows tied on the slowest item (the newest of them wins), a smaller one, and a NEWER row with no
           slowest item at all. */
        await Seed("fanout_ties", 500, "SUCCESS", durationMs: 40000, fanoutItems: 8, slowestItem: "db_old", slowestItemMs: 5000);
        await Seed("fanout_ties", 400, "SUCCESS", durationMs: 39000, fanoutItems: 7, slowestItem: "db_mid", slowestItemMs: 5000);
        await Seed("fanout_ties", 300, "SUCCESS", durationMs: 38000, fanoutItems: 6, slowestItem: "db_new_tied", slowestItemMs: 5000);
        await Seed("fanout_ties", 200, "SUCCESS", durationMs: 20, fanoutItems: 5, slowestItem: "db_small", slowestItemMs: 100);
        await Seed("fanout_ties", 100, "SUCCESS", durationMs: 10);

        /* No row has a slowest item: the newest row's own fan-out columns and duration come out. */
        await Seed("no_fanout_newest", 300, "SUCCESS", durationMs: 50);
        await Seed("no_fanout_newest", 200, "SUCCESS", durationMs: 60);
        await Seed("no_fanout_newest", 100, "SUCCESS", durationMs: 777, fanoutItems: 2, slowestItem: "x");

        /* Every status the counts name, with text on the failures. */
        await Seed("status_mix", 800, "EXTENSION_MISSING", "install the extension");
        await Seed("status_mix", 700, "SESSION_MISSING", "no session");
        await Seed("status_mix", 600, "YIELDED", "yielded");
        await Seed("status_mix", 500, "CANCELLED", "cancelled");
        await Seed("status_mix", 400, "PERMISSIONS", "denied");
        await Seed("status_mix", 300, "SKIPPED");
        await Seed("status_mix", 200, "ERROR", "failed");
        await Seed("status_mix", 100, "SUCCESS", "a note", rows: 2);

        /* Rows just before the window, a row exactly on its start, and one after. */
        var windowStartMinutes = 7 * 24 * 60.0;
        await Seed("before_window", windowStartMinutes + 1, "ERROR", "before window");
        await Seed("before_window", windowStartMinutes + 0.001, "SUCCESS", "old note");
        await Seed("before_window", windowStartMinutes, "SUCCESS", rows: 0);
        await Seed("before_window", 100, "SUCCESS", rows: 0);
    }

    /// <summary>Rows the real table cannot hold (its status is NOT NULL), into the session-local copy of the view.</summary>
    private static async Task SeedNullStatusCasesAsync(NpgsqlConnection c, DateTime now, CancellationToken ct)
    {
        Task Seed(double minutesAgo, string? status, string? message, int? rows) =>
            DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO pg_temp.v_collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1, $2, $3, 'null_status_tie', $4, 100, $5, $6, $7)",
                CollectionIdGenerator.Next(), ServerId, ServerName,
                DarlingMcpTestData.Naive(now.AddMinutes(-minutesAgo)), status, message, rows);

        /* A NULL status beside a SUCCESS at the newest instant: DESC puts the NULL first, so the newest run's
           status and note read as NULL; the NULL-status row with a zero count is no break, the one with a
           count is. */
        await Seed(300, "ERROR", "e", 0);
        await Seed(250, null, null, 10);
        await Seed(200, "SUCCESS", null, 0);
        await Seed(100, null, "null note", 0);
        await Seed(100, "SUCCESS", null, 0);
    }

    private static Task SeedAsync(
        NpgsqlConnection c, CancellationToken ct, string collector, DateTime atUtc, string status, string? message,
        int? durationMs, int? rows, int? fanoutItems, string? slowestItem, int? slowestItemMs) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, sql_duration_ms, duckdb_duration_ms, fanout_item_count, slowest_item, slowest_item_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(atUtc),
            durationMs, status, message, rows, 80, 20, fanoutItems, slowestItem, slowestItemMs);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }

    // ── reading and comparing ─────────────────────────────────────────────────────────────────────────────────

    private sealed record Column(string Name, string PgType, Type ClrType);

    private sealed class Result
    {
        public List<Column> Columns { get; } = new();

        public List<object?[]> Rows { get; } = new();

        public Dictionary<string, object?> Row(string collector)
        {
            var row = Rows.Single(r => (string)r[0]! == collector);
            return Columns.Select((col, i) => (col.Name, Value: row[i])).ToDictionary(p => p.Name, p => p.Value);
        }
    }

    private static NpgsqlCommand Bind(NpgsqlConnection connection, string sql, DateTime windowStart)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = windowStart });
        return command;
    }

    private static async Task<Result> ReadAsync(NpgsqlConnection connection, string sql, DateTime windowStart, CancellationToken ct)
    {
        await using var command = Bind(connection, sql, windowStart);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new Result();
        for (var i = 0; i < reader.FieldCount; i++)
            result.Columns.Add(new Column(reader.GetName(i), reader.GetDataTypeName(i), reader.GetFieldType(i)));
        while (await reader.ReadAsync(ct))
        {
            var values = new object?[reader.FieldCount];
            for (var i = 0; i < values.Length; i++)
                values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            result.Rows.Add(values);
        }

        return result;
    }

    private static void AssertSameRows(string label, Result oracle, Result current)
    {
        Assert.Equal(oracle.Columns, current.Columns);
        Assert.Equal(oracle.Rows.Count, current.Rows.Count);
        Assert.True(oracle.Rows.Count >= 10, label + ": the seed should give the oracle many collectors");
        for (var r = 0; r < oracle.Rows.Count; r++)
        {
            for (var i = 0; i < oracle.Columns.Count; i++)
            {
                Assert.True(
                    Equals(oracle.Rows[r][i], current.Rows[r][i]),
                    $"{label}: collector {oracle.Rows[r][0]}, column {oracle.Columns[i].Name}: "
                    + $"oracle <{oracle.Rows[r][i] ?? "NULL"}> but current <{current.Rows[r][i] ?? "NULL"}>");
            }
        }
    }

    private static async Task<int> CountWindowAggsAsync(
        NpgsqlConnection connection, string sql, DateTime windowStart, CancellationToken ct)
    {
        await using var command = Bind(connection, "EXPLAIN (FORMAT JSON) " + sql, windowStart);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        return CountNodes(document.RootElement, "WindowAgg");
    }

    private static int CountNodes(JsonElement element, string nodeType)
    {
        var count = 0;
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                if (element.TryGetProperty("Node Type", out var type) && type.GetString() == nodeType)
                    count++;
                foreach (var property in element.EnumerateObject())
                    count += CountNodes(property.Value, nodeType);
                break;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                    count += CountNodes(item, nodeType);
                break;
        }

        return count;
    }
}
