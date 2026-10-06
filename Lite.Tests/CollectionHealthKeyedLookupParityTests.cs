/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5371: Lite's Collection Health read no longer ranks every run of the 7-day window four times. It is one
/// pass of plain aggregates per collector, then one keyed lookup per newest-row column, each reading only the
/// rows AT an instant the aggregate already found (Darling's #4955 shape, with the lookups as aggregates so
/// DuckDB plans no window of its own).
///
/// <para><b>What these pin.</b> The oracle tests run today's statement (<see cref="CollectionHealthOracleSql"/>,
/// kept verbatim) and the shipped one over the SAME seeded store, hot table plus a real parquet archive file so
/// the view's union is real, and require every column of every row to agree in name, type and value - including
/// every tie-break the old ranks defined. The mutation tests then plant a tie-break flip in the shipped text and
/// require the comparison to go red, so a harness that compared nothing could not pass. The plan tests pin the
/// shape the rewrite exists for: no window operator, and a parquet scan that carries the window filter.</para>
///
/// <para><b>What they do NOT pin.</b> A timing. No store from the field was timed for #5371; the 248 s reading
/// is not reproduced here and these tests make no claim about it.</para>
///
/// <para>One tie the old statement left undefined is deliberately not seeded: two rows sharing BOTH an instant
/// and a status with different notes or fan-out columns, or a zero-row success sharing both with the first
/// streak breaker. The old ranks ordered those arbitrarily, so there is no answer to hold the new one to.</para>
/// </summary>
public sealed class CollectionHealthKeyedLookupParityTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 5371;
    private const int OtherServerId = 5372;
    private const string ArchiveFileName = "202610_collection_log.parquet";
    private const string BudgetNote = "wall-clock budget (600s) reached; cycle abandoned";

    private readonly DuckDbInitializer _duckDb;
    private readonly DateTime _now;
    private long _nextId = 1;
    private long _nextArchivedId = 1_000_000;

    public CollectionHealthKeyedLookupParityTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        var t = DateTime.UtcNow;
        _now = new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);
    }

    public void Dispose() => DeleteArchiveFile();

    private string ArchiveFile => Path.Combine(_duckDb.ArchivePath, ArchiveFileName);

    private void DeleteArchiveFile()
    {
        try { if (File.Exists(ArchiveFile)) File.Delete(ArchiveFile); }
        catch { /* best effort: the fixture removes the whole folder */ }
    }

    private sealed record Run(
        string Collector,
        int MinutesAgo,
        string Status,
        string? Message = null,
        int? DurationMs = 100,
        int? RowsCollected = 10,
        int? FanoutItems = null,
        string? SlowestItem = null,
        int? SlowestItemMs = null,
        bool Archived = false,
        int Server = ServerId);

    private static Run Ok(string c, int ago, string? message = null, int? rows = 0, int? duration = 100) =>
        new(c, ago, "SUCCESS", message, duration, rows);

    /// <summary>Every case the analysis names, one collector each, so a disagreement names the case.</summary>
    private List<Run> Cases()
    {
        var r = new List<Run>
        {
            /* Two rows at one instant, different statuses: the greater status is the newest run (SUCCESS beats
               SKIPPED), and its note is the latest_run_note. */
            Ok("tie_status", 60, rows: 5),
            Ok("tie_status", 10, "note at ten", rows: 0),
            new("tie_status", 10, "SKIPPED", null, 100, 0),
            /* The hazard the keyed lookup could have walked into: the winning status carries NO note while the
               loser does. Another row's note must not surface as the newest run's. */
            Ok("tie_status_null_winner", 10, null, rows: 0),
            new("tie_status_null_winner", 10, "SKIPPED", "other note", 100, 0),

            /* Two (three) failure texts at one instant: the greater message wins, whatever its status within
               the failing set. An older failure and a newer failure with NO text sit around them. */
            new("tie_error_text", 50, "ERROR", "mmm older", 100, 0),
            new("tie_error_text", 20, "ERROR", "aaa tie", 100, 0),
            new("tie_error_text", 20, "PERMISSIONS", "zzz tie", 100, 0),
            new("tie_error_text", 20, "ERROR", "qqq tie", 100, 0),
            new("tie_error_text", 5, "ERROR", null, 100, 0),

            /* Notes older than a clean run: the note survives a later clean run, the newest run's note is NULL. */
            Ok("notes_older_than_clean", 100, "old note"),
            Ok("notes_older_than_clean", 30, "newer note"),
            Ok("notes_older_than_clean", 5, null),
            /* Two notes at one instant: the greater message wins (a later clean run sits after them). */
            Ok("tie_note_text", 30, "aaa note"),
            Ok("tie_note_text", 30, "zzz note"),
            Ok("tie_note_text", 20, null),
            /* A SUCCESS note must never read as a last error when no failure carried text. */
            new("note_not_an_error", 40, "ERROR", null, 100, 0),
            Ok("note_not_an_error", 20, "only a note"),

            /* Fan-out tie: two runs share the dearest item (900 ms), the NEWER one is the row; a dearer-looking
               duration elsewhere must not win. */
            new("fanout_tie", 300, "SUCCESS", null, 1000, 5, 5, "dbA", 900),
            new("fanout_tie", 200, "SUCCESS", null, 1100, 5, 6, "dbB", 900),
            new("fanout_tie", 100, "SUCCESS", null, 2000, 5, 7, "dbC", 400),
            new("fanout_tie", 50, "SUCCESS", null, 50, 5),
            /* No dearest item at all: the row is the newest run, whatever it carries. */
            new("fanout_none", 300, "SUCCESS", null, 10, 5),
            new("fanout_none", 200, "SUCCESS", null, 20, 5),
            new("fanout_none", 100, "SUCCESS", null, 77, 5, 3, null, null),

            /* The zero-row-success streak, in every shape. */
            Ok("streak_trailing", 500, rows: 9),
            Ok("streak_trailing", 400), Ok("streak_trailing", 300), Ok("streak_trailing", 200), Ok("streak_trailing", 100),
            /* No run breaks it (a NULL rows_collected counts as zero): the answer is the window's run count. */
            Ok("streak_never_broken", 400), Ok("streak_never_broken", 300),
            new("streak_never_broken", 200, "SUCCESS", null, 100, null),
            Ok("streak_never_broken", 100),
            /* The newest run itself breaks it. */
            Ok("streak_broken_now", 300), Ok("streak_broken_now", 200),
            new("streak_broken_now", 100, "ERROR", "boom", 100, 0),
            /* A breaker and a zero-row SUCCESS at one instant. SUCCESS sorts after ERROR in status DESC, so it is
               AHEAD of the breaker and counts: 100, 50 and the tied SUCCESS = 3. */
            Ok("streak_tie_success_ahead", 400), Ok("streak_tie_success_ahead", 300),
            new("streak_tie_success_ahead", 200, "ERROR", "tied breaker", 100, 0),
            Ok("streak_tie_success_ahead", 200),
            Ok("streak_tie_success_ahead", 100), Ok("streak_tie_success_ahead", 50),
            /* YIELDED sorts BEFORE SUCCESS, so the tied SUCCESS is behind the breaker: only 100 and 50 count. */
            Ok("streak_tie_breaker_ahead", 400),
            new("streak_tie_breaker_ahead", 200, "YIELDED", null, 100, 0),
            Ok("streak_tie_breaker_ahead", 200),
            Ok("streak_tie_breaker_ahead", 100), Ok("streak_tie_breaker_ahead", 50),
            /* A pre-#2803 abandonment (SUCCESS, zero rows, the budget note) is data LOSS and breaks the streak. */
            Ok("streak_abandoned_note", 300),
            Ok("streak_abandoned_note", 200, BudgetNote),
            Ok("streak_abandoned_note", 100),
            new("streak_abandoned_note", 50, "ABANDONED", BudgetNote, 100, 0),

            /* A skip streak: the status changes along it, a failure with no text is the newest failure. */
            Ok("skip_streak", 500, rows: 3),
            new("skip_streak", 400, "SESSION_MISSING", null, 100, 0),
            new("skip_streak", 300, "EXTENSION_MISSING", "ext missing", 100, 0),
            new("skip_streak", 200, "PERMISSIONS", "denied", 100, 0),
            new("skip_streak", 100, "PERMISSIONS", null, 100, 0),

            /* Rows split between the hot table and the parquet archive, plus one older than the window (it must
               stay out). The newest note and newest text-bearing failure live in the archive; the failure tie
               at 8000 minutes ago spans BOTH sides of the union, so the greater message is the hot one. */
            new("split_hot_parquet", 12000, "ERROR", "ancient, outside the window", 100, 0, Archived: true),
            Ok("split_hot_parquet", 9000, "archived note", rows: 0) with { Archived = true },
            new("split_hot_parquet", 8000, "ERROR", "archived error", 100, 0, Archived: true),
            new("split_hot_parquet", 8000, "ERROR", "zz hot error", 100, 0),
            new("split_hot_parquet", 7000, "SUCCESS", null, 4000, 8, 4, "dbX", 1500, Archived: true),
            Ok("split_hot_parquet", 60, rows: 6),
            new("archive_only", 9500, "SUCCESS", "archived only", 100, 0, Archived: true),
            new("archive_only", 9400, "SUCCESS", null, 100, 5, Archived: true),

            /* Counters and the odd rows: yields, extension misses, NULL durations and row counts. */
            new("counters", 90, "YIELDED", null, null, null),
            new("counters", 80, "YIELDED", null, 120, 0),
            new("counters", 70, "EXTENSION_MISSING", "ext", 130, 0),
            new("counters", 60, "SESSION_MISSING", null, 140, 0),
            new("counters", 50, "SUCCESS", null, null, null),
            new("counters", 40, "ERROR", "err", 150, 2),

            /* Another server's rows for the same collector names, newer than ours: the server key must hold. */
            new("tie_status", 1, "ERROR", "other server error", 100, 0, Server: OtherServerId),
            new("fanout_tie", 1, "SUCCESS", "other server note", 9999, 5, 99, "dbZ", 9999, Server: OtherServerId),
        };
        return r;
    }

    private async Task SeedStoreAsync()
    {
        DeleteArchiveFile();
        Directory.CreateDirectory(_duckDb.ArchivePath);

        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using (var batch = new SeedBatch(_duckDb, connection))
            {
                foreach (var run in Cases())
                {
                    using var readLock = _duckDb.AcquireReadLock();
                    using var cmd = connection.CreateCommand();
                    cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms,
     fanout_item_count, slowest_item, slowest_item_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)";
                    cmd.Parameters.Add(new DuckDBParameter { Value = run.Archived ? _nextArchivedId++ : _nextId++ });
                    cmd.Parameters.Add(new DuckDBParameter { Value = run.Server });
                    cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
                    cmd.Parameters.Add(new DuckDBParameter { Value = run.Collector });
                    cmd.Parameters.Add(new DuckDBParameter { Value = _now.AddMinutes(-run.MinutesAgo) });
                    cmd.Parameters.Add(new DuckDBParameter { Value = (object?)run.DurationMs ?? DBNull.Value });
                    cmd.Parameters.Add(new DuckDBParameter { Value = run.Status });
                    cmd.Parameters.Add(new DuckDBParameter { Value = (object?)run.Message ?? DBNull.Value });
                    cmd.Parameters.Add(new DuckDBParameter { Value = (object?)run.RowsCollected ?? DBNull.Value });
                    cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
                    cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
                    cmd.Parameters.Add(new DuckDBParameter { Value = (object?)run.FanoutItems ?? DBNull.Value });
                    cmd.Parameters.Add(new DuckDBParameter { Value = (object?)run.SlowestItem ?? DBNull.Value });
                    cmd.Parameters.Add(new DuckDBParameter { Value = (object?)run.SlowestItemMs ?? DBNull.Value });
                    await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                }

                /* Archive the flagged rows the way archival does: COPY them out to the table's parquet file, then
                   remove them from the hot table, so the rows exist ONLY on the parquet side of the view. */
                var path = ArchiveFile.Replace("\\", "/");
                foreach (var sql in new[]
                {
                    $"COPY (SELECT * FROM collection_log WHERE log_id >= 1000000 ORDER BY log_id) TO '{path}' (FORMAT PARQUET)",
                    "DELETE FROM collection_log WHERE log_id >= 1000000",
                    $"INSERT INTO database_size_stats (collection_id, server_id, server_name, collection_time, database_name, database_id, file_id, file_type_desc, file_name, total_size_mb) VALUES (1, {ServerId}, 'TestSrv', TIMESTAMP '{_now.AddMinutes(-30):yyyy-MM-dd HH:mm:ss}', 'userdb', 5, 1, 'ROWS', 'data_0', 4.00)",
                })
                {
                    using var readLock = _duckDb.AcquireReadLock();
                    using var cmd = connection.CreateCommand();
                    cmd.CommandText = sql;
                    await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
                }

                batch.Commit();
            }
        }

        await _duckDb.CreateArchiveViewsAsync();
    }

    private sealed record Result(string[] Names, Type[] Types, List<object?[]> Rows);

    private async Task<Result> RunAsync(string sql)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = _now.AddDays(-7) });
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);

        var names = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        var types = Enumerable.Range(0, reader.FieldCount).Select(reader.GetFieldType).ToArray();
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return new Result(names, types, rows);
    }

    /// <summary>Every disagreement between the oracle and the candidate, named by collector and column.</summary>
    private static List<string> Differences(Result oracle, Result candidate)
    {
        var diffs = new List<string>();
        if (oracle.Names.Length != candidate.Names.Length)
        {
            diffs.Add($"column count {oracle.Names.Length} vs {candidate.Names.Length}");
            return diffs;
        }
        for (var i = 0; i < oracle.Names.Length; i++)
        {
            if (oracle.Names[i] != candidate.Names[i])
                diffs.Add($"column {i} name '{oracle.Names[i]}' vs '{candidate.Names[i]}'");
            if (oracle.Types[i] != candidate.Types[i])
                diffs.Add($"column {oracle.Names[i]} type {oracle.Types[i].Name} vs {candidate.Types[i].Name}");
        }
        if (oracle.Rows.Count != candidate.Rows.Count)
        {
            diffs.Add($"row count {oracle.Rows.Count} vs {candidate.Rows.Count}");
            return diffs;
        }
        for (var r = 0; r < oracle.Rows.Count; r++)
        {
            for (var c = 0; c < oracle.Names.Length; c++)
            {
                var a = oracle.Rows[r][c];
                var b = candidate.Rows[r][c];
                var same = a is double da && b is double db
                    ? Math.Abs(da - db) <= 1e-9 * Math.Max(1.0, Math.Abs(da))
                    : Equals(a, b);
                if (!same)
                    diffs.Add($"{oracle.Rows[r][0]}.{oracle.Names[c]}: oracle {Show(a)} vs candidate {Show(b)}");
            }
        }
        return diffs;
    }

    private static string Show(object? v) => v switch
    {
        null => "NULL",
        DateTime d => d.ToString("O", CultureInfo.InvariantCulture),
        _ => Convert.ToString(v, CultureInfo.InvariantCulture) ?? "",
    };

    private static object? Cell(Result result, string collector, string column)
    {
        var col = Array.IndexOf(result.Names, column);
        Assert.True(col >= 0, "no column " + column);
        return result.Rows.Single(r => (string?)r[0] == collector)[col];
    }

    [Fact]
    public async Task ShippedStatement_AgreesWithTheOldRankedOne_OnEveryColumnOfEveryRow()
    {
        await SeedStoreAsync();

        var oracle = await RunAsync(CollectionHealthOracleSql.Lite);
        var candidate = await RunAsync(LocalDataService.CollectionHealthSql);

        Assert.Equal(31, oracle.Names.Length);
        Assert.Equal(oracle.Rows.Select(r => (string?)r[0]).Distinct().Count(), oracle.Rows.Count);
        Assert.Equal(
            new[] { "tie_status", "tie_status_null_winner", "tie_error_text", "notes_older_than_clean", "tie_note_text", "note_not_an_error", "fanout_tie",
             "fanout_none", "streak_trailing", "streak_never_broken", "streak_broken_now", "streak_tie_success_ahead",
             "streak_tie_breaker_ahead", "streak_abandoned_note", "skip_streak", "split_hot_parquet", "archive_only", "counters" }
                .Order(StringComparer.Ordinal),
            oracle.Rows.Select(r => (string)r[0]!).ToArray());

        Assert.True(Differences(oracle, candidate).Count == 0, string.Join(Environment.NewLine, Differences(oracle, candidate)));
    }

    /// <summary>
    /// The oracle could be wrong the same way the candidate is, so the answers the analysis names are asserted
    /// against literals as well, on the shipped statement.
    /// </summary>
    [Fact]
    public async Task ShippedStatement_GivesTheNamedAnswers_ForEachTieAndEdge()
    {
        await SeedStoreAsync();
        var r = await RunAsync(LocalDataService.CollectionHealthSql);

        Assert.Equal("SUCCESS", Cell(r, "tie_status", "current_status"));
        Assert.Equal("note at ten", Cell(r, "tie_status", "latest_run_note"));
        Assert.Equal("SUCCESS", Cell(r, "tie_status_null_winner", "current_status"));
        Assert.Null(Cell(r, "tie_status_null_winner", "latest_run_note"));

        Assert.Equal("zzz tie", Cell(r, "tie_error_text", "last_error"));
        Assert.Equal(_now.AddMinutes(-5), Cell(r, "tie_error_text", "last_error_time"));

        Assert.Equal("newer note", Cell(r, "notes_older_than_clean", "last_note"));
        Assert.Null(Cell(r, "notes_older_than_clean", "latest_run_note"));
        Assert.Equal("zzz note", Cell(r, "tie_note_text", "last_note"));
        Assert.Null(Cell(r, "note_not_an_error", "last_error"));
        Assert.Equal("only a note", Cell(r, "note_not_an_error", "last_note"));

        Assert.Equal(6, Cell(r, "fanout_tie", "fanout_items"));
        Assert.Equal("dbB", Cell(r, "fanout_tie", "slowest_item"));
        Assert.Equal(900, Cell(r, "fanout_tie", "slowest_item_ms"));
        Assert.Equal(1100, Cell(r, "fanout_tie", "slowest_run_duration_ms"));
        Assert.Equal(3, Cell(r, "fanout_none", "fanout_items"));
        Assert.Null(Cell(r, "fanout_none", "slowest_item_ms"));
        Assert.Equal(77, Cell(r, "fanout_none", "slowest_run_duration_ms"));

        Assert.Equal(4L, Convert.ToInt64(Cell(r, "streak_trailing", "trailing_zero_row_success_runs")));
        Assert.Equal(4L, Convert.ToInt64(Cell(r, "streak_never_broken", "trailing_zero_row_success_runs")));
        Assert.Equal(0L, Convert.ToInt64(Cell(r, "streak_broken_now", "trailing_zero_row_success_runs")));
        Assert.Equal(3L, Convert.ToInt64(Cell(r, "streak_tie_success_ahead", "trailing_zero_row_success_runs")));
        Assert.Equal(2L, Convert.ToInt64(Cell(r, "streak_tie_breaker_ahead", "trailing_zero_row_success_runs")));
        Assert.Equal(0L, Convert.ToInt64(Cell(r, "streak_abandoned_note", "trailing_zero_row_success_runs")));

        Assert.Equal("PERMISSIONS", Cell(r, "skip_streak", "current_status"));
        Assert.Equal("denied", Cell(r, "skip_streak", "last_error"));
        Assert.Equal(_now.AddMinutes(-100), Cell(r, "skip_streak", "last_error_time"));

        /* Rows in the archive are read, the one older than the window is not, and a tie across the union goes
           to the greater message wherever it lives. */
        Assert.Equal("zz hot error", Cell(r, "split_hot_parquet", "last_error"));
        Assert.Equal("archived note", Cell(r, "split_hot_parquet", "last_note"));
        Assert.Equal(1500, Cell(r, "split_hot_parquet", "slowest_item_ms"));
        Assert.Equal("archived only", Cell(r, "archive_only", "last_note"));
        Assert.Equal(2L, Convert.ToInt64(Cell(r, "archive_only", "total_runs")));
        Assert.Equal(1, Convert.ToInt32(Cell(r, "archive_only", "has_user_databases")));
    }

    [Theory]
    [InlineData("MAX(f.error_message) AS error_message", "MIN(f.error_message) AS error_message", "failure text tie-break")]
    [InlineData("MAX(t.error_message) AS error_message", "MIN(t.error_message) AS error_message", "note tie-break")]
    [InlineData("arg_max(struct_pack(status := n.status", "arg_min(struct_pack(status := n.status", "newest-run status tie-break")]
    [InlineData("d.collection_time) AS pick", "-epoch_ms(d.collection_time)) AS pick", "dearest item newest-of-tied")]
    [InlineData("s.status > h.last_streak_break_status", "s.status < h.last_streak_break_status", "streak status tie-break")]
    public async Task APlantedTieBreakFlip_MakesTheOracleComparisonGoRed(string from, string to, string what)
    {
        var sql = LocalDataService.CollectionHealthSql;
        Assert.Contains(from, sql, StringComparison.Ordinal);
        var mutated = sql.Replace(from, to, StringComparison.Ordinal);

        await SeedStoreAsync();
        var oracle = await RunAsync(CollectionHealthOracleSql.Lite);
        var candidate = await RunAsync(mutated);

        Assert.True(Differences(oracle, candidate).Count > 0, $"flipping the {what} did not change any answer: the parity comparison cannot see it");
    }

    [Fact]
    public void ShippedStatement_HasNoRankingWindow_AndNoneOfTheRankNames()
    {
        var sql = LocalDataService.CollectionHealthSql;

        Assert.DoesNotContain("ROW_NUMBER() OVER (PARTITION BY collector_name", sql, StringComparison.Ordinal);
        foreach (var name in new[] { "note_rank", "error_rank", "slowest_rank", "recency_rank" })
            Assert.DoesNotContain(name, sql, StringComparison.Ordinal);

        /* No ROW_NUMBER anywhere in the statement proper and no ORDER BY ... LIMIT lookup, which DuckDB plans as
           a window per lookup. The comments discuss both, so they are stripped first. */
        var code = string.Join('\n', sql.Split('\n').Select(l => l.Contains("--", StringComparison.Ordinal) ? l[..l.IndexOf("--", StringComparison.Ordinal)] : l));
        Assert.DoesNotContain("ROW_NUMBER", code, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LIMIT", code, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FROM v_collection_log", code, StringComparison.Ordinal);

        /* The positive control for the negatives above: the oracle is the statement they are written against. */
        Assert.Contains("ROW_NUMBER() OVER", CollectionHealthOracleSql.Lite, StringComparison.Ordinal);
        Assert.Contains("PARTITION BY collector_name", CollectionHealthOracleSql.Lite, StringComparison.Ordinal);
        foreach (var name in new[] { "note_rank", "error_rank", "slowest_rank", "recency_rank" })
            Assert.Contains(name, CollectionHealthOracleSql.Lite, StringComparison.Ordinal);

        /* And the lookups ARE keyed on the aggregate's instants, each with the window bound repeated. */
        foreach (var key in new[] { "h.last_run_time", "h.last_failure_text_time", "h.last_note_time", "h.dearest_item_ms", "h.last_streak_break_time" })
            Assert.Contains("= " + key, sql, StringComparison.Ordinal);
    }

    /// <summary>The physical plan as JSON: one node per operator, each with a name and its extra_info.</summary>
    private async Task<JsonElement> ExplainAsync(string sql)
    {
        var result = await RunAsync("EXPLAIN (FORMAT JSON) " + sql);
        var json = Convert.ToString(result.Rows.Single()[^1], CultureInfo.InvariantCulture)!;
        var tmp = Environment.GetEnvironmentVariable("H5371_PLAN_DUMP");
        if (!string.IsNullOrEmpty(tmp)) File.AppendAllText(tmp, json + Environment.NewLine);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    private static IEnumerable<JsonElement> Nodes(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in element.EnumerateArray())
                foreach (var n in Nodes(item))
                    yield return n;
        }
        else if (element.ValueKind == JsonValueKind.Object)
        {
            if (element.TryGetProperty("name", out _))
                yield return element;
            if (element.TryGetProperty("children", out var children))
                foreach (var n in Nodes(children))
                    yield return n;
        }
    }

    private static List<string> OperatorNames(JsonElement plan) =>
        [.. Nodes(plan).Select(n => n.GetProperty("name").GetString() ?? "")];

    [Fact]
    public async Task ShippedPlan_HasNoWindowOperator_WhereTheOldOneHadFour()
    {
        await SeedStoreAsync();

        var oldOps = OperatorNames(await ExplainAsync(CollectionHealthOracleSql.Lite));
        var newOps = OperatorNames(await ExplainAsync(LocalDataService.CollectionHealthSql));

        /* The positive control: the same check finds the windows in the old plan. */
        Assert.True(oldOps.Count(o => o.Contains("WINDOW", StringComparison.Ordinal)) >= 1, string.Join(", ", oldOps));
        Assert.DoesNotContain(newOps, o => o.Contains("WINDOW", StringComparison.Ordinal));
        Assert.DoesNotContain(newOps, o => o.Contains("TOP_N", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ShippedPlan_EveryParquetScanCarriesTheWindowFilter()
    {
        await SeedStoreAsync();

        var plan = await ExplainAsync(LocalDataService.CollectionHealthSql);
        var scans = Nodes(plan).Where(n => (n.GetProperty("name").GetString() ?? "").Contains("PARQUET", StringComparison.Ordinal)).ToList();

        Assert.NotEmpty(scans);
        foreach (var scan in scans)
        {
            Assert.True(scan.TryGetProperty("extra_info", out var info), scan.GetRawText());
            Assert.Contains("collection_time", info.GetRawText(), StringComparison.Ordinal);
        }
    }
}
