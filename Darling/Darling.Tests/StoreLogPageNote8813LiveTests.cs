/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_store_log against a store that holds a capture (#8813, #8814): a page cut by <c>limit</c> says it is a page and
/// gives the total, the errors come ahead of the repeated LOG rows, a restart's two routine messages stay out of
/// <c>unclassified</c>, and an autovacuum cancel names the table that owns the TOAST relation in its CONTEXT.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreLogPageNote8813LiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #8813 store-log tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(connection, postgres, ct);
    }

    /// <summary>A slow statement per letter, each naming a different table so no two share a row: the LOG rows a short
    /// page used to fill before the errors.</summary>
    private static string SlowStatement(int i)
    {
        var table = "ledger_" + new string((char)('a' + i), 4);
        return "2026-09-22 14:03:02.551 UTC [5288] LOG:  duration: 5012.331 ms  execute <unnamed>: \n"
            + $"\tSELECT account_id, sum(amount) FROM {table} GROUP BY account_id\n";
    }

    private static string Complaint(int i) =>
        "2026-09-22 14:03:02.551 UTC [5288] ERROR:  reconciliation of journal_" + new string((char)('a' + i), 4) + " stopped\n";

    private static async Task<JsonElement> ReadAsync(NpgsqlDataSource postgres, int limit, int offset, CancellationToken ct)
    {
        var json = await DarlingMcpStoreLogTools.GetStoreLog(postgres, limit: limit, offset: offset, cancellationToken: ct);
        Assert.False(McpHelpers.IsErrorEnvelope(json), json[..Math.Min(json.Length, 400)]);
        return JsonDocument.Parse(json).RootElement.Clone();
    }

    [Fact]
    public async Task StoreLog_PageCutByLimit_SaysItIsAPage_GivesTheTotal_AndListsTheErrorsFirst()
    {
        await WithStoreAsync(async (_, postgres, ct) =>
        {
            var slab = string.Concat(Enumerable.Range(0, 11).Select(SlowStatement)) + string.Concat(Enumerable.Range(0, 2).Select(Complaint));
            var retained = StoreLogClassifier.Classify(slab).Groups.Count(g => g.MessageText is not null);
            Assert.Equal(13, retained);
            await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), slab, ct);

            var first = await ReadAsync(postgres, limit: 3, offset: 0, ct);
            Assert.Equal(3, first.GetProperty("retained_events_returned").GetInt32());
            Assert.Equal(13, first.GetProperty("retained_events_total").GetInt32());
            Assert.True(first.GetProperty("truncated").GetBoolean());
            Assert.False(first.GetProperty("budget_truncated").GetBoolean());
            var note = first.GetProperty("note").GetString()!;
            Assert.Contains("3 of 13 distinct messages retained with their text are listed here", note, StringComparison.Ordinal);
            Assert.Contains("offset 3 reads on", note, StringComparison.Ordinal);

            /* The two errors are the first two rows, ahead of the more repeated slow statements. */
            var severities = first.GetProperty("retained_events").EnumerateArray().Select(r => r.GetProperty("severity").GetString()).ToList();
            Assert.Equal(["ERROR", "ERROR", "LOG"], severities);

            var rest = await ReadAsync(postgres, limit: 50, offset: 3, ct);
            Assert.Equal(10, rest.GetProperty("retained_events_returned").GetInt32());
            Assert.Equal(13, rest.GetProperty("retained_events_total").GetInt32());
            Assert.False(rest.GetProperty("truncated").GetBoolean());
            var restNote = rest.GetProperty("note").GetString()!;
            Assert.Contains("10 of 13 distinct messages retained with their text are listed here", restNote, StringComparison.Ordinal);
            Assert.DoesNotContain("reads on", restNote, StringComparison.Ordinal);

            var whole = await ReadAsync(postgres, limit: 50, offset: 0, ct);
            Assert.Equal(13, whole.GetProperty("retained_events_returned").GetInt32());
            Assert.Equal(13, whole.GetProperty("retained_events_total").GetInt32());
            Assert.False(whole.GetProperty("truncated").GetBoolean());
            Assert.Contains("13 distinct messages are retained with their text", whole.GetProperty("note").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task StoreLog_RestartMessages_StayOutOfUnclassified_AndTheAutovacuumCancelNamesTheTableThatOwnsTheToastRelation()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            await using (var create = new NpgsqlCommand("CREATE TABLE public.toasty_8814 (id integer, body text)", connection))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            var toast = await ToastOfAsync(connection, "public.toasty_8814", ct);
            var database = await CurrentDatabaseAsync(connection, ct);
            var slab =
                "2026-11-15 14:27:52.639 UTC [5300] FATAL:  the database system is shutting down\n"
                + Cancel(database, toast);
            await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), slab, ct);

            var root = await ReadAsync(postgres, limit: 50, offset: 0, ct);
            var classes = root.GetProperty("classes").EnumerateArray().ToDictionary(c => c.GetProperty("event_class").GetString()!, c => c);
            Assert.False(classes.ContainsKey("unclassified"), "the two restart messages are not shapes nobody anticipated.");
            Assert.Equal(1, classes["shutdown_in_progress"].GetProperty("occurrences_window").GetInt64());
            Assert.False(classes["shutdown_in_progress"].GetProperty("text_retained").GetBoolean());

            var row = Assert.Single(root.GetProperty("retained_events").EnumerateArray());
            Assert.Equal("autovacuum_cancel", row.GetProperty("event_class").GetString());
            Assert.Equal($"{database}.pg_toast.{toast}", row.GetProperty("toast_relation").GetString());
            Assert.Equal("public.toasty_8814", row.GetProperty("toast_owner").GetString());
        });
    }

    /// <summary>A row an older build filed as an entry but that is the tail of a statement (its sample opens on a tab) has
    /// no text to show, so the total leaves it out and a page that reaches the end is not called cut.</summary>
    [Fact]
    public async Task StoreLog_AStoredFragmentRow_IsLeftOutOfTheTotal_SoTheLastPageIsNotCalledCut()
    {
        await WithStoreAsync(async (_, postgres, ct) =>
        {
            var slab = string.Concat(Enumerable.Range(0, 2).Select(Complaint));
            await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10), slab, ct);

            await using (var connection = await postgres.OpenConnectionAsync(ct))
            await using (var events = new NpgsqlCommand(StoreLogSweep.EventInsertSql, connection))
            {
                events.Parameters.AddWithValue(DateTime.SpecifyKind(DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), DateTimeKind.Unspecified));
                events.Parameters.AddWithValue(new[] { "unclassified" });
                events.Parameters.AddWithValue(new[] { "ERROR" });
                events.Parameters.AddWithValue(new[] { 3 });
                events.Parameters.AddWithValue(new[] { "noted" });
                events.Parameters.AddWithValue(new[] { "\t-- ERROR:  noted" });
                await events.ExecuteNonQueryAsync(ct);
            }

            var root = await ReadAsync(postgres, limit: 50, offset: 0, ct);
            Assert.Equal(2, root.GetProperty("retained_events_returned").GetInt32());
            Assert.Equal(2, root.GetProperty("retained_events_total").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());
        });
    }

    /// <summary>One cancelled autovacuum as PostgreSQL writes it: the message, then the CONTEXT that names the relation
    /// by number with the database it lives in.</summary>
    private static string Cancel(string database, string toast) =>
        "2026-11-15 14:29:57.132 UTC [5302] ERROR:  canceling autovacuum task\n"
        + $"2026-11-15 14:29:57.132 UTC [5302] CONTEXT:  while scanning block 117439 of relation \"pg_toast.{toast}\"\n"
        + $"\tautomatic vacuum of table \"{database}.pg_toast.{toast}\"\n";

    private static async Task<string> ToastOfAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var find = new NpgsqlCommand(
            "SELECT t.relname FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_class AS t ON t.oid = c.reltoastrelid WHERE c.oid = $1::regclass", connection);
        find.Parameters.AddWithValue(table);
        var toast = (string)(await find.ExecuteScalarAsync(ct))!;
        Assert.StartsWith("pg_toast_", toast, StringComparison.Ordinal);
        return toast;
    }

    private static async Task<string> CurrentDatabaseAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var find = new NpgsqlCommand("SELECT current_database()", connection);
        return (string)(await find.ExecuteScalarAsync(ct))!;
    }

    /// <summary>#8822: cancels on two relations in one capture are two rows, each with its own count and owner, and a
    /// relation the log names under another database keeps that name and has no owner, because this catalog cannot say
    /// what it is.</summary>
    [Fact]
    public async Task StoreLog_AutovacuumCancels_AreOneRowPerRelation_AndAnOwnerIsOnlyNamedInTheStoresOwnDatabase()
    {
        await WithStoreAsync(async (connection, postgres, ct) =>
        {
            foreach (var table in new[] { "toasty_a", "toasty_b" })
            {
                await using var create = new NpgsqlCommand($"CREATE TABLE public.{table} (id integer, body text)", connection);
                await create.ExecuteNonQueryAsync(ct);
            }

            var toastA = await ToastOfAsync(connection, "public.toasty_a", ct);
            var toastB = await ToastOfAsync(connection, "public.toasty_b", ct);
            var database = await CurrentDatabaseAsync(connection, ct);
            var slab = Cancel(database, toastA)
                + string.Concat(Enumerable.Range(0, 30).Select(_ => Cancel(database, toastB)))
                + string.Concat(Enumerable.Range(0, 4).Select(_ => Cancel("some_other_database", toastB)));
            await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), slab, ct);

            var root = await ReadAsync(postgres, limit: 50, offset: 0, ct);
            var rows = root.GetProperty("retained_events").EnumerateArray()
                .Where(r => r.GetProperty("event_class").GetString() == "autovacuum_cancel")
                .ToDictionary(r => r.GetProperty("toast_relation").GetString()!, r => r);

            Assert.Equal(3, rows.Count);
            Assert.Equal(1, rows[$"{database}.pg_toast.{toastA}"].GetProperty("occurrences").GetInt64());
            Assert.Equal("public.toasty_a", rows[$"{database}.pg_toast.{toastA}"].GetProperty("toast_owner").GetString());
            Assert.Equal(30, rows[$"{database}.pg_toast.{toastB}"].GetProperty("occurrences").GetInt64());
            Assert.Equal("public.toasty_b", rows[$"{database}.pg_toast.{toastB}"].GetProperty("toast_owner").GetString());

            var elsewhere = rows[$"some_other_database.pg_toast.{toastB}"];
            Assert.Equal(4, elsewhere.GetProperty("occurrences").GetInt64());
            Assert.False(elsewhere.TryGetProperty("toast_owner", out _), "a relation under another database has no owner here.");
        });
    }
}

/// <summary>Writes one hourly capture the way the sweep does: the classifier's groups, then the capture row.</summary>
internal static class StoreLogTestCapture
{
    public static async Task WriteAsync(NpgsqlDataSource postgres, DateTime captureUtc, string slab, CancellationToken ct, long bytesPending = 0)
    {
        var census = StoreLogClassifier.Classify(slab);
        var captureTime = DateTime.SpecifyKind(captureUtc, DateTimeKind.Unspecified);
        await using var connection = await postgres.OpenConnectionAsync(ct);

        await using (var events = new NpgsqlCommand(StoreLogSweep.EventInsertSql, connection))
        {
            events.Parameters.AddWithValue(captureTime);
            events.Parameters.AddWithValue(census.Groups.Select(g => g.EventClass).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.Severity).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.Occurrences).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.MessageText).ToArray());
            events.Parameters.AddWithValue(census.Groups.Select(g => g.SampleLine).ToArray());
            await events.ExecuteNonQueryAsync(ct);
        }

        await using var capture = new NpgsqlCommand(StoreLogSweep.CaptureInsertSql, connection);
        capture.Parameters.AddWithValue(captureTime);
        capture.Parameters.AddWithValue("postgresql-Tue.log");
        capture.Parameters.AddWithValue((long)Encoding.UTF8.GetByteCount(slab));
        capture.Parameters.AddWithValue(bytesPending);
        capture.Parameters.AddWithValue(census.LinesRead);
        capture.Parameters.AddWithValue(census.EntriesRead);
        capture.Parameters.AddWithValue(false);
        capture.Parameters.AddWithValue(census.GroupsDropped);
        await capture.ExecuteNonQueryAsync(ct);
    }
}
