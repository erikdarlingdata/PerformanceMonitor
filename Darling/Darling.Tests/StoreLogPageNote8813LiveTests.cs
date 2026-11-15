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

            string toast;
            await using (var find = new NpgsqlCommand(
                "SELECT t.relname FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_class AS t ON t.oid = c.reltoastrelid WHERE c.oid = 'public.toasty_8814'::regclass", connection))
            {
                toast = (string)(await find.ExecuteScalarAsync(ct))!;
            }

            Assert.StartsWith("pg_toast_", toast, StringComparison.Ordinal);
            var slab =
                "2026-11-15 14:27:52.639 UTC [5300] FATAL:  the database system is shutting down\n"
                + "2026-11-15 14:29:57.132 UTC [5302] ERROR:  canceling autovacuum task\n"
                + $"2026-11-15 14:29:57.132 UTC [5302] CONTEXT:  while scanning block 117439 of relation \"pg_toast.{toast}\"\n"
                + $"\tautomatic vacuum of table \"darling.pg_toast.{toast}\"\n";
            await StoreLogTestCapture.WriteAsync(postgres, DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5), slab, ct);

            var root = await ReadAsync(postgres, limit: 50, offset: 0, ct);
            var classes = root.GetProperty("classes").EnumerateArray().ToDictionary(c => c.GetProperty("event_class").GetString()!, c => c);
            Assert.False(classes.ContainsKey("unclassified"), "the two restart messages are not shapes nobody anticipated.");
            Assert.Equal(1, classes["shutdown_in_progress"].GetProperty("occurrences_window").GetInt64());
            Assert.False(classes["shutdown_in_progress"].GetProperty("text_retained").GetBoolean());

            var row = Assert.Single(root.GetProperty("retained_events").EnumerateArray());
            Assert.Equal("autovacuum_cancel", row.GetProperty("event_class").GetString());
            Assert.Equal(toast, row.GetProperty("toast_relation").GetString());
            Assert.Equal("public.toasty_8814", row.GetProperty("toast_owner").GetString());
        });
    }
}

/// <summary>Writes one hourly capture the way the sweep does: the classifier's groups, then the capture row.</summary>
internal static class StoreLogTestCapture
{
    public static async Task WriteAsync(NpgsqlDataSource postgres, DateTime captureUtc, string slab, CancellationToken ct)
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
        capture.Parameters.AddWithValue(0L);
        capture.Parameters.AddWithValue(census.LinesRead);
        capture.Parameters.AddWithValue(census.EntriesRead);
        capture.Parameters.AddWithValue(false);
        capture.Parameters.AddWithValue(census.GroupsDropped);
        await capture.ExecuteNonQueryAsync(ct);
    }
}
