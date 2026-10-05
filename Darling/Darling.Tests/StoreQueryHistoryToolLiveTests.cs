/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>get_store_query_history</c> (#5097) against a real server. The history is planted directly in the three tables, so
/// the figures are exactly what each fact says. Each fact mints its own scratch database.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreQueryHistoryToolLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DateTime Now = DateTime.UtcNow;

    private static string Ts(double hoursAgo) =>
        "'" + DateTime.SpecifyKind(Now.AddHours(-hoursAgo), DateTimeKind.Unspecified).ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "'";

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source)> StartAsync(CancellationToken ct, bool migrate = true)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the statement history reader's live pins (each mints its own scratch database).");
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        if (migrate)
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString));
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task Capture(NpgsqlDataSource s, double hoursAgo, string outcome, CancellationToken ct, int dealloc = 0, int seen = 3, int kept = 3) =>
        ExecAsync(s, $"INSERT INTO collect.store_statement_captures VALUES ({Ts(hoursAgo)}, 3600, NULL, 0, {dealloc}, {seen}, {kept}, 0, '{outcome}')", ct);

    private static Task Row(NpgsqlDataSource s, double hoursAgo, long queryId, long calls, double totalMs, string flags, CancellationToken ct, string role = "owner") =>
        ExecAsync(s, $"INSERT INTO collect.store_statement_history VALUES ({Ts(hoursAgo)}, 3600, '{role}', {queryId}, {calls}, {totalMs.ToString(CultureInfo.InvariantCulture)}, 1, 2, 3, 4, 9.5, {flags})", ct);

    private const string None = "false, false, false";

    private static async Task Finish(ScratchPostgres scratch, NpgsqlDataSource source, bool ok)
    {
        await source.DisposeAsync();
        await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        await scratch.DisposeAsync();
    }

    [Fact]
    public async Task TheSeries_IsOldestFirst_WithTheFlags_AndAMeanOfDeltaTotalOverDeltaCalls()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        var ok = false;
        try
        {
            await Capture(source, 3.5, "ok", ct);
            await Capture(source, 2.5, "ok", ct);
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 1.5, 9007199254740993, 4, 40, None, ct);
            await Row(source, 3.5, 9007199254740993, 10, 100, "true, false, false", ct);
            await Row(source, 2.5, 9007199254740993, 5, 150, "false, false, true", ct);

            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, query_id: "9007199254740993", cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            Assert.Equal("series", root.GetProperty("mode").GetString());
            Assert.Equal("9007199254740993", root.GetProperty("query_id").GetString());
            var series = root.GetProperty("series").EnumerateArray().ToArray();
            Assert.Equal(3, series.Length);
            Assert.Equal(new[] { 10L, 5L, 4L }, series.Select(r => r.GetProperty("calls").GetInt64()).ToArray());
            Assert.Equal(new[] { 10d, 30d, 10d }, series.Select(r => r.GetProperty("mean_ms").GetDouble()).ToArray());
            Assert.True(series[0].GetProperty("first_seen").GetBoolean());
            Assert.True(series[1].GetProperty("reset_in_interval").GetBoolean());
            Assert.False(series[2].GetProperty("entry_restarted").GetBoolean());
        }
        finally
        {
            ok = true;
            await Finish(scratch, source, ok);
        }
    }

    [Fact]
    public async Task TheRanking_IsByTotalTime_Truncates_AndPrintsTheQueryIdAsAString()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        try
        {
            await Capture(source, 2.5, "ok", ct);
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 2.5, 1, 10, 10, None, ct);
            await Row(source, 1.5, 1, 10, 10, None, ct);
            await Row(source, 2.5, 2, 1, 500, None, ct);
            await Row(source, 2.5, 3, 100, 90, None, ct, role: "viewer");

            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, top: 2, cancellationToken: ct);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;

            Assert.Equal("ranked", root.GetProperty("mode").GetString());
            Assert.True(root.GetProperty("truncated").GetBoolean());
            var rows = root.GetProperty("statements").EnumerateArray().ToArray();
            Assert.Equal(2, rows.Length);
            Assert.Equal(JsonValueKind.String, rows[0].GetProperty("query_id").ValueKind);
            Assert.Equal("2", rows[0].GetProperty("query_id").GetString());
            Assert.Equal("3", rows[1].GetProperty("query_id").GetString());
            Assert.Equal("viewer", rows[1].GetProperty("role").GetString());

            var all = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, top: 5, cancellationToken: ct)).RootElement;
            Assert.False(all.GetProperty("truncated").GetBoolean());
            var one = all.GetProperty("statements").EnumerateArray().First(r => r.GetProperty("query_id").GetString() == "1");
            Assert.Equal(20L, one.GetProperty("calls").GetInt64());
            Assert.Equal(1d, one.GetProperty("mean_ms").GetDouble());
            Assert.Equal(2, one.GetProperty("hours_present").GetInt32());
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }

    [Fact]
    public async Task AStoreWithoutTheRung_AnswersPrecondition()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct, migrate: false);
        try
        {
            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct);

            Assert.StartsWith("{\"status\":\"precondition\"", json, StringComparison.Ordinal);
            Assert.DoesNotContain("effective_start", json, StringComparison.Ordinal);
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }

    [Fact]
    public async Task AStoreWithNoCapturesYet_AnswersPrecondition_Bare()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        try
        {
            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct);

            Assert.StartsWith("{\"status\":\"precondition\"", json, StringComparison.Ordinal);
            Assert.DoesNotContain("window_truncated", json, StringComparison.Ordinal);
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }

    [Fact]
    public async Task TheCapturesSummary_CountsRebaselinedHours_AndTheEvictions()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        try
        {
            await Capture(source, 3.5, "ok", ct, dealloc: 2);
            await Capture(source, 2.5, "rebaselined", ct, seen: 0, kept: 0);
            await Capture(source, 1.5, "ok", ct, dealloc: 3, seen: 150, kept: 100);
            await Row(source, 1.5, 7, 1, 1, None, ct);

            var root = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct)).RootElement;
            var captures = root.GetProperty("captures");

            Assert.Equal(3L, captures.GetProperty("count").GetInt64());
            Assert.Equal(1L, captures.GetProperty("rebaselined").GetInt64());
            Assert.Equal(5L, captures.GetProperty("dealloc_delta_total").GetInt64());
            Assert.Equal(1L, captures.GetProperty("capped_hours").GetInt64());
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }

    [Fact]
    public async Task HistoryStartingInsideTheWindow_IsTruncated_AtTheFirstCapture_AndTheKeysComeAfterHoursBack()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        try
        {
            await Capture(source, 5.5, "rebaselined", ct, seen: 0, kept: 0);
            await Capture(source, 4.5, "ok", ct);
            await Row(source, 4.5, 7, 1, 1, None, ct);

            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 48, cancellationToken: ct);
            var root = JsonDocument.Parse(json).RootElement;

            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            var start = DateTime.Parse(root.GetProperty("effective_start").GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
            Assert.InRange((Now.AddHours(-5.5) - start).Duration().TotalSeconds, 0, 2);
            Assert.EndsWith("Z", root.GetProperty("effective_start").GetString(), StringComparison.Ordinal);
            Assert.Contains("History is kept 90 days and taken hourly.", root.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);

            var names = root.EnumerateObject().Select(p => p.Name).ToArray();
            Assert.Equal("hours_back", names[1]);
            Assert.Equal(new[] { "effective_start", "window_truncated", "truncation_note" }, names.Skip(2).Take(3).ToArray());
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }

    [Fact]
    public async Task HistoryFromBeforeTheWindow_IsNotTruncated()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        try
        {
            await Capture(source, 100, "ok", ct);
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 1.5, 7, 1, 1, None, ct);

            var root = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 24, cancellationToken: ct)).RootElement;

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }

    [Fact]
    public async Task AnEmptyWindow_CarriesTheKeysUnderHints()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        try
        {
            await Capture(source, 100, "ok", ct);

            var root = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 24, cancellationToken: ct)).RootElement;

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.False(hints.GetProperty("window_truncated").GetBoolean());
            Assert.True(hints.TryGetProperty("effective_start", out _));
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }

    [Fact]
    public async Task AFailedEarliestCaptureRead_CostsTheKeys_NotTheAnswer()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        try
        {
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 1.5, 7, 1, 1, None, ct);

            DarlingMcpStoreQueryHistoryTools.TestOnlyFailEarliestRead = true;
            string json;
            try
            {
                json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct);
            }
            finally
            {
                DarlingMcpStoreQueryHistoryTools.TestOnlyFailEarliestRead = false;
            }

            var root = JsonDocument.Parse(json).RootElement;
            Assert.Equal("ranked", root.GetProperty("mode").GetString());
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.Single(root.GetProperty("statements").EnumerateArray());
        }
        finally
        {
            await Finish(scratch, source, true);
        }
    }
}
