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

    [Fact]
    public async Task TheSeries_IsOldestFirst_WithTheFlags_AndAMeanOfDeltaTotalOverDeltaCalls()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
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
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheRanking_IsByTotalTime_Truncates_AndPrintsTheQueryIdAsAString()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
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
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreWithoutTheRung_AnswersPrecondition()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct, migrate: false);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct);

            Assert.StartsWith("{\"status\":\"precondition\"", json, StringComparison.Ordinal);
            Assert.DoesNotContain("effective_start", json, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreWithNoCapturesYet_AnswersPrecondition_Bare()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct);

            Assert.StartsWith("{\"status\":\"precondition\"", json, StringComparison.Ordinal);
            Assert.DoesNotContain("window_truncated", json, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheCapturesSummary_CountsRebaselinedHours_AndTheEvictions()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
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
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task HistoryStartingInsideTheWindow_IsTruncated_AtTheFirstCapture_AndTheKeysComeAfterHoursBack()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
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
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task HistoryFromBeforeTheWindow_IsNotTruncated()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await Capture(source, 100, "ok", ct);
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 1.5, 7, 1, 1, None, ct);

            var root = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 24, cancellationToken: ct)).RootElement;

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AnEmptyWindow_CarriesTheKeysUnderHints()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await Capture(source, 100, "ok", ct);

            var root = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, hours_back: 24, cancellationToken: ct)).RootElement;

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.False(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Contains(hints.EnumerateObject(), p => p.Name == "effective_start");
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AFailedEarliestCaptureRead_CostsTheKeys_NotTheAnswer()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 1.5, 7, 1, 1, None, ct);

            DarlingMcpStoreQueryHistoryTools.TestOnlyFailEarliestRead = true;
            /* The switch is per async flow, so it dies with this test; it is cleared right after the call all the same. */
            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct);
            DarlingMcpStoreQueryHistoryTools.TestOnlyFailEarliestRead = false;

            var root = JsonDocument.Parse(json).RootElement;
            Assert.Equal("ranked", root.GetProperty("mode").GetString());
            Assert.DoesNotContain(root.EnumerateObject(), p => p.Name == "effective_start");
            Assert.DoesNotContain(root.EnumerateObject(), p => p.Name == "window_truncated");
            Assert.Single(root.GetProperty("statements").EnumerateArray());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheRecentAndEarlierMeans_SplitTheNewestQuarter_AndAreNullUnderFourHours()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            for (var h = 8; h >= 1; h--)
            {
                await Capture(source, h + 0.5, "ok", ct);
            }

            /* Statement 1: eight hours; the newest two (25% of 8) cost 50 ms/call, the six before cost 10 ms/call. */
            for (var h = 8; h >= 3; h--)
            {
                await Row(source, h + 0.5, 1, 10, 100, None, ct);
            }

            await Row(source, 2.5, 1, 10, 500, None, ct);
            await Row(source, 1.5, 1, 10, 500, None, ct);
            /* Statement 2: three hours, too few to split. */
            for (var h = 3; h >= 1; h--)
            {
                await Row(source, h + 0.5, 2, 1, 1, None, ct);
            }

            var root = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct)).RootElement;
            var rows = root.GetProperty("statements").EnumerateArray().ToArray();
            var one = rows.First(r => r.GetProperty("query_id").GetString() == "1");
            Assert.Equal(50d, one.GetProperty("recent_mean_ms").GetDouble());
            Assert.Equal(10d, one.GetProperty("earlier_mean_ms").GetDouble());
            var two = rows.First(r => r.GetProperty("query_id").GetString() == "2");
            Assert.Equal(JsonValueKind.Null, two.GetProperty("recent_mean_ms").ValueKind);
            Assert.Equal(JsonValueKind.Null, two.GetProperty("earlier_mean_ms").ValueKind);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AFirstSeenHour_IsCounted_ButNeverFakesASlowdown_AndTheFlagsAreCountedPerStatement()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            for (var h = 8; h >= 1; h--)
            {
                await Capture(source, h + 0.5, "ok", ct);
            }

            /* Steady 10 ms/call for seven hours; the first hour is a first_seen upper bound that credits a lifetime at 1000 ms/call. */
            await Row(source, 8.5, 7, 10, 10000, "true, false, false", ct);
            for (var h = 7; h >= 3; h--)
            {
                await Row(source, h + 0.5, 7, 10, 100, h == 5 ? "false, true, false" : None, ct);
            }

            await Row(source, 2.5, 7, 10, 100, "false, false, true", ct);
            await Row(source, 1.5, 7, 10, 100, None, ct);

            var root = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct)).RootElement;
            var s7 = root.GetProperty("statements").EnumerateArray().First(r => r.GetProperty("query_id").GetString() == "7");
            Assert.Equal(1, s7.GetProperty("first_seen_hours").GetInt32());
            Assert.Equal(1, s7.GetProperty("restarted_hours").GetInt32());
            Assert.Equal(1, s7.GetProperty("reset_hours").GetInt32());
            Assert.Equal(8, s7.GetProperty("hours_present").GetInt32());
            Assert.Equal(10d, s7.GetProperty("recent_mean_ms").GetDouble());
            Assert.Equal(10d, s7.GetProperty("earlier_mean_ms").GetDouble());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheRoleFilter_KeepsOnlyThatRolesRows_InBothModes()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 1.5, 5, 1, 10, None, ct, role: "owner");
            await Row(source, 1.5, 5, 3, 30, None, ct, role: "viewer");

            var ranked = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, role: "viewer", cancellationToken: ct)).RootElement;
            var only = Assert.Single(ranked.GetProperty("statements").EnumerateArray());
            Assert.Equal("viewer", only.GetProperty("role").GetString());
            Assert.Equal(3L, only.GetProperty("calls").GetInt64());

            var series = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, query_id: "5", role: "owner", cancellationToken: ct)).RootElement;
            var row = Assert.Single(series.GetProperty("series").EnumerateArray());
            Assert.Equal(1L, row.GetProperty("calls").GetInt64());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AStoreWhoseCapturesAllCouldNotReadTheExtension_AnswersPrecondition_WithTheReason()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await Capture(source, 2.5, "precondition", ct);
            await Capture(source, 1.5, "precondition", ct);

            var json = await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct);
            Assert.StartsWith("{\"status\":\"precondition\"", json, StringComparison.Ordinal);
            Assert.Contains("could not read pg_stat_statements", json, StringComparison.Ordinal);
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AnUnusableReaderNow_StillAnswersTheHistory_WithTextUnavailable_AndATextNote()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, source) = await StartAsync(ct);
        await using var _ = scratch;
        await using var __ = source;
        var ok = false;
        try
        {
            await Capture(source, 1.5, "ok", ct);
            await Row(source, 1.5, 11, 2, 20, None, ct);

            /* A scratch database has no pg_stat_statements loaded, so the reader is not usable now. */
            var ranked = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, cancellationToken: ct)).RootElement;
            Assert.Equal("ranked", ranked.GetProperty("mode").GetString());
            var note = ranked.GetProperty("text_note").GetString();
            Assert.False(string.IsNullOrEmpty(note));
            Assert.Equal("text unavailable", ranked.GetProperty("statements")[0].GetProperty("query").GetString());

            var series = JsonDocument.Parse(await DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(source, query_id: "11", cancellationToken: ct)).RootElement;
            Assert.Equal("text unavailable", series.GetProperty("query").GetString());
            Assert.Equal(note, series.GetProperty("text_note").GetString());
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, ok, static (_, _) => Task.CompletedTask);
        }
    }
}
