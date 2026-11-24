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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ChangeWatchLiveTests;

namespace Darling.Tests;

/// <summary>
/// #9500: a serverless PostgreSQL target's platform resizes <c>shared_buffers</c> with its capacity, with no restart. The move is a
/// real value change in two snapshots, but not a configuration change anyone made. The rule is exactly: the target is Aurora, the
/// setting is <c>postmaster</c> context, and the server's start time is known and equal at the newest <c>pg_write_stats</c> row
/// at or before the previous snapshot and at the earliest row at or after the change. The MCP read keeps the row and labels it;
/// Change Watch emits nothing for it. Every other shape (stock PostgreSQL, no write-stats rows, a NULL start, a start time that
/// moved, a setting that is not <c>postmaster</c> context) reports as it always did. A row with no write-stats row after the
/// change yet is undecided: reported unflagged, and Change Watch holds it back only while it is inside the mark slack.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync (ScratchPostgres), so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class PlatformResizedConfigLiveTests
{
    private const int PgServerId = 31;
    private const string PgServerName = "pg-thirtyone";

    private static readonly DateTime StartA = new(2026, 3, 1, 8, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime StartB = new(2026, 3, 9, 8, 0, 0, DateTimeKind.Unspecified);

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>
    /// What a fact seeds beside the two config snapshots: which write-stats rows exist and the start time each carries. The row at
    /// the previous snapshot (when <paramref name="WriteStatsRows"/>), an optional row just before the change, an optional row just
    /// after it, and how late the second pass runs (the change is 5 minutes into the run).
    /// </summary>
    private sealed record Shape(
        string EngineKind, string Name, string Context, bool WriteStatsRows, DateTime? StartBeforePrevious, DateTime? StartAfterChange,
        bool RowAfterChange = true, DateTime? RowJustBeforeChange = null, bool RowBeforePrevious = false, int PassMinutes = 10);

    private static Shape Resized(string engineKind = MonitoredEngineKind.AuroraPostgres) =>
        new(engineKind, "shared_buffers", "postmaster", true, StartA, StartA);

    private static async Task AddServerAsync(NpgsqlConnection c, string engineKind, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) " +
            "VALUES ($1, $2, $2, true, $3, now(), now())", c);
        command.Parameters.AddWithValue(PgServerId);
        command.Parameters.AddWithValue(PgServerName);
        command.Parameters.AddWithValue(engineKind);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task ConfigAsync(NpgsqlConnection c, DateTime at, Shape shape, string value, CancellationToken ct) =>
        SeedAsync(c, "pg_server_config", new Dictionary<string, object>
        {
            ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(at), ["name"] = shape.Name,
            ["setting"] = value, ["source"] = "configuration file", ["context"] = shape.Context,
            ["database_name"] = DBNull.Value,
        }, ct);

    private static Task WriteStatsAsync(NpgsqlConnection c, DateTime at, DateTime? start, CancellationToken ct) =>
        SeedAsync(c, "pg_write_stats", new Dictionary<string, object>
        {
            ["server_id"] = PgServerId, ["server_name"] = PgServerName, ["collection_time"] = Naive(at),
            ["postmaster_start_time"] = (object?)start ?? DBNull.Value,
        }, ct);

    /// <summary>
    /// Two snapshots 40 minutes apart at one value, then a third that moves it. The pass runs once before the move (the first pass on a
    /// store with history sets the marks and opens nothing) and once after, as the service would. Returns the MCP page and how many
    /// change watches the second pass recorded.
    /// </summary>
    private static async Task<(JsonElement Page, long Watches)> RunShapeAsync(Shape shape)
    {
        JsonElement page = default;
        long watches = -1;
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, shape.EngineKind, ct);
            var start = h.Now.AddHours(-3);
            var previous = start.AddMinutes(-10);
            var change = start.AddMinutes(5);

            await ConfigAsync(h.Connection, start.AddMinutes(-50), shape, "16384", ct);
            await ConfigAsync(h.Connection, previous, shape, "16384", ct);
            if (shape.WriteStatsRows)
            {
                /* The newest write-stats row at or before the previous snapshot: the one the "before" start comes from. */
                await WriteStatsAsync(h.Connection, shape.RowBeforePrevious ? previous.AddMinutes(-5) : previous, shape.StartBeforePrevious, ct);
            }

            await h.NewPassAt(() => start).DetectAsync(ct);

            if (shape.RowJustBeforeChange is { } justBefore)
            {
                await WriteStatsAsync(h.Connection, change.AddMinutes(-1), justBefore, ct);
            }

            await ConfigAsync(h.Connection, change, shape, "524288", ct);
            if (shape.WriteStatsRows && shape.RowAfterChange)
            {
                /* The earliest write-stats row at or after the move: the one the "after" start comes from. */
                await WriteStatsAsync(h.Connection, change.AddMinutes(1), shape.StartAfterChange, ct);
            }

            await h.NewPassAt(() => start.AddMinutes(shape.PassMinutes)).DetectAsync(ct);
            watches = await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct);

            var asOf = start.AddMinutes(shape.PassMinutes).ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture);
            page = JsonDocument.Parse(await DarlingMcpPgServerStateTools.GetPgServerConfigChanges(h.Source, PgServerName, 168, 50, asOf)).RootElement.Clone();
        });
        return (page, watches);
    }

    private static JsonElement OnlyChange(JsonElement page)
    {
        Assert.Equal("config_changes", page.GetProperty("status").GetString());
        return Assert.Single(page.GetProperty("changes").EnumerateArray().ToList());
    }

    private static void AssertReportedUnflagged(JsonElement page, long watches, string name)
    {
        var row = OnlyChange(page);
        Assert.Equal(name, row.GetProperty("name").GetString());
        Assert.False(row.TryGetProperty("resized_by_platform", out _));
        Assert.False(page.TryGetProperty("resized_by_platform_note", out _));
        Assert.Equal(1, watches);
    }

    [Fact]
    public async Task AuroraSharedBuffersMoves_WithTheSameStart_IsKeptAndLabelled_AndChangeWatchOpensNothing()
    {
        var (page, watches) = await RunShapeAsync(Resized());

        var row = OnlyChange(page);
        Assert.Equal("shared_buffers", row.GetProperty("name").GetString());
        Assert.Equal("16384", row.GetProperty("old_value").GetString());
        Assert.Equal("524288", row.GetProperty("new_value").GetString());
        Assert.True(row.GetProperty("resized_by_platform").GetBoolean());
        Assert.Contains("resized", page.GetProperty("resized_by_platform_note").GetString(), StringComparison.Ordinal);
        Assert.Equal(0, watches);
    }

    [Fact]
    public async Task AuroraSharedBuffersMoves_WithADifferentStartBetweenTheSnapshots_IsReportedAndNotFlagged()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { StartAfterChange = StartB });

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    /* The start time read before the previous snapshot is the same row the old read took after the move, because the write-stats
       series stopped there. No row exists after the move, so nothing proves the server stayed up. */
    [Fact]
    public async Task AuroraWhoseWriteStatsStoppedBeforeThePreviousSnapshot_IsReportedAndNotFlagged_AndChangeWatchRecordsIt()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { RowAfterChange = false, RowBeforePrevious = true, PassMinutes = 30 });

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    [Fact]
    public async Task AuroraWhoseLastWriteStatsRowBeforeTheChangeCarriesTheOldStart_AndTheFirstRowAfterCarriesANewOne_IsReportedAndNotFlagged()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { RowJustBeforeChange = StartA, StartAfterChange = StartB });

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    /* Undecided: no write-stats row after the change yet. The page reports a plain change. Change Watch holds the row back while the
       change is inside the slack of this detect's end, because the next detect's mark sits that far back and reads it again. */
    [Fact]
    public async Task AuroraWithNoWriteStatsRowAfterTheChangeYet_InsideTheSlack_IsReportedUnflagged_AndChangeWatchRecordsNothingThisDetect()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { RowAfterChange = false, PassMinutes = 7 });

        var row = OnlyChange(page);
        Assert.Equal("shared_buffers", row.GetProperty("name").GetString());
        Assert.False(row.TryGetProperty("resized_by_platform", out _));
        Assert.False(page.TryGetProperty("resized_by_platform_note", out _));
        Assert.Equal(0, watches);
    }

    [Fact]
    public async Task AuroraWithNoWriteStatsRowAfterTheChange_OlderThanTheSlack_ChangeWatchRecordsIt()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { RowAfterChange = false, RowJustBeforeChange = StartA, PassMinutes = 30 });

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    [Fact]
    public async Task ANonAuroraTarget_WithTheSameStart_IsReportedAndNotFlagged()
    {
        var (page, watches) = await RunShapeAsync(Resized(MonitoredEngineKind.Postgres));

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    [Fact]
    public async Task AuroraWithNoWriteStatsRows_IsReportedAndNotFlagged()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { WriteStatsRows = false });

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    [Fact]
    public async Task AuroraWithANullStartAtTheMove_IsReportedAndNotFlagged()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { StartAfterChange = null });

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    [Fact]
    public async Task AuroraWithANullStartAtThePreviousSnapshot_IsReportedAndNotFlagged()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { StartBeforePrevious = null });

        AssertReportedUnflagged(page, watches, "shared_buffers");
    }

    [Fact]
    public async Task AuroraWithANonPostmasterSettingThatMoves_IsReportedAndNotFlagged()
    {
        var (page, watches) = await RunShapeAsync(Resized() with { Name = "effective_cache_size", Context = "user" });

        AssertReportedUnflagged(page, watches, "effective_cache_size");
    }

    [Fact]
    public void BothKnownAndEqual_IsTrueOnlyWhenBothStartTimesAreKnownAndEqual()
    {
        Assert.True(PostmasterRestart.BothKnownAndEqual(StartA, StartA));
        Assert.False(PostmasterRestart.BothKnownAndEqual(StartA, StartB));
        Assert.False(PostmasterRestart.BothKnownAndEqual(StartA, null));
        Assert.False(PostmasterRestart.BothKnownAndEqual(null, StartA));
        Assert.False(PostmasterRestart.BothKnownAndEqual(null, null));

        /* The case Spans answers "no restart" for: a NULL newer start. It is not evidence the server stayed up. */
        Assert.False(PostmasterRestart.Spans(StartA, StartA, null));
        Assert.False(PostmasterRestart.BothKnownAndEqual(StartA, null));
    }
}
