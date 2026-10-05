/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Queries tab's "Showing since" banner and the MCP tools' <c>window_truncated</c> read the raw window floor.
/// A floor found inside the window called a quiet start a cut: a server with older rows, quiet for the first day of
/// the window, was reported as holding less than the window. The floor is now the oldest row at or before the
/// window's end, the same way the Custom Views panels find where their data starts, and it is still null when the
/// window itself holds nothing, which the MCP tools report as an empty window.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class RawWindowFloorQuietStartLiveTests
{
    private const int QuietServerId = -497201;
    private const string QuietServerName = "raw-floor-quiet-start";
    private const int EndedServerId = -497202;
    private const string EndedServerName = "raw-floor-ended";
    private const int RecentServerId = -497203;
    private const string RecentServerName = "raw-floor-recent";

    [Fact]
    public async Task AQuietStartInsideTheWindow_IsNotACut_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var start = store.End.AddDays(-7);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.QueryStats, QuietServerId, start, store.End, cancellationToken: ct);

        Assert.Equal(store.End.AddDays(-8), floor);
        Assert.False(RawWindowFloor.IsTruncated(floor, start));
        Assert.Equal(start, RawWindowFloor.EffectiveStart(floor, start));
    }

    [Fact]
    public async Task AServerWhoseRowsEndBeforeTheWindow_ReadsAsNothing_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var start = store.End.AddDays(-7);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.QueryStats, EndedServerId, start, store.End, cancellationToken: ct);

        Assert.Null(floor);
    }

    [Fact]
    public async Task ARecentServer_IsCutAtItsFirstRow_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var start = store.End.AddDays(-7);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.QueryStats, RecentServerId, start, store.End, cancellationToken: ct);

        Assert.Equal(store.End.AddDays(-3), floor);
        Assert.True(RawWindowFloor.IsTruncated(floor, start));
        Assert.Equal(store.End.AddDays(-3), RawWindowFloor.EffectiveStart(floor, start));
    }

    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime end)
        {
            _scratch = scratch;
            DataSource = dataSource;
            End = end;
        }

        public NpgsqlDataSource DataSource { get; }

        public DateTime End { get; }

        public static async Task<SeededStore> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live raw window floor tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, QuietServerId, QuietServerName, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, EndedServerId, EndedServerName, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, RecentServerId, RecentServerName, ct);

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);

                /* One old row, then nothing until a day into a 7-day window. */
                await InsertEveryHalfHourAsync(connection, QuietServerId, QuietServerName, end.AddDays(-8), end.AddDays(-8), ct);
                await InsertEveryHalfHourAsync(connection, QuietServerId, QuietServerName, end.AddDays(-6), end, ct);
                /* Rows that all end before a 7-day window starts. */
                await InsertEveryHalfHourAsync(connection, EndedServerId, EndedServerName, end.AddDays(-12), end.AddDays(-9), ct);
                /* A server added 3 days ago. */
                await InsertEveryHalfHourAsync(connection, RecentServerId, RecentServerName, end.AddDays(-3), end, ct);

                return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        private static async Task InsertEveryHalfHourAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT
    row_number() OVER (),
    t,
    $1,
    $2,
    'FloorDb',
    'HASH1',
    '0x01',
    1000,
    1000,
    10,
    300
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(lastUtc, DateTimeKind.Unspecified));
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}

/// <summary>
/// The served window never starts before the one asked for. A floor older than the window's start means the rows
/// reach back past it, so the window was served whole.
/// </summary>
public sealed class RawWindowFloorEffectiveStartTests
{
    private static readonly DateTime s_start = new(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void AFloorBeforeTheStart_ServesTheWholeWindow() =>
        Assert.Equal(s_start, RawWindowFloor.EffectiveStart(s_start.AddDays(-1), s_start));

    [Fact]
    public void AFloorAfterTheStart_IsWhereTheWindowWasServedFrom() =>
        Assert.Equal(s_start.AddHours(2), RawWindowFloor.EffectiveStart(s_start.AddHours(2), s_start));

    [Fact]
    public void NoFloor_ReportsTheRequestedStart() =>
        Assert.Equal(s_start, RawWindowFloor.EffectiveStart(null, s_start));

    private const string NotCoveredHead =
        "The store holds no collection of query_snapshots for this server in this window, so nothing was read, and this empty answer is not a report that nothing happened. ";

    private const string NotCoveredTail =
        "The window may reach further back than the store retains, this server may have been monitored for less time than that, or collection may have stopped; get_collection_health shows which.";

    private const string TruncatedSentence =
        "The window reaches further back than this server's raw query_snapshots retains (or this server has been monitored for less time than that), so the older part of it was not read.";

    private static async Task<(McpWindowNotice Notice, int Probes)> ReadAsync(
        TimeSpan window, DateTime? floor, bool emptyAnswer, string? tail = null)
    {
        var probes = 0;
        var notice = await DarlingMcpWindowNotice.ReadAsync(
            () => { probes++; return Task.FromResult(floor); }, s_start, s_start + window, "query_snapshots", tail, emptyAnswer);
        return (notice, probes);
    }

    /// <summary>#4966: a probe that throws costs the notice, never the answer: no verdict, no hints, and a Warning.</summary>
    [Fact]
    public async Task AProbeThatThrows_AnswersNoNotice_AndLogsAWarning()
    {
        var logger = new CapturingLogger();
        var notice = await DarlingMcpWindowNotice.ReadAsync(
            () => throw new TimeoutException("deadline"), s_start, s_start + TimeSpan.FromDays(7), "query_snapshots", logger: logger);

        Assert.True(notice.IsUnavailable);
        Assert.False(notice.WindowTruncated);
        Assert.Null(notice.EffectiveStart);
        Assert.Null(notice.AsHints());
        Assert.Equal([LogLevel.Warning], logger.Levels);

        /* An empty answer fails the same way. */
        Assert.True((await DarlingMcpWindowNotice.ReadAsync(
            () => throw new TimeoutException("deadline"), s_start, s_start + TimeSpan.FromHours(1), "query_snapshots", emptyAnswer: true)).IsUnavailable);
    }

    /// <summary>#4966: only the CALLER's cancellation goes through; the probe's own deadline is a failed probe.</summary>
    [Fact]
    public async Task ACancelledCaller_StillCancels_ButAProbeDeadlineDoesNot()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingMcpWindowNotice.ReadAsync(
            () => throw new OperationCanceledException(cts.Token), s_start, s_start + TimeSpan.FromDays(7), "query_snapshots", cancellationToken: cts.Token));

        var notice = await DarlingMcpWindowNotice.ReadAsync(
            () => throw new OperationCanceledException("the probe's own deadline"), s_start, s_start + TimeSpan.FromDays(7), "query_snapshots",
            cancellationToken: CancellationToken.None);
        Assert.True(notice.IsUnavailable);
    }

    private sealed class CapturingLogger : ILogger
    {
        public List<LogLevel> Levels { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Levels.Add(logLevel);
    }

    /// <summary>#4966: an answer with rows over a window of 90 minutes or less never asks the store, and is covered at the start that was asked for.</summary>
    [Theory]
    [InlineData(60)]
    [InlineData(90)]
    public async Task ARowsAnswer_OverNinetyMinutesOrLess_MakesNoProbe_AndIsCoveredAtTheAskedStart(int minutes)
    {
        var (notice, probes) = await ReadAsync(TimeSpan.FromMinutes(minutes), s_start.AddMinutes(80), emptyAnswer: false);

        Assert.Equal(0, probes);
        Assert.Equal(McpHelpers.FormatEffectiveStart(s_start), notice.EffectiveStart);
        Assert.False(notice.WindowTruncated);
        Assert.Null(notice.TruncationNote);
    }

    [Fact]
    public async Task ARowsAnswer_PastNinetyMinutes_IsProbed_AndAFloorWithinTheSlackIsStillCovered()
    {
        var (notice, probes) = await ReadAsync(TimeSpan.FromMinutes(91), s_start.AddMinutes(90), emptyAnswer: false);

        Assert.Equal(1, probes);
        Assert.False(notice.WindowTruncated);
        Assert.Null(notice.TruncationNote);
        Assert.Equal(McpHelpers.FormatEffectiveStart(s_start.AddMinutes(90)), notice.EffectiveStart);
    }

    /// <summary>An empty answer is probed at any length, and a probe that finds nothing says NOT covered, with the sentence Lite says.</summary>
    [Fact]
    public async Task AnEmptyAnswer_AtSixtyMinutes_IsProbed_AndANullFloorIsNotCovered()
    {
        var (notice, probes) = await ReadAsync(TimeSpan.FromMinutes(60), floor: null, emptyAnswer: true);

        Assert.Equal(1, probes);
        Assert.Null(notice.EffectiveStart);
        Assert.True(notice.WindowTruncated);
        Assert.Equal(NotCoveredHead + NotCoveredTail, notice.TruncationNote);
    }

    [Fact]
    public async Task AnEmptyAnswer_WhoseProbeFindsCoverage_IsCoveredAtTheFloor_OrTheStartWhenTheFloorIsEarlier()
    {
        var (inside, _) = await ReadAsync(TimeSpan.FromMinutes(60), s_start.AddMinutes(35), emptyAnswer: true);
        Assert.Equal(McpHelpers.FormatEffectiveStart(s_start.AddMinutes(35)), inside.EffectiveStart);
        Assert.False(inside.WindowTruncated);
        Assert.Null(inside.TruncationNote);

        var (before, _) = await ReadAsync(TimeSpan.FromMinutes(60), s_start.AddDays(-3), emptyAnswer: true);
        Assert.Equal(McpHelpers.FormatEffectiveStart(s_start), before.EffectiveStart);
        Assert.False(before.WindowTruncated);
    }

    [Fact]
    public async Task AFloorPastTheSlack_IsTruncated_NamesTheTable_AndCarriesTheTail()
    {
        var floor = s_start.AddDays(2);
        var (notice, probes) = await ReadAsync(TimeSpan.FromDays(7), floor, emptyAnswer: false, tail: "More.");

        Assert.Equal(1, probes);
        Assert.True(notice.WindowTruncated);
        Assert.Equal(McpHelpers.FormatEffectiveStart(floor), notice.EffectiveStart);
        Assert.EndsWith("Z", notice.EffectiveStart, StringComparison.Ordinal);
        Assert.Equal(TruncatedSentence + " More.", notice.TruncationNote);
        Assert.Equal(TruncatedSentence, (await ReadAsync(TimeSpan.FromDays(7), floor, emptyAnswer: false)).Notice.TruncationNote);
    }

    [Fact]
    public void TheHints_CarryTheThreeKeys_WithTheSameValues()
    {
        var notice = DarlingMcpWindowNotice.Build(null, s_start, "waiting_tasks", emptyAnswer: true);
        var json = System.Text.Json.JsonSerializer.SerializeToElement(notice.AsHints());

        Assert.Equal(System.Text.Json.JsonValueKind.Null, json.GetProperty("effective_start").ValueKind);
        Assert.True(json.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(notice.TruncationNote, json.GetProperty("truncation_note").GetString());
    }

    /// <summary>
    /// The two sentences are copies of Lite's (#4966), so a client reads one wording from either app. Lite's source is
    /// read here and must hold each sentence as a string literal, and Darling's notice must be exactly those literals
    /// with the table put in. A reword on one side fails until the other follows.
    /// </summary>
    [Fact]
    public void TheNoticeSentences_EqualLitesText()
    {
        var lite = ReadRepoFile("Lite", "Mcp", "McpQueryTools.cs").ReplaceLineEndings("\n");

        const string head = "The store holds no collection of {table} for this server in this window, so nothing was read, and this empty answer is not a report that nothing happened. ";
        Assert.Contains("$\"" + head + "\"", lite, StringComparison.Ordinal);
        Assert.Contains("\"" + NotCoveredTail + "\"", lite, StringComparison.Ordinal);
        Assert.Equal(NotCoveredHead + NotCoveredTail, DarlingMcpWindowNotice.Build(null, s_start, "query_snapshots", emptyAnswer: true).TruncationNote);

        const string listHead = "The store holds no collection of {table} for this server in this window, so no windowed rows were read, and a list with no rows is not a report that nothing happened. ";
        Assert.Contains("$\"" + listHead + "\"", lite, StringComparison.Ordinal);
        Assert.Equal(
            listHead.Replace("{table}", "query_snapshots", StringComparison.Ordinal) + NotCoveredTail,
            DarlingMcpWindowNotice.Build(null, s_start, "query_snapshots", listOnly: true).TruncationNote);

        const string truncated = "The window reaches further back than this server's raw {table} retains (or this server has been monitored for less time than that), so the older part of it was not read.";
        Assert.Contains("$\"" + truncated + "\"", lite, StringComparison.Ordinal);
        Assert.Equal(TruncatedSentence, DarlingMcpWindowNotice.Build(s_start.AddDays(1), s_start, "query_snapshots").TruncationNote);
    }

    /// <summary>The helper probes with GetAsync: GetForServerAsync answers null for a window of 90 minutes or less, which an empty answer would read as "not covered".</summary>
    [Fact]
    public void TheHelperProbes_WithGetAsync_NeverGetForServerAsync()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpWindowNotice.cs");
        Assert.Contains("DataWindowFloor.GetAsync(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("GetForServerAsync(", source.Replace("<see cref=\"DataWindowFloor.GetForServerAsync\"/>", ""), StringComparison.Ordinal);
    }

    /// <summary>The floor walks each table's index in time order, so the walk stops at the first row it meets.</summary>
    [Theory]
    [InlineData("query_stats")]
    [InlineData("procedure_stats")]
    [InlineData("query_store_stats")]
    public void EveryRawFloorTable_HasTheIndexTheWalkUses(string table) =>
        Assert.True(DataWindowFloor.Source.TryForCollectorTable(table, out _));

    /// <summary>
    /// Every MCP tool takes its served start from <see cref="RawWindowFloor.EffectiveStart"/> and its truncation
    /// verdict from <see cref="RawWindowFloor.IsTruncated"/>, so none reports a start before the window it was asked
    /// about, and none keeps a tolerance of its own. The clutter tool once restated the 90 minutes as a private
    /// constant and compared the floor with <c>requestedStart +</c> by hand, a copy that went stale when the floor
    /// stopped being the first row inside the window. The lines below are the ways a tool computes either by hand.
    /// </summary>
    [Fact]
    public void NoMcpTool_ComputesTheServedStartByHand()
    {
        var byHand = new[]
        {
            "floor ?? requestedStart",
            "WindowFloorTolerance",
            "FromMinutes(90)",
            "requestedStart +",
            "requestedStart.Add(",
        };

        var found = new List<string>();
        foreach (var file in new[] { "DarlingMcpDataTools.cs", "DarlingMcpQueryStoreClutterTools.cs", "DarlingMcpQueryStoreHistoryTools.cs" })
        {
            var text = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);
            foreach (var line in byHand)
            {
                if (text.Contains(line, StringComparison.Ordinal))
                {
                    found.Add(file + " has `" + line + "`");
                }
            }
        }

        Assert.True(found.Count == 0, "an MCP tool computes the served start or the truncation verdict by hand: " + string.Join("; ", found));
    }
}
