/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres,
   seeds it once, and every fact then reads inside it and never writes. */
/// <summary>
/// The store behind <see cref="WebDataStartNoteConfigAndLogLiveTests"/>: one server per read and scenario, each holding
/// the snapshots (the configuration histories), settings (the PostgreSQL changes) or runs (the collection log) its read
/// shows. Every time below is measured back from <see cref="End"/>, the minute the store was seeded at, in naive UTC.
/// </summary>
public sealed class ConfigAndLogNoteStore : IAsyncLifetime
{
    public const string ServerConfigRead = "get_server_config_changes";
    public const string DatabaseConfigRead = "get_database_config_changes";
    public const string TraceFlagRead = "get_trace_flag_changes";
    public const string PgConfigRead = "get_pg_server_config_changes";
    public const string CollectionLogRead = "get_collection_log";

    /// <summary>Hours of hourly runs the "capped" collection log server holds, ending at <see cref="End"/>: more than
    /// the page limit the facts read with, at every window they read.</summary>
    public const int CappedRunHours = 100;

    /// <summary>Settings the "capped" PostgreSQL server changes, one an hour after the next: more than the page limit.</summary>
    public const int PgChangedSettings = 30;

    private ScratchPostgres? _scratch;
    private int _nextServerId = -496700;
    private long _nextLogId = 1;

    public NpgsqlDataSource? DataSource { get; private set; }

    /// <summary>The minute the history ends at, naive UTC.</summary>
    public DateTime End { get; private set; }

    /// <summary>The server a read's scenario seeds: <c>new</c> (added two days ago), <c>quiet</c> (monitored for months,
    /// its first row in a week's window comes days late), and for the two lists that can hit a row cap <c>capped</c>.</summary>
    public static string ServerName(string read, string scenario) =>
        "note-" + read.Replace("get_", string.Empty, StringComparison.Ordinal).Replace('_', '-') + "-" + scenario;

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var now = DateTime.UtcNow;
        End = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, CancellationToken.None);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, CancellationToken.None);
            await SeedAsync(connection);
        }

        DataSource = NpgsqlDataSource.Create(_scratch.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null)
        {
            await DataSource.DisposeAsync();
        }

        if (_scratch is not null)
        {
            await _scratch.DisposeAsync();
        }
    }

    private async Task SeedAsync(NpgsqlConnection connection)
    {
        /* The three SQL Server histories diff two snapshots a day apart or more. The new server's first collection is
           two days back and its two snapshots both fall after it; the quiet one has been monitored for months, its
           baseline sits before a week's window and its one change comes three days in. */
        foreach (var read in new[] { ServerConfigRead, DatabaseConfigRead, TraceFlagRead })
        {
            var (newId, newName) = await AddServerAsync(connection, ServerName(read, "new"), End.AddDays(-2));
            await SnapshotAsync(connection, read, newId, newName, End.AddDays(-2).AddMinutes(1), changed: false);
            await SnapshotAsync(connection, read, newId, newName, End.AddDays(-1), changed: true);

            var (quietId, quietName) = await AddServerAsync(connection, ServerName(read, "quiet"), End.AddDays(-120));
            await SnapshotAsync(connection, read, quietId, quietName, End.AddDays(-10), changed: false);
            await SnapshotAsync(connection, read, quietId, quietName, End.AddDays(-3), changed: true);
        }

        /* Added two days ago and nothing has changed: two identical snapshots, so each history answers "empty" (#4966), the
           answer a short history makes look like a quiet one. */
        foreach (var read in new[] { ServerConfigRead, DatabaseConfigRead, TraceFlagRead })
        {
            var (flatId, flatName) = await AddServerAsync(connection, ServerName(read, "flat"), End.AddDays(-2));
            await SnapshotAsync(connection, read, flatId, flatName, End.AddDays(-2).AddMinutes(1), changed: false);
            await SnapshotAsync(connection, read, flatId, flatName, End.AddDays(-1), changed: false);
        }

        /* The same answer over a server monitored for months: its snapshots sit inside a covered week. */
        var (oldFlatId, oldFlatName) = await AddServerAsync(connection, ServerName(ServerConfigRead, "flat-old"), End.AddDays(-120));
        await SnapshotAsync(connection, ServerConfigRead, oldFlatId, oldFlatName, End.AddDays(-10), changed: false);
        await SnapshotAsync(connection, ServerConfigRead, oldFlatId, oldFlatName, End.AddDays(-3), changed: false);

        var (pgFlatId, pgFlatName) = await AddServerAsync(connection, ServerName(PgConfigRead, "flat"), End.AddDays(-2));
        await PgSettingAsync(connection, pgFlatId, pgFlatName, End.AddDays(-2).AddMinutes(1), "work_mem", "4096");
        await PgSettingAsync(connection, pgFlatId, pgFlatName, End.AddDays(-1), "work_mem", "4096");

        /* PostgreSQL: the changes read compares snapshots inside the window, so a baseline sits inside it. The quiet and
           capped servers also hold one older row (a setting that never changes) so the store plainly covered the range. */
        var (pgNewId, pgNewName) = await AddServerAsync(connection, ServerName(PgConfigRead, "new"), End.AddDays(-2));
        await PgSettingAsync(connection, pgNewId, pgNewName, End.AddDays(-2).AddMinutes(1), "work_mem", "4096");
        await PgSettingAsync(connection, pgNewId, pgNewName, End.AddDays(-1), "work_mem", "8192");

        var (pgQuietId, pgQuietName) = await AddServerAsync(connection, ServerName(PgConfigRead, "quiet"), End.AddDays(-120));
        await PgSettingAsync(connection, pgQuietId, pgQuietName, End.AddDays(-10), "old_marker", "1");
        await PgSettingAsync(connection, pgQuietId, pgQuietName, End.AddDays(-5), "work_mem", "4096");
        await PgSettingAsync(connection, pgQuietId, pgQuietName, End.AddDays(-3), "work_mem", "8192");

        /* Thirty settings, all read at End less 6 days and each changed an hour after the one before it. */
        var (pgCappedId, pgCappedName) = await AddServerAsync(connection, ServerName(PgConfigRead, "capped"), End.AddDays(-120));
        await PgSettingAsync(connection, pgCappedId, pgCappedName, End.AddDays(-10), "old_marker", "1");
        for (var k = 0; k < PgChangedSettings; k++)
        {
            await PgSettingAsync(connection, pgCappedId, pgCappedName, End.AddDays(-6), "setting_" + k.ToString("D2", CultureInfo.InvariantCulture), "0");
            await PgSettingAsync(connection, pgCappedId, pgCappedName, End.AddDays(-6).AddHours(k + 1), "setting_" + k.ToString("D2", CultureInfo.InvariantCulture), "1");
        }

        /* The collection log: hourly runs. New: added two days ago, a run an hour since. Quiet: monitored for months, no
           run until three days back. Capped: monitored for months, a run an hour for CappedRunHours hours. */
        var (logNewId, logNewName) = await AddServerAsync(connection, ServerName(CollectionLogRead, "new"), End.AddDays(-2));
        await RunsAsync(connection, logNewId, logNewName, "wait_stats", End.AddDays(-2), End, "1 hour");

        var (logQuietId, logQuietName) = await AddServerAsync(connection, ServerName(CollectionLogRead, "quiet"), End.AddDays(-120));
        await RunsAsync(connection, logQuietId, logQuietName, "wait_stats", End.AddDays(-3), End, "1 hour");

        var (logCappedId, logCappedName) = await AddServerAsync(connection, ServerName(CollectionLogRead, "capped"), End.AddDays(-120));
        await RunsAsync(connection, logCappedId, logCappedName, "wait_stats", End.AddHours(-CappedRunHours), End, "1 hour");

        /* The window-start edge: a window of 24 hours ending at End less 24 hours starts at T0 (End less 48 hours). Each
           server holds 24 hourly runs from its first instant, and a second collector's run at that same first instant,
           so 25 runs fall in the window and a page of 24 is cut. The first instant is T0 itself for one server, and a
           second after it for the other. */
        var t0 = End.AddHours(-48);
        foreach (var (scenario, shift) in new[] { ("edge-equal", TimeSpan.Zero), ("edge-later", TimeSpan.FromSeconds(1)) })
        {
            var (id, name) = await AddServerAsync(connection, ServerName(CollectionLogRead, scenario), End.AddDays(-120));
            var first = t0 + shift;
            await RunsAsync(connection, id, name, "wait_stats", first, first.AddHours(23), "1 hour");
            await RunsAsync(connection, id, name, "latch_stats", first, first, "1 hour");
        }
    }

    private async Task<(int Id, string Name)> AddServerAsync(NpgsqlConnection connection, string name, DateTime createdUtc)
    {
        var id = _nextServerId--;
        await DarlingMcpTestData.RegisterServerAsync(connection, id, name, CancellationToken.None);
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None, "UPDATE collect.servers SET created_date = $2 WHERE server_id = $1",
            id, DarlingMcpTestData.Naive(createdUtc));
        return (id, name);
    }

    /* One snapshot of a SQL Server history. Unchanged: the baseline. Changed: max degree of parallelism moved, the
       recovery model went from FULL to SIMPLE, or trace flag 1118 appeared beside 3226. */
    private static async Task SnapshotAsync(NpgsqlConnection connection, string read, int serverId, string serverName, DateTime at, bool changed)
    {
        var time = DarlingMcpTestData.Naive(at);
        switch (read)
        {
            case ServerConfigRead:
                await DarlingMcpTestData.ExecAsync(
                    connection, CancellationToken.None,
                    "INSERT INTO collect.server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced) "
                    + "VALUES ($1, $2, $3, $4, 'max degree of parallelism', $5, $5, TRUE, TRUE)",
                    CollectionIdGenerator.Next(), time, serverId, serverName, changed ? 4L : 0L);
                break;
            case DatabaseConfigRead:
                await DarlingMcpTestData.ExecAsync(
                    connection, CancellationToken.None,
                    "INSERT INTO collect.database_config (config_id, capture_time, server_id, server_name, database_name, recovery_model) "
                    + "VALUES ($1, $2, $3, $4, 'NoteDb', $5)",
                    CollectionIdGenerator.Next(), time, serverId, serverName, changed ? "SIMPLE" : "FULL");
                break;
            default:
                foreach (var flag in changed ? new[] { 3226, 1118 } : new[] { 3226 })
                {
                    await DarlingMcpTestData.ExecAsync(
                        connection, CancellationToken.None,
                        "INSERT INTO collect.trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session) "
                        + "VALUES ($1, $2, $3, $4, $5, TRUE, TRUE, FALSE)",
                        CollectionIdGenerator.Next(), time, serverId, serverName, flag);
                }

                break;
        }
    }

    private static Task PgSettingAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, string setting, string value) =>
        DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            "INSERT INTO collect.pg_server_config (collection_id, collection_time, server_id, server_name, name, setting, source, boot_val, reset_val, sourceline, pending_restart, database_name, role_name) "
            + "VALUES ($1, $2, $3, $4, $5, $6, 'configuration file', $6, $6, 0, FALSE, NULL, NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, setting, value);

    private async Task RunsAsync(
        NpgsqlConnection connection, int serverId, string serverName, string collector, DateTime first, DateTime last, string step)
    {
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
            + "SELECT $5 + row_number() OVER (ORDER BY t), $1, $2, $6, t, 10 + (row_number() OVER (ORDER BY t) % 50), 'SUCCESS', 0 "
            + $"FROM generate_series($3::timestamp, $4::timestamp, interval '{step}') AS t",
            serverId, serverName, DarlingMcpTestData.Naive(first), DarlingMcpTestData.Naive(last), _nextLogId, collector);
        _nextLogId += 100_000;
    }
}

/// <summary>
/// The server page's Config Changes grids (server, database, trace flags and PostgreSQL's) and its Collection Log grids say
/// where their data starts (#4966), over a store. Each fact runs the real tool for its read against the seeded store and
/// passes the tool's own answer through <see cref="WebDataStartNote.AddAsync"/>, the path the web mirror takes, so the note
/// is judged on rows the tool really returned.
///
/// <para>A range that starts before the store covered the server gets the note, which names the coverage start and carries
/// the window's instants beside the sentence. A range the store covered gets none, even when its first row comes days late. A
/// collection log or PostgreSQL changes page that fills its row cap names its oldest returned row whatever the store covers,
/// and shows that note only when the row is later than the window's start.</para>
/// </summary>
public sealed class WebDataStartNoteConfigAndLogLiveTests : IClassFixture<ConfigAndLogNoteStore>
{
    private const int PageLimit = 20;

    private readonly ConfigAndLogNoteStore _store;

    public WebDataStartNoteConfigAndLogLiveTests(ConfigAndLogNoteStore store) => _store = store;

    private static DateTime ParseUtc(JsonNode? node) =>
        DateTime.Parse(node!.GetValue<string>(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    private static string Anchor(DateTime utc) => utc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private void RequireStore() =>
        Assert.SkipWhen(_store.DataSource is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live data-start tests.");

    /// <summary>What the web mirror answers for a grid: the tool's own payload for the read, then the data-start note.</summary>
    private async Task<JsonObject> AskAsync(
        string read, string scenario, int hours, int? limit = null, string? asOf = null, double? minDurationMs = null)
    {
        RequireStore();

        var ct = TestContext.Current.CancellationToken;
        var source = _store.DataSource!;
        var server = ConfigAndLogNoteStore.ServerName(read, scenario);
        var payload = read switch
        {
            ConfigAndLogNoteStore.ServerConfigRead => await DarlingMcpConfigHistoryTools.GetServerConfigChanges(source, server, hours, as_of: asOf, cancellationToken: ct),
            ConfigAndLogNoteStore.DatabaseConfigRead => await DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(source, server, hours, as_of: asOf, cancellationToken: ct),
            ConfigAndLogNoteStore.TraceFlagRead => await DarlingMcpConfigHistoryTools.GetTraceFlagChanges(source, server, hours, as_of: asOf, cancellationToken: ct),
            ConfigAndLogNoteStore.PgConfigRead => await DarlingMcpPgServerStateTools.GetPgServerConfigChanges(source, server, hours, limit ?? 100, as_of: asOf, cancellationToken: ct),
            ConfigAndLogNoteStore.CollectionLogRead => await DarlingMcpDataTools.GetCollectionLog(source, server, hours, limit ?? 200, asOf, null, minDurationMs, status: null, full_text: true, cancellationToken: ct),
            _ => throw new ArgumentOutOfRangeException(nameof(read), read, "not a read this class asks"),
        };

        var answered = await WebDataStartNote.AddAsync(source, read, server, hours, asOf, payload, null, ct);
        return Assert.IsType<JsonObject>(JsonNode.Parse(answered));
    }

    /// <summary>The answer is rows the read returned (a changes list, or runs), not an envelope: a fact that expects no note
    /// must not pass because the tool answered "nothing here".</summary>
    private static void AssertRows(string read, JsonObject answer)
    {
        var rows = answer[read == ConfigAndLogNoteStore.CollectionLogRead ? "runs" : "changes"];
        Assert.True(rows is JsonArray { Count: > 0 }, read + " answered no rows: " + answer.ToJsonString());
        if (read == ConfigAndLogNoteStore.PgConfigRead)
        {
            Assert.Equal("config_changes", answer["status"]?.GetValue<string>());
            Assert.Null(answer["message"]);
        }
    }

    /// <summary>Every field a note adds is absent, and so is the tool's own window floor: the web strips a listed read's own
    /// <c>effective_start</c>, <c>window_truncated</c> and <c>truncation_note</c> on every answer it returns, so the page sees only its own note.</summary>
    private static void AssertNoNote(JsonObject answer)
    {
        Assert.Null(answer["window_truncated"]);
        Assert.Null(answer["effective_start"]);
        Assert.Null(answer["truncation_note"]);
        Assert.Null(answer["data_start_utc"]);
        Assert.Null(answer["oldest_shown_utc"]);
        Assert.Null(answer["window_start_utc"]);
        Assert.Null(answer["window_end_utc"]);
    }

    /// <summary>The window's instants beside the sentence: <paramref name="hours"/> long, and ending at <paramref name="end"/>
    /// (or, with none given, when the grid was asked).</summary>
    private static void AssertWindowInstants(JsonObject answer, int hours, DateTime? end)
    {
        var windowStart = ParseUtc(answer["window_start_utc"]);
        var windowEnd = ParseUtc(answer["window_end_utc"]);
        Assert.EndsWith("Z", answer["window_start_utc"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.EndsWith("Z", answer["window_end_utc"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromHours(hours), windowEnd - windowStart);
        if (end is DateTime anchored)
        {
            Assert.Equal(anchored.Ticks, windowEnd.Ticks);
        }
        else
        {
            Assert.True(Math.Abs((windowEnd - DateTime.UtcNow).TotalSeconds) < 120, "the window ends when the grid was asked");
        }
    }

    private static void AssertCoverageNote(JsonObject answer, DateTime dataStart, int hours)
    {
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        var effectiveStart = ParseUtc(answer["effective_start"]);
        Assert.True(Math.Abs((effectiveStart - dataStart).TotalSeconds) < 1, "the note names the server's first collection, not its first row");
        Assert.Equal(effectiveStart.Ticks, ParseUtc(answer["data_start_utc"]).Ticks);
        Assert.EndsWith("Z", answer["data_start_utc"]!.GetValue<string>(), StringComparison.Ordinal);
        Assert.Null(answer["oldest_shown_utc"]);
        var note = answer["truncation_note"]!.GetValue<string>();
        Assert.StartsWith("partial window:", note, StringComparison.Ordinal);
        Assert.Contains(dataStart.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", note, StringComparison.Ordinal);
        AssertWindowInstants(answer, hours, end: null);
    }

    private static void AssertCappedNote(JsonObject answer, DateTime oldestShown, int hours, DateTime? end = null)
    {
        Assert.True(answer["truncated"]?.GetValue<bool>(), "the page was cut at its limit");
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
        Assert.True(Math.Abs((ParseUtc(answer["effective_start"]) - oldestShown).TotalSeconds) < 1);
        Assert.Equal(oldestShown.Ticks, ParseUtc(answer["oldest_shown_utc"]).Ticks);
        Assert.Null(answer["data_start_utc"]);
        var note = answer["truncation_note"]!.GetValue<string>();
        Assert.Contains("only the newest rows, back to " + oldestShown.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " UTC", note, StringComparison.Ordinal);
        AssertWindowInstants(answer, hours, end);
    }

    /// <summary>A server added two days ago, read over a week: each of the five reads says its data starts at the server's
    /// first collection, with the three instants beside the sentence.</summary>
    [Theory]
    [InlineData(ConfigAndLogNoteStore.ServerConfigRead)]
    [InlineData(ConfigAndLogNoteStore.DatabaseConfigRead)]
    [InlineData(ConfigAndLogNoteStore.TraceFlagRead)]
    [InlineData(ConfigAndLogNoteStore.PgConfigRead)]
    [InlineData(ConfigAndLogNoteStore.CollectionLogRead)]
    public async Task ARangeThatStartsBeforeCoverage_GetsTheNoteNamingTheCoverageStart_OnEachRead_AgainstDevPostgres(string read)
    {
        var answer = await AskAsync(read, "new", hours: 168);

        AssertRows(read, answer);
        Assert.NotEqual(true, answer["truncated"]?.GetValue<bool>());
        AssertCoverageNote(answer, _store.End.AddDays(-2), hours: 168);
    }

    /// <summary>A server added two days ago whose configuration has not changed, read over a week: the tool answers that
    /// nothing changed (<c>empty</c>, and <c>no_changes</c> for PostgreSQL), and the note still says the data starts at the
    /// server's first collection, with the three instants beside it, so the grid does not read as a quiet week.</summary>
    [Theory]
    [InlineData(ConfigAndLogNoteStore.ServerConfigRead, "empty")]
    [InlineData(ConfigAndLogNoteStore.DatabaseConfigRead, "empty")]
    [InlineData(ConfigAndLogNoteStore.TraceFlagRead, "empty")]
    [InlineData(ConfigAndLogNoteStore.PgConfigRead, "no_changes")]
    public async Task AnEmptyAnswerOverANewServer_GetsTheNoteNamingTheCoverageStart_OnEachChangeRead_AgainstDevPostgres(string read, string status)
    {
        var answer = await AskAsync(read, "flat", hours: 168);

        Assert.Equal(status, answer["status"]?.GetValue<string>());
        Assert.NotNull(answer["message"]);
        AssertCoverageNote(answer, _store.End.AddDays(-2), hours: 168);
    }

    /// <summary>The same envelope over a server monitored for months, whose snapshots sit inside a covered week, says
    /// nothing: the note follows the coverage, not the empty answer.</summary>
    [Fact]
    public async Task AnEmptyAnswerOverACoveredServer_GetsNoNote_AgainstDevPostgres()
    {
        var answer = await AskAsync(ConfigAndLogNoteStore.ServerConfigRead, "flat-old", hours: 168);

        Assert.Equal("empty", answer["status"]?.GetValue<string>());
        AssertNoNote(answer);
    }

    /// <summary>A server monitored for months whose first change or run in the window comes days after it starts: the store
    /// covered the whole range, so a quiet start is not a cut and no read says anything.</summary>
    [Theory]
    [InlineData(ConfigAndLogNoteStore.ServerConfigRead)]
    [InlineData(ConfigAndLogNoteStore.DatabaseConfigRead)]
    [InlineData(ConfigAndLogNoteStore.TraceFlagRead)]
    [InlineData(ConfigAndLogNoteStore.PgConfigRead)]
    [InlineData(ConfigAndLogNoteStore.CollectionLogRead)]
    public async Task ACoveredRange_WhoseFirstRowComesLate_GetsNoNote_OnEachRead_AgainstDevPostgres(string read)
    {
        var answer = await AskAsync(read, "quiet", hours: 168);

        AssertRows(read, answer);
        AssertNoNote(answer);
    }

    /// <summary>A collection log page that fills its cap ends at the oldest run it returned, so the note names that run even
    /// though the store covered the whole range, and at a window past the 168 hours the other reads stop at.</summary>
    [Theory]
    [InlineData(24)]
    [InlineData(168)]
    [InlineData(720)]
    public async Task ACappedCollectionLog_NamesItsOldestReturnedRun_EvenOverACoveredRange_AgainstDevPostgres(int hours)
    {
        var answer = await AskAsync(ConfigAndLogNoteStore.CollectionLogRead, "capped", hours, limit: PageLimit);

        AssertRows(ConfigAndLogNoteStore.CollectionLogRead, answer);
        Assert.Equal(PageLimit, answer["runs"]!.AsArray().Count);
        Assert.Equal("collection_time_desc", answer["order"]?.GetValue<string>());
        AssertCappedNote(answer, _store.End.AddHours(-(PageLimit - 1)), hours);
    }

    /// <summary>The same read under a duration floor is ranked slowest first, a sample of the whole window whose oldest run
    /// names no reach: the coverage rule stands. A covered server gets no note; a server added two days ago gets the
    /// coverage note, not one naming the oldest run, and the same page newest first names that run instead.</summary>
    [Fact]
    public async Task ACollectionLogRankedSlowestFirst_KeepsTheCoverageRule_AgainstDevPostgres()
    {
        var covered = await AskAsync(ConfigAndLogNoteStore.CollectionLogRead, "capped", hours: 720, limit: PageLimit, minDurationMs: 0);
        AssertRows(ConfigAndLogNoteStore.CollectionLogRead, covered);
        Assert.True(covered["truncated"]?.GetValue<bool>(), "the ranked page was cut at its limit");
        Assert.Equal("duration_ms_desc", covered["order"]?.GetValue<string>());
        AssertNoNote(covered);

        var added = await AskAsync(ConfigAndLogNoteStore.CollectionLogRead, "new", hours: 168, limit: PageLimit, minDurationMs: 0);
        Assert.True(added["truncated"]?.GetValue<bool>(), "the ranked page was cut at its limit");
        Assert.Equal("duration_ms_desc", added["order"]?.GetValue<string>());
        AssertCoverageNote(added, _store.End.AddDays(-2), hours: 168);

        var newestFirst = await AskAsync(ConfigAndLogNoteStore.CollectionLogRead, "new", hours: 168, limit: PageLimit);
        AssertCappedNote(newestFirst, _store.End.AddHours(-(PageLimit - 1)), hours: 168);
    }

    /// <summary>A PostgreSQL changes page cut at its limit names its earliest changed_at (the page names no oldest-returned
    /// field), and its <c>config_changes</c> status is read as rows. The same server read whole is covered and says nothing:
    /// the note follows the cut, not the store.</summary>
    [Fact]
    public async Task ACappedPostgresChangesPage_NamesItsEarliestChange_AndItsStatusIsRowsNotAnEnvelope_AgainstDevPostgres()
    {
        const int Limit = 10;
        var capped = await AskAsync(ConfigAndLogNoteStore.PgConfigRead, "capped", hours: 168, limit: Limit);

        AssertRows(ConfigAndLogNoteStore.PgConfigRead, capped);
        Assert.Equal(Limit, capped["changes"]!.AsArray().Count);
        /* Setting k changed at End less 6 days plus k+1 hours, newest first: the page holds the last ten of thirty. */
        var earliest = _store.End.AddDays(-6).AddHours(ConfigAndLogNoteStore.PgChangedSettings - Limit + 1);
        AssertCappedNote(capped, earliest, hours: 168);

        var whole = await AskAsync(ConfigAndLogNoteStore.PgConfigRead, "capped", hours: 168);
        AssertRows(ConfigAndLogNoteStore.PgConfigRead, whole);
        Assert.Equal(ConfigAndLogNoteStore.PgChangedSettings, whole["changes"]!.AsArray().Count);
        Assert.False(whole["truncated"]?.GetValue<bool>());
        AssertNoNote(whole);
    }

    /// <summary>A page that fills its cap shows the note only when its oldest run is later than the window's start, with no
    /// slack: a page whose oldest run is the window's start shows the whole range and says nothing, and one whose oldest run
    /// is a second after it was cut there. The window is anchored (as_of) so its start is exact.</summary>
    [Fact]
    public async Task ACappedPage_WhoseOldestRunIsTheWindowsStart_GetsNoNote_AndOneASecondLaterGetsIt_AgainstDevPostgres()
    {
        RequireStore();
        const int Hours = 24;
        const int Limit = 24;
        var windowStart = _store.End.AddHours(-48);
        var anchor = Anchor(windowStart.AddHours(Hours));

        var equal = await AskAsync(ConfigAndLogNoteStore.CollectionLogRead, "edge-equal", Hours, Limit, anchor);
        AssertRows(ConfigAndLogNoteStore.CollectionLogRead, equal);
        Assert.True(equal["truncated"]?.GetValue<bool>(), "the page was cut at its limit");
        Assert.Equal(windowStart.Ticks, ParseUtc(equal["oldest_returned_collection_time"]).Ticks);
        AssertNoNote(equal);

        var later = await AskAsync(ConfigAndLogNoteStore.CollectionLogRead, "edge-later", Hours, Limit, anchor);
        AssertRows(ConfigAndLogNoteStore.CollectionLogRead, later);
        AssertCappedNote(later, windowStart.AddSeconds(1), Hours, windowStart.AddHours(Hours));
        Assert.Equal(windowStart.Ticks, ParseUtc(later["window_start_utc"]).Ticks);
    }
}
