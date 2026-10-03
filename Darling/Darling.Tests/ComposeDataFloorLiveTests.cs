/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A Custom Views panel whose data starts later than its window says so, and names the window it really covers.
/// A 30-day panel over a table that holds 7 days used to draw 7 days with nothing above the chart. The start comes
/// from the rows the store holds for the panel's servers, so a quiet stretch at the start of the window, or a server
/// with an older row before it, does not raise the notice.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it, so a fleet-wide panel sees only
   the rows these tests planted. */
public sealed class ComposeDataFloorLiveTests
{
    private const int RecentServerId = -497101;
    private const string RecentServerName = "data-floor-recent";
    private const int QuietServerId = -497102;
    private const string QuietServerName = "data-floor-quiet-start";

    private const int IdleServerId = -497103;
    private const string IdleServerName = "data-floor-idle-overnight";
    private const int SparseServerId = -497104;
    private const string SparseServerName = "data-floor-first-wait-late";
    private const int NewServerId = -497105;
    private const string NewServerName = "data-floor-added-two-days-ago";
    private const int BaselineServerId = -497106;
    private const string BaselineServerName = "data-floor-baseline-floored-old-server";
    private const int OldServerId = -497107;
    private const string OldServerName = "data-floor-first-collected-ninety-days-ago";

    private const string WaitDurationPanel =
        "{\"source\":\"waiting_tasks\",\"measure\":\"waiting_task_duration_ms\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    private const string FileReadsPanel =
        "{\"source\":\"file_io_stats\",\"measure\":\"file_reads\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    [Fact]
    public async Task AThirtyDayPanel_OverSevenDaysOfRows_SaysWhereTheDataStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var outcome = await RunAsync(store.DataSource, RecentServerName, hours: 720, ct);

        var notice = NoticeOf(outcome);
        Assert.NotNull(notice);
        Assert.StartsWith("partial window:", notice, StringComparison.Ordinal);
        Assert.Contains(Minute(store.RecentFirstRow) + " UTC", notice, StringComparison.Ordinal);
        Assert.DoesNotContain("—", notice, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AWindowTheRowsCover_GetsNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var outcome = await RunAsync(store.DataSource, RecentServerName, hours: 72, ct);

        Assert.Null(NoticeOf(outcome));
    }

    /// <summary>
    /// The quiet server held one row 8 days back, then nothing until 6 days back. A 7-day window's own oldest row
    /// is 6 days back, a day after the window starts, but the store held this server's data before the window
    /// began, so nothing is missing from the start of the window: no notice.
    /// </summary>
    [Fact]
    public async Task AQuietStartInsideTheWindow_GetsNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var outcome = await RunAsync(store.DataSource, QuietServerName, hours: 168, ct);

        Assert.Null(NoticeOf(outcome));
    }

    /// <summary>
    /// A server monitored for months, idle overnight, whose older rows the purge dropped: the 7-day window's first
    /// waiting task comes 5 hours after its start and no older row exists. The store covered the whole window, so
    /// there is no notice.
    /// </summary>
    [Fact]
    public async Task AnIdleStart_WithNoOlderRow_OnAServerMonitoredForMonths_GetsNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        Assert.Null(NoticeOf(await RunAsync(store.DataSource, IdleServerName, hours: 168, ct)));
    }

    /// <summary>
    /// A server collected for 30 days whose first waiting task ever came 3 days ago: a 7-day window is covered,
    /// so there is no notice.
    /// </summary>
    [Fact]
    public async Task AFirstWaitThreeDaysAgo_OnAServerCollectedForThirtyDays_GetsNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        Assert.Null(NoticeOf(await RunAsync(store.DataSource, SparseServerName, hours: 168, ct)));
    }

    /// <summary>
    /// A server added 2 days ago, whose first waiting task came a day later: a 7-day window starts before the
    /// server's coverage, and the notice names where coverage starts (its first collection), not its first row.
    /// </summary>
    [Fact]
    public async Task AServerAddedTwoDaysAgo_SaysSinceItsFirstCollection_NotItsFirstRow_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var notice = NoticeOf(await RunAsync(store.DataSource, NewServerName, hours: 168, ct));
        Assert.NotNull(notice);
        Assert.Contains("data starts at " + Minute(store.NewServerAdded) + " UTC", notice, StringComparison.Ordinal);
    }

    /// <summary>
    /// A fleet panel whose only older history belongs to servers that stopped 10 days ago: their old rows do not
    /// cover a 7-day window, so the panel says where the server it draws starts (added 2 days ago).
    /// </summary>
    [Fact]
    public async Task AFleetPanel_StoppedServersOldRows_DoNotHideALateStart_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateStoppedFleetAsync(ct);

        var notice = NoticeOf(await RunAsync(store.DataSource, server: null, hours: 168, ct));
        Assert.NotNull(notice);
        Assert.Contains("data starts at " + Minute(store.NewServerAdded) + " UTC", notice, StringComparison.Ordinal);
    }

    /// <summary>
    /// file_io_stats is one of the collectors whose purge is floored at the baseline window, so the schedule gives it
    /// no retention edge, and a Custom Views panel over it has no tier notice to say where its data starts (that one
    /// covers a rollup route on a TimescaleDB store). A server registered 100 days ago whose oldest row sits 10 days
    /// back, the older rows long purged, still reads "data starts at" that row for a 30-day window: its coverage is
    /// the oldest row the table holds for it, not its first collection, which would put the start 100 days back and
    /// show no notice at all. A window those rows cover shows none.
    /// </summary>
    [Fact]
    public async Task ABaselineFlooredTable_ForAServerOlderThanTheWindow_StillSaysWhereItsOldestRowIs_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateBaselineFlooredAsync(ct);

        var notice = NoticeOf(await RunAsync(store.DataSource, FileReadsPanel, BaselineServerName, hours: 720, ct));
        Assert.NotNull(notice);
        Assert.Contains("data starts at " + Minute(store.BaselineFirstRow) + " UTC", notice, StringComparison.Ordinal);

        Assert.Null(NoticeOf(await RunAsync(store.DataSource, FileReadsPanel, BaselineServerName, hours: 168, ct)));
    }

    /// <summary>A fleet-wide panel starts where its oldest server's data starts: the quiet server's row 8 days
    /// back covers a 7-day window, though the recent server's rows start a week back.</summary>
    [Fact]
    public async Task AFleetPanel_StartsAtItsOldestServer_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        Assert.Null(NoticeOf(await RunAsync(store.DataSource, server: null, hours: 168, ct)));

        var month = NoticeOf(await RunAsync(store.DataSource, server: null, hours: 720, ct));
        Assert.NotNull(month);
        Assert.Contains(Minute(store.QuietFirstRow) + " UTC", month, StringComparison.Ordinal);
    }

    /// <summary>
    /// The page shows it: the run's own answer, drawn by the shipped compose.js under Node
    /// (web-compose-notice-harness.mjs), puts the notice above the chart. Node is skipped when it is not installed,
    /// the way <see cref="AlertNotebookRenderBehaviourTests"/> does.
    /// </summary>
    [Fact]
    public async Task TheRenderedPanel_ShowsWhereTheDataStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var mcpOutcome = await RunAsync(store.DataSource, RecentServerName, hours: 720, ct);
        Assert.True(mcpOutcome.Error is null, $"compose run failed: {mcpOutcome.Error}");
        Assert.NotNull(mcpOutcome.Payload!["notice"]);
        Assert.Null(mcpOutcome.Payload["data_start_note"]);
        Assert.Null(mcpOutcome.Payload["data_start_utc"]);
        Assert.Null(mcpOutcome.Payload["window_start_utc"]);
        Assert.Null(mcpOutcome.Payload["window_end_utc"]);

        var outcome = await RunAsync(store.DataSource, RecentServerName, hours: 720, ct, includeDataStartFields: true);
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");

        var scenario = new JsonObject
        {
            ["panel"] = JsonNode.Parse(WaitDurationPanel),
            ["scope"] = new JsonObject { ["server"] = RecentServerName, ["hours"] = 720 },
            ["answer"] = outcome.Payload!.DeepClone(),
        };

        /* #4966: the answer carries the instants the notice names, and the page writes the notice again in the browser's
           zone, as it does every time it prints. A zone 5.5 hours from UTC with no daylight saving shows a UTC notice
           from a local one: the sentence the page drew names no UTC. */
        Assert.True(outcome.Payload!["data_start_utc"] is not null && outcome.Payload["window_start_utc"] is not null && outcome.Payload["window_end_utc"] is not null);
        Assert.Contains("UTC", outcome.Payload["notice"]!.GetValue<string>(), StringComparison.Ordinal);

        if (!TryRender(scenario, out var drawn, "Asia/Kolkata"))
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return;
        }

        Assert.Empty(drawn.GetProperty("errors").EnumerateArray());
        var shown = Assert.Single(drawn.GetProperty("notices").EnumerateArray()).GetString();
        Assert.StartsWith("partial window:", shown, StringComparison.Ordinal);
        Assert.DoesNotContain("UTC", shown, StringComparison.Ordinal);
        Assert.DoesNotContain(Minute(store.RecentFirstRow), shown, StringComparison.Ordinal);
    }

    /// <summary>
    /// The desktop viewer's form of the probe, for one server: Active Queries and Current Waits compare it with the
    /// tab's range. Over a 30-day range the quiet server's start is its row 8 days back, not its first row inside a
    /// 7-day range, and a server the registry does not know has no start.
    /// </summary>
    [Fact]
    public async Task TheViewerProbe_ForOneServer_AnswersWhereItsCoverageStarts_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var waitingTasks = DataWindowFloor.Source.ForCollectorTable("waiting_tasks");
        var end = DateTime.UtcNow.AddMinutes(1);

        Assert.Equal(store.QuietFirstRow, await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, QuietServerId, end.AddDays(-30), end, 30, ct));
        Assert.Equal(store.RecentFirstRow, await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, RecentServerId, end.AddDays(-30), end, 30, ct));
        Assert.Null(await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, -497199, end.AddDays(-30), end, 30, ct));
    }

    /// <summary>
    /// The purge keeps waiting_tasks for 7 days, but the run log keeps 60, so a server first collected 90 days ago
    /// still counts in a window 45 to 40 days back (its logged runs are there) while its coverage starts at the
    /// purge edge, a week back, after the window ended. A pinned cell sends exactly this window. The panel shows no
    /// notice, rather than a start that comes after the window's own end.
    /// </summary>
    [Fact]
    public async Task AWindowWhollyBeforeTheRetentionEdge_WithLoggedRuns_GetsNoNotice_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateOldServerAsync(ct);

        var outcome = await RunInWindowAsync(store.DataSource, OldServerName, store.SeededAt.AddDays(-45), store.SeededAt.AddDays(-40), ct);

        Assert.Null(NoticeOf(outcome));
    }

    /// <summary>
    /// The same server, a window 10 to 5 days back: its rows start 7 days back, inside the window, so the panel is
    /// cut at the retention edge and says so, and the window it covers ends where the window does, not at now.
    /// </summary>
    [Fact]
    public async Task AWindowStraddlingTheRetentionEdge_NamesTheEdge_AndCoversToTheWindowsEnd_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateOldServerAsync(ct);
        var windowStart = store.SeededAt.AddDays(-10);
        var windowEnd = store.SeededAt.AddDays(-5);

        var notice = NoticeOf(await RunInWindowAsync(store.DataSource, OldServerName, windowStart, windowEnd, ct));

        Assert.NotNull(notice);
        Assert.Contains("data starts at " + Minute(store.OldServerFirstRow) + " UTC", notice, StringComparison.Ordinal);
        Assert.Contains("after the window's start at " + Minute(windowStart) + " UTC", notice, StringComparison.Ordinal);
        Assert.Contains("covers " + Minute(store.OldServerFirstRow) + " to " + Minute(windowEnd) + " UTC", notice, StringComparison.Ordinal);
    }

    /// <summary>
    /// The desktop viewer's probe for one server, and the fleet form the panels use, over the window wholly before
    /// the retention edge: there is no coverage to report, so the Queries tab shows no "Showing since" banner that
    /// names a time after its own range. A window that straddles the edge still answers it.
    /// </summary>
    [Fact]
    public async Task TheProbes_AnswerNothing_ForAWindowWhollyBeforeTheRetentionEdge_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateOldServerAsync(ct);
        var waitingTasks = DataWindowFloor.Source.ForCollectorTable("waiting_tasks");
        var windowStart = store.SeededAt.AddDays(-45);
        var windowEnd = store.SeededAt.AddDays(-40);

        Assert.Null(await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, OldServerId, windowStart, windowEnd, 30, ct));
        Assert.Null(await DataWindowFloor.GetAsync(store.DataSource, [waitingTasks], null, windowStart, windowEnd, 30, ct));

        Assert.Equal(
            store.OldServerFirstRow,
            await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, OldServerId, store.SeededAt.AddDays(-10), store.SeededAt.AddDays(-5), 30, ct));
    }

    private static string Minute(DateTime utc) => utc.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);

    private static string? NoticeOf(DarlingWebEndpoints.ComposeRunOutcome outcome)
    {
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");
        return outcome.Payload!["notice"]?.GetValue<string>();
    }

    private static Task<DarlingWebEndpoints.ComposeRunOutcome> RunAsync(
        NpgsqlDataSource dataSource, string? server, int hours, CancellationToken ct, bool includeDataStartFields = false) =>
        RunAsync(dataSource, WaitDurationPanel, server, hours, ct, includeDataStartFields);

    private static Task<DarlingWebEndpoints.ComposeRunOutcome> RunAsync(
        NpgsqlDataSource dataSource, string panel, string? server, int hours, CancellationToken ct, bool includeDataStartFields = false)
    {
        var body = new JsonObject
        {
            ["panel"] = JsonNode.Parse(panel),
            ["hours"] = hours,
        };
        if (server is not null)
        {
            body["server"] = server;
        }

        return DarlingWebEndpoints.RunComposedPanelAsync(dataSource, body, ct, includeDataStartFields: includeDataStartFields);
    }

    /* A pinned Custom Views cell's run: the absolute window pair, as ISO-8601 UTC text, in place of the relative hours. */
    private static Task<DarlingWebEndpoints.ComposeRunOutcome> RunInWindowAsync(
        NpgsqlDataSource dataSource, string server, DateTime windowStartUtc, DateTime windowEndUtc, CancellationToken ct)
    {
        var body = new JsonObject
        {
            ["panel"] = JsonNode.Parse(WaitDurationPanel),
            ["server"] = server,
            ["windowStart"] = windowStartUtc.ToString("o", CultureInfo.InvariantCulture),
            ["windowEnd"] = windowEndUtc.ToString("o", CultureInfo.InvariantCulture),
        };

        return DarlingWebEndpoints.RunComposedPanelAsync(dataSource, body, ct);
    }

    internal static bool TryRender(JsonObject scenario, out JsonElement result, string? zone = null)
    {
        result = default;
        var scenarioPath = Path.Combine(Path.GetTempPath(), "compose-notice-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(scenarioPath, scenario.ToJsonString());
        try
        {
            var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
            if (zone is not null)
            {
                psi.Environment["TZ"] = zone;
            }

            psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-compose-notice-harness.mjs"));
            psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
            psi.ArgumentList.Add(scenarioPath);

            Process proc;
            try
            {
                proc = Process.Start(psi)!;
            }
            catch (Win32Exception)
            {
                return false;
            }

            using (proc)
            {
                var error = proc.StandardError.ReadToEndAsync();
                var output = proc.StandardOutput.ReadToEnd().Trim();
                if (!proc.WaitForExit(20000))
                {
                    proc.Kill(entireProcessTree: true);
                    Assert.Fail("the compose notice harness did not finish in 20 s");
                }

                Assert.True(proc.ExitCode == 0, "the compose notice harness failed: " + error.Result);
                using var doc = JsonDocument.Parse(output);
                result = doc.RootElement.Clone();
                return true;
            }
        }
        finally
        {
            File.Delete(scenarioPath);
        }
    }

    /// <summary>
    /// A scratch store holding two servers' waiting_tasks rows, written every 30 minutes up to now: the recent
    /// server's start 7 days back, and the quiet server holds one row 8 days back, then rows from 6 days back.
    /// </summary>
    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(
            ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime recentFirstRow, DateTime quietFirstRow, DateTime newServerAdded,
            DateTime baselineFirstRow = default, DateTime seededAt = default, DateTime oldServerFirstRow = default)
        {
            _scratch = scratch;
            DataSource = dataSource;
            RecentFirstRow = recentFirstRow;
            QuietFirstRow = quietFirstRow;
            NewServerAdded = newServerAdded;
            BaselineFirstRow = baselineFirstRow;
            SeededAt = seededAt;
            OldServerFirstRow = oldServerFirstRow;
        }

        /// <summary>The minute the store was seeded at: the "now" the old server's windows are measured back from
        /// (<see cref="CreateOldServerAsync"/>).</summary>
        public DateTime SeededAt { get; }

        /// <summary>The oldest waiting_tasks row of the server first collected 90 days ago: the purge edge, 7 days
        /// before <see cref="SeededAt"/>.</summary>
        public DateTime OldServerFirstRow { get; }

        /// <summary>When the server added 2 days ago was first collected: its registry row and first logged run.</summary>
        public DateTime NewServerAdded { get; }

        /// <summary>The oldest file_io_stats row of the server registered 100 days ago (<see cref="CreateBaselineFlooredAsync"/>).</summary>
        public DateTime BaselineFirstRow { get; }

        public NpgsqlDataSource DataSource { get; }

        public DateTime RecentFirstRow { get; }

        public DateTime QuietFirstRow { get; }

        public static async Task<SeededStore> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live panel data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
                {
                    await connection.OpenAsync(ct);
                    await PgMigrations.MigrateAsync(connection, ct);
                    await DarlingMcpTestData.RegisterServerAsync(connection, RecentServerId, RecentServerName, ct);
                    await DarlingMcpTestData.RegisterServerAsync(connection, QuietServerId, QuietServerName, ct);

                    var now = DateTime.UtcNow;
                    var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                    var recentFirst = end.AddDays(-7);
                    var quietFirst = end.AddDays(-8);

                    await InsertEveryHalfHourAsync(connection, RecentServerId, RecentServerName, recentFirst, end, ct);
                    await InsertEveryHalfHourAsync(connection, QuietServerId, QuietServerName, quietFirst, quietFirst, ct);
                    await InsertEveryHalfHourAsync(connection, QuietServerId, QuietServerName, end.AddDays(-6), end, ct);
                    await AddedAtAsync(connection, RecentServerId, recentFirst, ct);
                    await AddedAtAsync(connection, QuietServerId, quietFirst, ct);

                    /* Monitored for months and idle overnight: the purge left no row before the 7-day window, and
                       the window's first waiting task came 5 hours in. */
                    await DarlingMcpTestData.RegisterServerAsync(connection, IdleServerId, IdleServerName, ct);
                    await AddedAtAsync(connection, IdleServerId, end.AddDays(-120), ct);
                    await LogRunsAsync(connection, IdleServerId, IdleServerName, end.AddDays(-8), end, ct);
                    await InsertEveryHalfHourAsync(connection, IdleServerId, IdleServerName, end.AddDays(-7).AddHours(5), end, ct);

                    /* Collected for 30 days, and its first waiting task ever came 3 days ago. */
                    await DarlingMcpTestData.RegisterServerAsync(connection, SparseServerId, SparseServerName, ct);
                    await AddedAtAsync(connection, SparseServerId, end.AddDays(-30), ct);
                    await LogRunsAsync(connection, SparseServerId, SparseServerName, end.AddDays(-30), end, ct);
                    await InsertEveryHalfHourAsync(connection, SparseServerId, SparseServerName, end.AddDays(-3), end, ct);

                    var newServerAdded = await AddNewServerAsync(connection, end, ct);

                    return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), recentFirst, quietFirst, newServerAdded);
                }
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>
        /// A store holding only two servers that stopped 10 days ago, with rows and runs before that, and the server
        /// added 2 days ago: the fleet's start is the new server's.
        /// </summary>
        public static async Task<SeededStore> CreateStoppedFleetAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live panel data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                foreach (var (id, name) in new[] { (RecentServerId, "data-floor-stopped-a"), (QuietServerId, "data-floor-stopped-b") })
                {
                    await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
                    await AddedAtAsync(connection, id, end.AddDays(-40), ct);
                    await LogRunsAsync(connection, id, name, end.AddDays(-40), end.AddDays(-10), ct);
                    await InsertEveryHalfHourAsync(connection, id, name, end.AddDays(-40), end.AddDays(-10), ct);
                }

                var newServerAdded = await AddNewServerAsync(connection, end, ct);
                return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), end.AddDays(-40), end.AddDays(-40), newServerAdded);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>
        /// A store holding one server first collected 90 days ago: its collector's runs are in the run log for the last
        /// 60 days (the log's own horizon, longer than the table's), and its waiting_tasks rows run from 7 days back
        /// (the purge edge) to now, one every 30 minutes. A window 45 to 40 days back therefore holds logged runs and
        /// no rows, wholly before the table's coverage.
        /// </summary>
        public static async Task<SeededStore> CreateOldServerAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live panel data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                var firstRow = end.AddDays(-7);
                await DarlingMcpTestData.RegisterServerAsync(connection, OldServerId, OldServerName, ct);
                await AddedAtAsync(connection, OldServerId, end.AddDays(-90), ct);
                await LogRunsAsync(connection, OldServerId, OldServerName, end.AddDays(-60), end, ct);
                await InsertEveryHalfHourAsync(connection, OldServerId, OldServerName, firstRow, end, ct);

                return new SeededStore(
                    scratch, NpgsqlDataSource.Create(scratch.ConnectionString), firstRow, firstRow, firstRow,
                    seededAt: end, oldServerFirstRow: firstRow);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /// <summary>
        /// A store holding one server registered 100 days ago, whose file_io_stats rows run from 10 days back to now
        /// (the collector writes one every 30 minutes; the purge has dropped everything older).
        /// </summary>
        public static async Task<SeededStore> CreateBaselineFlooredAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live panel data-start tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);

                var now = DateTime.UtcNow;
                var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
                var firstRow = end.AddDays(-10);
                await DarlingMcpTestData.RegisterServerAsync(connection, BaselineServerId, BaselineServerName, ct);
                await AddedAtAsync(connection, BaselineServerId, end.AddDays(-100), ct);
                await LogRunsAsync(connection, BaselineServerId, BaselineServerName, "file_io_stats", end.AddDays(-100), end, ct);

                await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, num_of_reads, delta_reads, sample_interval_seconds)
SELECT row_number() OVER (), t, $1, $2, 'FloorDb', 'FloorFile', 1000, 10, 1800
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
                insert.Parameters.AddWithValue(BaselineServerId);
                insert.Parameters.AddWithValue(BaselineServerName);
                insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstRow, DateTimeKind.Unspecified));
                insert.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
                await insert.ExecuteNonQueryAsync(ct);

                return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), firstRow, firstRow, firstRow, firstRow);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        /* The server added 2 days ago: registered and first collected then, its first waiting task a day later. */
        private static async Task<DateTime> AddNewServerAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
        {
            var added = end.AddDays(-2);
            await DarlingMcpTestData.RegisterServerAsync(connection, NewServerId, NewServerName, ct);
            await AddedAtAsync(connection, NewServerId, added, ct);
            await LogRunsAsync(connection, NewServerId, NewServerName, added, end, ct);
            await InsertEveryHalfHourAsync(connection, NewServerId, NewServerName, end.AddDays(-1), end, ct);
            return added;
        }

        /* The registry's created_date: the server's first successful connect, which the service writes once. */
        private static async Task AddedAtAsync(NpgsqlConnection connection, int serverId, DateTime addedUtc, CancellationToken ct)
        {
            await using var update = new NpgsqlCommand("UPDATE collect.servers SET created_date = $2 WHERE server_id = $1", connection);
            update.Parameters.AddWithValue(serverId);
            update.Parameters.AddWithValue(DateTime.SpecifyKind(addedUtc, DateTimeKind.Unspecified));
            await update.ExecuteNonQueryAsync(ct);
        }

        /* The waiting_tasks collector's runs in collection_log, every 30 minutes, whether or not anything waited. */
        private static Task LogRunsAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct) =>
            LogRunsAsync(connection, serverId, serverName, "waiting_tasks", firstUtc, lastUtc, ct);

        private static async Task LogRunsAsync(
            NpgsqlConnection connection, int serverId, string serverName, string collector, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT
    row_number() OVER (),
    $1,
    $2,
    $5,
    t,
    12,
    'SUCCESS',
    0
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(firstUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(lastUtc, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(collector);
            await insert.ExecuteNonQueryAsync(ct);
        }

        private static async Task InsertEveryHalfHourAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime firstUtc, DateTime lastUtc, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.waiting_tasks
    (collection_id, collection_time, server_id, server_name, wait_type, wait_duration_ms, database_name)
SELECT
    row_number() OVER (),
    t,
    $1,
    $2,
    'LCK_M_X',
    250,
    'FloorDb'
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
