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

    private const string WaitDurationPanel =
        "{\"source\":\"waiting_tasks\",\"measure\":\"waiting_task_duration_ms\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

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

        var outcome = await RunAsync(store.DataSource, RecentServerName, hours: 720, ct);
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");

        var scenario = new JsonObject
        {
            ["panel"] = JsonNode.Parse(WaitDurationPanel),
            ["scope"] = new JsonObject { ["server"] = RecentServerName, ["hours"] = 720 },
            ["answer"] = outcome.Payload!.DeepClone(),
        };

        if (!TryRender(scenario, out var drawn)) return;

        Assert.Empty(drawn.GetProperty("errors").EnumerateArray());
        var shown = Assert.Single(drawn.GetProperty("notices").EnumerateArray()).GetString();
        Assert.StartsWith("partial window:", shown, StringComparison.Ordinal);
        Assert.Contains(Minute(store.RecentFirstRow) + " UTC", shown, StringComparison.Ordinal);
    }

    /// <summary>
    /// The desktop viewer's form of the probe, for one server: Active Queries and Current Waits compare it with the
    /// tab's range. The quiet server's start is its row 8 days back, not its first row inside a 7-day range.
    /// </summary>
    [Fact]
    public async Task TheViewerProbe_ForOneServer_FindsItsOldestRow_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        var waitingTasks = DataWindowFloor.Source.ForCollectorTable("waiting_tasks");
        var end = DateTime.UtcNow.AddMinutes(1);

        Assert.Equal(store.QuietFirstRow, await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, QuietServerId, end, 30, ct));
        Assert.Equal(store.RecentFirstRow, await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, RecentServerId, end, 30, ct));
        Assert.Null(await DataWindowFloor.GetForServerAsync(store.DataSource, waitingTasks, -497199, end, 30, ct));
    }

    private static string Minute(DateTime utc) => utc.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);

    private static string? NoticeOf(DarlingWebEndpoints.ComposeRunOutcome outcome)
    {
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");
        return outcome.Payload!["notice"]?.GetValue<string>();
    }

    private static Task<DarlingWebEndpoints.ComposeRunOutcome> RunAsync(NpgsqlDataSource dataSource, string? server, int hours, CancellationToken ct)
    {
        var body = new JsonObject
        {
            ["panel"] = JsonNode.Parse(WaitDurationPanel),
            ["hours"] = hours,
        };
        if (server is not null)
        {
            body["server"] = server;
        }

        return DarlingWebEndpoints.RunComposedPanelAsync(dataSource, body, ct);
    }

    private static bool TryRender(JsonObject scenario, out JsonElement result)
    {
        result = default;
        var scenarioPath = Path.Combine(Path.GetTempPath(), "compose-notice-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(scenarioPath, scenario.ToJsonString());
        try
        {
            var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
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

        private SeededStore(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime recentFirstRow, DateTime quietFirstRow)
        {
            _scratch = scratch;
            DataSource = dataSource;
            RecentFirstRow = recentFirstRow;
            QuietFirstRow = quietFirstRow;
        }

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

                    return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), recentFirst, quietFirst);
                }
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
