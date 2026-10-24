/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6390 S6: the Viewer runs a saved Custom View's composed panels, and the alert Triage link opens the right server tab. Needs no
/// database.
/// </summary>
public sealed class ViewerCustomViewRunTests
{
    private const string TablePanel =
        "{\"title\":\"Waits\",\"span\":6,\"source\":\"waiting_tasks\",\"measure\":\"waiting_task_duration_ms\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"table\"}";

    [Fact]
    public void ADashboardPlansItsComposedPanels_AndAV1ReadPanelIsNotRun()
    {
        var definition = "{\"panels\":[" + TablePanel + ",{\"title\":\"CPU\",\"read\":\"get_cpu_utilization\",\"params\":{\"hours\":4},\"viz\":\"line\"}],\"range\":{\"hours\":24}}";

        var (items, error) = ViewerCustomViewRun.Plan(definition, "SQL01");

        Assert.Null(error);
        Assert.Equal(2, items.Count);
        Assert.Equal(ViewerRunItemKind.Composed, items[0].Kind);
        var body = items[0].Body!;
        Assert.Equal("SQL01", (string?)body["server"]);
        Assert.Equal(24, (int?)body["hours"]);
        /* The page's own keys (title, span) never reach the run. */
        Assert.Null(body["panel"]!["title"]);
        Assert.Null(body["panel"]!["span"]);
        Assert.Equal("waiting_tasks", (string?)body["panel"]!["source"]);

        Assert.Equal(ViewerRunItemKind.ReadPanel, items[1].Kind);
        Assert.Null(items[1].Body);
        Assert.Equal("This panel reads get_cpu_utilization. Open it in the web dashboard.", items[1].Text);
    }

    [Fact]
    public void ANotebookPlansItsMarkdownAndComposedCells_AndAReadCellIsNotRun()
    {
        var definition = "{\"kind\":\"notebook\",\"cells\":[{\"type\":\"markdown\",\"text\":\"# Notes\"},"
            + "{\"type\":\"panel\",\"title\":\"Waits\",\"source\":\"waiting_tasks\",\"measure\":\"waiting_task_duration_ms\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"range\":{\"hours\":6}},"
            + "{\"type\":\"read\",\"read\":\"get_wait_stats\",\"viz\":\"table\"}]}";

        var (items, error) = ViewerCustomViewRun.Plan(definition, null);

        Assert.Null(error);
        Assert.Equal(
            new[] { ViewerRunItemKind.Markdown, ViewerRunItemKind.Composed, ViewerRunItemKind.ReadPanel },
            items.Select(i => i.Kind).ToArray());
        Assert.Equal("# Notes", items[0].Text);
        /* The cell's own window pin wins over the view's, as in the web. */
        Assert.Equal(6, (int?)items[1].Body!["hours"]);
        Assert.Null(items[1].Body!["server"]);
        Assert.Equal("This panel reads get_wait_stats. Open it in the web dashboard.", items[2].Text);
    }

    [Fact]
    public void ADefinitionThatIsNotJson_IsRefusedWithoutThrowing()
    {
        var (items, error) = ViewerCustomViewRun.Plan("not json", null);

        Assert.Empty(items);
        Assert.NotNull(error);
    }

    [Theory]
    [InlineData(-5)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(60)]
    [InlineData(600)]
    [InlineData(9999)]
    public void TheViewersComposeTimeoutClamp_EqualsTheServices(int value) =>
        Assert.Equal(StoreConfigProvider.ClampComposeStatementTimeoutSeconds(value), ViewerDataService.ClampComposeSeconds(value));

    [Fact]
    public void TheViewersServerNameResolver_PlacesADisplayName_AndRefusesAnAmbiguousOne()
    {
        var servers = new List<RegisteredServer>
        {
            new(1, "sql01.corp", "SQL01"),
            new(2, "sql02.corp", "Shared"),
            new(3, "sql03.corp", "Shared"),
        };

        var (scope, refusal) = ViewerDataService.ResolveComposeNames(new[] { "SQL01", "removed-server" }, servers);
        Assert.Null(refusal);
        Assert.Equal(new[] { "sql01.corp", "removed-server" }, scope);

        var (_, ambiguous) = ViewerDataService.ResolveComposeNames(new[] { "Shared" }, servers);
        Assert.NotNull(ambiguous);
        Assert.Contains("matches 2 monitored servers", ambiguous, StringComparison.Ordinal);
    }

    [Fact]
    public void RowsBecomeAGridTableAndLines()
    {
        var rows = (JsonArray)JsonNode.Parse(
            "[{\"bucket\":\"2026-10-23T01:00:00\",\"db\":\"A\",\"value\":2.5},{\"bucket\":\"2026-10-23T02:00:00\",\"db\":\"A\",\"value\":3},{\"bucket\":\"2026-10-23T01:00:00\",\"db\":\"B\",\"value\":null}]")!;

        var table = ViewerCustomViewRun.ToTable(rows);
        Assert.Equal(new[] { "bucket", "db", "value" }, table.Columns.Cast<System.Data.DataColumn>().Select(c => c.ColumnName).ToArray());
        Assert.Equal(3, table.Rows.Count);

        var lines = ViewerCustomViewRun.Lines(rows)!;
        Assert.Equal(new[] { "A" }, lines.Select(l => l.Name).ToArray());
        Assert.Equal(2, lines[0].Points.Count);
        Assert.Equal("2.5", ViewerCustomViewRun.StatText((JsonArray)JsonNode.Parse("[{\"value\":2.5}]")!));
    }

    /* The Triage link: ViewFor is unchanged (Lite pins it), TabFor says where every other metric goes. */
    [Theory]
    [InlineData("High CPU", false, AlertServerTab.Cpu, ViewerServerTab.CpuInnerTabIndex)]
    [InlineData("tempdb Space", false, AlertServerTab.TempDb, ViewerServerTab.TempDbInnerTabIndex)]
    [InlineData("Poison Wait", false, AlertServerTab.WaitStats, ViewerServerTab.WaitStatsInnerTabIndex)]
    [InlineData("Failed Agent Job", false, AlertServerTab.RunningJobs, ViewerServerTab.RunningJobsInnerTabIndex)]
    [InlineData("Blocking Detected", true, AlertServerTab.PgActivity, ViewerServerTab.PgActivityInnerTabIndex)]
    [InlineData("PostgreSQL Wraparound Risk", true, AlertServerTab.PgVacuum, ViewerServerTab.PgVacuumInnerTabIndex)]
    [InlineData("Custom Alert Rules Unhealthy", false, AlertServerTab.None, -1)]
    public void TheTriageLink_OpensTheServerTabForTheMetric(string metric, bool postgres, AlertServerTab tab, int innerTabIndex)
    {
        Assert.Equal(tab, AlertChainLink.TabFor(metric, postgres));
        Assert.Equal(innerTabIndex, ViewerServerTab.InnerTabIndexFor(tab));
        Assert.Equal(tab != AlertServerTab.None, AlertChainLink.TabLinkText(tab) is not null);
    }

    /* The link's text and its target come from one choice (round 1 of #6612): a PostgreSQL server's Blocking Detected opens the Activity
       tab, so the text names that tab; a SQL Server's keeps the Blocking view's text. */
    [Theory]
    [InlineData("Blocking Detected", true, AlertChainView.None, AlertServerTab.PgActivity, "Open the Activity tab at this time")]
    [InlineData("Blocking Detected", false, AlertChainView.Blocking, AlertServerTab.None, "Open the Blocking view at this time")]
    [InlineData("Deadlocks Detected", false, AlertChainView.Deadlocks, AlertServerTab.None, "Open the Deadlocks view at this time")]
    [InlineData("High CPU", false, AlertChainView.None, AlertServerTab.Cpu, "Open the CPU tab at this time")]
    [InlineData("High CPU", true, AlertChainView.None, AlertServerTab.PgOverview, "Open the Overview tab at this time")]
    public void TheTriageLink_TextNamesWhereItOpens(string metric, bool postgres, AlertChainView view, AlertServerTab tab, string text)
    {
        var target = AlertChainLink.TargetFor(metric, postgres);
        Assert.NotNull(target);
        Assert.Equal(view, target!.View);
        Assert.Equal(tab, target.Tab);
        Assert.Equal(text, target.Text);
    }

    [Theory]
    [InlineData("Deadlocks Detected", true)]
    [InlineData("Custom Alert Rules Unhealthy", false)]
    public void TheTriageLink_HasNoTarget_WhenNoViewOrTabHoldsTheMetric(string metric, bool postgres) =>
        Assert.Null(AlertChainLink.TargetFor(metric, postgres));

    [Theory]
    [InlineData(12, false)]
    [InlineData(13, true)]
    public void TheSeriesCap_SaysHowManyAreShown_OfHowManyThereAre(int total, bool notice)
    {
        var text = ViewerCustomViewRun.SeriesCapNotice(total);
        Assert.Equal(notice, text is not null);
        if (notice)
        {
            Assert.Equal($"Showing the first 12 of {total} series.", text);
        }
    }

    [Fact]
    public void TheTriageLink_LeavesTheChainViewsAlone_AndOpensOverTheAlertsWindow()
    {
        Assert.Equal(AlertChainView.None, AlertChainLink.ViewFor("High CPU"));
        Assert.Equal(AlertChainView.Blocking, AlertChainLink.ViewFor("Blocking Detected"));
        var alert = new DateTime(2026, 10, 23, 9, 0, 0);
        var (from, to) = AlertChainLink.WindowUtc(alert);
        Assert.True(from < alert && alert < to);
    }
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */
public sealed class ViewerCustomViewRunLiveTests
{
    private const int ServerId = -603901;
    private const string ServerName = "viewer-run-server";

    private const string Panel = "{\"source\":\"waiting_tasks\",\"measure\":\"waiting_task_duration_ms\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"viz\":\"VIZ\"}";

    private static string Definition(string viz) =>
        "{\"kind\":\"notebook\",\"range\":{\"hours\":24},\"cells\":[{\"type\":\"markdown\",\"text\":\"Notes\"},{\"type\":\"panel\",\"title\":\"Waits\",\"span\":6,"
        + Panel.TrimStart('{').Replace("VIZ", viz) + "]}";

    [Fact]
    public async Task APanelTheViewerRuns_GivesTheWebsRows_AgainstDevPostgres()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnection), "Set DARLING_TEST_PG to a Postgres connection string to run the Viewer custom view run live tests.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnection!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DateTime.UtcNow;
            var end = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.waiting_tasks (collection_id, collection_time, server_id, server_name, wait_type, wait_duration_ms, database_name)
SELECT row_number() OVER (), t, $1, $2, 'LCK_M_X', 100 + (extract(hour FROM t))::int, 'RunDb'
FROM generate_series($3::timestamp, $4::timestamp, interval '30 minutes') AS t", connection);
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(ServerName);
            insert.Parameters.AddWithValue(end.AddHours(-4));
            insert.Parameters.AddWithValue(end.AddHours(-1));
            await insert.ExecuteNonQueryAsync(ct);
        }

        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = "collect,config,public" };
        await using var viewer = new ViewerDataService(builder.ConnectionString);
        await using var webSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var bodySucceeded = false;
        try
        {
            foreach (var viz in new[] { "table", "line" })
            {
                var (items, error) = ViewerCustomViewRun.Plan(Definition(viz), ServerName);
                Assert.Null(error);
                var item = Assert.Single(items, i => i.Kind == ViewerRunItemKind.Composed);

                /* The web's body for the same panel and server, as compose.js builds it. */
                var webBody = new JsonObject { ["panel"] = JsonNode.Parse(Panel.Replace("VIZ", viz)), ["server"] = ServerName, ["hours"] = 24 };
                var web = await ComposePanelRunner.RunComposedPanelCoreAsync(
                    DarlingWebEndpoints.ComposeHost, webSource, webBody, ComposePanelRunner.ComposeClientDeadlineHeadroomSeconds,
                    remapClientTimeout: true, includeDataStartFields: false, ct);
                var run = await viewer.RunComposedPanelAsync(item.Body!, ct);

                Assert.NotNull(web.Payload);
                Assert.True(run.Ok, run.Error);
                var webRows = web.Payload!["rows"]!.ToJsonString();
                Assert.NotEqual("[]", webRows);
                Assert.Equal(webRows, run.Payload!["rows"]!.ToJsonString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => Task.CompletedTask);
        }
    }
}
