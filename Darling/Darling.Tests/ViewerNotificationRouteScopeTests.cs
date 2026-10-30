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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5750 in the viewer: the scope validation, the scope picker and the Scope and Status text (the same words Lite's window uses),
/// the shape of the SQL and the two windows' source, and a live round trip of both scope columns through the viewer's write
/// and read paths, with orphan status worked out on the same three inputs the service's loader reads.
/// </summary>
public sealed class ViewerNotificationRouteScopeTests
{
    private static NotificationRouteRow Row(string match, int? server = null, int? tag = null, bool enabled = true, int id = 1) => new()
    {
        RouteId = id, MetricMatch = match, SlackUrl = "https://hooks.example.invalid/x", ScopeServerId = server, ScopeTagId = tag, Enabled = enabled,
    };

    private static ViewerRouteScopeInputs Inputs(bool tagsReadable = true) => new(
        new List<(int, string)> { (1, "sql-a"), (2, "sql-b") },
        tagsReadable
            ? new List<DarlingTag> { new(10, "West", null, 0), new(11, "Prod", 10, 0), new(12, "East", null, 0) }
            : new List<DarlingTag>(),
        new Dictionary<int, IReadOnlyCollection<int>> { [1] = new[] { 11 } },
        tagsReadable);

    /* ---- validation -------------------------------------------------------------------------------------- */

    [Fact]
    public void Validation_RefusesBothScopes_AndAnUnscopedEveryMetric_WithAMessage_AndAllowsAScopedOne()
    {
        var both = ViewerDataService.ValidateNotificationRoute(Row("performance", server: 1, tag: 10));
        Assert.Equal("A route is scoped to one server or one tag, not both.", both);

        Assert.StartsWith("Every metric (*) needs a server or a tag scope",
            ViewerDataService.ValidateNotificationRoute(Row("*")), StringComparison.Ordinal);
        Assert.NotNull(ViewerDataService.ValidateNotificationRoute(Row(" * ")));

        Assert.Null(ViewerDataService.ValidateNotificationRoute(Row("*", server: 1)));
        Assert.Null(ViewerDataService.ValidateNotificationRoute(Row("*", tag: 10)));
        Assert.Null(ViewerDataService.ValidateNotificationRoute(Row("performance")));
        Assert.Null(ViewerDataService.ValidateNotificationRoute(Row("Deadlocks Detected", server: 2)));

        /* The write paths run the same check before the statement, so a refusal never reaches the store. */
        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.NotificationRoutes.cs");
        Assert.True(viewer.Split("ValidateRoute(").Length >= 4, "Insert, Update and the sealed save each validate.");
    }

    [Fact]
    public void TheSql_ReadsAndWritesBothScopeColumns_InEveryStatement()
    {
        foreach (var sql in new[]
        {
            ViewerDataService.NotificationRoutesSelectSql, ViewerDataService.NotificationRoutesSelectNoSecretSql,
            ViewerDataService.NotificationRouteInsertSql, ViewerDataService.NotificationRouteUpdateSql,
        })
        {
            Assert.Contains("scope_server_id", sql, StringComparison.Ordinal);
            Assert.Contains("scope_tag_id", sql, StringComparison.Ordinal);
        }

        Assert.Contains("scope_server_id = $9", ViewerDataService.NotificationRouteUpdateSql, StringComparison.Ordinal);
        Assert.Contains("scope_tag_id = $10", ViewerDataService.NotificationRouteUpdateSql, StringComparison.Ordinal);
        /* #5632: the agent columns, $10-$14, follow the scope pair. */
        Assert.Contains("$8, $9, $10, $11, $12, $13, $14, $15, $16, $17, (now()", ViewerDataService.NotificationRouteInsertSql, StringComparison.Ordinal);
    }

    /* ---- the picker ---------------------------------------------------------------------------------------- */

    [Fact]
    public void TheScopePicker_ListsAllServers_ThenServersByName_ThenTagPaths_AndAnOrphansOwnScopeStaysVisible()
    {
        var options = ViewerNotificationRouteScope.ScopeOptions(Inputs());
        Assert.Equal(new[] { "All servers", "Server: sql-a", "Server: sql-b", "Tag: East", "Tag: West", "Tag: West / Prod" },
            options.Select(o => o.Label));
        Assert.Equal(options[1].Id, 1);

        var orphanServer = ViewerNotificationRouteScope.OptionFor(Row("*", server: 99), options);
        Assert.Equal((RouteScopeKind.Server, 99), (orphanServer.Kind, orphanServer.Id));
        Assert.Equal("Server: (removed, id 99)", orphanServer.Label);
        var orphanTag = ViewerNotificationRouteScope.OptionFor(Row("*", tag: 98), options);
        Assert.Equal("Tag: (deleted, id 98)", orphanTag.Label);
        Assert.Same(options[0], ViewerNotificationRouteScope.OptionFor(Row("performance"), options));

        /* The inputs could not be read: All servers is still there. */
        Assert.Single(ViewerNotificationRouteScope.ScopeOptions(null));
    }

    [Fact]
    public void TheEveryMetricMatch_IsOfferedOnlyWhileAScopeIsChosen_AndTheIdsAreNeverBoth()
    {
        var options = ViewerNotificationRouteScope.ScopeOptions(Inputs());
        Assert.False(ViewerNotificationRouteScope.WildcardOffered(options[0]));
        Assert.False(ViewerNotificationRouteScope.WildcardOffered(null));
        Assert.True(ViewerNotificationRouteScope.WildcardOffered(options[1]));
        Assert.True(ViewerNotificationRouteScope.WildcardOffered(options.Last()));

        Assert.Equal(((int?)null, (int?)null), ViewerNotificationRouteScope.ScopeIds(options[0]));
        Assert.Equal(((int?)1, (int?)null), ViewerNotificationRouteScope.ScopeIds(options[1]));
        var tag = options.First(o => o.Kind == RouteScopeKind.Tag);
        var (s, t) = ViewerNotificationRouteScope.ScopeIds(tag);
        Assert.Null(s);
        Assert.Equal(tag.Id, t);
    }

    /* ---- Scope and Status text ------------------------------------------------------------------------------- */

    [Fact]
    public void ScopeAndStatus_ReadAllServers_TheServerName_TheTagPath_Orphaned_Disabled_AndOk()
    {
        var rows = new List<NotificationRouteRow>
        {
            Row("performance", id: 1),                        // fleet
            Row("*", server: 1, id: 2),                       // server, there
            Row("*", server: 99, id: 3),                      // server, removed
            Row("*", tag: 11, id: 4),                         // tag, there (a child)
            Row("*", tag: 98, id: 5),                         // tag, deleted
            Row("performance", server: 2, enabled: false, id: 6),
            Row("performance", server: 99, enabled: false, id: 7),
        };
        ViewerNotificationRouteScope.Annotate(rows, Inputs());

        Assert.Equal(new[] { "All servers", "sql-a", "(server id 99)", "West / Prod", "(tag id 98)", "sql-b", "(server id 99)" }, rows.Select(r => r.ScopeDisplay));
        Assert.Equal(
            new[] { "OK", "OK", "Orphaned: server removed", "OK", "Orphaned: tag deleted", "Disabled", "Orphaned: server removed" },
            rows.Select(r => r.StatusDisplay));
    }

    [Fact]
    public void ATagRoute_WhoseTagDataCouldNotBeRead_IsNeverOk_AndNotCalledOrphaned()
    {
        var rows = new List<NotificationRouteRow> { Row("*", tag: 11, id: 1), Row("*", server: 1, id: 2), Row("performance", id: 3) };
        ViewerNotificationRouteScope.Annotate(rows, Inputs(tagsReadable: false));
        Assert.Equal("Not active: tag data unreadable", rows[0].StatusDisplay);
        Assert.Equal("OK", rows[1].StatusDisplay);
        Assert.Equal("OK", rows[2].StatusDisplay);

        /* Nothing could be read: every scoped route is not active, a fleet route is untouched. */
        ViewerNotificationRouteScope.Annotate(rows, null);
        Assert.Equal("Not active: scope data unreadable", rows[0].StatusDisplay);
        Assert.Equal("Not active: scope data unreadable", rows[1].StatusDisplay);
        Assert.Equal("OK", rows[2].StatusDisplay);
        Assert.Equal("(tag id 11)", rows[0].ScopeDisplay);
    }

    [Fact]
    public void TheKindColumn_NamesTheEveryMetricMatch()
    {
        Assert.Equal("Every metric", Row("*", server: 1).MatchKindDisplay);
        Assert.Equal("Family", Row("performance").MatchKindDisplay);
        Assert.Equal("Exact metric", Row("Deadlocks Detected").MatchKindDisplay);
    }

    /* ---- the windows' source --------------------------------------------------------------------------------- */

    [Fact]
    public void TheWindows_ShowScopeAndStatus_TheDialogPicksAScope_KeepsTheWildcardBehindIt_AndShowsTheMessageInline()
    {
        var grid = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "NotificationRoutesWindow.xaml");
        Assert.Contains("Header=\"Scope\" Binding=\"{Binding ScopeDisplay}\"", grid, StringComparison.Ordinal);
        Assert.Contains("Header=\"Status\" Binding=\"{Binding StatusDisplay}\"", grid, StringComparison.Ordinal);

        var window = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "NotificationRoutesWindow.xaml.cs");
        Assert.Contains("OrderByDescending(r => r.RouteId)", window, StringComparison.Ordinal);
        Assert.Contains("GetNotificationRouteScopeInputsAsync", window, StringComparison.Ordinal);
        Assert.Contains("ViewerNotificationRouteScope.Annotate", window, StringComparison.Ordinal);

        var dialogXaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "NotificationRouteEditDialog.xaml");
        Assert.Contains("x:Name=\"ScopeCombo\"", dialogXaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"ValidationText\"", dialogXaml, StringComparison.Ordinal);
        Assert.Contains("stay app-wide", dialogXaml, StringComparison.Ordinal);

        var dialog = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "NotificationRouteEditDialog.xaml.cs");
        Assert.Contains("WildcardOffered", dialog, StringComparison.Ordinal);
        Assert.Contains("ValidationText.Text = error", dialog, StringComparison.Ordinal);
        Assert.DoesNotContain("MessageBox.Show(error", dialog, StringComparison.Ordinal);
        Assert.Equal("Every metric (*)", ViewerNotificationRouteScope.WildcardChoiceLabel);
    }

}

/// <summary>
/// The live half of #5750's viewer scope tests (split out so the pure facts above stay out of any collection): the round trip of
/// both scope columns through the viewer's write and read paths, and orphan status on the service's own inputs.
/// </summary>
/* #1776 own-store: this class mints its own scratch database per fact, so it is not [Collection("live-postgres")]. */
public sealed class ViewerNotificationRouteScopeLivePostgresTests
{
    /* ---- live: the round trip and orphan status on the service's own inputs ----------------------------------- */

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Live_BothScopeColumns_RoundTripThroughTheViewer_AndOrphanStatusUsesTheServicesInputs()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the viewer's scoped-route round trip.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var key = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES (1, 'sql-a', 'sql-a', true), (2, 'sql-b', 'sql-b', true)", ct);

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var west = await viewer.CreateServerTagAsync("West", null, ct);
            var prod = await viewer.CreateServerTagAsync("Prod", west, ct);
            await ExecAsync(connection, FormattableString.Invariant($"INSERT INTO config.server_tag_map (server_id, tag_id) VALUES (1, {prod})"), ct);

            /* Insert: a server route, a tag route; the sealed save: a scoped '*' route (the scope must be in the INSERT). */
            var serverRoute = await viewer.InsertNotificationRouteAsync(
                new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "a@example.invalid", ScopeServerId = 2 }, ct);
            var tagRoute = await viewer.InsertNotificationRouteAsync(
                new NotificationRouteRow { MetricMatch = "Deadlocks Detected", SmtpRecipients = "b@example.invalid", ScopeTagId = west }, ct);
            var sealer = new ViewerPasswordSealer(key.PublicKey);
            var wildcardRoute = await viewer.SaveNotificationRouteSealedAsync(
                new NotificationRouteRow { MetricMatch = "*", SlackUrl = "https://hooks.example.invalid/w", ScopeServerId = 1 },
                null, new NotificationRow(), sealer, null, ct);
            var fleetRoute = await viewer.InsertNotificationRouteAsync(
                new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "c@example.invalid" }, ct);

            /* A refused row never reaches the store: the count is unchanged. */
            await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertNotificationRouteAsync(
                new NotificationRouteRow { MetricMatch = "*", SmtpRecipients = "d@example.invalid" }, ct));
            await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertNotificationRouteAsync(
                new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "d@example.invalid", ScopeServerId = 1, ScopeTagId = west }, ct));

            var read = await viewer.GetNotificationRoutesAsync(ct);
            Assert.Equal(4, read.Count);
            Assert.Equal((2, (int?)null), (read.Single(r => r.RouteId == serverRoute).ScopeServerId!.Value, read.Single(r => r.RouteId == serverRoute).ScopeTagId));
            Assert.Equal(west, read.Single(r => r.RouteId == tagRoute).ScopeTagId);
            Assert.Null(read.Single(r => r.RouteId == tagRoute).ScopeServerId);
            var wildcard = read.Single(r => r.RouteId == wildcardRoute);
            Assert.Equal("*", wildcard.MetricMatch);
            Assert.Equal(1, wildcard.ScopeServerId);
            Assert.Null(read.Single(r => r.RouteId == fleetRoute).ScopeServerId);
            Assert.Null(read.Single(r => r.RouteId == fleetRoute).ScopeTagId);

            /* Update moves a route from a server to a tag, and back to fleet-wide. */
            var edited = read.Single(r => r.RouteId == serverRoute).Clone();
            edited.ScopeServerId = null;
            edited.ScopeTagId = prod;
            await viewer.UpdateNotificationRouteAsync(edited, ct);
            edited.ScopeTagId = null;
            edited.MetricMatch = "reports";
            await viewer.UpdateNotificationRouteAsync(edited, ct);
            var again = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == serverRoute);
            Assert.Null(again.ScopeServerId);
            Assert.Null(again.ScopeTagId);
            edited.ScopeTagId = prod;
            await viewer.UpdateNotificationRouteAsync(edited, ct);
            Assert.Equal(prod, (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == serverRoute).ScopeTagId);

            /* The read-only seat's projection names the scope columns too, then the five agent columns (V184, #5632),
               then the route's send-resolutions override (V187, #5825) and its fallback route (#6036): 15. */
            await using (var noSecret = new NpgsqlCommand(ViewerDataService.NotificationRoutesSelectNoSecretSql, connection))
            await using (var reader = await noSecret.ExecuteReaderAsync(ct))
            {
                Assert.Equal(15, reader.FieldCount);
                Assert.Equal("scope_server_id", reader.GetName(6));
                Assert.Equal("scope_tag_id", reader.GetName(7));
                Assert.Equal("agent_level", reader.GetName(8));
                Assert.Equal("agent_expiry_minutes", reader.GetName(12));
                Assert.Equal("send_resolutions", reader.GetName(13));
                Assert.Equal("fallback_route_id", reader.GetName(14));
            }

            /* The viewer's inputs are the service's inputs, read from the same store. */
            var viewerInputs = await viewer.GetNotificationRouteScopeInputsAsync(ct);
            var serviceInputs = await StoreConfigProvider.ReadRouteScopeInputsAsync(connection, NullLogger.Instance, ct);
            Assert.True(viewerInputs.TagsReadable);
            Assert.Equal(serviceInputs.KnownServerIds.OrderBy(i => i), viewerInputs.Servers.Select(s => s.Id).OrderBy(i => i));
            Assert.Equal(serviceInputs.TagParents.OrderBy(p => p.Key), viewerInputs.TagParents.OrderBy(p => p.Key));
            Assert.Equal(
                serviceInputs.ServerTags.OrderBy(p => p.Key).Select(p => (p.Key, string.Join(",", p.Value.OrderBy(i => i)))),
                viewerInputs.ServerTags.OrderBy(p => p.Key).Select(p => (p.Key, string.Join(",", p.Value.OrderBy(i => i)))));

            /* All present: nothing is orphaned. */
            var rows = (await viewer.GetNotificationRoutesAsync(ct)).ToList();
            ViewerNotificationRouteScope.Annotate(rows, viewerInputs);
            Assert.All(rows, r => Assert.Equal("OK", r.StatusDisplay));
            Assert.Equal("sql-a", rows.Single(r => r.RouteId == wildcardRoute).ScopeDisplay);
            Assert.Equal("West / Prod", rows.Single(r => r.RouteId == serverRoute).ScopeDisplay);

            /* Delete the tag and the server: both routes are orphaned, still in the store, and the service agrees. */
            await ExecAsync(connection, FormattableString.Invariant($"DELETE FROM config.server_tags WHERE id = {west}"), ct);
            await ExecAsync(connection, "DELETE FROM config.config_monitored_servers WHERE server_id = 1", ct);
            viewerInputs = await viewer.GetNotificationRouteScopeInputsAsync(ct);
            rows = (await viewer.GetNotificationRoutesAsync(ct)).ToList();
            ViewerNotificationRouteScope.Annotate(rows, viewerInputs);
            Assert.Equal("Orphaned: server removed", rows.Single(r => r.RouteId == wildcardRoute).StatusDisplay);
            Assert.Equal("Orphaned: tag deleted", rows.Single(r => r.RouteId == tagRoute).StatusDisplay);
            Assert.Equal("Orphaned: tag deleted", rows.Single(r => r.RouteId == serverRoute).StatusDisplay);
            Assert.Equal("OK", rows.Single(r => r.RouteId == fleetRoute).StatusDisplay);
            Assert.Equal(4, rows.Count);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var view = await new StoreConfigProvider(postgres).LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var orphanedByService = view!.NotificationRoutes.Where(r => r.Orphaned).Select(r => r.RouteId).OrderBy(i => i);
            Assert.Equal(rows.Where(r => r.StatusDisplay.StartsWith("Orphaned", StringComparison.Ordinal)).Select(r => r.RouteId).OrderBy(i => i), orphanedByService);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
