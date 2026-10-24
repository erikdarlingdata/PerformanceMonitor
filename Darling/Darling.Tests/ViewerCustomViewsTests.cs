/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6390 S5: the Viewer's Custom Views manager gives the refusal text the web gives, imports and exports the web's file shape, and
/// lists the way the web sidebar does. Needs no database.
/// </summary>
public sealed class ViewerCustomViewsTests
{
    private const string BadNotebook = "{\"kind\":\"notebook\",\"cells\":[{\"type\":\"markdown\"}]}";

    private const string WrongParameter =
        "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\",\"params\":{\"no_such_parameter\":1}}]}";

    /* A required parameter missing, so the refusal comes from the declared-parameter table and not from the shape checks. */
    private const string MissingRequired =
        "{\"panels\":[{\"read\":\"get_alert_details\",\"viz\":\"table\",\"params\":{\"server_id\":1}}]}";

    [Theory]
    [InlineData(BadNotebook)]
    [InlineData(WrongParameter)]
    [InlineData(MissingRequired)]
    [InlineData("{\"panels\":[]}")]
    [InlineData("not json")]
    public async Task ABadDefinition_GetsTheViewerAndTheWebTheSameRefusalText_AndNothingIsWritten(string definition)
    {
        var web = DarlingWebEndpoints.ValidateDefinition(definition);
        Assert.False(web.IsValid);
        Assert.False(string.IsNullOrEmpty(web.Error));

        Assert.Equal(web.Error, ViewerDataService.ValidateCustomViewDefinition(definition));

        /* Create and update refuse before they touch the store, so a service with no reachable store still answers with the sentence. */
        await using var viewer = new ViewerDataService("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");
        var created = await viewer.CreateCustomViewAsync("a view", null, definition, null, TestContext.Current.CancellationToken);
        Assert.Equal(web.Error, Assert.IsType<CustomViewResult.Invalid>(created).Message);
        var updated = await viewer.UpdateCustomViewAsync(1, "a view", null, definition, 1, null, TestContext.Current.CancellationToken);
        Assert.Equal(web.Error, Assert.IsType<CustomViewResult.Invalid>(updated).Message);
    }

    [Fact]
    public void AGoodDefinition_IsAcceptedByTheViewerAsByTheWeb()
    {
        const string good = "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\"}]}";
        Assert.True(DarlingWebEndpoints.ValidateDefinition(good).IsValid);
        Assert.Null(ViewerDataService.ValidateCustomViewDefinition(good));
        Assert.Null(ViewerDataService.ValidateCustomViewDefinition(CustomViewsEditorModel.NewViewTemplate));
    }

    [Fact]
    public void ARoundTrip_ThroughExportAndImport_KeepsTheNameDescriptionAndDefinition()
    {
        var view = new CustomView(
            7, "Nightly waits", "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\"}]}", "what runs at night", 3,
            DateTime.UtcNow, DateTime.UtcNow, "erik", "Ops", false, "erik", false);

        var (imported, error) = CustomViewsEditorModel.ParseImport(CustomViewsEditorModel.ExportText(view));

        Assert.Null(error);
        Assert.NotNull(imported);
        Assert.Equal("Nightly waits", imported!.Name);
        Assert.Equal("what runs at night", imported.Description);
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(view.DefinitionJson), JsonNode.Parse(imported.DefinitionJson)));
    }

    [Theory]
    [InlineData("{ nope", "That is not valid JSON")]
    [InlineData("{\"name\":\"x\"}", "The JSON must contain a 'definition'")]
    [InlineData("{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\"}]}", "The imported JSON has no \"name\"")]
    public void AnImportFileTheWebWouldRefuse_IsRefusedWithTheWebsSentence(string text, string expectedStart)
    {
        var (imported, error) = CustomViewsEditorModel.ParseImport(text);

        Assert.Null(imported);
        Assert.StartsWith(expectedStart, error, StringComparison.Ordinal);
    }

    [Fact]
    public void ABareDefinitionWithATopLevelName_IsImportedLikeTheWebDoes()
    {
        var (imported, error) = CustomViewsEditorModel.ParseImport(
            "{\"name\":\"  Bare  \",\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\"}]}");

        Assert.Null(error);
        Assert.Equal("Bare", imported!.Name);
        Assert.Null(imported.Description);
    }

    [Fact]
    public void TheList_PutsTheTopLevelFirst_ThenEachFolderByName_AndViewsByNameInside()
    {
        CustomViewSummary S(long id, string name, string? folder) =>
            new(id, name, null, 1, DateTime.UtcNow, "erik", null, folder, false, null, false);

        var rows = CustomViewsEditorModel.Rows(new[] { S(1, "b", "Zeta"), S(2, "z", null), S(3, "a", "alpha"), S(4, "a", null), S(5, "c", "Zeta") });

        Assert.Equal(new long[] { 4, 2, 3, 1, 5 }, rows.Select(r => r.Id).ToArray());
        Assert.Equal(CustomViewsEditorModel.TopLevelLabel, rows[0].FolderDisplay);
    }

    [Fact]
    public void TheWriteBanner_NamesTheMissingPrivileges_AndIsAbsentWhenNoneIsMissing()
    {
        Assert.Null(new ViewerDataService.CustomViewWriteAccess(true, true, true).Banner);
        var banner = new ViewerDataService.CustomViewWriteAccess(false, true, false).Banner;
        Assert.Contains("INSERT or DELETE", banner, StringComparison.Ordinal);
        Assert.DoesNotContain("UPDATE", banner, StringComparison.Ordinal);
        Assert.False(new ViewerDataService.CustomViewWriteAccess(false, false, false).CanWrite);
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(203, false)]
    [InlineData(204, true)]
    public void TheManager_NeedsTheV204Columns(int? probedVersion, bool expected) =>
        Assert.Equal(expected, ViewerDataService.CustomViewsAvailable(probedVersion));
}

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and its own roles, and never touches another test's
   rows, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6390 S5 against a real store: a view the Viewer creates is the view the web reads, a stale version is refused with the store's own
/// text, and the write gate follows the login's own privileges on the views table.
/// </summary>
public sealed class ViewerCustomViewsLiveTests
{
    private const string Definition = "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\",\"params\":{\"hours\":4}}]}";
    private const string RolePassword = "ViewerCvPw01";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string StoreConnection(ScratchPostgres scratch, string? role = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = "collect,config,public" };
        if (role is not null)
        {
            builder.Username = role;
            builder.Password = RolePassword;
            builder.Pooling = false;
        }

        return builder.ConnectionString;
    }

    private static async Task<ScratchPostgres> NewStoreAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return scratch;
    }

    [Fact]
    public async Task AViewTheViewerCreates_IsReadBackByTheStoreAndTheWebWithTheSameFields_StampedWithTheWindowsUser()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Viewer custom view live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await NewStoreAsync(ct);
        await using var viewer = new ViewerDataService(StoreConnection(scratch));
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);

        var created = Assert.IsType<CustomViewResult.Ok>(
            await viewer.CreateCustomViewAsync("  Night waits  ", "what runs at night", Definition, " Ops ", ct)).View!;

        var store = new CustomViewStore(owner, 30);
        var read = Assert.IsType<CustomViewResult.Ok>(await store.GetAsync(created.Id, cancellationToken: ct)).View!;
        Assert.Equal(DarlingWebEndpoints.BuildFullViewNode(read).ToJsonString(), DarlingWebEndpoints.BuildFullViewNode(created).ToJsonString());
        Assert.Equal("Night waits", read.Name);
        Assert.Equal("Ops", read.Folder);
        Assert.Equal(ViewerDataService.CustomViewWriter, read.UpdatedBy);
        Assert.Equal(ViewerDataService.CustomViewWriter, read.CreatedBy);
        Assert.False(read.Personal);
        Assert.Equal(1, read.Version);

        var listed = await viewer.GetCustomViewsAsync(ct);
        var row = Assert.Single(listed);
        Assert.Equal(created.Id, row.Id);
        Assert.Equal("dashboard", row.Kind ?? "dashboard");
    }

    [Fact]
    public async Task AStaleVersion_IsRefusedWithTheStoresOwnText_AndTheNewerSaveStands()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Viewer custom view live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await NewStoreAsync(ct);
        await using var viewer = new ViewerDataService(StoreConnection(scratch));

        var created = Assert.IsType<CustomViewResult.Ok>(await viewer.CreateCustomViewAsync("Stale", null, Definition, null, ct)).View!;
        var second = Assert.IsType<CustomViewResult.Ok>(
            await viewer.UpdateCustomViewAsync(created.Id, "Stale", null, Definition.Replace("\"hours\":4", "\"hours\":8", StringComparison.Ordinal), created.Version, null, ct)).View!;
        Assert.Equal(2, second.Version);

        var stale = await viewer.UpdateCustomViewAsync(created.Id, "Stale", null, Definition, created.Version, null, ct);

        Assert.Equal(
            "This view was changed by someone else (current version 2). Reload it and re-apply your edit.",
            Assert.IsType<CustomViewResult.Conflict>(stale).Message);
        var stored = Assert.IsType<CustomViewResult.Ok>(await viewer.GetCustomViewAsync(created.Id, ct)).View!;
        Assert.Equal(2, stored.Version);
        Assert.Contains("\"hours\": 8", stored.DefinitionJson, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RenameMoveAndDelete_GoThroughTheStore_AndADuplicateNameInTheFolderIsRefused()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Viewer custom view live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await NewStoreAsync(ct);
        await using var viewer = new ViewerDataService(StoreConnection(scratch));

        var first = Assert.IsType<CustomViewResult.Ok>(await viewer.CreateCustomViewAsync("One", "d", Definition, null, ct)).View!;
        Assert.IsType<CustomViewResult.Ok>(await viewer.CreateCustomViewAsync("Two", null, Definition, "Ops", ct));

        var moved = Assert.IsType<CustomViewResult.Ok>(
            await viewer.UpdateCustomViewAsync(first.Id, "One", first.Description, first.DefinitionJson, first.Version, "Ops", ct)).View!;
        Assert.Equal("Ops", moved.Folder);
        Assert.Equal("d", moved.Description);

        var clash = await viewer.UpdateCustomViewAsync(moved.Id, "Two", moved.Description, moved.DefinitionJson, moved.Version, "Ops", ct);
        Assert.IsType<CustomViewResult.Conflict>(clash);

        Assert.IsType<CustomViewResult.Ok>(await viewer.DeleteCustomViewAsync(moved.Id, ct));
        Assert.IsType<CustomViewResult.NotFound>(await viewer.GetCustomViewAsync(moved.Id, ct));
        Assert.Single(await viewer.GetCustomViewsAsync(ct));
    }

    [Fact]
    public async Task TheWriteGate_FollowsTheLoginsOwnPrivilegesOnTheViewsTable()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Viewer custom view live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await NewStoreAsync(ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var suffix = Guid.NewGuid().ToString("N")[..10];
        var readOnly = "cv_ro_" + suffix;
        var insertOnly = "cv_ins_" + suffix;
        var bodySucceeded = false;
        try
        {
            foreach (var role in new[] { readOnly, insertOnly })
            {
                await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
                await ExecAsync(owner, $"GRANT CONNECT ON DATABASE \"{scratch.DatabaseName}\" TO {role}; GRANT USAGE ON SCHEMA config TO {role}; GRANT SELECT ON config.custom_views TO {role}", ct);
            }

            await ExecAsync(owner, $"GRANT INSERT ON config.custom_views TO {insertOnly}", ct);

            await using (var ownerSeat = new ViewerDataService(StoreConnection(scratch)))
            {
                Assert.True((await ownerSeat.GetCustomViewWriteAccessAsync(ct)).CanWrite);
            }

            await using (var readSeat = new ViewerDataService(StoreConnection(scratch, readOnly)))
            {
                var access = await readSeat.GetCustomViewWriteAccessAsync(ct);
                Assert.False(access.CanInsert);
                Assert.False(access.CanUpdate);
                Assert.False(access.CanDelete);
                Assert.Contains("INSERT or UPDATE or DELETE", access.Banner, StringComparison.Ordinal);
                Assert.Empty(await readSeat.GetCustomViewsAsync(ct));
            }

            await using (var insertSeat = new ViewerDataService(StoreConnection(scratch, insertOnly)))
            {
                var access = await insertSeat.GetCustomViewWriteAccessAsync(ct);
                Assert.True(access.CanInsert);
                Assert.False(access.CanUpdate);
                Assert.False(access.CanDelete);
                Assert.Contains("UPDATE or DELETE", access.Banner, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                foreach (var role in new[] { readOnly, insertOnly })
                {
                    await ExecAsync(owner, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};", CancellationToken.None);
                }
            });
        }
    }

    [Fact]
    public async Task TheRealViewerRole_HoldsTheWritePrivileges_SoALockedDownSeatCanStillManageViews()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Viewer custom view live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await NewStoreAsync(ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var role = "cv_viewer_" + Guid.NewGuid().ToString("N")[..10];
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {role} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(role, scratch.DatabaseName, "viewer"))
            {
                await ExecAsync(owner, statement, ct);
            }

            await using var seat = new ViewerDataService(StoreConnection(scratch, role));
            Assert.True((await seat.GetCustomViewWriteAccessAsync(ct)).CanWrite);
            var created = await seat.CreateCustomViewAsync("From a viewer seat", null, Definition, null, ct);
            Assert.IsType<CustomViewResult.Ok>(created);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {role}; DROP ROLE IF EXISTS {role};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
