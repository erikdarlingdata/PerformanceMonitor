/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5366 against a real store: a route saved with a sealed destination still lists its channel in the presence column and is
/// opened by the service read; the one-transaction route save leaves no row behind when the second statement fails or a value
/// is refused; and the control-plane migration waits (and moves nothing) when the service has published no key.
/// </summary>
public sealed class ViewerWebhookSealLiveTests
{
    private const string SlackUrl = "https://example.test/slack-not-real";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task MigrateStoreAsync(ScratchPostgres scratch, System.Threading.CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
    }

    private static async Task<long> RouteCountAsync(ScratchPostgres scratch, System.Threading.CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand("SELECT count(*) FROM config.config_notification_routes", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task ASealedRoute_StillListsItsChannel_AndTheServiceReadOpensIt()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live webhook seal checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var key = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var sealer = new ViewerPasswordSealer(key.PublicKey);

            var typed = new NotificationRouteRow { MetricMatch = "High CPU", SlackUrl = SlackUrl };
            var routeId = await viewer.SaveNotificationRouteSealedAsync(typed, null, new NotificationRow(), sealer, null, ct);

            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var command = new NpgsqlCommand(
                    "SELECT slack_url, configured_channels FROM config.config_notification_routes WHERE route_id = $1", connection);
                command.Parameters.AddWithValue(routeId);
                await using var reader = await command.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(PasswordSeal.IsSealed(reader.GetString(0)));
                Assert.DoesNotContain("example.test", reader.GetString(0), StringComparison.Ordinal);
                Assert.Equal(new[] { "Slack" }, reader.GetFieldValue<string[]>(1));
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var view = await new StoreConfigProvider(postgres).LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var read = Assert.Single(view!.NotificationRoutes, r => r.RouteId == routeId);
            Assert.True(PasswordSeal.IsSealed(read.SlackUrl));
            var opened = DarlingWebhookSecrets.Open(new WebhooksConfig(), new[] { read }, DarlingPasswordKey.FromPrivateKey(key));
            Assert.Equal(SlackUrl, Assert.Single(opened.Routes).SlackUrl);
            Assert.Empty(opened.Failures);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TheRouteSave_IsOneTransaction_AFailedUpdateOrARefusedValueLeavesNoRow()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live webhook seal checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var key = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var sealer = new ViewerPasswordSealer(key.PublicKey);
            var before = await RouteCountAsync(scratch, ct);

            /* A typed value with no key to seal it: refused after the INSERT, so the INSERT is rolled back. */
            var refused = new NotificationRouteRow { MetricMatch = "High CPU", SlackUrl = SlackUrl };
            await Assert.ThrowsAsync<ViewerPasswordRefusedException>(
                () => viewer.SaveNotificationRouteSealedAsync(refused, null, new NotificationRow(), null, "No key.", ct));
            Assert.Equal(before, await RouteCountAsync(scratch, ct));

            /* The UPDATE itself fails: a trigger on the scratch store raises on every update. */
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await using var ddl = new NpgsqlCommand(@"
CREATE FUNCTION config.fail_route_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'update refused for the test'; END $$;
CREATE TRIGGER fail_route_update BEFORE UPDATE ON config.config_notification_routes FOR EACH ROW EXECUTE FUNCTION config.fail_route_update();", connection);
                await ddl.ExecuteNonQueryAsync(ct);
            }

            var typed = new NotificationRouteRow { MetricMatch = "High CPU", SlackUrl = SlackUrl };
            await Assert.ThrowsAsync<PostgresException>(
                () => viewer.SaveNotificationRouteSealedAsync(typed, null, new NotificationRow(), sealer, null, ct));
            Assert.Equal(before, await RouteCountAsync(scratch, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TheMigration_WithNoPublishedKey_WaitsAndLeavesTheUrlsAsTheyWere()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live webhook seal checks.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await MigrateStoreAsync(scratch, ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var row = new NotificationRow { TeamsUrl = "https://example.test/teams-not-real", SlackUrl = SlackUrl };

            var outcome = await ViewerControlPlaneMigration.SealWebhookValuesAsync(viewer, row, ct);

            Assert.Equal(ViewerControlPlaneMigration.SmtpSealOutcome.NoKey, outcome);
            Assert.Equal("https://example.test/teams-not-real", row.TeamsUrl);
            Assert.Equal(SlackUrl, row.SlackUrl);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TheMigration_WithNoUrlsToMove_NeedsNoKey()
    {
        var row = new NotificationRow();

        var outcome = await ViewerControlPlaneMigration.SealWebhookValuesAsync(null!, row, TestContext.Current.CancellationToken);

        Assert.Equal(ViewerControlPlaneMigration.SmtpSealOutcome.Done, outcome);
    }

    [Fact]
    public void TheMigrationSeal_BindsTheUrlsToTheSettingsRowAndEachChannelsProxy()
    {
        using var key = PasswordPrivateKey.Generate();
        var row = new NotificationRow
        {
            TeamsUrl = "https://example.test/teams-not-real", TeamsProxy = "http://proxy.example:8080",
            SlackUrl = SlackUrl, SlackProxy = "",
        };

        ViewerControlPlaneMigration.SealWebhookValues(row, new ViewerPasswordSealer(key.PublicKey));

        Assert.True(PasswordSeal.IsSealed(row.TeamsUrl));
        Assert.True(PasswordSeal.IsSealed(row.SlackUrl));
        Assert.Equal(
            "https://example.test/teams-not-real",
            PasswordSeal.Open(row.TeamsUrl, key, PasswordBinding.ForWebhook("teams", PasswordBinding.WebhookSettingsRow, "http://proxy.example:8080", null)));
        Assert.Equal(
            SlackUrl,
            PasswordSeal.Open(row.SlackUrl, key, PasswordBinding.ForWebhook("slack", PasswordBinding.WebhookSettingsRow, "", null)));
        Assert.Throws<PasswordSealException>(
            () => PasswordSeal.Open(row.TeamsUrl, key, PasswordBinding.ForWebhook("teams", PasswordBinding.WebhookSettingsRow, "", null)));
    }

    [Fact]
    public void TheMigrationSeal_OneUrlThatCannotBeStored_DoesNotStopTheOther()
    {
        using var key = PasswordPrivateKey.Generate();
        var row = new NotificationRow { TeamsUrl = "https://example.test/teams\uD800", SlackUrl = SlackUrl };

        ViewerControlPlaneMigration.SealWebhookValues(row, new ViewerPasswordSealer(key.PublicKey));

        Assert.Equal("", row.TeamsUrl);
        Assert.True(PasswordSeal.IsSealed(row.SlackUrl));
        Assert.True(new[] { row.TeamsUrl, row.SlackUrl }.All(v => !v.Contains("example.test", StringComparison.Ordinal)));
    }
}
