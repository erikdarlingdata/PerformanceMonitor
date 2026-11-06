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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7656: the Slack bot token and app-level token on the notification settings row. They are sealed as the webhook URLs are, to
/// their own slots on the settings row and to no proxy; the service opens them with the URLs; a value that is not sealed is read as
/// stored; a value that will not open names the token and never carries it; the Viewer seals a typed token and keeps a saved one
/// when its box is blank.
/// </summary>
public sealed class SlackBotTokenSettingsTests
{
    private const string BotToken = "xoxb-1111-2222-bot-token-not-real";
    private const string AppToken = "xapp-1-A111-2222-app-token-not-real";

    private static string Seal(PasswordPrivateKey key, string value, string slot) =>
        PasswordSeal.Seal(value, key.PublicKey, PasswordBinding.ForWebhook(slot, PasswordBinding.WebhookSettingsRow, null, null));

    private static void AssertNoToken(string text)
    {
        Assert.DoesNotContain(BotToken, text, StringComparison.Ordinal);
        Assert.DoesNotContain(AppToken, text, StringComparison.Ordinal);
    }

    [Fact]
    public void SealedBotAndAppTokens_OnTheSettingsRow_OpenToThePlainValue()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new WebhooksConfig
        {
            SlackBotToken = Seal(key, BotToken, "slack_bot"),
            SlackAppToken = Seal(key, AppToken, "slack_app"),
            SlackProxy = "http://proxy.example:8080",
        };

        var opened = DarlingWebhookSecrets.Open(stored, Array.Empty<NotificationRoute>(), DarlingPasswordKey.FromPrivateKey(key));

        Assert.Equal(BotToken, opened.Webhooks.SlackBotToken);
        Assert.Equal(AppToken, opened.Webhooks.SlackAppToken);
        Assert.Empty(opened.Failures);
        /* The tokens are bound to no proxy: the Slack webhook proxy is the URL's, not the bot's. */
        Assert.Equal("http://proxy.example:8080", opened.Webhooks.SlackProxy);
    }

    [Fact]
    public void ATokenThatIsNotSealed_IsReadAsStored_EvenWithoutAKey()
    {
        var stored = new WebhooksConfig { SlackBotToken = BotToken, SlackAppToken = AppToken };

        var opened = DarlingWebhookSecrets.Open(stored, Array.Empty<NotificationRoute>(), DarlingPasswordKey.Refusing("The password key is not available here."));

        Assert.Equal(BotToken, opened.Webhooks.SlackBotToken);
        Assert.Equal(AppToken, opened.Webhooks.SlackAppToken);
        Assert.Empty(opened.Failures);
    }

    [Fact]
    public void ATokenSealedForTheWrongSlot_DoesNotOpen()
    {
        using var key = PasswordPrivateKey.Generate();
        /* The bot token's sealed text copied into the app token's column opens in neither direction. */
        var stored = new WebhooksConfig { SlackAppToken = Seal(key, BotToken, "slack_bot") };

        var opened = DarlingWebhookSecrets.Open(stored, Array.Empty<NotificationRoute>(), DarlingPasswordKey.FromPrivateKey(key));

        Assert.Equal("", opened.Webhooks.SlackAppToken);
        var failure = Assert.Single(opened.Failures);
        Assert.StartsWith("The Slack app-level token could not be opened: ", failure.Text, StringComparison.Ordinal);
        AssertNoToken(failure.Text);
    }

    [Fact]
    public void ATokenThatWillNotOpen_IsLoggedByName_AndNoLogLineOrProblemCarriesAToken()
    {
        using var other = PasswordPrivateKey.Generate();
        using var key = PasswordPrivateKey.Generate();
        var config = new DarlingConfig();
        config.Webhooks.SlackBotToken = Seal(other, BotToken, "slack_bot");
        config.Webhooks.SlackAppToken = AppToken;
        config.Webhooks.SlackUrl = "https://example.test/slack-not-real";
        var logger = new CapturingLogger();
        var settings = new DarlingAlertSettings(config, logger, DarlingPasswordKey.FromPrivateKey(key));

        var problems = settings.WebhookChannelProblems;
        _ = settings.NotificationRoutes;

        var problem = Assert.Single(problems);
        Assert.StartsWith("The Slack bot token could not be opened: it was sealed to password key ", problem, StringComparison.Ordinal);
        Assert.Equal("", settings.SlackBotToken);
        Assert.Equal(AppToken, settings.SlackAppToken);
        /* A token that will not open is the bot's problem, not the webhook's: the Slack webhook channel stays on. */
        Assert.True(settings.SlackWebhookEnabled);
        Assert.NotEmpty(logger.Lines);
        foreach (var line in logger.Lines.Append(problem))
        {
            AssertNoToken(line);
        }
    }

    [Fact]
    public void TheSignature_ChangesWhenEitherTokenChanges()
    {
        var status = DarlingPasswordKey.Refusing("none").Status;
        var none = DarlingWebhookSecrets.Signature(new WebhooksConfig(), Array.Empty<NotificationRoute>(), status);
        var bot = DarlingWebhookSecrets.Signature(new WebhooksConfig { SlackBotToken = BotToken }, Array.Empty<NotificationRoute>(), status);
        var app = DarlingWebhookSecrets.Signature(new WebhooksConfig { SlackAppToken = AppToken }, Array.Empty<NotificationRoute>(), status);

        Assert.NotEqual(none, bot);
        Assert.NotEqual(none, app);
        Assert.NotEqual(bot, app);
    }

    [Fact]
    public void TheViewerSave_SealsATypedToken_ForItsSlotOnTheSettingsRow_AndNotForTheSlackProxy()
    {
        using var key = PasswordPrivateKey.Generate();
        var row = new NotificationRow { SlackBotToken = " " + BotToken + " ", SlackAppToken = AppToken, SlackProxy = "http://proxy.example:8080" };

        ViewerWebhookSealing.ResolveSettingsRow(row, null, new ViewerPasswordSealer(key.PublicKey), null);

        Assert.True(PasswordSeal.IsSealed(row.SlackBotToken));
        Assert.True(PasswordSeal.IsSealed(row.SlackAppToken));
        AssertNoToken(row.SlackBotToken);
        AssertNoToken(row.SlackAppToken);
        Assert.Equal(BotToken, PasswordSeal.Open(row.SlackBotToken, key, PasswordBinding.ForWebhook("slack_bot", PasswordBinding.WebhookSettingsRow, null, null)));
        Assert.Equal(AppToken, PasswordSeal.Open(row.SlackAppToken, key, PasswordBinding.ForWebhook("slack_app", PasswordBinding.WebhookSettingsRow, null, null)));
        Assert.Throws<PasswordSealException>(
            () => PasswordSeal.Open(row.SlackBotToken, key, PasswordBinding.ForWebhook("slack_bot", PasswordBinding.WebhookSettingsRow, "http://proxy.example:8080", null)));
    }

    [Fact]
    public void TheViewerSave_KeepsASavedToken_WhenItsBoxIsBlank_AndRemovesItOnTheClearMarker()
    {
        using var key = PasswordPrivateKey.Generate();
        var stored = new NotificationRow { SlackBotToken = Seal(key, BotToken, "slack_bot"), SlackAppToken = Seal(key, AppToken, "slack_app") };
        var row = new NotificationRow
        {
            SlackBotToken = ViewerWebhookSealing.CarryKept("", stored.SlackBotToken),
            SlackAppToken = ViewerWebhookSealing.CarryKept(ViewerWebhookSealing.ClearMarker, stored.SlackAppToken),
        };
        Assert.False(ViewerWebhookSealing.NeedsKey(row, stored));

        ViewerWebhookSealing.ResolveSettingsRow(row, stored, null, "No key.");

        Assert.Equal(stored.SlackBotToken, row.SlackBotToken);
        Assert.Equal("", row.SlackAppToken);
        Assert.Equal("", ViewerWebhookSealing.ShownText(stored.SlackBotToken));
        Assert.True(ViewerWebhookSealing.IsSaved(stored.SlackBotToken));
        Assert.True(ViewerWebhookSealing.NeedsKey(new NotificationRow { SlackAppToken = AppToken }, stored));
        Assert.False(stored.ValueEquals(row));
    }

    [Fact]
    public void TheViewerSave_WithNoKey_RefusesATypedToken_ByName()
    {
        var row = new NotificationRow { SlackAppToken = AppToken };

        var refused = Assert.Throws<ViewerPasswordRefusedException>(() => ViewerWebhookSealing.ResolveSettingsRow(row, null, null, "The service has published no key."));

        Assert.Equal("The service has published no key.", refused.Message);
    }

    [Fact]
    public async Task TheStartSeal_SealsPlainTokensOnTheSettingsRow_AndTheServiceOpensThem_WithNoTokenInAnyLogLine()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live Slack bot token checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                await PgMigrations.MigrateAsync(owner, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var logger = new CapturingLogger();
            var provider = new StoreConfigProvider(postgres, logger);
            var seeded = new DarlingConfig();
            seeded.Webhooks.SlackBotToken = BotToken;
            seeded.Webhooks.SlackAppToken = AppToken;
            await provider.SeedIfEmptyAsync(seeded, ct);
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);

            await provider.SealPlainWebhookSettingsAsync(ring, ct);

            await using (var command = postgres.CreateCommand("SELECT slack_bot_token, slack_app_token FROM config.config_notification WHERE id = 1"))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.True(PasswordSeal.IsSealed(reader.GetString(0)));
                Assert.True(PasswordSeal.IsSealed(reader.GetString(1)));
                AssertNoToken(reader.GetString(0));
                AssertNoToken(reader.GetString(1));
            }

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var opened = DarlingWebhookSecrets.Open(view!.Webhooks, Array.Empty<NotificationRoute>(), ring);
            Assert.Empty(opened.Failures);
            Assert.Equal(BotToken, opened.Webhooks.SlackBotToken);
            Assert.Equal(AppToken, opened.Webhooks.SlackAppToken);
            foreach (var line in logger.Lines)
            {
                AssertNoToken(line);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    internal sealed class CapturingLogger : ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception) + (exception is null ? "" : " " + exception));
    }
}

/// <summary>
/// #7656: <c>update_notification_channels</c> sets the Slack bot token and app-level token as it sets the Slack webhook URL: sealed by the
/// service, written through <c>set_notification_channels</c> as the provisioned <c>mcp</c> role, opened by the service's own reader to the
/// text that was typed, and answered only as set or not set.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class SlackBotTokenMcpLiveTests
{
    private const string BotToken = "xoxb-3333-4444-bot-token-not-real";
    private const string AppToken = "xapp-1-A333-4444-app-token-not-real";

    [Fact]
    public async Task TheChannelsTool_SetsTheTokensSealed_TheServiceOpensThem_AndNoAnswerCarriesOne()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the MCP Slack bot token checks.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await using var probe = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", owner);
            Assert.SkipWhen((bool)(await probe.ExecuteScalarAsync(ct))!, "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
        }

        var bodySucceeded = false;
        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                var ownerRole = new NpgsqlConnectionStringBuilder(owner.ConnectionString).Username ?? "darling";
                var batch = DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName));
                await using var provision = new NpgsqlCommand(batch, owner);
                await provision.ExecuteNonQueryAsync(ct);
            }

            var mcpString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = "mcp", Password = ProvisioningTestSecrets.McpPassword, SearchPath = PgSchemaGenerator.SearchPath, Pooling = false,
            }.ConnectionString;
            await using var mcp = NpgsqlDataSource.Create(mcpString);
            await using var owned = NpgsqlDataSource.Create(scratch.ConnectionString);
            var provider = new StoreConfigProvider(owned);
            await provider.SeedIfEmptyAsync(new DarlingConfig(), ct);
            using var key = PasswordPrivateKey.Generate();
            var ring = DarlingPasswordKey.FromPrivateKey(key);

            var set = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(
                mcp, new DarlingMcpAlertTools.NotificationChannelsRequest(
                    null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                    BotToken, AppToken),
                ring, null, ct);
            var doc = JsonDocument.Parse(set).RootElement;
            Assert.Equal("updated", doc.GetProperty("status").GetString());
            Assert.Equal("set", doc.GetProperty("secrets").GetProperty("slack_bot_token").GetProperty("after").GetString());
            Assert.Equal("not set", doc.GetProperty("secrets").GetProperty("slack_app_token").GetProperty("before").GetString());
            Assert.DoesNotContain(BotToken, set, StringComparison.Ordinal);
            Assert.DoesNotContain(AppToken, set, StringComparison.Ordinal);
            Assert.DoesNotContain("sealed:", set, StringComparison.Ordinal);

            var view = await provider.LoadViewAsync(new DarlingConfig(), ct);
            Assert.NotNull(view);
            var opened = DarlingWebhookSecrets.Open(view!.Webhooks, Array.Empty<NotificationRoute>(), ring);
            Assert.Empty(opened.Failures);
            Assert.Equal(BotToken, opened.Webhooks.SlackBotToken);
            Assert.Equal(AppToken, opened.Webhooks.SlackAppToken);

            /* The role cannot read a token column; the function stores only sealed text; a clear removes one and keeps the other. */
            await using (var denied = mcp.CreateCommand("SELECT slack_bot_token FROM config.config_notification"))
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => denied.ExecuteNonQueryAsync(ct));
                Assert.Equal("42501", ex.SqlState);
            }

            var cleared = await DarlingMcpAlertTools.UpdateNotificationChannelsAsync(
                mcp, new DarlingMcpAlertTools.NotificationChannelsRequest(
                    null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                    null, "-"),
                ring, null, ct);
            Assert.Equal("updated", JsonDocument.Parse(cleared).RootElement.GetProperty("status").GetString());
            var after = DarlingWebhookSecrets.Open((await provider.LoadViewAsync(new DarlingConfig(), ct))!.Webhooks, Array.Empty<NotificationRoute>(), ring);
            Assert.Equal(BotToken, after.Webhooks.SlackBotToken);
            Assert.Equal("", after.Webhooks.SlackAppToken);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var drop = new NpgsqlCommand($"{LiveCleanupBatch.DropOwnedByExistingProductRolesSql} DROP ROLE IF EXISTS admin; DROP ROLE IF EXISTS viewer; DROP ROLE IF EXISTS web; DROP ROLE IF EXISTS mcp", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
