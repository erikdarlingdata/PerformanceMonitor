/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7295: a failed load of the saved Blocking Detected watermark is not "no saved watermark". The first pass of a new process
/// used to mark the server seeded before it loaded, and the load answered null for a store that could not be read, so the
/// gate ran from 0 for the rest of the run and the root blockers a page had already carried were paged again once the cooldown
/// ran out. The load now throws, the pass is skipped, the server stays unseeded, and the pass after the read works loads the
/// saved count, as a clean restart does. Drives <c>EvaluatePgBlockingAsync</c> itself over a role that is refused the table.
/// </summary>
/* #1776 own-store: ScratchPostgres mints a database of its own. */
[Collection("live-postgres")]
public sealed class PgBlockingRestartWorkerLiveTests
{
    private const string ServerName = "pgblocking-restart-worker";
    private const string Metric = "Blocking Detected";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly string Key = ServerId.ToString(CultureInfo.InvariantCulture);

    private static async Task PlantRootsAsync(NpgsqlConnection connection, long collectionId, int roots, int firstRoot, CancellationToken ct)
    {
        /* Each root has one victim that has waited a minute, over every wait bar, so a fire pages. */
        for (var root = firstRoot; root < firstRoot + roots; root++)
        {
            await using var command = connection.CreateCommand();
            command.CommandText = """
                INSERT INTO collect.pg_blocking_edges
                    (collection_id, collection_time, server_id, server_name, blocked_pid, blocking_pid,
                     blocking_backend_id, database_name, blocked_query, blocking_query,
                     query_text_may_be_truncated, blocking_is_idle_in_transaction, blocked_query_duration_ms)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, 'appdb', 'select victim', 'update root set x = 1', false, false, 60000)
                """;
            command.Parameters.AddWithValue(collectionId * 10 + root);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-2), DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(52_000 + root);
            command.Parameters.AddWithValue(51_000 + root);
            command.Parameters.AddWithValue((long)(900_000 + root));
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>One life of the service: a fresh worker, so nothing is in memory, sending to <paramref name="webhookUrl"/>.</summary>
    private static (Func<Task> Pass, DarlingWorker Worker) StartLife(
        NpgsqlDataSource dataSource, string webhookUrl, CancellationToken ct, Action<WebhookAlertService>? onWebhooks = null)
    {
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = webhookUrl;
        var settings = new DarlingAlertSettings(config);
        var history = new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        onWebhooks?.Invoke(webhooks);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance);

        var worker = new DarlingWorker(
            NullLogger<DarlingWorker>.Instance,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator())
        {
            StoreForTests = dataSource,
        };
        var flags = BindingFlags.NonPublic | BindingFlags.Instance;
        typeof(DarlingWorker).GetField("_alertDeliverer", flags)!.SetValue(worker, deliverer);
        var evaluate = typeof(DarlingWorker).GetMethod("EvaluatePgBlockingAsync", flags)!;

        var runtime = new ServerRuntime
        {
            Config = new MonitoredServer { Name = ServerName, Host = ServerName },
            ConnectionString = "Host=unused",
            Target = new CollectorTargetInfo(),
            StorageName = ServerName,
            ServerId = ServerId,
        };
        var snapshot = new AlertServerSnapshot(Key, ServerName, true, null, null, false, false, null) { ServerId = ServerId };

        return (() => (Task)evaluate.Invoke(worker, new object[] { runtime, snapshot, config, ct })!, worker);
    }

    private static T Field<T>(DarlingWorker worker, string name) =>
        (T)typeof(DarlingWorker).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(worker)!;

    [Fact]
    public async Task AFailedWatermarkLoadOnTheFirstPass_SkipsThePass_AndTheNextPassSeedsTheSavedWatermark_LikeACleanRestart()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the PostgreSQL blocking restart worker test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var roleName = "bl7295_" + Guid.NewGuid().ToString("N")[..8];
        var rolePassword = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
        var bodySucceeded = false;
        await using var owner = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString);
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            /* The role the worker connects as in the second life: every right the pass needs, so that the one revoked below is
               the only thing that stops it. A superuser would ignore the revoke. */
            foreach (var sql in new[]
            {
                $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'",
                $"GRANT USAGE ON SCHEMA collect, config, public TO {roleName}",
                $"GRANT ALL ON ALL TABLES IN SCHEMA collect, config, public TO {roleName}",
                $"GRANT ALL ON ALL SEQUENCES IN SCHEMA collect, config, public TO {roleName}",
            })
            {
                await using var grant = owner.CreateCommand(sql);
                await grant.ExecuteNonQueryAsync(ct);
            }

            await using var asRole = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = rolePassword,
                SearchPath = "collect,config,public",
                Pooling = false,
            }.ConnectionString);
            var state = new PgAlertStateStore(owner);

            await using var plant = new NpgsqlConnection(scratch.ConnectionString);
            await plant.OpenAsync(ct);
            await PlantRootsAsync(plant, 1, 3, 0, ct);

            /* First life: the three root blockers page, the page is delivered, and the watermark 3 is saved. */
            using var firstEndpoint = new CapturingWebhookEndpoint();
            var (firstPass, _) = StartLife(owner, firstEndpoint.Url, ct);
            await firstPass();
            Assert.Single(firstEndpoint.Bodies);
            Assert.Equal(3, await state.LoadEdgeTriggerWatermarkAsync(Key, Metric));

            /* Second life, first pass: the watermark cannot be read. The pass is skipped: no page, the server is not marked
               seeded, and no watermark is in memory to run the gate from. */
            await using (var revoke = owner.CreateCommand($"REVOKE SELECT ON config.config_edge_trigger_watermarks FROM {roleName}"))
            {
                await revoke.ExecuteNonQueryAsync(ct);
            }

            using var secondEndpoint = new CapturingWebhookEndpoint();
            WebhookAlertService? secondWebhooks = null;
            var (secondPass, secondWorker) = StartLife(asRole, secondEndpoint.Url, ct, webhooks => secondWebhooks = webhooks);
            /* The webhook cooldown has run out, as it has in the field when the service restarts minutes after the last page:
               its seed from the alert history would otherwise hold a second page for the same incident, and the test could
               not tell a skipped pass from a held one. Replaced the way the deadlock restart tests do it. */
            var webhookFlags = BindingFlags.NonPublic | BindingFlags.Instance;
            typeof(WebhookAlertService).GetField("_cooldown", webhookFlags)!.SetValue(secondWebhooks, new IncidentCooldown("webhook:", null));
            typeof(WebhookAlertService).GetField("_repeatBudget", webhookFlags)!.SetValue(secondWebhooks, new RepeatDeliveryBudget());
            await secondPass();
            Assert.Empty(secondEndpoint.Bodies);
            var seededAfterFailedPass = Field<ConcurrentDictionary<string, bool>>(secondWorker, "_pgBlockingWatermarkSeeded").ContainsKey(Key);
            var watermarkAfterFailedPass = Field<ConcurrentDictionary<string, int>>(secondWorker, "_lastAlertedPgBlockingCount").ContainsKey(Key);

            /* The read works again. The next pass loads the 3 the first page carried, so the same three roots page nothing. */
            await using (var restore = owner.CreateCommand($"GRANT SELECT ON config.config_edge_trigger_watermarks TO {roleName}"))
            {
                await restore.ExecuteNonQueryAsync(ct);
            }

            await secondPass();
            Assert.Empty(secondEndpoint.Bodies);
            Assert.True(Field<ConcurrentDictionary<string, bool>>(secondWorker, "_pgBlockingWatermarkSeeded").ContainsKey(Key));
            Assert.Equal(3, Field<ConcurrentDictionary<string, int>>(secondWorker, "_lastAlertedPgBlockingCount")[Key]);
            await secondPass();
            Assert.Empty(secondEndpoint.Bodies);
            Assert.Equal(3, await state.LoadEdgeTriggerWatermarkAsync(Key, Metric));

            /* The seed is done and the gate still works: a fourth root is a rise past the saved 3, so it is the pass that
               moves the watermark to 4. */
            await PlantRootsAsync(plant, 2, 1, 3, ct);
            await secondPass();
            Assert.Equal(4, await state.LoadEdgeTriggerWatermarkAsync(Key, Metric));

            /* The positive control: that pass is also the one that pages. The path that paged nothing above can page, and it pages the
               fourth root once. */
            Assert.Single(secondEndpoint.Bodies);

            /* The failed pass left the worker unseeded and with no watermark to run from. */
            Assert.False(seededAfterFailedPass);
            Assert.False(watermarkAfterFailedPass);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await using var drop = owner.CreateCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};");
                await drop.ExecuteNonQueryAsync(CancellationToken.None);
            });
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    /// <summary>
    /// #7295 round 3: the state table stays unreadable while the collected edges are readable. The watermark load fails on three
    /// passes in a row. The first two skip the pass, and the third seeds an empty watermark and pages (at worst one duplicate page,
    /// which is what every pass did before #7295) instead of staying silent for as long as the failure lasts.
    /// </summary>
    [Fact]
    public async Task AStateTableThatStaysUnreadable_SeedsTheWatermarkEmptyOnTheThirdFailedPass_AndThePageGoesOut()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the PostgreSQL blocking restart worker test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var roleName = "bl7295c_" + Guid.NewGuid().ToString("N")[..8];
        var rolePassword = Convert.ToHexString(System.Security.Cryptography.RandomNumberGenerator.GetBytes(16));
        var bodySucceeded = false;
        await using var owner = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString);
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            foreach (var sql in new[]
            {
                $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{rolePassword}'",
                $"GRANT USAGE ON SCHEMA collect, config, public TO {roleName}",
                $"GRANT ALL ON ALL TABLES IN SCHEMA collect, config, public TO {roleName}",
                $"GRANT ALL ON ALL SEQUENCES IN SCHEMA collect, config, public TO {roleName}",
            })
            {
                await using var grant = owner.CreateCommand(sql);
                await grant.ExecuteNonQueryAsync(ct);
            }

            await using var asRole = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = rolePassword,
                SearchPath = "collect,config,public",
                Pooling = false,
            }.ConnectionString);
            var state = new PgAlertStateStore(owner);

            await using var plant = new NpgsqlConnection(scratch.ConnectionString);
            await plant.OpenAsync(ct);
            await PlantRootsAsync(plant, 1, 3, 0, ct);

            /* First life: the three root blockers page, the page is delivered, and the watermark 3 is saved. */
            using var firstEndpoint = new CapturingWebhookEndpoint();
            var (firstPass, _) = StartLife(owner, firstEndpoint.Url, ct);
            await firstPass();
            Assert.Single(firstEndpoint.Bodies);
            Assert.Equal(3, await state.LoadEdgeTriggerWatermarkAsync(Key, Metric));

            /* Second life: the state table stays unreadable (the grant is revoked and not given back) while the collected edges are
               still readable. Before the cap this silenced the alert for as long as the failure lasted. */
            await using (var revoke = owner.CreateCommand($"REVOKE SELECT ON config.config_edge_trigger_watermarks FROM {roleName}"))
            {
                await revoke.ExecuteNonQueryAsync(ct);
            }

            using var secondEndpoint = new CapturingWebhookEndpoint();
            WebhookAlertService? secondWebhooks = null;
            var (secondPass, secondWorker) = StartLife(asRole, secondEndpoint.Url, ct, webhooks => secondWebhooks = webhooks);
            var webhookFlags = BindingFlags.NonPublic | BindingFlags.Instance;
            typeof(WebhookAlertService).GetField("_cooldown", webhookFlags)!.SetValue(secondWebhooks, new IncidentCooldown("webhook:", null));
            typeof(WebhookAlertService).GetField("_repeatBudget", webhookFlags)!.SetValue(secondWebhooks, new RepeatDeliveryBudget());

            for (var pass = 1; pass < SeedLoadCap.MaxFailedLoads; pass++)
            {
                await secondPass();
                Assert.Empty(secondEndpoint.Bodies);
                Assert.False(Field<ConcurrentDictionary<string, bool>>(secondWorker, "_pgBlockingWatermarkSeeded").ContainsKey(Key));
            }

            /* The third failed read seeds an empty watermark and the pass carries on: the three roots page once more (the duplicate the
               cap accepts). */
            await secondPass();
            Assert.Single(secondEndpoint.Bodies);
            Assert.True(Field<ConcurrentDictionary<string, bool>>(secondWorker, "_pgBlockingWatermarkSeeded").ContainsKey(Key));

            /* The read works again. The seed is done, so nothing is loaded again, nothing pages again, and the page that went out stays sent. */
            await using (var restore = owner.CreateCommand($"GRANT SELECT ON config.config_edge_trigger_watermarks TO {roleName}"))
            {
                await restore.ExecuteNonQueryAsync(ct);
            }

            await secondPass();
            await secondPass();
            Assert.Single(secondEndpoint.Bodies);
            Assert.Equal(3, await state.LoadEdgeTriggerWatermarkAsync(Key, Metric));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await using var drop = owner.CreateCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};");
                await drop.ExecuteNonQueryAsync(CancellationToken.None);
            });
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
