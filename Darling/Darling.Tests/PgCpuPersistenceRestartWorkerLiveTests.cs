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
/// #7295: a failed load of the saved High CPU persistence record is not "no saved record". The first pass of a new process used to
/// mark the server seeded before it loaded, and the load answered null for a store that could not be read, so an incident that was
/// already open was forgotten: the gate restarted from zero, announced the incident again, and a page that was due came late. The
/// load now throws, the pass is skipped, the server stays unseeded, and the pass after the read works restores the saved record,
/// as a clean restart does. Drives <c>EvaluatePgCpuAsync</c> itself over a role that is refused the table.
/// </summary>
/* #1776 own-store: ScratchPostgres mints a database of its own. */
[Collection("live-postgres")]
public sealed class PgCpuPersistenceRestartWorkerLiveTests
{
    private const string ServerName = "pgcpu-restart-worker";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly string Key = ServerId.ToString(CultureInfo.InvariantCulture);

    /// <summary>One life of the service: a fresh worker, so nothing is in memory, sending to <paramref name="webhookUrl"/>.</summary>
    private static (Func<Task> Pass, DarlingWorker Worker) StartLife(
        NpgsqlDataSource dataSource, string webhookUrl, CancellationToken ct, Action<WebhookAlertService>? onWebhooks = null)
    {
        var config = new DarlingConfig();
        config.Webhooks.GenericUrl = webhookUrl;
        config.Alerts.CpuEnabled = true;
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
        var evaluate = typeof(DarlingWorker).GetMethod("EvaluatePgCpuAsync", flags)!;

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
    public async Task AFailedPersistenceLoadOnTheFirstPass_SkipsThePass_AndTheNextPassRestoresTheSavedRecord_LikeACleanRestart()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the PostgreSQL CPU restart worker test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var roleName = "cp7295_" + Guid.NewGuid().ToString("N")[..8];
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

            /* The first life left an incident open: the gate fired, so the saved record carries the Firing bit and a full streak. */
            var saved = new AlertPersistenceRecord(new PersistenceState(3, 0, true), null);
            await state.SaveAlertPersistenceAsync(Key, AlertEngine.CpuPersistenceMetric, saved);
            Assert.Equal(saved, await state.LoadAlertPersistenceAsync(Key, AlertEngine.CpuPersistenceMetric));

            /* Second life, first pass: the record cannot be read. The pass is skipped and the server is not marked seeded. */
            await using (var revoke = owner.CreateCommand($"REVOKE SELECT ON config.alert_persistence_state FROM {roleName}"))
            {
                await revoke.ExecuteNonQueryAsync(ct);
            }

            using var endpoint = new CapturingWebhookEndpoint();
            var (pass, worker) = StartLife(asRole, endpoint.Url, ct);
            await pass();
            var seededAfterFailedPass = Field<ConcurrentDictionary<string, bool>>(worker, "_pgCpuPersistenceSeeded").ContainsKey(Key);
            var recordAfterFailedPass = Field<ConcurrentDictionary<string, AlertPersistenceRecord>>(worker, "_pgCpuPersistence").ContainsKey(Key);

            /* The read works again. The next pass is the first load: it restores the saved record, and an incident that was open is
               stamped as just announced so no reminder follows it at once. This is the positive control for the skipped pass. */
            await using (var restore = owner.CreateCommand($"GRANT SELECT ON config.alert_persistence_state TO {roleName}"))
            {
                await restore.ExecuteNonQueryAsync(ct);
            }

            await pass();
            Assert.True(Field<ConcurrentDictionary<string, bool>>(worker, "_pgCpuPersistenceSeeded").ContainsKey(Key));
            Assert.True(Field<ConcurrentDictionary<string, AlertPersistenceRecord>>(worker, "_pgCpuPersistence")[Key].State.Firing);
            Assert.True(Field<ConcurrentDictionary<string, DateTime>>(worker, "_lastPgCpuAlert").ContainsKey(Key));

            /* The failed pass left the worker unseeded and with no record to run from. */
            Assert.False(seededAfterFailedPass);
            Assert.False(recordAfterFailedPass);

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
