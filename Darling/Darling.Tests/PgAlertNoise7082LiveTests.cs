/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7082 on a live store: the PostgreSQL deadlock count against the real signature store (a new pair pages, a pair seen in
/// the last 30 days is held, 5 reports in the hour page), the #6005 agent gate for those pages (a first-ever pair opens its
/// investigation request, a repeat pair held for the digest opens none), and the provisioned-instance signal High CPU reads
/// from the target's own stored samples.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class PgAlertNoise7082LiveTests
{
    private const int ServerId = 42;
    private const string Server = "42";
    private const string Metric = "Deadlocks Detected";

    private const string UpdateOrders = "UPDATE orders SET qty = ? WHERE id = ?";
    private const string UpdateStock = "UPDATE stock SET qty = ? WHERE id = ?";
    private const string UpdateLedger = "UPDATE ledger SET qty = ? WHERE id = ?";

    private static string Graph(string first, string second) =>
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + $"Process 3101: {first}\n"
        + $"Process 3102: {second}";

    private static DarlingPgDeadlockReader.PgDeadlockRow Report(string hash, string first, string second, DateTime now) =>
        new(now.AddMinutes(-1), 3101, 2, hash, "ShareLock", "transaction", first, 1, Graph(first, second));

    private static string ValueOf(string first, string second) =>
        PgDeadlockSignature.For(Graph(first, second), 2, "ShareLock", "transaction").Value;

    private static AlertOutcome Page(AlertHold? hold = null) =>
        new(Server, "srv", Metric, "3", "1", null, "detail", NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: false, Severity: null, Hold: hold);

    private sealed record Harness(
        NpgsqlConnection Connection, string StoreString, DeadlockSignatureStore Store, DarlingDeadlockRollup Rollup, DarlingAlertDeliverer Deliverer);

    private static async Task RunAsync(Func<Harness, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the #7082 live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var historySource = NpgsqlDataSource.Create(storeString);
        await using var investigationSource = NpgsqlDataSource.Create(InvestigationDispatcher.WithLockTimeout(storeString));
        var signatures = new DeadlockSignatureStore(investigationSource, 30);
        var rollup = new DarlingDeadlockRollup(signatures);

        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.Summary;
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(1, AlertFamily.Performance, "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelInvestigate },
        };
        var settings = new DarlingAlertSettings(config);
        var history = new PgAlertHistoryStore(historySource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var dispatcher = new InvestigationDispatcher(
            new InvestigationRequestStore(investigationSource, 30), settings, NullLogger.Instance, agentGate: rollup.AllowsAgentRequestAsync);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, investigationDispatcher: dispatcher);

        await body(new Harness(connection, storeString, signatures, rollup, deliverer), ct);
    }

    private static async Task<long> RequestsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM config.investigation_requests", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task TheCount_PagesANewPair_HoldsAPairSeenInThirtyDays_AndPagesAtFiveReportsInTheHour()
    {
        await RunAsync(async (h, ct) =>
        {
            var now = DateTime.UtcNow;
            var counted = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            var unannounced = new HashSet<string>(StringComparer.Ordinal);

            Task<DarlingWorker.PgDeadlockPatterns> Count(IReadOnlyList<DarlingPgDeadlockReader.PgDeadlockRow> rows) =>
                DarlingWorker.CountPgDeadlockPatternsAsync(
                    h.Store, counted, unannounced, null, NullLogger.Instance, Server, ServerId, rows, now, ct);

            /* A pair the store has never seen: new, so it pages. */
            var first = await Count([Report("r1", UpdateOrders, UpdateStock, now)]);
            Assert.True(first.StoreOk);
            Assert.Equal(new[] { ValueOf(UpdateOrders, UpdateStock) }, first.NewKeys.ToArray());
            Assert.True(DarlingWorker.PgDeadlockPagesNow(1, first.NewKeys.Count, first.StoreOk));

            /* The page went out, so the worker forgets the announced pair. The same pair again, two reports: held. */
            unannounced.Clear();
            var again = await Count([Report("r1", UpdateOrders, UpdateStock, now), Report("r2", UpdateOrders, UpdateStock, now)]);
            Assert.Empty(again.NewKeys);
            Assert.False(DarlingWorker.PgDeadlockPagesNow(2, again.NewKeys.Count, again.StoreOk));

            /* A pair counted ten days ago is inside the 30 days: not new, so held. */
            await h.Store.AddAsync(ServerId, new Dictionary<string, int> { [ValueOf(UpdateOrders, UpdateLedger)] = 1 },
                DateOnly.FromDateTime(now.AddDays(-10)), ct);
            var known = await Count([Report("k1", UpdateOrders, UpdateLedger, now)]);
            Assert.Empty(known.NewKeys);
            Assert.False(DarlingWorker.PgDeadlockPagesNow(1, known.NewKeys.Count, known.StoreOk));

            /* Five reports in the hour page whatever their pair. */
            var five = Enumerable.Range(0, 5).Select(i => Report($"f{i}", UpdateOrders, UpdateStock, now)).ToList();
            var storm = await Count(five);
            Assert.Empty(storm.NewKeys);
            Assert.True(DarlingWorker.PgDeadlockPagesNow(five.Count, storm.NewKeys.Count, storm.StoreOk));
        });
    }

    [Fact]
    public async Task AFirstEverPostgresPair_OpensItsInvestigationRequest_AndARepeatPairHeldForTheDigest_OpensNone()
    {
        await RunAsync(async (h, ct) =>
        {
            var now = DateTime.UtcNow;
            var counted = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            var unannounced = new HashSet<string>(StringComparer.Ordinal);

            Task<DarlingWorker.PgDeadlockPatterns> Count(IReadOnlyList<DarlingPgDeadlockReader.PgDeadlockRow> rows) =>
                DarlingWorker.CountPgDeadlockPatternsAsync(
                    h.Store, counted, unannounced, fresh => h.Rollup.NoteNewSignatures(Server, fresh), NullLogger.Instance,
                    Server, ServerId, rows, now, ct);

            /* The first-ever pair pages, and the page opens its request. */
            var first = await Count([Report("r1", UpdateOrders, UpdateStock, now)]);
            Assert.True(DarlingWorker.PgDeadlockPagesNow(1, first.NewKeys.Count, first.StoreOk));
            await h.Deliverer.DeliverAsync(Page(), ct);
            Assert.Equal(1, await RequestsAsync(h.Connection, ct));
            unannounced.Clear();

            /* The same pair again is held for the digest: a held row is not delivered and opens nothing. */
            var again = await Count([Report("r1", UpdateOrders, UpdateStock, now), Report("r2", UpdateOrders, UpdateStock, now)]);
            Assert.False(DarlingWorker.PgDeadlockPagesNow(2, again.NewKeys.Count, again.StoreOk));
            await h.Deliverer.DeliverAsync(Page(new AlertHold("known deadlock pair")), ct);
            Assert.Equal(1, await RequestsAsync(h.Connection, ct));
        });
    }

    private static async Task SeedCpuAsync(
        NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime collectedUtc,
        double? acuPercent, double? capacityAcu, double? maxAcu)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO pg_cpu_utilization (collection_id, collection_time, server_id, server_name, sample_time, cpu_percent, "
            + "acu_utilization_percent, serverless_capacity_acu, max_configured_acu) VALUES ($1, $2, $3, 'srv', $2, 100, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(collectedUtc);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(acuPercent.HasValue ? acuPercent.Value : DBNull.Value);
        command.Parameters.AddWithValue(capacityAcu.HasValue ? capacityAcu.Value : DBNull.Value);
        command.Parameters.AddWithValue(maxAcu.HasValue ? maxAcu.Value : DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task OnlyATargetWhoseWeekOfSamplesCarriesNoCapacityFigure_IsKnownToBeProvisioned()
    {
        await RunAsync(async (h, ct) =>
        {
            var now = DateTime.UtcNow;
            const int provisioned = 1;
            const int serverless = 2;
            const int ceilingOnly = 3;
            const int oldFigureOnly = 4;

            for (var minute = 1; minute <= 5; minute++)
            {
                var at = now.AddMinutes(-minute);
                await SeedCpuAsync(h.Connection, ct, provisioned, at, null, null, null);
                /* Serverless: one minute in three has no ACU point, the others carry the figure. */
                var noPoint = minute % 3 == 0;
                await SeedCpuAsync(h.Connection, ct, serverless, at, noPoint ? null : 40, noPoint ? null : 4, noPoint ? null : 12);
                await SeedCpuAsync(h.Connection, ct, ceilingOnly, at, null, null, 12);
                await SeedCpuAsync(h.Connection, ct, oldFigureOnly, at, null, null, null);
            }

            /* A figure older than the lookback does not count against the target. */
            await SeedCpuAsync(h.Connection, ct, oldFigureOnly, now.AddDays(-30), 40, 4, 12);

            await using var source = NpgsqlDataSource.Create(h.StoreString);
            Assert.True(await DarlingPgCpuUtilizationReader.IsKnownProvisionedAsync(source, provisioned, now, ct));
            Assert.False(await DarlingPgCpuUtilizationReader.IsKnownProvisionedAsync(source, serverless, now, ct));
            Assert.False(await DarlingPgCpuUtilizationReader.IsKnownProvisionedAsync(source, ceilingOnly, now, ct));
            Assert.True(await DarlingPgCpuUtilizationReader.IsKnownProvisionedAsync(source, oldFigureOnly, now, ct));
        });
    }
}
