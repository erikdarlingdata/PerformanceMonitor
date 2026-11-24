/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
/// #9501 on a live store: the "Capacity at ceiling" alert, driven through <c>EvaluatePgCapacityCeilingAsync</c> and
/// <c>EvaluatePgCpuAsync</c> themselves over seeded <c>pg_cpu_utilization</c> series on an Aurora PostgreSQL target. A touch of the
/// ceiling for a minute or two at low instance CPU raises nothing, five or more consecutive samples raise one alert that carries the
/// figures, a mute rule on the new name is honoured, and no High CPU row speaks of the ceiling.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class PgCapacityCeilingWorkerLiveTests
{
    private const string ServerName = "capacity-ceiling-live";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly string Key = ServerId.ToString(CultureInfo.InvariantCulture);
    private const long Gib = 1073741824L;
    private const string CapacityMetric = AlertEngine.PgCapacityCeilingMetric;
    private const string HighCpuMetric = AlertEngine.CpuPersistenceMetric;

    /// <summary>One sample of the series: minutes before now, instance CPU, capacity percent, and free and cached GiB.</summary>
    private sealed record Seed(double MinutesAgo, double Cpu, double? CapacityPercent, long FreeGib = 4, long CachedGib = 4);

    /// <summary>A sample at the ceiling (100 percent of the configured capacity, 4 of 8 ACU) with instance CPU as stated.</summary>
    private static Seed Ceiling(double minutesAgo, double cpu = 2, long freeGib = 4, long cachedGib = 4) =>
        new(minutesAgo, cpu, 100, freeGib, cachedGib);

    /// <summary>A sample well under the ceiling.</summary>
    private static Seed Below(double minutesAgo) => new(minutesAgo, 3, 40);

    private sealed class Life
    {
        public required DarlingWorker Worker { get; init; }
        public required Func<Task> CapacityPass { get; init; }
        public required Func<Task> CpuPass { get; init; }

        /// <summary>The whole PostgreSQL alert sweep, the one call site the worker's sweep loop reaches the gates through.</summary>
        public required Func<Task> SweepPass { get; init; }
    }

    private sealed class Harness
    {
        public required NpgsqlDataSource Source { get; init; }
        public required CapturingWebhookEndpoint Endpoint { get; init; }
        public required CancellationToken Ct { get; init; }

        /// <summary>One life of the service: a fresh worker, so nothing is in memory.</summary>
        public Life StartLife(Func<AlertMuteContext, bool>? isMuted = null, string? webhookUrl = null)
        {
            var config = new DarlingConfig();
            config.Webhooks.GenericUrl = webhookUrl ?? Endpoint.Url;
            config.Alerts.CpuEnabled = true;
            var settings = new DarlingAlertSettings(config);
            var history = new PgAlertHistoryStore(Source, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
            var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
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
                StoreForTests = Source,
            };
            var flags = BindingFlags.NonPublic | BindingFlags.Instance;
            typeof(DarlingWorker).GetField("_alertDeliverer", flags)!.SetValue(worker, deliverer);
            if (isMuted is not null)
            {
                typeof(DarlingWorker).GetField("_isAlertMuted", flags)!.SetValue(worker, isMuted);
            }

            var runtime = new ServerRuntime
            {
                Config = new MonitoredServer { Name = ServerName, Host = ServerName },
                ConnectionString = "Host=unused",
                Target = new CollectorTargetInfo(),
                StorageName = ServerName,
                ServerId = ServerId,
            };
            var snapshot = new AlertServerSnapshot(Key, ServerName, true, null, null, false, false, null) { ServerId = ServerId };
            var ct = Ct;

            Func<Task> Pass(string method)
            {
                var evaluate = typeof(DarlingWorker).GetMethod(method, flags)!;
                return () => (Task)evaluate.Invoke(worker, new object[] { runtime, snapshot, config, ct })!;
            }

            return new Life
            {
                Worker = worker, CapacityPass = Pass("EvaluatePgCapacityCeilingAsync"), CpuPass = Pass("EvaluatePgCpuAsync"),
                SweepPass = Pass("EvaluatePostgresAlertsAsync"),
            };
        }

        public async Task SeedAsync(IEnumerable<Seed> series, DateTime now)
        {
            foreach (var seed in series)
            {
                var at = DateTime.SpecifyKind(now.AddMinutes(-seed.MinutesAgo), DateTimeKind.Unspecified);
                await using var command = Source.CreateCommand(
                    "INSERT INTO pg_cpu_utilization (collection_id, collection_time, server_id, server_name, sample_time, cpu_percent, "
                    + "acu_utilization_percent, serverless_capacity_acu, max_configured_acu, memory_total_bytes, memory_free_bytes, "
                    + "memory_cached_bytes) VALUES ($1, $2, $3, $4, $2, $5, $6, $7, 8, $8, $9, $10)");
                command.Parameters.AddWithValue(CollectionIdGenerator.Next());
                command.Parameters.AddWithValue(at);
                command.Parameters.AddWithValue(ServerId);
                command.Parameters.AddWithValue(ServerName);
                command.Parameters.AddWithValue(seed.Cpu);
                command.Parameters.AddWithValue(seed.CapacityPercent.HasValue ? seed.CapacityPercent.Value : DBNull.Value);
                command.Parameters.AddWithValue(seed.CapacityPercent.HasValue ? seed.CapacityPercent.Value / 100 * 4 : DBNull.Value);
                command.Parameters.AddWithValue(16 * Gib);
                command.Parameters.AddWithValue(seed.FreeGib * Gib);
                command.Parameters.AddWithValue(seed.CachedGib * Gib);
                await command.ExecuteNonQueryAsync(Ct);
            }
        }

        public async Task<List<(string Metric, bool Muted, string? Detail)>> RowsAsync()
        {
            var rows = new List<(string, bool, string?)>();
            await using var command = Source.CreateCommand(
                "SELECT metric_name, muted, detail_text FROM config_alert_log WHERE server_id = $1 ORDER BY alert_time");
            command.Parameters.AddWithValue(ServerId);
            await using var reader = await command.ExecuteReaderAsync(Ct);
            while (await reader.ReadAsync(Ct))
            {
                rows.Add((reader.GetString(0), reader.GetBoolean(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }

            return rows;
        }
    }

    private static async Task RunAsync(Func<Harness, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the #9501 capacity at ceiling worker tests.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(storeString);
            await using (var stop = source.CreateCommand("SELECT _timescaledb_functions.stop_background_workers()"))
            {
                await stop.ExecuteNonQueryAsync(ct);
            }

            await using (var connection = new NpgsqlConnection(storeString))
            {
                await connection.OpenAsync(ct);
                await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, "aurora-postgres", 16, ct);
            }

            using var endpoint = new CapturingWebhookEndpoint();
            await body(new Harness { Source = source, Endpoint = endpoint, Ct = ct });
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    private static T Field<T>(DarlingWorker worker, string name) =>
        (T)typeof(DarlingWorker).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(worker)!;

    private static bool CapacityAlertIsOpen(DarlingWorker worker) =>
        Field<ConcurrentDictionary<string, AlertPersistenceRecord>>(worker, "_pgCapacityPersistence").TryGetValue(Key, out var record)
        && record.State.Firing;

    /// <summary>A series that touches the ceiling for one to two minutes at a time and never holds five in a row.</summary>
    private static IEnumerable<Seed> TouchesOfOneToTwoMinutes() => new[]
    {
        Ceiling(13), Ceiling(12), Below(11), Ceiling(10), Below(9), Ceiling(8), Ceiling(7), Ceiling(6), Below(5),
        Ceiling(4), Below(3), Ceiling(2), Ceiling(1),
    };

    [Fact]
    public async Task CeilingTouchesOfOneToTwoMinutesAtLowCpu_RaiseNeitherAlert_AndWriteNoHighCpuRow()
    {
        await RunAsync(async h =>
        {
            await h.SeedAsync(TouchesOfOneToTwoMinutes(), DateTime.UtcNow);
            var life = h.StartLife();

            await life.CpuPass();
            await life.CapacityPass();

            Assert.Empty(await h.RowsAsync());
            Assert.Empty(h.Endpoint.Bodies);
            Assert.False(CapacityAlertIsOpen(life.Worker));
        });
    }

    [Fact]
    public async Task FiveConsecutiveCeilingSamplesAtLowCpu_OpenOneCapacityAlert_WithTheFiguresAtTheStartAndEndOfTheRun()
    {
        await RunAsync(async h =>
        {
            var now = DateTime.UtcNow;
            /* The run starts at 8 GiB free plus cached and ends at 3 GiB; instance CPU is 2 percent at the start and 1 at the end. */
            await h.SeedAsync(new[]
            {
                Ceiling(6, cpu: 2, freeGib: 3, cachedGib: 5),
                Ceiling(5, cpu: 2, freeGib: 3, cachedGib: 4),
                Ceiling(4, cpu: 2, freeGib: 2, cachedGib: 4),
                Ceiling(3, cpu: 2, freeGib: 2, cachedGib: 3),
                Ceiling(2, cpu: 1, freeGib: 1, cachedGib: 2),
            }, now);
            var life = h.StartLife();

            /* Through the sweep, so the test also fails if the worker stops calling the gate. */
            await life.SweepPass();
            /* A second sweep with no new sample is inside the cooldown and sends nothing more. */
            await life.SweepPass();

            var rows = await h.RowsAsync();
            var row = Assert.Single(rows);
            Assert.Equal(CapacityMetric, row.Metric);
            Assert.False(row.Muted);
            Assert.Single(h.Endpoint.Bodies);
            Assert.True(CapacityAlertIsOpen(life.Worker));
            Assert.Contains("4 ACU of a 8 ACU maximum", row.Detail, StringComparison.Ordinal);
            Assert.Contains("Instance CPU: 1%", row.Detail, StringComparison.Ordinal);
            Assert.Contains("8.0 GiB at the start of the run, 3.0 GiB now", row.Detail, StringComparison.Ordinal);
            /* The run began in this process from a count of zero, so its start is exact. */
            Assert.Contains(
                $"since {now.AddMinutes(-6):HH:mm} UTC", row.Detail, StringComparison.Ordinal);
            Assert.DoesNotContain("since at least", row.Detail, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ARunFoundHalfHeldAfterARestart_SaysSinceAtLeast_BecauseItsStartWasNotSeenByThisProcess()
    {
        await RunAsync(async h =>
        {
            var now = DateTime.UtcNow;
            /* The first life held two samples and was stopped; its record says so. The second life sees the next three. */
            await new PgAlertStateStore(h.Source).SaveAlertPersistenceAsync(
                Key, CapacityMetric, new AlertPersistenceRecord(new PersistenceState(2, 0, false), now.AddMinutes(-5)));
            await h.SeedAsync(new[] { Ceiling(4), Ceiling(3), Ceiling(2) }, now);
            var life = h.StartLife();

            await life.CapacityPass();

            var row = Assert.Single(await h.RowsAsync());
            Assert.Equal(CapacityMetric, row.Metric);
            Assert.Contains($"since at least {now.AddMinutes(-4):HH:mm} UTC", row.Detail, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AMuteRuleOnCapacityAtCeiling_IsHonoured_NothingIsDelivered_AndTheMutedRowIsRecorded()
    {
        await RunAsync(async h =>
        {
            await h.SeedAsync(new[] { Ceiling(6), Ceiling(5), Ceiling(4), Ceiling(3), Ceiling(2) }, DateTime.UtcNow);
            var life = h.StartLife(isMuted: context => context.MetricName == CapacityMetric);

            await life.CapacityPass();

            var row = Assert.Single(await h.RowsAsync());
            Assert.Equal(CapacityMetric, row.Metric);
            Assert.True(row.Muted);
            Assert.Empty(h.Endpoint.Bodies);
        });
    }

    [Fact]
    public async Task AMuteRuleOnHighCpu_DoesNotCoverTheCapacityAlert()
    {
        await RunAsync(async h =>
        {
            await h.SeedAsync(new[] { Ceiling(6), Ceiling(5), Ceiling(4), Ceiling(3), Ceiling(2) }, DateTime.UtcNow);
            var life = h.StartLife(isMuted: context => context.MetricName == HighCpuMetric);

            await life.CapacityPass();

            var row = Assert.Single(await h.RowsAsync());
            Assert.Equal(CapacityMetric, row.Metric);
            Assert.False(row.Muted);
            Assert.Single(h.Endpoint.Bodies);
        });
    }

    [Fact]
    public async Task NoHighCpuRowSpeaksOfTheCeiling_WhenTheCeilingRunHandsOverToARealCpuBreach()
    {
        await RunAsync(async h =>
        {
            /* Five ceiling samples at low CPU, one more, then three with instance CPU over the bar as well. */
            await h.SeedAsync(
                new[]
                {
                    Ceiling(9), Ceiling(8), Ceiling(7), Ceiling(6), Ceiling(5), Ceiling(4),
                    new Seed(3, 95, 100), new Seed(2, 96, 100), new Seed(1, 97, 100),
                },
                DateTime.UtcNow);
            var life = h.StartLife();

            await life.CpuPass();
            await life.CapacityPass();

            var rows = await h.RowsAsync();
            var highCpu = Assert.Single(rows, r => r.Metric == HighCpuMetric);
            Assert.DoesNotContain("capacity at ceiling", highCpu.Detail ?? string.Empty, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Sustained: 3 consecutive samples", highCpu.Detail, StringComparison.Ordinal);
            Assert.Single(rows, r => r.Metric == CapacityMetric);
            Assert.All(rows.Where(r => r.Metric == HighCpuMetric),
                r => Assert.DoesNotContain("capacity at ceiling", r.Detail ?? string.Empty, StringComparison.OrdinalIgnoreCase));

            /* The both-over samples are not held for this gate: two of them clear the capacity alert while High CPU is open. */
            Assert.True(CapacityAlertIsOpen(life.Worker));
            await life.CapacityPass();
            Assert.False(CapacityAlertIsOpen(life.Worker));
        });
    }

    [Fact]
    public async Task AOneSampleDipUnderAnOpenAlert_KeepsTheRunsStartAndItsMemory_ForTheReminderThatFollows()
    {
        await RunAsync(async h =>
        {
            var now = DateTime.UtcNow;
            /* The run starts at 8 GiB free plus cached. The alert opens on the fifth sample. */
            await h.SeedAsync(new[]
            {
                Ceiling(10, freeGib: 3, cachedGib: 5), Ceiling(9), Ceiling(8), Ceiling(7), Ceiling(6),
            }, now);
            var life = h.StartLife();
            await life.CapacityPass();
            Assert.True(CapacityAlertIsOpen(life.Worker));
            Assert.Single(await h.RowsAsync());

            /* One sample under the ceiling, then the ceiling again with 2 GiB free plus cached. One clear is not the two that resolve
               the alert, so it stays open, and the run it belongs to did not end. */
            await h.SeedAsync(new[] { Below(5), Ceiling(4, freeGib: 1, cachedGib: 1) }, now);
            Field<ConcurrentDictionary<string, DateTime>>(life.Worker, "_lastPgCapacityAlert")[Key] = DateTime.UtcNow.AddHours(-1);
            await life.CapacityPass();

            Assert.True(CapacityAlertIsOpen(life.Worker));
            var rows = await h.RowsAsync();
            Assert.Equal(2, rows.Count);
            var reminder = rows[1].Detail;
            Assert.Contains($"since {now.AddMinutes(-10):HH:mm} UTC", reminder, StringComparison.Ordinal);
            Assert.DoesNotContain("since at least", reminder, StringComparison.Ordinal);
            Assert.Contains("8.0 GiB at the start of the run, 2.0 GiB now", reminder, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ADipThatResolvesTheAlert_EndsTheRun_SoALaterRunStartsAfresh()
    {
        await RunAsync(async h =>
        {
            var now = DateTime.UtcNow;
            await h.SeedAsync(new[] { Ceiling(12), Ceiling(11), Ceiling(10), Ceiling(9), Ceiling(8) }, now);
            var life = h.StartLife();
            await life.CapacityPass();
            Assert.True(CapacityAlertIsOpen(life.Worker));

            /* Two samples under the ceiling resolve it, and the run ends with it. */
            await h.SeedAsync(new[] { Below(7), Below(6) }, now);
            await life.CapacityPass();
            Assert.False(CapacityAlertIsOpen(life.Worker));
            Assert.False(Field<ConcurrentDictionary<string, DarlingWorker.PgCapacityRun>>(life.Worker, "_pgCapacityRun").ContainsKey(Key));
        });
    }

    [Fact]
    public async Task ACapacityAlertWhoseSendFailedOnEveryChannel_IsSentAgainOnALaterEvaluation_NotAfterTheWholeCooldown()
    {
        await RunAsync(async h =>
        {
            var now = DateTime.UtcNow;
            await h.SeedAsync(new[] { Ceiling(6), Ceiling(5), Ceiling(4), Ceiling(3), Ceiling(2) }, now);
            /* The first post is refused, the second is accepted. */
            using var flaky = new CapturingWebhookEndpoint(statusSequence: new[] { 500, 200 });
            var life = h.StartLife(webhookUrl: flaky.Url);
            var stamps = Field<ConcurrentDictionary<string, DateTime>>(life.Worker, "_lastPgCapacityAlert");
            var cooldown = TimeSpan.FromMinutes(Field<int>(life.Worker, "_alertCooldownMinutes"));

            await life.CapacityPass();
            Assert.Single(flaky.Bodies);
            Assert.True(CapacityAlertIsOpen(life.Worker));
            /* The stamp was put back so the retry opens in about a minute, not after the whole cooldown. */
            Assert.True(stamps[Key] < DateTime.UtcNow - cooldown + TimeSpan.FromMinutes(2));

            /* A sweep right away is still inside that delay and sends nothing. */
            await life.CapacityPass();
            Assert.Single(flaky.Bodies);

            /* Once the delay has passed the same open alert is sent again, with no new sample, and this time it is accepted. */
            stamps[Key] = stamps[Key].AddMinutes(-2);
            await life.CapacityPass();
            Assert.Equal(2, flaky.Bodies.Count);

            /* Delivered now, it waits out the whole cooldown like any other. */
            await life.CapacityPass();
            Assert.Equal(2, flaky.Bodies.Count);
        });
    }

    [Fact]
    public async Task SamplesWithNoCapacityFigure_MoveTheWatermarkPastEachBatch_AndSaveNothingToTheStore()
    {
        await RunAsync(async h =>
        {
            var now = DateTime.UtcNow;
            /* A provisioned target: instance CPU and no ACU at all. Forty samples, more than one batch of GateBatchLimit (32). */
            var series = Enumerable.Range(0, 40).Select(i => new Seed(14 - i * 0.3, 5, null)).ToArray();
            await h.SeedAsync(series, now);
            var life = h.StartLife();
            var persistence = Field<ConcurrentDictionary<string, AlertPersistenceRecord>>(life.Worker, "_pgCapacityPersistence");

            await life.CapacityPass();
            var afterFirst = persistence[Key].LastObservedSampleUtc!.Value;
            /* The first batch is the 32 oldest samples: the watermark sits on the 32nd, not on the start. */
            Assert.InRange(afterFirst, now.AddMinutes(-(14 - 31 * 0.3) - 0.05), now.AddMinutes(-(14 - 31 * 0.3) + 0.05));

            await life.CapacityPass();
            var afterSecond = persistence[Key].LastObservedSampleUtc!.Value;
            /* The second read started after the first batch and took the last eight. */
            Assert.InRange(afterSecond, now.AddMinutes(-(14 - 39 * 0.3) - 0.05), now.AddMinutes(-(14 - 39 * 0.3) + 0.05));

            /* Nothing counted and nothing cleared, so the state never left its initial value and nothing was written. */
            Assert.Equal(PersistenceState.Initial, persistence[Key].State);
            Assert.Null(await new PgAlertStateStore(h.Source).LoadAlertPersistenceAsync(Key, CapacityMetric));
            Assert.Empty(await h.RowsAsync());
        });
    }
}
