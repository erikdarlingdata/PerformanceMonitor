/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The service-sampler arm of <c>pg_wait_sampling</c> (#3604) run for real: the live store doubles as a
/// stock PostgreSQL TARGET without the extension, a lock wait and a CPU burner are held open on it, and the
/// real <see cref="DarlingCollectorRunner"/> runs one cycle. What must come out the other end: rows in
/// <c>pg_wait_sampling</c> at the sampler's period, a <c>Lock</c> series and a <c>CPU</c> series among them,
/// the collector's state row saying <c>service_sampled</c>, and the read disclosing it.
///
/// <para>Gated on <c>DARLING_TEST_PG</c> like every live class. Takes ~30 s by construction — the window IS
/// the test — so it is one cycle, not two; the cumulative-across-cycles property is proven with a fixture
/// reader in <c>Lite.Tests.PgWaitSamplerArmTests</c>, where it costs nothing.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgWaitSamplerLiveTests
{
    private const int ServerId = 360_4;

    [Fact]
    public async Task OneCycleAgainstAStockTarget_LandsServiceSampledRowsTheReadDiscloses()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live sampler test.");

        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        /* The TARGET is the store's own instance, reached with a distinct application_name so the sampler's
           self-exclusion (by the service's names) does not hide the workload, and so the workload's own
           backends are attributable. */
        var targetBuilder = new NpgsqlConnectionStringBuilder(connectionString) { ApplicationName = "pm3604-target", Pooling = false };
        var workloadBuilder = new NpgsqlConnectionStringBuilder(connectionString) { ApplicationName = "pm3604-workload", Pooling = false };

        var bodySucceeded = false;
        try
        {
            await using (var setup = new NpgsqlConnection(workloadBuilder.ConnectionString))
            {
                await setup.OpenAsync(ct);
                await using var cmd = new NpgsqlCommand("CREATE TABLE IF NOT EXISTS pm3604_lock_target (id int)", setup);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            /* Workload 1: a lock wait. Holder takes ACCESS EXCLUSIVE and sits; the waiter blocks on it for the
               whole window — wait_event_type Lock, wait_event relation. */
            await using var holder = new NpgsqlConnection(workloadBuilder.ConnectionString);
            await holder.OpenAsync(ct);
            await using var holderTx = await holder.BeginTransactionAsync(ct);
            await using (var lockCmd = new NpgsqlCommand("LOCK TABLE pm3604_lock_target IN ACCESS EXCLUSIVE MODE", holder, holderTx))
            {
                await lockCmd.ExecuteNonQueryAsync(ct);
            }

            using var workloadStop = new CancellationTokenSource();
            var waiter = Task.Run(async () =>
            {
                await using var c = new NpgsqlConnection(workloadBuilder.ConnectionString);
                await c.OpenAsync(workloadStop.Token);
                await using var q = new NpgsqlCommand("SELECT count(*) FROM pm3604_lock_target", c) { CommandTimeout = 120 };
                try { await q.ExecuteScalarAsync(workloadStop.Token); } catch (OperationCanceledException) { }
            }, CancellationToken.None);

            /* Workload 2: CPU. A backend on processor with no wait event — the arm's CPU/Running row. */
            var burner = Task.Run(async () =>
            {
                await using var c = new NpgsqlConnection(workloadBuilder.ConnectionString);
                await c.OpenAsync(workloadStop.Token);
                while (!workloadStop.IsCancellationRequested)
                {
                    await using var q = new NpgsqlCommand("SELECT count(*) FROM generate_series(1, 20000000)", c) { CommandTimeout = 120 };
                    try { await q.ExecuteScalarAsync(workloadStop.Token); } catch (OperationCanceledException) { }
                }
            }, CancellationToken.None);

            await Task.Delay(TimeSpan.FromSeconds(2), ct);

            var runtime = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "pm3604-stock", Host = targetBuilder.Host ?? "localhost", Engine = "postgres" },
                ConnectionString = targetBuilder.ConnectionString,
                Target = new CollectorTargetInfo
                {
                    Engine = CollectorTargetEngine.PostgreSql,
                    PostgresMajorVersion = 18,
                    PostgresVersionNum = 180000,
                    /* The rig has no pg_wait_sampling: the connect probe would say false, and so does this. */
                    HasPgWaitSamplingExtension = false,
                },
                StorageName = "pm3604-stock",
                ServerId = ServerId,
            };

            var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator(), NullLogger<DarlingCollectorRunner>.Instance);

            var wall = Stopwatch.StartNew();
            var result = await runner.RunAsync(PgWaitSamplingCollector.Instance, runtime, ct);
            wall.Stop();

            workloadStop.Cancel();
            await holderTx.RollbackAsync(CancellationToken.None);
            await Task.WhenAll(waiter, burner);

            /* 1. The run itself: rows landed, the window was the window. */
            Assert.True(result.Rows > 0, $"the sampler wrote no rows; note={result.HostNote}");
            var expectedWindowMs = (PgWaitSamplingCollector.SamplerSnapshotsPerCycle - 1) * PgWaitSamplingCollector.SamplerPeriodMs;
            Assert.InRange(wall.ElapsedMilliseconds, expectedWindowMs, expectedWindowMs + 30_000);

            /* 2. The rows: at the sampler's period, with the two workloads visible. */
            var rows = new List<(string Type, string Event, long Samples, int PeriodMs, int Backends, int? SampledMs)>();
            await using (var read = postgres.CreateCommand(
                "SELECT event_type, event, sample_count, profile_period_ms, backend_count, sampled_ms FROM pg_wait_sampling WHERE server_id = $1"))
            {
                read.Parameters.AddWithValue(ServerId);
                await using var reader = await read.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt64(2), reader.GetInt32(3), reader.GetInt32(4),
                        reader.IsDBNull(5) ? null : reader.GetInt32(5)));
                }
            }

            Assert.NotEmpty(rows);
            Assert.All(rows, r => Assert.Equal(PgWaitSamplingCollector.SamplerPeriodMs, r.PeriodMs));
            /* V133 (#3691): the real batch yields exactly SamplerSnapshotsPerCycle four-column result sets, so
               every row of the cycle stores the full window as its observed time - the denominator a rate read
               divides by instead of the 300 s interval. Through the real runner and the real COPY, so the
               appended column's position is proven against the migrated table, not a fake writer. */
            Assert.All(rows, r => Assert.Equal(PgWaitSamplingCollector.SamplerSnapshotsPerCycle * PgWaitSamplingCollector.SamplerPeriodMs, r.SampledMs));
            var lockRow = Assert.Single(rows, r => r.Type == "Lock" && r.Event == "relation");
            /* Held for the whole window, so seen in nearly every snapshot; allow for the first snapshot racing the waiter. */
            Assert.InRange(lockRow.Samples, PgWaitSamplingCollector.SamplerSnapshotsPerCycle / 2, PgWaitSamplingCollector.SamplerSnapshotsPerCycle);
            Assert.Equal(1, lockRow.Backends);
            Assert.Contains(rows, r => r.Type == "CPU" && r.Event == "Running");
            /* The shared exclusions hold on this arm too: the holder is idle in transaction (Client), and no
               background sleeper (Activity/Timeout) is counted. */
            Assert.DoesNotContain(rows, r => PgWaitStatsCollector.IgnoredWaitTypes.Contains(r.Type));

            /* 3. The instrument, recorded by the collector and read back by the reader the tools use. */
            var instrument = await DarlingPgWaitSamplingReader.GetWaitInstrumentAsync(postgres, ServerId, ct);
            Assert.NotNull(instrument);
            Assert.Equal(PgWaitInstrument.ServiceSampled, instrument.Instrument);

            var tally = await ReadStateAsync(postgres, PgWaitSamplingCollector.TallyStateKey, ct);
            Assert.Equal(lockRow.Samples, PgWaitSamplingCollector.ParseTally(tally)[("Lock", "relation", 0)]);

            /* 4. The read discloses it. */
            var page = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(
                postgres, ServerId, DateTime.UtcNow.AddHours(-1), DateTime.UtcNow.AddMinutes(1), 21, ct);
            var json = JsonDocument.Parse(DarlingMcpPgWaitSamplingTools.BuildWaitSamplingJson("pm3604-stock", 1, page, 20, instrument)).RootElement;
            Assert.Equal("service_sampled", json.GetProperty("instrument").GetString());
            Assert.Contains("FLOOR", json.GetProperty("instrument_note").GetString(), StringComparison.Ordinal);

            /* Reported, not asserted: the measured cost of one cycle on this rig. */
            Console.WriteLine(
                $"pg_wait_sampling service arm: wall {wall.ElapsedMilliseconds} ms, sql {result.SqlMs} ms, store {result.StorageMs} ms, "
                + $"{result.Rows} rows, {rows.Count} series; Lock/relation {lockRow.Samples}/{PgWaitSamplingCollector.SamplerSnapshotsPerCycle} snapshots");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                /* One statement per command: Npgsql does not bind positional parameters across a
                   multi-statement command. */
                foreach (var sql in new[]
                {
                    "DELETE FROM pg_wait_sampling WHERE server_id = $1",
                    "DELETE FROM collector_state WHERE server_id = $1",
                    "DELETE FROM collection_log WHERE server_id = $1",
                })
                {
                    await using var command = new NpgsqlCommand(sql, cleanup);
                    command.Parameters.AddWithValue(ServerId);
                    await command.ExecuteNonQueryAsync(cleanupCt);
                }

                await using var drop = new NpgsqlCommand("DROP TABLE IF EXISTS pm3604_lock_target", cleanup);
                await drop.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task<string?> ReadStateAsync(NpgsqlDataSource postgres, string key, CancellationToken ct)
    {
        await using var command = postgres.CreateCommand(
            "SELECT state_value FROM collector_state WHERE server_id = $1 AND collector_name = 'pg_wait_sampling' AND state_key = $2");
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(key);
        return await command.ExecuteScalarAsync(ct) as string;
    }
}
