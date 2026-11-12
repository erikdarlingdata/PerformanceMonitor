/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7446, proven against a real managed store on a throwaway cluster from the bundled runtime: the store runs with
/// <c>timescaledb.telemetry_level = off</c>, on a new store and on an existing store at its next start, an operator's
/// <c>ALTER SYSTEM</c> still wins, and TimescaleDB's <c>policy_telemetry</c> job follows the level (unscheduled while
/// off, scheduled again when the level is <c>basic</c>).
///
/// <para>#1776 own-store: deliberately NOT [Collection("live-postgres")]. This class never reads DARLING_TEST_PG. It
/// reads DARLING_TEST_PGRUNTIME and stands up its own cluster, like <see cref="DarlingManagedPostgresTests"/>.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class TelemetryLevelLiveTests
{
    private const string SettingProbeSql =
        "SELECT setting, boot_val, source, context, COALESCE(sourcefile, '') FROM pg_settings WHERE name = 'timescaledb.telemetry_level'";

    private const string JobScheduledSql =
        "SELECT scheduled FROM timescaledb_information.jobs WHERE proc_name = 'policy_telemetry'";

    [Fact]
    public async Task ManagedStore_RunsWithTelemetryOff_OnNewAndExistingStores_AndTheJobFollowsTheLevel_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) " +
            "to run the #7446 telemetry E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-telemetry-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = DarlingManagedPostgresTests.FindFreeTcpPort(),
            DataDirectory = dataDirectory,
        };
        var managedConfPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);

        DarlingManagedPostgres? owner = null;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));

            /* 1. A new store. */
            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var connectionString = await owner.EnsureRunningAsync(timeout.Token);
            var fresh = await ReadSettingAsync(connectionString, timeout.Token);

            /* boot_val is the positive control: the compiled-in default is basic, so an observed off cannot be
               the value this server would have had anyway. */
            Assert.Equal("basic", fresh.BootVal);
            Assert.Equal("off", fresh.Setting);
            Assert.EndsWith(ManagedConfFile.FileName, fresh.SourceFile.Replace('\\', '/'), StringComparison.Ordinal);

            /* The service creates the extension later in its start. Do it here so the job exists. */
            await ExecuteAsync(connectionString, "CREATE EXTENSION IF NOT EXISTS timescaledb", timeout.Token);
            Assert.True(await ReadJobScheduledAsync(connectionString, timeout.Token), "A new extension schedules policy_telemetry.");
            await owner.StopIfStartedByThisProcessAsync();
            NpgsqlConnection.ClearAllPools();

            /* 2. An existing store: the file as the previous version wrote it, with no telemetry line and a hash
               that matches what is left. The next service-owned start rewrites it. */
            var previous = StripTelemetryLine(await File.ReadAllTextAsync(managedConfPath, timeout.Token));
            Assert.DoesNotContain("timescaledb.telemetry_level", previous, StringComparison.Ordinal);
            await File.WriteAllTextAsync(managedConfPath, previous, timeout.Token);

            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            connectionString = await owner.EnsureRunningAsync(timeout.Token);
            var existing = await ReadSettingAsync(connectionString, timeout.Token);
            Assert.Equal("off", existing.Setting);
            Assert.EndsWith(ManagedConfFile.FileName, existing.SourceFile.Replace('\\', '/'), StringComparison.Ordinal);
            Assert.Contains("timescaledb.telemetry_level = 'off'", await File.ReadAllTextAsync(managedConfPath, timeout.Token), StringComparison.Ordinal);

            /* The job is unscheduled while the level is off: with it off the run returns false and is recorded as
               Failed, a daily false failure. */
            Assert.False(await ReadJobScheduledAsync(connectionString, timeout.Token), "The job must be unscheduled while the level is off.");

            /* 3. An operator's ALTER SYSTEM wins, and the job comes back at the next start. */
            await ExecuteAsync(connectionString, "ALTER SYSTEM SET timescaledb.telemetry_level = 'basic'", timeout.Token);
            await owner.StopIfStartedByThisProcessAsync();
            NpgsqlConnection.ClearAllPools();

            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            connectionString = await owner.EnsureRunningAsync(timeout.Token);
            var overridden = await ReadSettingAsync(connectionString, timeout.Token);
            Assert.Equal("basic", overridden.Setting);
            Assert.EndsWith("postgresql.auto.conf", overridden.SourceFile.Replace('\\', '/'), StringComparison.Ordinal);
            Assert.True(await ReadJobScheduledAsync(connectionString, timeout.Token), "The job must be scheduled again when the level is basic.");

            await owner.StopIfStartedByThisProcessAsync();
        }
        finally
        {
            if (owner is not null)
            {
                await owner.StopIfStartedByThisProcessAsync();
            }

            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// A new store's extension does not exist when the bootstrap reconciles the job, so the worker runs the pass again
    /// right after it creates the extension. Without that, the job stays scheduled on a new store whose service stays up
    /// and records a false Failed run every day until the first restart. This runs the worker's real collection loop on a
    /// new managed store and reads the job without a second start. Passing no reconcile delegate to the loop (or deleting
    /// the loop's call) leaves the job scheduled and fails the last assertion.
    /// </summary>
    [Fact]
    public async Task NewStore_TheJobIsUnscheduled_WithoutASecondStart_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) " +
            "to run the #7446 telemetry E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-telemetry-new-");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = DarlingManagedPostgresTests.FindFreeTcpPort(),
            DataDirectory = Path.Combine(root.FullName, "pg"),
        };
        var previousConfig = Environment.GetEnvironmentVariable("DARLING_CONFIG");

        DarlingManagedPostgres? owner = null;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));

            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var connectionString = await owner.EnsureRunningAsync(timeout.Token);

            /* The bootstrap's own pass found no extension: nothing exists to unschedule yet. */
            Assert.False(await ExtensionExistsAsync(connectionString, timeout.Token), "A new store has no TimescaleDB extension before the worker runs.");

            var configPath = Path.Combine(root.FullName, "darling.json");
            await File.WriteAllTextAsync(configPath, JsonSerializer.Serialize(new
            {
                postgres = new { managed = false, connectionString },
            }), timeout.Token);
            Environment.SetEnvironmentVariable("DARLING_CONFIG", configPath);
            var darlingConfig = DarlingConfig.Load();

            var logger = new DarlingStartWithNoServersLiveTests.RecordingLogger();
            var worker = new DarlingWorker(
                logger, NullLoggerFactory.Instance, new McpRuntimeState(), new WebRuntimeState(), new MonitoredServerRegistryState(),
                new CollectorRuntimeState(), new WebTlsCertificateState(), new BaselineCache(), new ReadLatencyAccumulator());
            var loop = typeof(DarlingWorker).GetMethod("RunCollectionLoopAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

            /* The same delegate the bootstrap hands the loop for a managed store. */
            Func<string, CancellationToken, Task> reconcile = async (cs, ct) => await owner.ReconcileTelemetryJobAsync(cs, ct);

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
            stop.CancelAfter(TimeSpan.FromMinutes(5));
            var task = (Task)loop.Invoke(
                worker, [darlingConfig, connectionString, null, null, null, null, false, null, null, reconcile, stop.Token])!;

            while (!logger.Lines.Any(l => l.Contains("No monitored servers are configured yet", StringComparison.Ordinal))
                && !task.IsCompleted)
            {
                await Task.Delay(100, timeout.Token);
            }

            Assert.False(task.IsFaulted, "the loop failed: " + task.Exception?.GetBaseException().Message);
            stop.Cancel();
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
            }

            Assert.True(await ExtensionExistsAsync(connectionString, timeout.Token), "The worker creates the extension.");
            Assert.False(await ReadJobScheduledAsync(connectionString, timeout.Token),
                "The job must be unscheduled on a new store without a second start.");
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_CONFIG", previousConfig);
            if (owner is not null)
            {
                await owner.StopIfStartedByThisProcessAsync();
            }

            NpgsqlConnection.ClearAllPools();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// The start that reloads the server so a legacy conf's new managed file takes effect (a new store) does it only
    /// when the migration verified. A start whose pending migration has no backup to resume from reports Failed and
    /// leaves the files unverified, and a reload there would put files the migration never verified in force. The first
    /// start is the positive control: its migration verifies, so the server logs the reload. The second start must not.
    /// </summary>
    [Fact]
    public async Task APendingMigrationWithNoBackup_DoesNotReloadTheServer_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) " +
            "to run the #7446 telemetry E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-telemetry-pending-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = DarlingManagedPostgresTests.FindFreeTcpPort(),
            DataDirectory = dataDirectory,
        };

        DarlingManagedPostgres? owner = null;
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));

            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await owner.EnsureRunningAsync(timeout.Token);
            Assert.Equal(ManagedConfVerificationStatus.Verified, owner.LastManagedConfVerification?.Status);
            await owner.StopIfStartedByThisProcessAsync();
            NpgsqlConnection.ClearAllPools();
            var reloadsAfterFirstStart = CountReloads(dataDirectory);
            Assert.True(reloadsAfterFirstStart >= 1, "Positive control: a verified legacy migration reloads the server, and the server logs it.");

            /* The state a crashed migration leaves with its backup gone: the pending file is there, the backup is not. */
            await File.WriteAllTextAsync(Path.Combine(dataDirectory, "darling-managed.conf.pending"), string.Empty, timeout.Token);
            foreach (var backup in Directory.GetFiles(dataDirectory, "postgresql.conf.pre-4215.*.bak"))
            {
                File.Delete(backup);
            }

            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await owner.EnsureRunningAsync(timeout.Token);
            Assert.Equal(ManagedConfVerificationStatus.Failed, owner.LastManagedConfVerification?.Status);
            await owner.StopIfStartedByThisProcessAsync();

            Assert.Equal(reloadsAfterFirstStart, CountReloads(dataDirectory));
        }
        finally
        {
            if (owner is not null)
            {
                await owner.StopIfStartedByThisProcessAsync();
            }

            NpgsqlConnection.ClearAllPools();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>How many times the server's own log says it reloaded its configuration files.</summary>
    private static int CountReloads(string dataDirectory)
    {
        var total = 0;
        foreach (var file in Directory.GetFiles(Path.Combine(dataDirectory, "log"), "*.log"))
        {
            using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            total += reader.ReadToEnd().Split("reloading configuration files").Length - 1;
        }

        return total;
    }

    private static async Task<bool> ExtensionExistsAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')", connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The file text without its telemetry line, re-hashed so it is not read as a hand edit.</summary>
    private static string StripTelemetryLine(string fileText)
    {
        var normalized = fileText.Replace("\r\n", "\n", StringComparison.Ordinal);
        var lines = normalized.Split('\n').ToList();
        var hashIndex = lines.FindIndex(l => l.StartsWith(ManagedConfFile.BodyHashPrefix, StringComparison.Ordinal));
        Assert.True(hashIndex >= 0, "The managed file should carry a body hash line.");
        var header = lines.Take(hashIndex).ToList();
        var body = string.Join('\n', lines.Skip(hashIndex + 1).Where(l => !l.StartsWith("timescaledb.telemetry_level", StringComparison.Ordinal)));
        header.Add(ManagedConfFile.BodyHashPrefix + ManagedConfFile.ComputeBodyHash(body));
        return string.Join('\n', header) + "\n" + body;
    }

    private static async Task<(string Setting, string BootVal, string Source, string Context, string SourceFile)> ReadSettingAsync(
        string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        /* The extension's own library defines the setting, so a session that has not touched it does not know it. */
        await using (var load = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS timescaledb", connection))
        {
            await load.ExecuteNonQueryAsync(ct);
        }

        await using var command = new NpgsqlCommand(SettingProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "pg_settings should know timescaledb.telemetry_level once the extension is loaded.");
        return (reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetString(4));
    }

    private static async Task<bool> ReadJobScheduledAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(JobScheduledSql, connection);
        var value = await command.ExecuteScalarAsync(ct);
        Assert.NotNull(value);
        return (bool)value!;
    }

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
