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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
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
