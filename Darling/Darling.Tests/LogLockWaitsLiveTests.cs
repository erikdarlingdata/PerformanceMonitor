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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9267, proven against a real managed store on a throwaway cluster from the bundled runtime: the store runs with
/// <c>log_lock_waits = on</c>, on a new store and on an existing store at its next start (a file written before the setting
/// was managed), and an operator's <c>ALTER SYSTEM</c> still wins.
///
/// <para>#1776 own-store: deliberately NOT [Collection("live-postgres")]. This class never reads DARLING_TEST_PG. It
/// reads DARLING_TEST_PGRUNTIME and stands up its own cluster, like <see cref="TelemetryLevelLiveTests"/>.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class LogLockWaitsLiveTests
{
    private const string SettingProbeSql =
        "SELECT setting, boot_val, COALESCE(sourcefile, '') FROM pg_settings WHERE name = 'log_lock_waits'";

    [Fact]
    public async Task ManagedStore_LogsLockWaits_OnNewAndExistingStores_AndAnAlterSystemStillWins_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) " +
            "to run the #9267 lock-wait logging E2E.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-lockwaits-");
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

            /* 1. A new store. boot_val is the positive control: the compiled-in default is off, so an observed on cannot be
               the value this server would have had anyway. */
            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var connectionString = await owner.EnsureRunningAsync(timeout.Token);
            var fresh = await ReadSettingAsync(connectionString, timeout.Token);
            Assert.Equal("off", fresh.BootVal);
            Assert.Equal("on", fresh.Setting);
            Assert.EndsWith(ManagedConfFile.FileName, fresh.SourceFile.Replace('\\', '/'), StringComparison.Ordinal);
            await owner.StopIfStartedByThisProcessAsync();
            NpgsqlConnection.ClearAllPools();

            /* 2. An existing store: the file as the previous version wrote it, with no lock-wait line and a hash that matches
               what is left. The next service-owned start rewrites it. */
            var previous = StripLine(await File.ReadAllTextAsync(managedConfPath, timeout.Token));
            Assert.DoesNotContain("log_lock_waits", previous, StringComparison.Ordinal);
            await File.WriteAllTextAsync(managedConfPath, previous, timeout.Token);

            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            connectionString = await owner.EnsureRunningAsync(timeout.Token);
            var existing = await ReadSettingAsync(connectionString, timeout.Token);
            Assert.Equal("on", existing.Setting);
            Assert.Contains("log_lock_waits = 'on'", await File.ReadAllTextAsync(managedConfPath, timeout.Token), StringComparison.Ordinal);

            /* 3. An operator's ALTER SYSTEM wins at the next start. */
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(timeout.Token);
                await using var alter = new NpgsqlCommand("ALTER SYSTEM SET log_lock_waits = 'off'", connection);
                await alter.ExecuteNonQueryAsync(timeout.Token);
            }

            await owner.StopIfStartedByThisProcessAsync();
            NpgsqlConnection.ClearAllPools();

            owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            connectionString = await owner.EnsureRunningAsync(timeout.Token);
            var overridden = await ReadSettingAsync(connectionString, timeout.Token);
            Assert.Equal("off", overridden.Setting);
            Assert.EndsWith("postgresql.auto.conf", overridden.SourceFile.Replace('\\', '/'), StringComparison.Ordinal);

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

    /// <summary>The file text without its lock-wait line, re-hashed so it is not read as a hand edit.</summary>
    private static string StripLine(string fileText)
    {
        var normalized = fileText.Replace("\r\n", "\n", StringComparison.Ordinal);
        var lines = normalized.Split('\n').ToList();
        var hashIndex = lines.FindIndex(l => l.StartsWith(ManagedConfFile.BodyHashPrefix, StringComparison.Ordinal));
        Assert.True(hashIndex >= 0, "The managed file should carry a body hash line.");
        var header = lines.Take(hashIndex).ToList();
        var body = string.Join('\n', lines.Skip(hashIndex + 1).Where(l => !l.StartsWith("log_lock_waits", StringComparison.Ordinal)));
        header.Add(ManagedConfFile.BodyHashPrefix + ManagedConfFile.ComputeBodyHash(body));
        return string.Join('\n', header) + "\n" + body;
    }

    private static async Task<(string Setting, string BootVal, string SourceFile)> ReadSettingAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(SettingProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "pg_settings should know log_lock_waits.");
        return (reader.GetString(0), reader.GetString(1), reader.GetString(2));
    }
}
