/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Versioning;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live coverage for #4215's stored verdicts (<c>collect.managed_conf_verdicts</c>, V146) and #4251's restart
/// check — <see cref="DarlingStoreHostProfile.ComputeAndStoreManagedConfVerdictsAsync"/> and
/// <see cref="DarlingStoreHostProfile.ReadStoredManagedConfVerdictsAsync"/>, which no test reached against a
/// real PostgreSQL before PR #4336's body. Gated on DARLING_TEST_PGRUNTIME exactly like
/// <see cref="ManagedConfFileLiveTests"/>, which this file sits beside.
/// </summary>
/* #1776 own-store: every test here stands up its own throwaway cluster from the bundled runtime, same as
   ManagedConfFileLiveTests -- not a DARLING_TEST_PG store. */
[SupportedOSPlatform("windows")]
public sealed class ManagedConfVerdictsLiveTests
{
    private const string RuntimeGateMessage =
        "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe; " +
        "Darling\\tools\\fetch-pg-runtime.ps1 -KeepWork leaves one under artifacts\\pg-runtime-work\\assemble\\pg-runtime) " +
        "to run the managed-conf-verdicts live tests.";

    private static string SkipUnlessRuntimeAvailable()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot), RuntimeGateMessage);
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");
        return runtimeRoot!;
    }

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task ReloadAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = await OpenAsync(connectionString, ct);
        await using var command = new NpgsqlCommand("SELECT pg_reload_conf()", connection);
        await command.ExecuteScalarAsync(ct);
        /* SIGHUP-context settings need a beat to take effect in pg_settings after the reload signal. */
        await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
    }

    /// <summary>
    /// One managed store, one owner connection sequence, covering every verdict #4215/#4251 introduced:
    /// port/listen_addresses always <see cref="HostSettingVerdict.CommandLine"/>; a restart-only change
    /// (<c>shared_buffers</c>) reloaded rather than restarted reads <see cref="HostSettingVerdict.PendingRestart"/>
    /// from a NEW connection as the non-superuser viewer role; a reload-context invalid value
    /// (<c>max_wal_size</c>) reads <see cref="HostSettingVerdict.RejectedValue"/>, never PendingRestart; an
    /// <c>ALTER SYSTEM</c> on an owned key (<c>work_mem</c>) reads <see cref="HostSettingVerdict.OperatorOverride"/>
    /// attributed to <c>postgresql.auto.conf</c>; and the baseline itself proves every managed sizing key and
    /// <c>timezone</c> read <see cref="HostSettingVerdict.Matches"/> from <c>darling-managed.conf</c>,
    /// <c>log_timezone</c> stores no row, and the ssl trio is absent on a loopback-only store.
    /// </summary>
    [Fact]
    public async Task ComputeAndStoreManagedConfVerdicts_CoversRestartRejectedOverrideAndCommandLine_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-verdicts-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory };
        var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            var ct = timeout.Token;
            var connectionString = await owner.EnsureRunningAsync(ct);
            Assert.True(owner.StartedByThisProcess);

            await using (var migrateConnection = await OpenAsync(connectionString, ct))
            {
                await PgMigrations.MigrateAsync(migrateConnection, ct);
            }

            await using (var dataSource = NpgsqlDataSource.Create(
                new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString))
            {
                await DarlingManagedRoles.EnsureProvisionedAsync(dataSource, dataDirectory, NullLogger.Instance, ct);
            }

            var managedConfPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);

            async Task<IReadOnlyList<ManagedConfVerdictRow>> RecomputeAndReadAsViewerAsync()
            {
                await using (var ownerConnection = await OpenAsync(connectionString, ct))
                {
                    var profile = await DarlingStoreHostProfile.GatherStartupProfileAsync(config, ownerConnection, ct);
                    await DarlingStoreHostProfile.ComputeAndStoreManagedConfVerdictsAsync(
                        ownerConnection, dataDirectory, profile.Memory.EffectiveBytes, profile.DataVolume.FreeBytes,
                        owner.LastManagedConfWriteResult, NullLogger.Instance, ct);
                }

                /* Read from a NEW connection, as the viewer role -- never the owner/superuser -- both to prove
                   #4215's "the stored rows are readable by the viewer role" and to exercise the exact seam a
                   real --check-settings/MCP caller uses. */
                var viewerConnectionString = DarlingManagedPostgres.TryBuildViewerConnectionStringFromStoredCredential(config);
                Assert.NotNull(viewerConnectionString);
                await using var viewerConnection = await OpenAsync(viewerConnectionString!, ct);
                return await DarlingStoreHostProfile.ReadStoredManagedConfVerdictsAsync(viewerConnection, ct);
            }

            var baseline = await RecomputeAndReadAsViewerAsync();
            var byName = baseline.ToDictionary(r => r.SettingName, StringComparer.Ordinal);

            Assert.Equal(HostSettingVerdict.CommandLine, byName["port"].Verdict);
            Assert.Equal(HostSettingVerdict.CommandLine, byName["listen_addresses"].Verdict);

            /* #4215: on a store nobody has touched, every key the service manages reads Matches,
               attributed to darling-managed.conf. The include of that file is postgresql.conf's LAST line, so
               the file wins every key it sets; before this was fixed, AttributeManagedSetting's included-file branch
               called any winning line outside postgresql.conf an operator override, this file included, and
               all eight sizing keys plus timezone read OperatorOverride here. */
            var sizingKeys = new[]
            {
                "shared_buffers", "effective_cache_size", "maintenance_work_mem", "work_mem",
                "timescaledb.max_background_workers", "max_worker_processes", "max_connections", "max_wal_size",
            };
            foreach (var key in sizingKeys.Append("timezone"))
            {
                Assert.True(byName.TryGetValue(key, out var row), $"no stored verdict for {key}.");
                Assert.True(row.Verdict == HostSettingVerdict.Matches,
                    $"{key}: expected Matches on a fresh store, got {row.Verdict} ({row.SourceDescription}).");
                Assert.NotNull(row.SourceFile);
                Assert.EndsWith(ManagedConfFile.FileName, row.SourceFile, StringComparison.OrdinalIgnoreCase);
            }

            /* log_timezone: the service never sets it, so there is no
               managed value to compare, and initdb's own line would read OperatorOverride on every store. No
               row is stored for it at all. */
            Assert.False(byName.ContainsKey("log_timezone"), "log_timezone is never managed, so no verdict is stored for it.");

            /* A loopback-only store never passes the ssl trio on the command line -- nothing is stored for
               any of the three, same as any other key nothing here touches. */
            Assert.False(byName.ContainsKey("ssl"), "a loopback-only store must not store a verdict for ssl.");
            Assert.False(byName.ContainsKey("ssl_cert_file"), "a loopback-only store must not store a verdict for ssl_cert_file.");
            Assert.False(byName.ContainsKey("ssl_key_file"), "a loopback-only store must not store a verdict for ssl_key_file.");

            /* Restart-only change: rewrite the file, reload (never restart), and see PendingRestart. */
            var body = File.ReadAllText(managedConfPath);
            var rewrittenSharedBuffers = Regex.Replace(body, @"(?m)^shared_buffers = '.*'$", "shared_buffers = '256MB'");
            Assert.NotEqual(body, rewrittenSharedBuffers);
            File.WriteAllText(managedConfPath, rewrittenSharedBuffers, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            await ReloadAsync(connectionString, ct);

            var afterPendingRestart = await RecomputeAndReadAsViewerAsync();
            var pendingRow = afterPendingRestart.Single(r => r.SettingName == "shared_buffers");
            Assert.Equal(HostSettingVerdict.PendingRestart, pendingRow.Verdict);
            Assert.Contains("restart", pendingRow.Detail, StringComparison.OrdinalIgnoreCase);

            /* Reload-context invalid value: never PendingRestart, always RejectedValue -- the same guc.c
               errmsg text covers both causes (#4215), so context is what tells them apart. */
            var body2 = File.ReadAllText(managedConfPath);
            var rewrittenMaxWal = Regex.Replace(body2, @"(?m)^max_wal_size = '.*'$", "max_wal_size = 'not-a-size'");
            Assert.NotEqual(body2, rewrittenMaxWal);
            File.WriteAllText(managedConfPath, rewrittenMaxWal, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            await ReloadAsync(connectionString, ct);

            var afterRejected = await RecomputeAndReadAsViewerAsync();
            var rejectedRow = afterRejected.Single(r => r.SettingName == "max_wal_size");
            Assert.Equal(HostSettingVerdict.RejectedValue, rejectedRow.Verdict);

            /* ALTER SYSTEM on an owned key: OperatorOverride, attributed to postgresql.auto.conf + its line. */
            await using (var alterConnection = await OpenAsync(connectionString, ct))
            {
                await using (var alterCommand = new NpgsqlCommand("ALTER SYSTEM SET work_mem = '77MB'", alterConnection))
                {
                    await alterCommand.ExecuteNonQueryAsync(ct);
                }
            }
            await ReloadAsync(connectionString, ct);

            var afterOverride = await RecomputeAndReadAsViewerAsync();
            var overrideRow = afterOverride.Single(r => r.SettingName == "work_mem");
            Assert.Equal(HostSettingVerdict.OperatorOverride, overrideRow.Verdict);
            Assert.NotNull(overrideRow.SourceFile);
            Assert.EndsWith("postgresql.auto.conf", overrideRow.SourceFile, StringComparison.OrdinalIgnoreCase);
            Assert.NotNull(overrideRow.SourceLine);
        }
        finally
        {
            await owner.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>#4215: a hand edit of darling-managed.conf is stored with its changed keys as
    /// <see cref="HostSettingVerdict.OperatorOverride"/>, detail = "hand edit of darling-managed.conf" — the
    /// most specific evidence available wins over the plain managed-block classification.</summary>
    [Fact]
    public async Task HandEditedManagedConf_StoresChangedKeysAsOperatorOverride_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-verdicts-handedit-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory };
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            var ct = timeout.Token;

            var first = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var firstConnectionString = await first.EnsureRunningAsync(ct);
            await using (var migrateConnection = await OpenAsync(firstConnectionString, ct))
            {
                await PgMigrations.MigrateAsync(migrateConnection, ct);
            }
            await first.StopIfStartedByThisProcessAsync();

            var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
            var original = File.ReadAllText(managedPath);
            var handEdited = Regex.Replace(original, @"(?m)^maintenance_work_mem = '.*'$", "maintenance_work_mem = '111MB'");
            Assert.NotEqual(original, handEdited);
            File.WriteAllText(managedPath, handEdited, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));

            var second = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var connectionString = await second.EnsureRunningAsync(ct);
            try
            {
                Assert.True(second.StartedByThisProcess);
                Assert.True(second.LastManagedConfWriteResult is { HandEdited: true });

                await using var connection = await OpenAsync(connectionString, ct);
                var profile = await DarlingStoreHostProfile.GatherStartupProfileAsync(config, connection, ct);
                await DarlingStoreHostProfile.ComputeAndStoreManagedConfVerdictsAsync(
                    connection, dataDirectory, profile.Memory.EffectiveBytes, profile.DataVolume.FreeBytes,
                    second.LastManagedConfWriteResult, NullLogger.Instance, ct);

                var rows = await DarlingStoreHostProfile.ReadStoredManagedConfVerdictsAsync(connection, ct);
                var row = rows.Single(r => r.SettingName == "maintenance_work_mem");
                Assert.Equal(HostSettingVerdict.OperatorOverride, row.Verdict);
                Assert.Equal("hand edit of darling-managed.conf", row.Detail);
            }
            finally
            {
                await second.StopIfStartedByThisProcessAsync();
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>#4215: the ssl trio is stored as
    /// <see cref="HostSettingVerdict.CommandLine"/> only when <c>pg_settings.source</c> actually reads
    /// <c>command line</c> — an exposed store's own condition, reproduced directly here (via
    /// <see cref="DarlingManagedPostgres.BuildServerRuntimeOptions"/>'s exact runtime-override string and a
    /// throwaway cert from <see cref="StoreTlsCertificates"/>) rather than through the full network/firewall
    /// exposure path, which this test does not touch.</summary>
    [Fact]
    public async Task SslTrio_StoredAsCommandLine_WhenTheServerActuallyCarriesIt_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-verdicts-ssl-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var port = DarlingManagedPostgresTests.FindFreeTcpPort();
        var pgCtl = Path.Combine(runtimeRoot, "pgsql", "bin", "pg_ctl.exe");
        var initdb = Path.Combine(runtimeRoot, "pgsql", "bin", "initdb.exe");
        var logPath = Path.Combine(root.FullName, "pg.log");
        var started = false;
        try
        {
            RunProcess(initdb, $"-D \"{dataDirectory}\" -U darling -A trust --encoding=UTF8");

            var certPath = Path.Combine(root.FullName, "test-ssl-cert.pem").Replace('\\', '/');
            var keyPath = Path.Combine(root.FullName, "test-ssl-key.pem").Replace('\\', '/');
            var generated = StoreTlsCertificates.Create("127.0.0.1", IPAddress.Loopback, 1);
            File.WriteAllText(certPath, generated.ServerCertChainPem);
            File.WriteAllText(keyPath, generated.ServerKeyPem);

            var options = DarlingManagedPostgres.BuildServerRuntimeOptions(port, networkListenIp: null, certPath, keyPath);
            RunProcess(pgCtl, $"-D \"{dataDirectory}\" -l \"{logPath}\" -o \"{options}\" -w start");
            started = true;

            var connectionString =
                $"Host=127.0.0.1;Port={port};Username=darling;Database=postgres;Pooling=false;SSL Mode=Disable";
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(2));
            var ct = timeout.Token;

            await using var connection = await OpenAsync(connectionString, ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var profile = await DarlingStoreHostProfile.GatherStartupProfileAsync(
                new PostgresConfig { Managed = true, Port = port, DataDirectory = dataDirectory }, connection, ct);
            await DarlingStoreHostProfile.ComputeAndStoreManagedConfVerdictsAsync(
                connection, dataDirectory, profile.Memory.EffectiveBytes, profile.DataVolume.FreeBytes,
                writeResult: null, NullLogger.Instance, ct);

            var rows = await DarlingStoreHostProfile.ReadStoredManagedConfVerdictsAsync(connection, ct);
            var byName = rows.ToDictionary(r => r.SettingName, StringComparer.Ordinal);
            Assert.Equal(HostSettingVerdict.CommandLine, byName["ssl"].Verdict);
            Assert.Equal(HostSettingVerdict.CommandLine, byName["ssl_cert_file"].Verdict);
            Assert.Equal(HostSettingVerdict.CommandLine, byName["ssl_key_file"].Verdict);
        }
        finally
        {
            if (started)
            {
                RunProcess(pgCtl, $"-D \"{dataDirectory}\" -w stop -m immediate");
            }

            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>Deliberately does NOT redirect stdout/stderr: pg_ctl's own children (postgres and its forked
    /// workers) inherit those handles on Windows and keep the pipe's write end open long after pg_ctl itself
    /// exits, which hangs a <c>ReadToEnd()</c> against it forever. pg_ctl's <c>-l</c> already captures the
    /// server's own log for a failure to read back instead.</summary>
    private static void RunProcess(string fileName, string arguments)
    {
        using var process = new System.Diagnostics.Process
        {
            StartInfo = new System.Diagnostics.ProcessStartInfo(fileName, arguments)
            {
                UseShellExecute = false,
            },
        };
        process.Start();
        Assert.True(process.WaitForExit(TimeSpan.FromSeconds(60)), $"{fileName} {arguments} did not exit within 60s.");
        Assert.True(process.ExitCode == 0, $"{fileName} {arguments} failed ({process.ExitCode}).");
    }
}
