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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live coverage for the #4215 service-owned settings file (<c>darling-managed.conf</c>) that <see
/// cref="ManagedConfFileTests"/>'s pure render/hash/hand-edit-detection tests and <see
/// cref="DarlingManagedPostgresTests"/>'s conf-text tests cannot reach on their own: a real <c>initdb</c>'d
/// cluster, a real <c>postgres -C</c> validation, and a real <c>pg_ctl</c> start/stop. Gated on
/// DARLING_TEST_PGRUNTIME exactly like <see cref="DarlingManagedPostgresTests"/>.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], and NOT for the reason a sweep might assume.
   This class never reads DARLING_TEST_PG at all — it reads DARLING_TEST_PGRUNTIME, which merely shares that
   prefix, and every test here stands up its OWN throwaway cluster from the bundled runtime. A substring search
   for "DARLING_TEST_PG" matches it anyway, so this note is here to stop the next one serializing a class that
   touches no shared store. */
public sealed class ManagedConfFileLiveTests
{
    private const string RuntimeGateMessage =
        "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe; " +
        "Darling\\tools\\fetch-pg-runtime.ps1 -KeepWork leaves one under artifacts\\pg-runtime-work\\assemble\\pg-runtime) " +
        "to run the darling-managed.conf live tests.";

    private static string SkipUnlessRuntimeAvailable()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot), RuntimeGateMessage);
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");
        return runtimeRoot!;
    }

    /// <summary>
    /// Rewrites a freshly rendered darling-managed.conf's <c>shared_buffers</c> line to a value <c>postgres
    /// -C</c> refuses (a known-bad example). A render only ever produces a bad value through a bug,
    /// so this is the seam a live test uses to reach the rejected-value path without one: installed through
    /// <see cref="DarlingManagedPostgres.TestOnlyRenderOverride"/>, never through anything a config file sets.
    /// </summary>
    private static string RejectSharedBuffers(string rendered)
        => Regex.Replace(rendered, @"(?m)^shared_buffers = '.*'$", "shared_buffers = 'bogus'");

    /// <summary>Names <see cref="DarlingManagedPostgres.LastManagedConfVerification"/>,
    /// <see cref="ManagedConfMigrationState.Classify"/> and the data directory's file names, so a migration-path
    /// assertion failure shows which of the four <see cref="ManagedConfMigrationState.Kind"/> states this start
    /// actually landed in rather than just the assertion's own value.</summary>
    private static string MigrationDiagnostics(DarlingManagedPostgres owner, string dataDirectory)
        => $"LastManagedConfVerification={owner.LastManagedConfVerification} | " +
           $"Classify(dataDir)={ManagedConfMigrationState.Classify(dataDirectory)} | " +
           $"files=[{string.Join(",", Directory.GetFiles(dataDirectory).Select(Path.GetFileName))}]";

    [Fact]
    public async Task FirstStartMigrates_SecondStartSourcesFromManagedConf_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory };
        var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            /* First start: the data directory is brand new, so ManagedConfMigrationState.Classify reads it
               as Legacy. The server boots on the legacy v-marker blocks appended into postgresql.conf, and
               ONLY AFTER that start does MigrateManagedConfAsync run Step A -- it rewrites postgresql.conf
               down to the one include line and writes darling-managed.conf with the effective values, but
               deliberately never reloads, so pg_settings still attributes everything to postgresql.conf until
               the NEXT start. */
            var first = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await first.EnsureRunningAsync(timeout.Token);
            Assert.True(first.StartedByThisProcess);

            Assert.Equal(
                ManagedConfVerificationStatus.Verified,
                first.LastManagedConfVerification?.Status);
            Assert.Equal(ManagedConfMigrationState.Kind.Verified, ManagedConfMigrationState.Classify(dataDirectory));
            Assert.True(File.Exists(managedPath), MigrationDiagnostics(first, dataDirectory));

            var postgresqlConfAfterFirst = File.ReadAllText(Path.Combine(dataDirectory, "postgresql.conf"));
            Assert.Equal(
                1,
                CountOccurrences(postgresqlConfAfterFirst, ManagedConfFile.IncludeLine));

            await first.StopIfStartedByThisProcessAsync();

            /* Second start: the same data directory now classifies Verified, so the pre-start write
               (EnsureManagedConfReadyAsync -> WriteManagedConfFile) runs and the server boots directly on
               darling-managed.conf. This is the start pg_settings.sourcefile can actually be checked against. */
            var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            var connectionString = await owner.EnsureRunningAsync(timeout.Token);
            try
            {
                Assert.True(owner.StartedByThisProcess);

                var postgresqlConf = File.ReadAllText(Path.Combine(dataDirectory, "postgresql.conf"));
                Assert.Equal(1, CountOccurrences(postgresqlConf, ManagedConfFile.IncludeLine));
                Assert.True(ManagedConfFile.HasManagedInclude(postgresqlConf));

                var managedBody = ManagedConfFile.ParseExisting(File.ReadAllText(managedPath)).Body;
                var keys = DarlingManagedPostgres.ParseConfText(managedBody).Select(entry => entry.Name).Distinct().ToList();
                Assert.NotEmpty(keys);

                /* port and listen_addresses always ride pg_ctl's "-o" runtime override
                   (StartServerAsync/BuildServerRuntimeOptions, unconditionally, loopback or not) -- a
                   command-line-set GUC reports a NULL sourcefile no matter what any config file also says.
                   darling-managed.conf still carries both for an operator reading the rendered file, but they are
                   exactly the "later override" this assertion's own wording carves out. timezone/log_timezone and
                   every timescaledb.* extension GUC are excluded on the same evidence, confirmed empirically on
                   this rig (see the PR body for the full first run's dump): PostgreSQL never records a
                   sourcefile/sourceline for them even though the config file (here, darling-managed.conf) is what
                   set the value actually in force -- current_setting still reports the darling-managed.conf value
                   correctly; only the attribution column is blank. */
                var keysWithoutReliableSourcefile = new HashSet<string>(StringComparer.Ordinal)
                {
                    "port", "listen_addresses", "timezone", "log_timezone",
                };

                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(timeout.Token);
                var checkedKeys = keys.Where(key => !keysWithoutReliableSourcefile.Contains(key) && !key.StartsWith("timescaledb.", StringComparison.Ordinal)).ToList();
                Assert.NotEmpty(checkedKeys);

                using var command = new NpgsqlCommand("SELECT name, sourcefile FROM pg_settings WHERE name = ANY(@names)", connection);
                command.Parameters.AddWithValue("names", checkedKeys.ToArray());
                using var reader = await command.ExecuteReaderAsync(timeout.Token);
                var sourcefiles = new Dictionary<string, string?>(StringComparer.Ordinal);
                while (await reader.ReadAsync(timeout.Token))
                {
                    sourcefiles[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
                }

                var mismatches = new List<string>();
                foreach (var key in checkedKeys)
                {
                    sourcefiles.TryGetValue(key, out var sourcefile);

                    /* A key ALSO set in postgresql.auto.conf (ALTER SYSTEM) sources from there instead -- read
                       after the whole postgresql.conf include chain finishes, so it always wins. Nothing writes
                       auto.conf on a fresh managed store, but the brief's own wording carries this caveat, so this
                       honors it rather than assumes it away. */
                    if (sourcefile is not null && string.Equals(Path.GetFileName(sourcefile), "postgresql.auto.conf", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (sourcefile is null || !string.Equals(Path.GetFileName(sourcefile), ManagedConfFile.FileName, StringComparison.Ordinal))
                    {
                        mismatches.Add($"{key}: sourcefile={sourcefile ?? "(null)"}");
                    }
                }

                Assert.True(
                    mismatches.Count == 0,
                    string.Join(" | ", mismatches) + " | " + MigrationDiagnostics(owner, dataDirectory));
            }
            finally
            {
                await owner.StopIfStartedByThisProcessAsync();
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    [Fact]
    public async Task RejectedValue_WithLastGood_FallsBackAndStarts_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory };
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            /* A clean first start is also what produces darling-managed.conf.last-good --
               SaveLastGoodManagedConf runs right after a successful service-owned start. */
            var first = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await first.EnsureRunningAsync(timeout.Token);
            await first.StopIfStartedByThisProcessAsync();

            var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
            var lastGoodPath = Path.Combine(dataDirectory, ManagedConfFile.LastGoodFileName);
            Assert.True(File.Exists(lastGoodPath));
            var lastGoodBytes = File.ReadAllBytes(lastGoodPath);

            /* Second start: the render is corrupted through the test-only seam, so postgres -C rejects it and
               the fallback to the last-good copy should be what actually starts. */
            var logger = new CapturingTestLogger();
            var second = new DarlingManagedPostgres(config, logger, runtimeRoot);
            DarlingManagedPostgres.TestOnlyRenderOverride.Value = RejectSharedBuffers;
            try
            {
                var connectionString = await second.EnsureRunningAsync(timeout.Token);
                Assert.True(second.StartedByThisProcess);

                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(timeout.Token);

                await second.StopIfStartedByThisProcessAsync();
            }
            finally
            {
                DarlingManagedPostgres.TestOnlyRenderOverride.Value = null;
            }

            /* The file in force after the fallback is exactly the last-good copy -- "the store starts on
               darling-managed.conf.last-good". */
            Assert.Equal(lastGoodBytes, File.ReadAllBytes(managedPath));

            Assert.True(logger.CountAtLevel(LogLevel.Error) >= 1, logger.Joined);
            Assert.Contains("was rejected by postgres -C", logger.Joined, StringComparison.Ordinal);
            Assert.Contains("Restored", logger.Joined, StringComparison.Ordinal);
            Assert.Contains("which still starts", logger.Joined, StringComparison.Ordinal);
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    [Fact]
    public async Task RejectedValue_NoLastGood_FailsStartWithRecoveryMessage_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory };
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            /* A brand-new data directory classifies Legacy on its first start (EnsureManagedConfReadyAsync
               never runs on Legacy -- the render-override seam it consults is inert until the conf reaches
               Verified), so a plain first start is needed just to drive this data directory to Verified
               before the render override can be reached at all. */
            var first = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await first.EnsureRunningAsync(timeout.Token);
            await first.StopIfStartedByThisProcessAsync();

            Assert.Equal(
                ManagedConfMigrationState.Kind.Verified,
                ManagedConfMigrationState.Classify(dataDirectory));

            /* That same first start's own SaveLastGoodManagedConf call (right after its successful start) is
               what would otherwise make "no last good" impossible on this data directory -- delete the copy
               it left so the second start below has nothing to fall back to. */
            var lastGoodPath = Path.Combine(dataDirectory, ManagedConfFile.LastGoodFileName);
            if (File.Exists(lastGoodPath))
            {
                File.Delete(lastGoodPath);
            }

            Assert.False(File.Exists(lastGoodPath));

            var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            try
            {
                DarlingManagedPostgres.TestOnlyRenderOverride.Value = RejectSharedBuffers;
                InvalidOperationException thrown;
                try
                {
                    thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => owner.EnsureRunningAsync(timeout.Token));
                }
                finally
                {
                    DarlingManagedPostgres.TestOnlyRenderOverride.Value = null;
                }

                Assert.Contains(
                    "was rejected by postgres -C",
                    thrown.Message,
                    StringComparison.Ordinal);
                Assert.Contains(
                    "ALTER SYSTEM",
                    thrown.Message,
                    StringComparison.Ordinal);

                /* EnsureManagedConfReadyAsync throws BEFORE StartServerAsync ever runs -- no ownership grab, no
                   postmaster left behind. */
                Assert.False(owner.StartedByThisProcess, MigrationDiagnostics(owner, dataDirectory));
                Assert.False(
                    File.Exists(Path.Combine(dataDirectory, "postmaster.pid")),
                    MigrationDiagnostics(owner, dataDirectory));
            }
            finally
            {
                await owner.StopIfStartedByThisProcessAsync();
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    [Fact]
    public async Task HandEditedFile_SurvivesRestartByteForByte_LogsDifferingKeys_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory };
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            var first = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await first.EnsureRunningAsync(timeout.Token);
            await first.StopIfStartedByThisProcessAsync();

            var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
            var original = File.ReadAllText(managedPath);

            /* A hand edit: work_mem changed to a different, still-valid value. The header's body-sha256 line is
               left exactly as the render wrote it, so it no longer matches the body -- ManagedConfFile.IsHandEdited. */
            var handEdited = Regex.Replace(original, @"(?m)^work_mem = '.*'$", "work_mem = '55MB'");
            Assert.NotEqual(original, handEdited);
            File.WriteAllText(managedPath, handEdited, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            var handEditedBytes = File.ReadAllBytes(managedPath);
            var handEditedWriteTime = File.GetLastWriteTimeUtc(managedPath);

            var logger = new CapturingTestLogger();
            var second = new DarlingManagedPostgres(config, logger, runtimeRoot);
            var connectionString = await second.EnsureRunningAsync(timeout.Token);
            try
            {
                Assert.True(second.StartedByThisProcess);

                /* Never overwritten -- byte for byte, same modified time, across the restart. */
                Assert.Equal(handEditedBytes, File.ReadAllBytes(managedPath));
                Assert.Equal(handEditedWriteTime, File.GetLastWriteTimeUtc(managedPath));

                Assert.True(logger.CountAtLevel(LogLevel.Warning) >= 1, logger.Joined);
                Assert.Contains("was hand-edited", logger.Joined, StringComparison.Ordinal);
                Assert.Contains("work_mem", logger.Joined, StringComparison.Ordinal);
                Assert.Contains("55MB", logger.Joined, StringComparison.Ordinal);

                await using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(timeout.Token);
                using var command = new NpgsqlCommand("SELECT current_setting('work_mem')", connection);
                Assert.Equal("55MB", (string)(await command.ExecuteScalarAsync(timeout.Token))!);
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

    [Fact]
    public async Task SecondStart_UnchangedInputs_DoesNotRewriteFile_Gated()
    {
        var runtimeRoot = SkipUnlessRuntimeAvailable();
        var root = Directory.CreateTempSubdirectory("darling-managedconf-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var config = new PostgresConfig { Managed = true, Port = DarlingManagedPostgresTests.FindFreeTcpPort(), DataDirectory = dataDirectory };
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));

            var first = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await first.EnsureRunningAsync(timeout.Token);
            await first.StopIfStartedByThisProcessAsync();

            var managedPath = Path.Combine(dataDirectory, ManagedConfFile.FileName);
            var bytesAfterFirst = File.ReadAllBytes(managedPath);
            var writeTimeAfterFirst = File.GetLastWriteTimeUtc(managedPath);

            var second = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
            await second.EnsureRunningAsync(timeout.Token);
            try
            {
                Assert.True(second.StartedByThisProcess);
                Assert.Equal(bytesAfterFirst, File.ReadAllBytes(managedPath));
                Assert.Equal(writeTimeAfterFirst, File.GetLastWriteTimeUtc(managedPath));
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

    private static int CountOccurrences(string text, string value)
    {
        var count = 0;
        var index = 0;
        while ((index = text.IndexOf(value, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += value.Length;
        }

        return count;
    }
}
