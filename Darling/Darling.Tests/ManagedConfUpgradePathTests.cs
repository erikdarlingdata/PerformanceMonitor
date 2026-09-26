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
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The major-upgrade path under the migrated managed-conf design (#4215): a MIGRATED conf's legacy appenders
/// never run again, the v14 <c>maintenance_work_mem</c> cap is major-aware, a fresh cluster the upgrade just
/// initdb'd is Legacy so it CAN heal, and the upgrade's own append call is wired outside the classifier.
/// </summary>
public sealed class ManagedConfUpgradePathTests : IDisposable
{
    private readonly string _dataDir;

    public ManagedConfUpgradePathTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "pm-4336-7-upgrade-path-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dataDir, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup; leftover temp dirs don't fail the run.
        }
    }

    private string ConfPath => Path.Combine(_dataDir, "postgresql.conf");
    private string ManagedPath => Path.Combine(_dataDir, ManagedConfFile.FileName);

    private void WriteConf(string text) => File.WriteAllText(ConfPath, text);
    private void WriteManaged(string text) => File.WriteAllText(ManagedPath, text);

    private static ManagedConfFile.RenderInputs SampleInputs(int postgresMajor = 18, long ramBytes = 17_179_869_184L) => new(
        FormulaVersion: ManagedConfFile.CurrentFormulaVersion,
        Platform: "windows",
        RamBytes: ramBytes,
        RamAuthoritative: true,
        ProcessorCount: 8,
        HypertableCount: 42,
        PostgresMajor: postgresMajor,
        DataVolumeFreeBytes: 100_000_000_000L,
        DataVolumeTotalBytes: 500_000_000_000L,
        DataVolumeAuthoritative: true,
        Port: 5432,
        EffectivePreloadList: null);

    private static Dictionary<string, string> DerivedValues(ManagedConfFile.RenderInputs inputs)
    {
        var derivedBody = ManagedConfFile.RenderBody(inputs);
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(derivedBody))
        {
            values[name] = value;
        }

        return values;
    }

    /// <summary>Builds a migrated <c>postgresql.conf</c> + <c>darling-managed.conf</c> pair by running the
    /// real <see cref="ManagedConfMigration.Rewrite"/> over the fixture base conf — the same shape
    /// <see cref="ManagedConfMigrationStateTests"/>'s <c>BuildMigratedPair</c> produces.</summary>
    private static (string PostgresqlConf, string ManagedConf) BuildMigratedPair(ManagedConfFile.RenderInputs inputs)
    {
        var baseConf = RepoFile.ReadRepoFile("Darling", "Darling.Tests", "Fixtures", "ManagedConf", "rehearsal-postgresql.conf");
        var derived = DerivedValues(inputs);
        var rewrite = ManagedConfMigration.Rewrite(baseConf, derived, inputs.Port);
        var managedConf = ManagedConfFile.RenderWithValues(inputs, derived);
        return (rewrite.NewConfText, managedConf);
    }

    /// <summary>
    /// Pin 1 (scenario-3 guard): a MIGRATED conf — the <see cref="ManagedConfMigration.Rewrite"/> output plus
    /// a valid stamp, exactly <see cref="ManagedConfMigrationStateTests.Classify_RewriteOutputWithValidStamp_IsVerified"/>'s
    /// setup — classifies as Verified, not Legacy, and its Rewrite output carries none of the fifteen legacy
    /// v-markers. Together with pin 4 (the append call is gated on Kind.Legacy) this is the proof that a
    /// migrated conf never gets the legacy blocks appended again on a major upgrade's restart of it.
    /// </summary>
    [Fact]
    public void MigratedConf_WithValidStamp_IsNotLegacy_AndRewriteOutputCarriesNoVMarker()
    {
        var inputs = SampleInputs();
        var (postgresqlConf, managedConf) = BuildMigratedPair(inputs);
        WriteConf(postgresqlConf);
        WriteManaged(managedConf);
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, managedConf);

        Assert.NotEqual(ManagedConfMigrationState.Kind.Legacy, ManagedConfMigrationState.Classify(_dataDir));

        foreach (var marker in DarlingManagedPostgres.AllManagedConfMarkers)
        {
            Assert.DoesNotContain(marker, postgresqlConf, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Pin 2 (major-aware cap): <see cref="DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap"/> is the
    /// ONE predicate both <c>ManagedConfFile.RenderBody</c>'s v14 append and
    /// <c>DarlingManagedPostgres.HealLegacyMaintenanceWorkMem</c>'s live heal call (#3909); the brief calls
    /// for reading it directly rather than re-deriving its threshold. Major is the only variable that flips
    /// its answer on the SAME value: one kB over
    /// <see cref="DarlingManagedPostgres.LegacyMaintenanceWorkMemMaxKb"/> needs the cap on 17 (and every
    /// earlier major) but not on 18, and the exact limiting value itself needs no cap on either major.
    /// </summary>
    [Fact]
    public void NeedsLegacyMaintenanceWorkMemCap_IsMajorAware_Pg17NeedsItPg18DoesNot()
    {
        var overLimitKb = DarlingManagedPostgres.LegacyMaintenanceWorkMemMaxKb + 1;
        var atLimitKb = DarlingManagedPostgres.LegacyMaintenanceWorkMemMaxKb;

        Assert.True(DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(17, overLimitKb + ""));
        Assert.False(DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(18, overLimitKb + ""));
        Assert.False(DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(17, atLimitKb + ""));
        Assert.False(DarlingManagedPostgres.NeedsLegacyMaintenanceWorkMemCap(18, atLimitKb + ""));

        var pg17Body = ManagedConfFile.RenderBody(SampleInputs(postgresMajor: 17));
        var pg18Body = ManagedConfFile.RenderBody(SampleInputs(postgresMajor: 18));

        Assert.DoesNotContain(DarlingManagedPostgres.ConfMarkerV14, pg17Body, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingManagedPostgres.ConfMarkerV14, pg18Body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Pin 3: a fresh cluster the upgrade's <c>initdb-new-cluster</c> step just created — a stock initdb
    /// conf, then the real <see cref="DarlingManagedPostgres.EnsureConfAppended"/> the upgrade's
    /// <c>AppendManagedConf</c> delegate calls — classifies Legacy (so it CAN heal on this and every later
    /// start) and its text carries <c>shared_preload_libraries</c> with no include of any other file.
    /// </summary>
    [Fact]
    public void FreshUpgradeInitdbDirectory_AfterEnsureConfAppended_IsLegacy()
    {
        WriteConf(
            "# -----------------------------\n" +
            "# PostgreSQL configuration file\n" +
            "# -----------------------------\n" +
            "port = 5432\n" +
            "#shared_preload_libraries = ''\t# (change requires restart)\n");

        var pg = new DarlingManagedPostgres(
            new PostgresConfig { Managed = true, Port = 5996, DataDirectory = _dataDir },
            NullLogger.Instance);
        pg.EnsureConfAppended(_dataDir);

        var conf = File.ReadAllText(ConfPath);

        Assert.Equal(ManagedConfMigrationState.Kind.Legacy, ManagedConfMigrationState.Classify(_dataDir));
        Assert.False(ManagedConfFile.HasManagedInclude(conf), "a freshly-appended-to conf has no managed include yet.");
        Assert.Contains("shared_preload_libraries", conf, StringComparison.Ordinal);
    }

    /// <summary>
    /// Pin 4 (source-text): <c>DarlingStoreUpgrade.cs</c>'s <c>context.AppendManagedConf(newDataDirectory);</c>
    /// call is NOT inside any <c>Classify</c>/<c>Kind.</c> condition — the 15 lines before it name neither. The
    /// SIBLING claim (that <c>EnsureConfAppended</c>'s call site inside <c>EnsureRunningAsync</c> IS gated on
    /// <c>Kind.Legacy</c>) is already pinned by
    /// <see cref="ManagedConfMigrationWiringTests.EnsureConfAppended_InEnsureRunningAsync_IsGatedOnLegacyConfState"/>
    /// and <see cref="ManagedConfMigrationWiringTests.UpgradeContextEnsureConfAppendedDelegate_IsNotGated"/> (that
    /// one reads the DELEGATE REFERENCE site in <c>DarlingManagedPostgres.cs</c>, not this file); this pin reads
    /// the CALL SITE in <c>DarlingStoreUpgrade.cs</c> instead, so it is not a duplicate.
    /// </summary>
    [Fact]
    public void AppendManagedConfCall_InDarlingStoreUpgrade_IsNotInsideAClassifyCondition()
    {
        var source = RepoFile
            .ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingStoreUpgrade.cs")
            .ReplaceLineEndings("\n");

        const string Needle = "context.AppendManagedConf(newDataDirectory);";
        var index = source.IndexOf(Needle, StringComparison.Ordinal);
        Assert.True(index >= 0, $"'{Needle}' is gone from DarlingStoreUpgrade.cs");

        var lines = source[..index].Split('\n');
        var precedingCount = Math.Min(15, lines.Length);
        var preceding = string.Join('\n', lines[^precedingCount..]);

        Assert.DoesNotContain("ManagedConfMigrationState", preceding, StringComparison.Ordinal);
        Assert.DoesNotContain("Classify", preceding, StringComparison.Ordinal);
        Assert.DoesNotContain("Kind.", preceding, StringComparison.Ordinal);
    }

    /// <summary>
    /// #4358 live pin: an operator line the OLD cluster's <c>postgresql.conf</c> holds below the
    /// <c>darling-managed.conf</c> include is present in the NEW cluster's <c>postgresql.conf</c> after a
    /// real major upgrade, and — read through <c>pg_file_settings</c>, not <c>pg_settings.sourcefile</c>,
    /// because sourcefile still names postgresql.conf for a value that has not changed since the last reload
    /// — it is APPLIED on the first start of the new cluster. No managed key is duplicated between the
    /// legacy-appended block and the carried operator block. RED on the pre-#4358 code
    /// (<c>DarlingStoreUpgrade</c> has no <c>CarryOperatorConfLinesAsync</c>, and
    /// <c>UpgradeDataDirectoryAsync</c> never calls it): the operator line never reaches the new cluster at
    /// all, so both the presence assertion and the pg_file_settings.applied assertion fail. Needs
    /// DARLING_TEST_PGRUNTIME_OLD (a PREVIOUS-major pg-runtime) and DARLING_TEST_PGRUNTIME_NEWZIP (a
    /// CURRENT-pins pg-runtime.zip) — the managed runtime this exercises is Windows-only, so this pin
    /// cannot run on macOS; CI decides it.
    /// </summary>
    [Fact]
    public async Task UpgradeInPlace_OperatorLineBelowInclude_CarriedAndApplied_NoManagedKeyDuplicated_Gated()
    {
        var oldRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_OLD");
        var newZip = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_NEWZIP");

        Assert.SkipWhen(string.IsNullOrWhiteSpace(oldRuntime) || string.IsNullOrWhiteSpace(newZip),
            "Set DARLING_TEST_PGRUNTIME_OLD to an assembled pg-runtime directory built from the PREVIOUS " +
            "PostgreSQL major (the folder containing pgsql\\bin\\pg_ctl.exe) and DARLING_TEST_PGRUNTIME_NEWZIP " +
            "to a pg-runtime.zip built from the CURRENT one. Darling\\tools\\new-upgraded-store-fixture.ps1 " +
            "produces both.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(oldRuntime!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME_OLD={oldRuntime} does not contain pgsql\\bin\\pg_ctl.exe.");
        Assert.SkipUnless(File.Exists(newZip!), $"DARLING_TEST_PGRUNTIME_NEWZIP={newZip} does not exist.");

        var root = Directory.CreateTempSubdirectory("darling-4358-opline-");
        try
        {
            var deployment = Path.Combine(root.FullName, "deploy");
            var runtimeRoot = Path.Combine(deployment, "pg-runtime");
            Directory.CreateDirectory(deployment);
            CopyDirectoryForTests(Path.Combine(oldRuntime!, "pgsql"), Path.Combine(runtimeRoot, "pgsql"));

            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            var config = new PostgresConfig { Managed = true, Port = FindFreeTcpPortForTests(), DataDirectory = dataDirectory };

            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(20));

            /* ---- 1. Build the OLD store on the OLD runtime through the product's own bootstrap. A SECOND
                    start migrates postgresql.conf to the darling-managed.conf include shape (#4336) —
                    #4358 is scoped to that migrated shape, so a second start is needed before the operator
                    line is added below the include. ---- */
            var bootstrap = new DarlingManagedPostgres(config, NullLogger<DarlingManagedPostgres>.Instance, runtimeRoot);
            await bootstrap.EnsureRunningAsync(timeout.Token);
            await bootstrap.StopIfStartedByThisProcessAsync();

            var bootstrap2 = new DarlingManagedPostgres(config, NullLogger<DarlingManagedPostgres>.Instance, runtimeRoot);
            await bootstrap2.EnsureRunningAsync(timeout.Token);
            await bootstrap2.StopIfStartedByThisProcessAsync();

            var confPath = Path.Combine(dataDirectory, "postgresql.conf");
            var conf = await File.ReadAllTextAsync(confPath, timeout.Token);
            Assert.True(ManagedConfFile.HasManagedInclude(conf),
                "expected postgresql.conf to carry the darling-managed.conf include after a second start (#4336)");

            /* ---- 2. The operator's own line, below the include — exactly what #4358 carries. ---- */
            await File.AppendAllTextAsync(confPath, "log_min_duration_statement = 4358\n", timeout.Token);

            /* Stamp the installed runtime, matching what extraction does at deploy time. */
            var stampSource = Path.Combine(root.FullName, "old-runtime-stamp-source.zip");
            ZipFile.CreateFromDirectory(
                Path.Combine(runtimeRoot, "pgsql"), stampSource, CompressionLevel.NoCompression, includeBaseDirectory: true);
            File.WriteAllText(
                Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName),
                DarlingStoreUpgrade.ComputeFileHash(stampSource));

            File.Copy(newZip!, Path.Combine(deployment, "pg-runtime.zip"));

            /* ---- 3. The real bootstrap runs the upgrade end to end. ---- */
            var log = new DarlingSelfAlertTests.CapturingLogger();
            var managed = new DarlingManagedPostgres(config, log, runtimeRoot);
            string connectionString;
            try
            {
                connectionString = await managed.EnsureRunningAsync(timeout.Token);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"The store upgrade bootstrap threw: {ex.Message}\n\n--- orchestration log ---\n{log}", ex);
            }

            try
            {
                Assert.Equal(DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded, managed.LastUpgradeOutcome.Status);

                var newConf = await File.ReadAllTextAsync(confPath, timeout.Token);
                Assert.Contains("log_min_duration_statement = 4358", newConf, StringComparison.Ordinal);
                Assert.Contains(ManagedConfMigration.MovedOperatorLinesComment, newConf, StringComparison.Ordinal);

                /* No managed key duplicated between the legacy-appended block and the carried operator block. */
                var occurrences = 0;
                var searchFrom = 0;
                while (true)
                {
                    var found = newConf.IndexOf("shared_preload_libraries = 'timescaledb'", searchFrom, StringComparison.Ordinal);
                    if (found < 0)
                    {
                        break;
                    }

                    occurrences++;
                    searchFrom = found + 1;
                }

                Assert.Equal(1, occurrences);

                await using var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString);
                await connection.OpenAsync(timeout.Token);
                await using var command = new NpgsqlCommand(
                    "SELECT applied FROM pg_file_settings WHERE name = 'log_min_duration_statement' AND setting = '4358'",
                    connection);
                var applied = await command.ExecuteScalarAsync(timeout.Token) as bool?;
                Assert.True(applied == true,
                    $"expected log_min_duration_statement = 4358 to be APPLIED per pg_file_settings after the upgrade's first start.\n\n--- orchestration log ---\n{log}");
            }
            finally
            {
                await managed.StopIfStartedByThisProcessAsync();
            }
        }
        finally
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_KEEP")))
            {
                TryDeleteTreeForTests(root.FullName);
            }
        }
    }

    private static int FindFreeTcpPortForTests()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static void CopyDirectoryForTests(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var file in Directory.GetFiles(source))
        {
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)));
        }

        foreach (var directory in Directory.GetDirectories(source))
        {
            CopyDirectoryForTests(directory, Path.Combine(destination, Path.GetFileName(directory)));
        }
    }

    private static void TryDeleteTreeForTests(string path)
    {
        try
        {
            foreach (var file in Directory.EnumerateFiles(path, "*", SearchOption.AllDirectories))
            {
                File.SetAttributes(file, FileAttributes.Normal);
            }

            Directory.Delete(path, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup.
        }
        catch (UnauthorizedAccessException)
        {
            // Best-effort cleanup.
        }
    }
}
