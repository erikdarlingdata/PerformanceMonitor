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
using Microsoft.Extensions.Logging.Abstractions;
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
}
