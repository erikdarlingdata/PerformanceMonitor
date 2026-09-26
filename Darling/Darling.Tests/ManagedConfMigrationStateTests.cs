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
using PerformanceMonitor.Darling.Service;
using Xunit;
using static PerformanceMonitor.Darling.Service.ManagedConfMigrationState;

namespace Darling.Tests;

/// <summary>
/// <see cref="ManagedConfMigrationState.Classify"/> (#4336): the pure gate read
/// once before the legacy appenders, over a real temp data directory so the include/marker/pending/stamp
/// checks all run against files on disk exactly as the caller sees them.
/// </summary>
public sealed class ManagedConfMigrationStateTests : IDisposable
{
    private readonly string _dataDir;

    public ManagedConfMigrationStateTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "pm-4336-5c-classify-" + Guid.NewGuid().ToString("N"));
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

    private static ManagedConfFile.RenderInputs SampleInputs(int port = 5432) => new(
        FormulaVersion: ManagedConfFile.CurrentFormulaVersion,
        Platform: "windows",
        RamBytes: 17_179_869_184L,
        RamAuthoritative: true,
        ProcessorCount: 8,
        HypertableCount: 42,
        PostgresMajor: 18,
        DataVolumeFreeBytes: 100_000_000_000L,
        DataVolumeTotalBytes: 500_000_000_000L,
        DataVolumeAuthoritative: true,
        Port: port,
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

    /// <summary>Builds a migrated <c>postgresql.conf</c> + <c>darling-managed.conf</c> pair by running the real
    /// <see cref="ManagedConfMigration.Rewrite"/> over a fresh initdb-style base conf — the same shape
    /// <see cref="ManagedConfMigrationRunner.RunStepA"/> would produce.</summary>
    private (string PostgresqlConf, string ManagedConf) BuildMigratedPair(ManagedConfFile.RenderInputs inputs)
    {
        var baseConf = ReadFixture();
        var derived = DerivedValues(inputs);
        var rewrite = ManagedConfMigration.Rewrite(baseConf, derived, inputs.Port);
        var managedConf = ManagedConfFile.RenderWithValues(inputs, derived);
        return (rewrite.NewConfText, managedConf);
    }

    private const string FixtureRelativePath = "Darling.Tests/Fixtures/ManagedConf/rehearsal-postgresql.conf";

    private static string ReadFixture() => File.ReadAllText(FindRepoRootedFile(FixtureRelativePath));

    /// <summary>Resolves a repo-rooted relative path from wherever the test assembly happens to run, the same
    /// upward-walk idiom <see cref="ManagedConfRehearsalTests"/> uses.</summary>
    private static string FindRepoRootedFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 10 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = Path.GetDirectoryName(dir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        }

        throw new FileNotFoundException($"Could not locate '{relativePath}' walking up from '{AppContext.BaseDirectory}'.");
    }

    /// <summary>A fresh initdb-style conf — no markers, no include — is Legacy.</summary>
    [Fact]
    public void Classify_FreshInitdbConf_IsLegacy()
    {
        WriteConf("# -----------------------------\n# PostgreSQL configuration file\n# -----------------------------\nport = 5432\n");

        Assert.Equal(Kind.Legacy, Classify(_dataDir));
    }

    /// <summary>The committed rehearsal fixture — a stock conf with product v-markers appended — is Legacy.</summary>
    [Fact]
    public void Classify_RehearsalFixture_IsLegacy()
    {
        WriteConf(ReadFixture());

        Assert.Equal(Kind.Legacy, Classify(_dataDir));
    }

    /// <summary>The Rewrite output (no markers, has the include) plus a pending file (a crash between
    /// WriteTwoSteps and the stamp) is PendingVerify.</summary>
    [Fact]
    public void Classify_RewriteOutputWithPendingFile_IsPendingVerify()
    {
        var inputs = SampleInputs();
        var (postgresqlConf, managedConf) = BuildMigratedPair(inputs);
        WriteConf(postgresqlConf);
        WriteManaged(managedConf);
        File.WriteAllText(Path.Combine(_dataDir, ManagedConfMigrationSteps.PendingFileName), "N\n");

        Assert.Equal(Kind.PendingVerify, Classify(_dataDir));
    }

    /// <summary>The Rewrite output plus a valid stamp (Step A already proved it) is Verified.</summary>
    [Fact]
    public void Classify_RewriteOutputWithValidStamp_IsVerified()
    {
        var inputs = SampleInputs();
        var (postgresqlConf, managedConf) = BuildMigratedPair(inputs);
        WriteConf(postgresqlConf);
        WriteManaged(managedConf);
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, managedConf);

        Assert.Equal(Kind.Verified, Classify(_dataDir));
    }

    /// <summary>A migrated file since hand-edited (the stamp no longer matches its bytes), no pending file, is
    /// MigratedUnstamped.</summary>
    [Fact]
    public void Classify_MutatedManagedFile_IsMigratedUnstamped()
    {
        var inputs = SampleInputs();
        var (postgresqlConf, managedConf) = BuildMigratedPair(inputs);
        WriteConf(postgresqlConf);
        WriteManaged(managedConf);
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, managedConf);

        /* The hand edit: mutate the managed file's bytes after the stamp was written against the original. */
        WriteManaged(managedConf + "\n# hand edit\n");

        Assert.Equal(Kind.MigratedUnstamped, Classify(_dataDir));
    }

    /// <summary>
    /// Proof test (kept here because it exercises the REAL classifier, not a
    /// stub): under a naive rule ("appenders run whenever the stamp is missing; a hash
    /// mismatch counts as missing"), a migrated-but-unstamped conf would be classified the same as a genuinely
    /// legacy one — which must not happen, because it would re-append all fifteen
    /// blocks below the include on a conf that already migrated. This test names that naive rule directly
    /// (a local stub, not <see cref="Classify"/>) and shows it disagrees with the correct behavior above:
    /// <see cref="Classify_MutatedManagedFile_IsMigratedUnstamped"/> asserts <c>MigratedUnstamped</c> for the
    /// same fixture this stub calls <c>Legacy</c>.
    /// </summary>
    [Fact]
    public void MigratedConf_WithoutStamp_IsNotLegacy()
    {
        var inputs = SampleInputs();
        var (postgresqlConf, managedConf) = BuildMigratedPair(inputs);
        WriteConf(postgresqlConf);
        WriteManaged(managedConf);
        ManagedConfMigrationSteps.WriteVerifiedStamp(_dataDir, managedConf);
        WriteManaged(managedConf + "\n# hand edit\n");

        // The naive rule: "no valid stamp -> Legacy". Applied to this exact fixture it would
        // return Legacy, which must not happen (it would re-append on an already-migrated conf).
        static Kind NaiveRule(string dataDir) =>
            ManagedConfMigrationSteps.IsVerified(dataDir) ? Kind.Verified : Kind.Legacy;

        Assert.NotEqual(NaiveRule(_dataDir), Classify(_dataDir));
        Assert.Equal(Kind.Legacy, NaiveRule(_dataDir));
        Assert.Equal(Kind.MigratedUnstamped, Classify(_dataDir));
    }
}
