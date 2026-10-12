/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5459: the <c>max_wal_size</c> and <c>min_wal_size</c> in <c>darling-managed.conf</c> are derived from the data
/// volume's free space, which a busy machine moves by a few hundred MB between two service starts. A volume
/// sitting within that distance of a ladder edge (16, 32, 64 or 128 GiB free) used to render a different BODY on
/// the second start, so <see cref="ManagedConfFile.ShouldReplaceManagedConf"/> saw a real change and rewrote a
/// file nothing had changed. #5511 gave the postgresql.conf stamp, the settings check and the stored verdicts a
/// tolerance of one eighth of the free space; the managed-file render did not have it, so the live
/// "second start does not rewrite" test failed when the runner's disk stood next to an edge. Ungated: plain
/// file I/O in a temp directory with the volume read replaced, so the same free-space movement is planted on
/// every machine.
/// </summary>
public sealed class ManagedConfWalRungStabilityTests
{
    private const long Gib = 1024L * 1024 * 1024;
    private const long Mib = 1024L * 1024;
    private const long TotalBytes = 512 * Gib;

    private static ManagedConfFile.RenderInputs SampleInputs(long freeBytes, long? inForceMaxWalSizeMb = null)
        => new(
            FormulaVersion: ManagedConfFile.CurrentFormulaVersion,
            Platform: "Windows",
            RamBytes: 17_179_869_184L,
            RamAuthoritative: true,
            ProcessorCount: 8,
            HypertableCount: 42,
            PostgresMajor: 18,
            DataVolumeFreeBytes: freeBytes,
            DataVolumeTotalBytes: TotalBytes,
            DataVolumeAuthoritative: true,
            Port: 55432,
            EffectivePreloadList: null,
            InForceMaxWalSizeMb: inForceMaxWalSizeMb);

    private static DarlingManagedPostgres NewCluster(string dir, long freeBytes)
        => new(new PostgresConfig { Managed = true, Port = 55432, DataDirectory = dir }, new CapturingTestLogger())
        {
            TestOnlyVolumeSpaceReader = _ => (freeBytes, TotalBytes),
        };

    private static string WalLines(string fileText)
    {
        var body = ManagedConfFile.ParseExisting(fileText).Body;
        var lines = body.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        return string.Join("|", Array.FindAll(lines, l => l.StartsWith("max_wal_size", StringComparison.Ordinal)
            || l.StartsWith("min_wal_size", StringComparison.Ordinal)));
    }

    /// <summary>A busy runner moves free space by a few hundred MB between two starts. Planted on each edge of the
    /// ladder, in both directions: the file must stay byte-identical with an unchanged write time.</summary>
    [Theory]
    [InlineData(16, true)]
    [InlineData(16, false)]
    [InlineData(32, true)]
    [InlineData(32, false)]
    [InlineData(64, true)]
    [InlineData(64, false)]
    [InlineData(128, true)]
    [InlineData(128, false)]
    public void SecondStart_FreeSpaceCrossesLadderEdge_DoesNotRewriteFile(long edgeGib, bool crossesUp)
    {
        var dir = Directory.CreateTempSubdirectory("darling-5459-edge-");
        try
        {
            File.WriteAllText(Path.Combine(dir.FullName, "postgresql.conf"), "# base conf\n");
            var below = edgeGib * Gib - 200 * Mib;
            var above = edgeGib * Gib + 200 * Mib;
            var firstFree = crossesUp ? below : above;
            var secondFree = crossesUp ? above : below;

            var first = NewCluster(dir.FullName, firstFree).WriteManagedConfFile(dir.FullName, postgresMajor: 18);
            Assert.True(first.Written);
            var managedPath = Path.Combine(dir.FullName, ManagedConfFile.FileName);
            var bytesAfterFirst = File.ReadAllBytes(managedPath);
            var writeTimeAfterFirst = File.GetLastWriteTimeUtc(managedPath);

            var second = NewCluster(dir.FullName, secondFree).WriteManagedConfFile(dir.FullName, postgresMajor: 18);

            Assert.False(second.Written, "the second start rewrote the file for a free-space movement of 400 MB");
            Assert.Equal(bytesAfterFirst, File.ReadAllBytes(managedPath));
            Assert.Equal(writeTimeAfterFirst, File.GetLastWriteTimeUtc(managedPath));
            /* The text this start reports it rendered is the file in force, not a rung the file does not hold. */
            Assert.Equal(WalLines(first.RenderedText), WalLines(second.RenderedText));
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }

    /// <summary>A real change in headroom still rewrites: the tolerance is one eighth of the free space, and
    /// these move it by far more.</summary>
    [Theory]
    [InlineData(40, 128, "max_wal_size = '4096MB'", "max_wal_size = '16384MB'")]
    [InlineData(128, 40, "max_wal_size = '16384MB'", "max_wal_size = '4096MB'")]
    [InlineData(20, 100, "max_wal_size = '2048MB'", "max_wal_size = '8192MB'")]
    [InlineData(100, 20, "max_wal_size = '8192MB'", "max_wal_size = '2048MB'")]
    public void SecondStart_HeadroomChangesByFarMoreThanTheTolerance_RewritesWithTheNewRung(
        long firstGib, long secondGib, string firstLine, string secondLine)
    {
        var dir = Directory.CreateTempSubdirectory("darling-5459-real-");
        try
        {
            File.WriteAllText(Path.Combine(dir.FullName, "postgresql.conf"), "# base conf\n");

            var first = NewCluster(dir.FullName, firstGib * Gib).WriteManagedConfFile(dir.FullName, postgresMajor: 18);
            Assert.True(first.Written);
            Assert.Contains(firstLine, first.RenderedText, StringComparison.Ordinal);

            var second = NewCluster(dir.FullName, secondGib * Gib).WriteManagedConfFile(dir.FullName, postgresMajor: 18);

            Assert.True(second.Written);
            var managedPath = Path.Combine(dir.FullName, ManagedConfFile.FileName);
            Assert.Equal(second.RenderedText, File.ReadAllText(managedPath));
            Assert.Contains(secondLine, second.RenderedText, StringComparison.Ordinal);
            Assert.DoesNotContain(firstLine, second.RenderedText, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }

    /// <summary>A file an operator edited is never given the in-force rung: the hand-edit path keeps reporting
    /// what a fresh render would write, as before.</summary>
    [Fact]
    public void HandEditedFile_StillReportsTheFreshRenderForTheWalKeys()
    {
        var dir = Directory.CreateTempSubdirectory("darling-5459-hand-");
        try
        {
            File.WriteAllText(Path.Combine(dir.FullName, "postgresql.conf"), "# base conf\n");
            var first = NewCluster(dir.FullName, 128 * Gib - 200 * Mib).WriteManagedConfFile(dir.FullName, postgresMajor: 18);
            Assert.True(first.Written);
            var managedPath = Path.Combine(dir.FullName, ManagedConfFile.FileName);
            File.AppendAllText(managedPath, "work_mem = '1MB'\n");

            var second = NewCluster(dir.FullName, 128 * Gib + 200 * Mib).WriteManagedConfFile(dir.FullName, postgresMajor: 18);

            Assert.True(second.HandEdited);
            Assert.False(second.Written);
            Assert.Contains("max_wal_size = '16384MB'", second.RenderedText, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir.FullName, recursive: true);
        }
    }

    [Fact]
    public void RenderBody_InForceRungWithinTolerance_KeepsTheInForceRung()
    {
        var kept = ManagedConfFile.RenderBody(SampleInputs(128 * Gib + 200 * Mib, inForceMaxWalSizeMb: 8192));
        var atRung = ManagedConfFile.RenderBody(SampleInputs(128 * Gib - 200 * Mib));
        var fresh = ManagedConfFile.RenderBody(SampleInputs(128 * Gib + 200 * Mib));

        Assert.Equal(atRung, kept);
        Assert.Contains("max_wal_size = '8192MB'", kept, StringComparison.Ordinal);
        Assert.Contains("min_wal_size = '2048MB'", kept, StringComparison.Ordinal);
        Assert.Contains("max_wal_size = '16384MB'", fresh, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(8192L)]   // two rungs below the free space: not within one eighth of it
    [InlineData(5000L)]   // not a rung of the ladder at all
    [InlineData(0L)]
    [InlineData(-4096L)]
    public void RenderBody_InForceValueOutsideToleranceOrOffTheLadder_IsIgnored(long inForceMb)
    {
        var free = 400 * Gib;
        Assert.Equal(
            ManagedConfFile.RenderBody(SampleInputs(free)),
            ManagedConfFile.RenderBody(SampleInputs(free, inForceMaxWalSizeMb: inForceMb)));
    }

    [Fact]
    public void RenderBody_NoDataVolumeReading_IgnoresTheInForceRung()
    {
        var inputs = SampleInputs(128 * Gib, inForceMaxWalSizeMb: 8192) with { DataVolumeAuthoritative = false };
        Assert.Equal(
            ManagedConfFile.RenderBody(inputs with { InForceMaxWalSizeMb = null }),
            ManagedConfFile.RenderBody(inputs));
    }

    [Fact]
    public void DeriveWalSettings_WithInForceRung_AnswersTheRungItKeeps()
    {
        var free = 128 * Gib + 200 * Mib;
        Assert.Equal(new DarlingManagedPostgres.WalSettings(8192, 2048), DarlingManagedPostgres.DeriveWalSettings(free, 8192));
        Assert.Equal(new DarlingManagedPostgres.WalSettings(16384, 4096), DarlingManagedPostgres.DeriveWalSettings(free, null));
        Assert.Equal(new DarlingManagedPostgres.WalSettings(16384, 4096), DarlingManagedPostgres.DeriveWalSettings(free, 1024));
        Assert.Equal(DarlingManagedPostgres.DeriveWalSettings(free), DarlingManagedPostgres.DeriveWalSettings(free, null));
    }

    [Fact]
    public void ReadInForceMaxWalSizeMb_ReadsTheRenderedFileAndRefusesAnythingElse()
    {
        var rendered = ManagedConfFile.Render(SampleInputs(64 * Gib));
        Assert.Equal(8192L, ManagedConfFile.ReadInForceMaxWalSizeMb(rendered));

        /* A hand edit, text with no header, a value that is not whole megabytes, or no WAL line at all. */
        var parsed = ManagedConfFile.ParseExisting(rendered);
        Assert.Null(ManagedConfFile.ReadInForceMaxWalSizeMb(rendered + "work_mem = '1MB'\n"));
        Assert.Null(ManagedConfFile.ReadInForceMaxWalSizeMb(parsed.Body));
        Assert.Null(ManagedConfFile.ReadInForceMaxWalSizeMb(string.Empty));

        /* Without an authoritative disk reading the file carries v4's fixed 4GB, not a whole-MB figure. */
        var noWal = ManagedConfFile.Render(SampleInputs(64 * Gib) with { DataVolumeAuthoritative = false });
        Assert.Null(ManagedConfFile.ReadInForceMaxWalSizeMb(noWal));
    }
}
