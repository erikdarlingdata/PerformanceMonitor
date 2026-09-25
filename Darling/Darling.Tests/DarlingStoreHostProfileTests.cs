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

/// <summary>Pure-function pins for the #4214 store host profile: one per platform branch of the RAM/cgroup
/// resolution (ruling 2/10), the managed-block line detection, and the four-way verdict classification.</summary>
public sealed class DarlingStoreHostProfileTests
{
    /* ---------------------------- /proc/meminfo (ruling 2/10: pure over TEXT) ---------------------------- */

    [Fact]
    public void ParseProcMeminfoTotalBytes_ReadsMemTotalKbAsBytes()
    {
        const string meminfo = "MemTotal:       16384000 kB\nMemFree:         1000000 kB\n";
        Assert.Equal(16384000L * 1024, DarlingStoreHostProfile.ParseProcMeminfoTotalBytes(meminfo));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("MemFree: 1000 kB\n")]
    [InlineData("MemTotal: notanumber kB\n")]
    public void ParseProcMeminfoTotalBytes_ReturnsNullWhenUnreadable(string? meminfo)
    {
        Assert.Null(DarlingStoreHostProfile.ParseProcMeminfoTotalBytes(meminfo));
    }

    /* -------------------------------------- cgroup v2 memory.max -------------------------------------- */

    [Fact]
    public void ParseCgroupV2MemoryMaxBytes_ParsesARealLimit()
    {
        Assert.Equal(2147483648L, DarlingStoreHostProfile.ParseCgroupV2MemoryMaxBytes("2147483648\n"));
    }

    [Theory]
    [InlineData("max")]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not-a-number")]
    public void ParseCgroupV2MemoryMaxBytes_MaxOrUnreadableMeansNoLimit(string? text)
    {
        Assert.Null(DarlingStoreHostProfile.ParseCgroupV2MemoryMaxBytes(text));
    }

    /* --------------------------- cgroup v1 memory.limit_in_bytes (the sentinel) --------------------------- */

    [Fact]
    public void ParseCgroupV1MemoryLimitBytes_ParsesARealLimit()
    {
        Assert.Equal(1073741824L, DarlingStoreHostProfile.ParseCgroupV1MemoryLimitBytes("1073741824\n"));
    }

    [Theory]
    [InlineData("9223372036854771712")]
    [InlineData("9223372036854775807")]
    public void ParseCgroupV1MemoryLimitBytes_NearLongMaxValueMeansNoLimit(string text)
    {
        Assert.Null(DarlingStoreHostProfile.ParseCgroupV1MemoryLimitBytes(text));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-5")]
    public void ParseCgroupV1MemoryLimitBytes_UnreadableOrNonPositiveIsNull(string? text)
    {
        Assert.Null(DarlingStoreHostProfile.ParseCgroupV1MemoryLimitBytes(text));
    }

    /* --------------------------------- effective limit + containerization --------------------------------- */

    [Fact]
    public void ComputeEffectiveMemoryLimitBytes_TakesTheSmallerCgroupCeiling()
    {
        Assert.Equal(2_000_000_000L, DarlingStoreHostProfile.ComputeEffectiveMemoryLimitBytes(8_000_000_000L, 2_000_000_000L));
    }

    [Fact]
    public void ComputeEffectiveMemoryLimitBytes_NoLimitMeansTotalRam()
    {
        Assert.Equal(8_000_000_000L, DarlingStoreHostProfile.ComputeEffectiveMemoryLimitBytes(8_000_000_000L, null));
    }

    [Fact]
    public void ComputeEffectiveMemoryLimitBytes_ACgroupLimitAboveTotalRamIsIgnored()
    {
        Assert.Equal(8_000_000_000L, DarlingStoreHostProfile.ComputeEffectiveMemoryLimitBytes(8_000_000_000L, 64_000_000_000L));
    }

    [Fact]
    public void IsContainerized_TrueOnAFiniteCgroupLimit()
    {
        Assert.True(DarlingStoreHostProfile.IsContainerized(2_000_000_000L, null));
    }

    [Fact]
    public void IsContainerized_TrueOnTheDotnetEnvVarEvenWithoutACgroupLimit()
    {
        Assert.True(DarlingStoreHostProfile.IsContainerized(null, "true"));
    }

    [Fact]
    public void IsContainerized_FalseOnBareMetal()
    {
        Assert.False(DarlingStoreHostProfile.IsContainerized(null, null));
    }

    /* -------------------------------------- managed-block line detection -------------------------------------- */

    [Fact]
    public void IsLineInsideManagedBlock_TrueForAContentLineInsideTheBlock()
    {
        var conf = "shared_buffers = 128MB\n"
            + "\n"
            + PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ConfMarkerV3 + "\n"
            + "shared_buffers = 1024MB\n"
            + "effective_cache_size = 3072MB\n"
            + "\n"
            + "# an operator's own line, after the block\n"
            + "work_mem = 999MB\n";

        // line 4 = "shared_buffers = 1024MB" (1-based)
        Assert.True(DarlingStoreHostProfile.IsLineInsideManagedBlock(conf, 4));
        Assert.True(DarlingStoreHostProfile.IsLineInsideManagedBlock(conf, 5));
    }

    [Fact]
    public void IsLineInsideManagedBlock_FalseForALineAfterTheBlocksBlankTerminator()
    {
        var conf = "\n"
            + PerformanceMonitor.Darling.Service.DarlingManagedPostgres.ConfMarkerV3 + "\n"
            + "shared_buffers = 1024MB\n"
            + "\n"
            + "work_mem = 999MB\n";

        // line 5 = "work_mem = 999MB", spliced in after the blank line that ends the v3 block
        Assert.False(DarlingStoreHostProfile.IsLineInsideManagedBlock(conf, 5));
    }

    [Fact]
    public void IsLineInsideManagedBlock_FalseWhenNoMarkerPrecedesTheLine()
    {
        var conf = "shared_buffers = 128MB\nwork_mem = 4MB\n";
        Assert.False(DarlingStoreHostProfile.IsLineInsideManagedBlock(conf, 2));
    }

    /* -------------------------------------------- verdict classification -------------------------------------------- */

    [Fact]
    public void ClassifyVerdict_ManagedBlockMatchingDerivedValueIsMatches()
    {
        var attribution = new PerformanceMonitor.Darling.Service.ConfSettingAttribution(
            PerformanceMonitor.Darling.Service.ConfSettingOrigin.ManagedBlock, "postgresql.conf", 42, "1024MB");
        var (_, verdict) = DarlingStoreHostProfile.ClassifyVerdict(attribution, 1024, 1024);
        Assert.Equal(HostSettingVerdict.Matches, verdict);
    }

    [Fact]
    public void ClassifyVerdict_ManagedBlockDisagreeingWithDerivedValueIsStale()
    {
        var attribution = new PerformanceMonitor.Darling.Service.ConfSettingAttribution(
            PerformanceMonitor.Darling.Service.ConfSettingOrigin.ManagedBlock, "postgresql.conf", 42, "512MB");
        var (_, verdict) = DarlingStoreHostProfile.ClassifyVerdict(attribution, 512, 2048);
        Assert.Equal(HostSettingVerdict.StaleAfterHardwareChange, verdict);
    }

    [Fact]
    public void ClassifyVerdict_OperatorOverrideIsAlwaysOperatorOverride()
    {
        var attribution = new PerformanceMonitor.Darling.Service.ConfSettingAttribution(
            PerformanceMonitor.Darling.Service.ConfSettingOrigin.OperatorOverride, "postgresql.auto.conf", 3, "1GB");
        var (source, verdict) = DarlingStoreHostProfile.ClassifyVerdict(attribution, 1024, 1024);
        Assert.Equal(HostSettingVerdict.OperatorOverride, verdict);
        Assert.Contains("postgresql.auto.conf", source, StringComparison.Ordinal);
    }

    [Fact]
    public void ClassifyVerdict_UnsetFallsBackToOperatorOverride()
    {
        var attribution = new PerformanceMonitor.Darling.Service.ConfSettingAttribution(
            PerformanceMonitor.Darling.Service.ConfSettingOrigin.Unset, null, 0, null);
        var (_, verdict) = DarlingStoreHostProfile.ClassifyVerdict(attribution, 0, 1024);
        Assert.Equal(HostSettingVerdict.OperatorOverride, verdict);
    }

    /* ------------------------------------- FormatSourceForMcp (round-1 review, Medium 1) ------------------------------------- */
    /* Three shapes: a file inside the managed data directory redacts to a directory-relative path, a file
       outside it (an include elsewhere) redacts to the bare file name only, and anything with no SourceFile
       (not-managed / unreadable / not-visible) passes SourceDescription through unchanged — it is already a
       kind word, never a path. */

    [Fact]
    public void FormatSourceForMcp_FileInsideDataDirectory_ReturnsRelativePathNoParentNoRoot()
    {
        var dataDirectory = Path.Combine(Path.GetTempPath(), "pmdarling-test-pgdata");
        var confFile = Path.Combine(dataDirectory, "postgresql.conf");
        var setting = new HostSettingProfile(
            "shared_buffers", "1024MB", 1024, "unused-when-sourcefile-set", "1024MB", 1024,
            HostSettingVerdict.Matches, confFile, 42);

        var source = DarlingStoreHostProfile.FormatSourceForMcp(setting, dataDirectory);

        Assert.Equal("managed block (postgresql.conf:42)", source);
        Assert.DoesNotContain("..", source, StringComparison.Ordinal);
        Assert.DoesNotContain(dataDirectory, source, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void FormatSourceForMcp_FileOutsideDataDirectory_ReturnsBareFileNameOnly()
    {
        var dataDirectory = Path.Combine(Path.GetTempPath(), "pmdarling-test-pgdata");
        var includedFile = Path.Combine(Path.GetTempPath(), "pmdarling-test-elsewhere", "included.conf");
        var setting = new HostSettingProfile(
            "work_mem", "4MB", 4, "unused-when-sourcefile-set", "4MB", 4,
            HostSettingVerdict.OperatorOverride, includedFile, 7);

        var source = DarlingStoreHostProfile.FormatSourceForMcp(setting, dataDirectory);

        Assert.Equal("operator override (included.conf:7)", source);
        Assert.DoesNotContain(dataDirectory, source, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(Path.DirectorySeparatorChar.ToString(), source, StringComparison.Ordinal);
        Assert.DoesNotContain(Path.AltDirectorySeparatorChar.ToString(), source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(HostSettingVerdict.NotManaged, "not-managed (bring-your-own store; pg_settings.source = configuration file)")]
    [InlineData(HostSettingVerdict.NotManaged, "unreadable — this connection's role, or this build of TimescaleDB, does not expose it")]
    public void FormatSourceForMcp_NoSourceFile_ReturnsSourceDescriptionVerbatim(HostSettingVerdict verdict, string sourceDescription)
    {
        var setting = new HostSettingProfile(
            "max_connections", "100", 100, sourceDescription, "100", 100, verdict);

        var source = DarlingStoreHostProfile.FormatSourceForMcp(setting, dataDirectory: null);

        Assert.Equal(sourceDescription, source);
    }

    /* --------------------------------------------- pg_settings unit normalization --------------------------------------------- */

    [Theory]
    [InlineData("16384", "8kB", 128L)]     // shared_buffers-shaped: 16384 * 8kB = 131072 kB = 128 MB
    [InlineData("65536", "kB", 64L)]       // work_mem-shaped: 65536 kB = 64 MB
    [InlineData("4096", "MB", 4096L)]
    [InlineData("4", "GB", 4096L)]
    [InlineData("200", "", 200L)]
    [InlineData("74", null, 74L)]
    public void NormalizePgSetting_ConvertsEveryKnownUnitToMb(string setting, string? unit, long expectedMb)
    {
        Assert.Equal(expectedMb, DarlingStoreHostProfile.NormalizePgSetting(setting, unit));
    }

    [Fact]
    public void NormalizePgSetting_UnknownUnitReturnsNull()
    {
        Assert.Null(DarlingStoreHostProfile.NormalizePgSetting("1", "fortnight"));
    }

    /* --------------------------------------------- verdict text --------------------------------------------- */

    [Theory]
    [InlineData(HostSettingVerdict.Matches, "matches")]
    [InlineData(HostSettingVerdict.StaleAfterHardwareChange, "stale-after-hardware-change")]
    [InlineData(HostSettingVerdict.OperatorOverride, "operator-override")]
    [InlineData(HostSettingVerdict.NotManaged, "not-managed")]
    public void DescribeVerdict_MatchesTheRulingsFourNames(HostSettingVerdict verdict, string expected)
    {
        Assert.Equal(expected, DarlingStoreHostProfile.DescribeVerdict(verdict));
    }

    /* --------------------------------------------- CLI verb recognition --------------------------------------------- */

    [Fact]
    public void IsCheckSettingsVerb_RecognizesTheFlagCaseInsensitively()
    {
        Assert.True(DarlingCliCommands.IsCheckSettingsVerb("--check-settings"));
        Assert.True(DarlingCliCommands.IsCheckSettingsVerb("--Check-Settings"));
        Assert.False(DarlingCliCommands.IsCheckSettingsVerb("--check-setting"));
    }

    [Fact]
    public void IsKnownVerb_IncludesCheckSettings()
    {
        Assert.True(DarlingCliCommands.IsKnownVerb("--check-settings"));
    }
}
