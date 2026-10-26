/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6791: the free-disk check in front of <c>--recompress-plan-dim</c>'s closing VACUUM FULL is ONE step shared by
/// the real run and the dry run, and the dry run's estimate is the post-conversion size. Behavior only: the step
/// writes to two writers and answers fits / does not fit, so no store is needed.
/// </summary>
public sealed class PlanDimVacuumPreflightTests
{
    private const long Gb = 1024L * 1024 * 1024;

    private static (bool Fits, string Output, string Error) Run(
        long estimated, long freeBytes, string? freeError, bool freeFromOption, bool dryRun, bool conversionWouldRun = false)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var fits = DarlingCliCommands.WritePlanDimVacuumPreflight(
            estimated, freeBytes, freeError, freeFromOption, dryRun, conversionWouldRun, output, error);
        return (fits, output.ToString(), error.ToString());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Fits_PrintsNoRefusal_AndADryRunPrintsTheComparison(bool dryRun)
    {
        var (fits, output, error) = Run(10 * Gb, 13 * Gb, null, freeFromOption: false, dryRun);

        Assert.True(fits);
        Assert.Equal(string.Empty, error);
        Assert.DoesNotContain("REFUS", output, StringComparison.OrdinalIgnoreCase);
        if (dryRun)
        {
            Assert.Contains("estimated compacted size : 10.0 GB", output, StringComparison.Ordinal);
            Assert.Contains("required (x1.2 headroom) : 12.0 GB", output, StringComparison.Ordinal);
            Assert.Contains("free on the store volume : 13.0 GB", output, StringComparison.Ordinal);
        }
        else
        {
            /* The real run stays quiet when the copy fits: the compaction announces itself next. */
            Assert.Equal(string.Empty, output);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Short_RefusesWithTheTable_AndTheDryRunSaysWhatTheRealRunWouldDo(bool dryRun)
    {
        var (fits, output, error) = Run(10 * Gb, 11 * Gb, null, freeFromOption: false, dryRun, conversionWouldRun: true);

        Assert.False(fits);
        Assert.Equal(string.Empty, output);
        Assert.Contains("not enough free disk for the compacted copy", error, StringComparison.Ordinal);
        Assert.Contains("estimated compacted size : 10.0 GB", error, StringComparison.Ordinal);
        Assert.Contains("required (x1.2 headroom) : 12.0 GB", error, StringComparison.Ordinal);
        Assert.Contains("free on the store volume : 11.0 GB", error, StringComparison.Ordinal);
        if (dryRun)
        {
            Assert.Contains("DRY RUN", error, StringComparison.Ordinal);
            Assert.Contains("the real run would REFUSE", error, StringComparison.Ordinal);
            Assert.Contains("conversion itself would still run and commit", error, StringComparison.Ordinal);
        }
        else
        {
            Assert.StartsWith("  REFUSING to VACUUM FULL: not enough free disk", error, StringComparison.Ordinal);
            Assert.Contains("The conversion is committed", error, StringComparison.Ordinal);
            Assert.DoesNotContain("DRY RUN", error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ADryRunWithNothingPending_DoesNotClaimAConversionWouldRun()
    {
        var (_, _, error) = Run(10 * Gb, 1 * Gb, null, freeFromOption: false, dryRun: true, conversionWouldRun: false);

        Assert.DoesNotContain("conversion itself would still run", error, StringComparison.Ordinal);
        Assert.Contains("Nothing would be converted", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AFreeSpaceRefusalPassesThrough(bool dryRun)
    {
        var (fits, output, error) = Run(10 * Gb, 0, "the store's volume could not be measured: nope", freeFromOption: false, dryRun);

        Assert.False(fits);
        Assert.Equal(string.Empty, output);
        Assert.Contains("the store's volume could not be measured: nope", error, StringComparison.Ordinal);
        Assert.Contains("--free-space <size>", error, StringComparison.Ordinal);
        Assert.DoesNotContain("estimated compacted size", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    public void TheProvenanceLine_AppearsOnlyWhenTheNumberCameFromTheOperator(bool freeFromOption, bool expectLine)
    {
        var (_, output, _) = Run(1 * Gb, 20 * Gb, null, freeFromOption, dryRun: true);

        Assert.Equal(expectLine, output.Contains("comes from --free-space; it was not measured", StringComparison.Ordinal));
        if (expectLine)
        {
            Assert.Contains("Free space 20.0 GB comes from --free-space", output, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SizesAreInvariant_UnderACommaDecimalCulture()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            /* Guard the premise: this culture really would write "10,5" and "1.234,5" for a plain :N1. */
            Assert.Equal("10,5", 10.5.ToString("N1", CultureInfo.CurrentCulture));

            var (_, output, error) = Run(
                (long)(1234.5 * Gb), (long)(1500.3 * Gb), null, freeFromOption: true, dryRun: true);

            Assert.Contains("estimated compacted size : 1,234.5 GB", output, StringComparison.Ordinal);
            Assert.Contains("Free space 1,500.3 GB comes from --free-space", output, StringComparison.Ordinal);
            Assert.DoesNotContain("1.234,5", output + error, StringComparison.Ordinal);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void TheDryRunEstimate_IsHeapPlusIndexesPlusEveryRowAtTheSampledAverage()
    {
        /* 5 GB of heap + indexes, 2,000,000 rows at 3,000.5 bytes of gzip each: the TOAST term is rows x average,
           NOT a stored average (none exists before the first conversion). */
        var estimate = PlanDimRecompression.ProjectCompactedBytes(5 * Gb, 2_000_000, 3000.5);

        Assert.Equal((5 * Gb) + 6_001_000_000L, estimate);
        Assert.Equal(5 * Gb, PlanDimRecompression.ProjectCompactedBytes(5 * Gb, 0, 3000.5));
    }

    [Theory]
    [InlineData("--free-space", "10G", "--no-vacuum-full")]
    [InlineData("--no-vacuum-full", "--free-space", "10G")]
    public void FreeSpaceWithNoVacuumFull_IsRefused(string a, string b, string c)
    {
        var parsed = DarlingCliCommands.ParseRecompressArgs(new[] { a, b, c });

        Assert.NotNull(parsed.Error);
        Assert.Contains("--no-vacuum-full", parsed.Error, StringComparison.Ordinal);
        Assert.Contains("--free-space", parsed.Error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("--free-space 10G --dry-run", true, DarlingCliCommands.RecompressVacuumMode.Auto)]
    [InlineData("--free-space 10G --vacuum-full", false, DarlingCliCommands.RecompressVacuumMode.Force)]
    [InlineData("--vacuum-full --dry-run --free-space 10G", true, DarlingCliCommands.RecompressVacuumMode.Force)]
    public void FreeSpaceWithTheOtherModes_IsAccepted(string args, bool dryRun, DarlingCliCommands.RecompressVacuumMode mode)
    {
        var parsed = DarlingCliCommands.ParseRecompressArgs(args.Split(' '));

        Assert.Null(parsed.Error);
        Assert.Equal(10L * Gb, parsed.FreeBytes);
        Assert.Equal(dryRun, parsed.DryRun);
        Assert.Equal(mode, parsed.Mode);
    }
}
