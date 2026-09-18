/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Alerting;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3539 A8c: the File Growth rise threshold means ONE thing — megabytes per hour, averaged over the lookback
/// — and every surface that shows the number says so in the same words.
///
/// <para><b>The defect this holds shut.</b> The knob shipped (#2349) as "a file grew at least this many MB
/// inside the lookback window", and the lookback shipped as a second knob clamped 5–1440 minutes. Nothing
/// related the two: the same 10,240 meant 10 GB per five minutes on a store whose operator had shortened the
/// window and 10 GB per day on one who had lengthened it, a 288× swing in what the threshold asked for, and
/// each surface described the number in its own words — "MB within N min" on the Settings rows,
/// "10240MB/60m" on the preview line, "rise ≥ 10240 MB" on the alert, a bare <c>rise_mb</c> on the wire —
/// none of which was wrong on its own and none of which could be compared with another. The fix made the
/// number a rate; this census is what keeps the surfaces from drifting back into private vocabularies, one
/// label at a time.</para>
///
/// <para><b>What it asserts.</b> Each surface either references
/// <see cref="AlertContextBuilders.FileGrowthRiseUnit"/> (code) or spells its literal value (XAML, which cannot
/// reference a C# constant without <c>x:Static</c> plumbing this row does not otherwise need), beside the
/// phrase that names the averaging window. And the phrases of the per-window reading are asserted ABSENT, so
/// a revert of one surface is a red build rather than a quiet regression — presence alone would pass with the
/// old label re-added beside the new one, which is the likelier accident.</para>
///
/// <para><b>Both SKUs, one roster.</b> The Darling viewer's Settings window is a copy of Lite's, so the two
/// XAML files and the two code-behinds are four rows here rather than two, and a fix that lands on one twin
/// without the other fails by name. The MCP descriptions are the fifth pair: Darling's are asserted by
/// reflection in <c>DarlingMcpAlertToolsTests</c> (where its wire contract is pinned) and by source here, so
/// the roster is complete on its own; Lite's has no reflection pin and is source-only.</para>
/// </summary>
public class FileGrowthRiseUnitCensusTests
{
    /// <summary>The literal the XAML labels carry. Pinned to the constant so the two cannot drift apart:
    /// XAML cannot reference the constant, so the constant's VALUE is the contract the XAML rows below are
    /// held to.</summary>
    [Fact]
    public void TheUnitPhrase_IsMbPerHour()
    {
        Assert.Equal("MB/hr", AlertContextBuilders.FileGrowthRiseUnit);
    }

    public static IEnumerable<object[]> Surfaces()
    {
        /* Settings rows, both SKUs: the label between the rise box and the lookback box names the unit and
           the averaging, and the old "MB within" (which read the box as a per-window total) is gone. */
        foreach (var xaml in new[]
        {
            "Lite/Windows/SettingsWindow.xaml",
            "Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml",
        })
        {
            yield return new object[]
            {
                xaml,
                new[] { $"Text=\"{AlertContextBuilders.FileGrowthRiseUnit} averaged over\"" },
                new[] { "Text=\"MB within\"" },
            };
        }

        /* Preview lines, both SKUs: "Will alert when: file growth > 10240 MB/hr over 60m" — the constant,
           and the old "10240MB/60m" shape gone. */
        foreach (var codeBehind in new[]
        {
            "Lite/Windows/SettingsWindow.xaml.cs",
            "Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml.cs",
        })
        {
            yield return new object[]
            {
                codeBehind,
                new[] { "{AlertContextBuilders.FileGrowthRiseUnit} over {AlertFileGrowthLookbackMinutesBox.Text}m" },
                new[] { "}MB/{AlertFileGrowthLookbackMinutesBox.Text}m" },
            };
        }

        /* The alert's threshold line: rate, unit, the window it was averaged over, and the bar in megabytes
           that rate amounts to inside it. The old "rise ≥ N MB or" is gone. */
        yield return new object[]
        {
            "PerformanceMonitor.Alerting/AlertEngine.cs",
            new[]
            {
                "{AlertContextBuilders.FileGrowthRiseUnit} averaged over {_settings.FileGrowthLookbackMinutes} min",
                "MB in the window)",
            },
            new[] { "rise ≥ {_settings.FileGrowthRiseMb} MB or" },
        };

        /* The card's Growth field — the rate the operator compares against the threshold line. */
        yield return new object[]
        {
            "PerformanceMonitor.Alerting/AlertContextBuilders.cs",
            new[] { "({f.GrowthMbPerHour:F0} {FileGrowthRiseUnit})" },
            new[] { "MB/hr)\")," },
        };

        /* The engine settings contract — the doc every implementation reads. */
        yield return new object[]
        {
            "PerformanceMonitor.Alerting/IAlertEngineSettings.cs",
            new[] { "at least this many MB PER HOUR, averaged over" },
            new[] { "grew at least this many MB inside the lookback window" },
        };

        /* The MCP descriptions, both SKUs: the key keeps its spelling, so the unit lives in the description. */
        yield return new object[]
        {
            "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpAlertTools.cs",
            new[]
            {
                "rise_mb is megabytes per HOUR, averaged over file_growth.lookback_minutes",
                "file_growth.rise_mb is megabytes per HOUR averaged over file_growth.lookback_minutes",
            },
            Array.Empty<string>(),
        };
        yield return new object[]
        {
            "Lite/Mcp/McpAlertTools.cs",
            new[] { "rise_mb is megabytes per HOUR, averaged over file_growth.lookback_minutes" },
            Array.Empty<string>(),
        };
    }

    [Theory]
    [MemberData(nameof(Surfaces))]
    public void EverySurface_SpellsTheRiseAsARate_AndNotAsAPerWindowTotal(
        string path, string[] mustContain, string[] mustNotContain)
    {
        var source = ReadRepoFile(path);

        foreach (var phrase in mustContain)
        {
            Assert.True(
                source.Contains(phrase, StringComparison.Ordinal),
                $"{path} no longer says «{phrase}» — the File Growth rise is MB per hour averaged over the lookback, and this surface must say so in the shared words");
        }

        foreach (var phrase in mustNotContain)
        {
            Assert.False(
                source.Contains(phrase, StringComparison.Ordinal),
                $"{path} still says «{phrase}», the per-window reading the rate replaced");
        }
    }

    /// <summary>The roster above is the population; this is the floor under it, so a roster edited down to
    /// nothing cannot read as clean. Nine rows: two XAML, two code-behinds, engine, builder, settings contract,
    /// two MCP files — counted here rather than trusted.</summary>
    [Fact]
    public void TheRoster_CoversBothSkusAndTheSharedLibrary()
    {
        var paths = Surfaces().Select(row => (string)row[0]).ToList();

        Assert.Equal(9, paths.Count);
        Assert.Equal(paths.Count, paths.Distinct(StringComparer.Ordinal).Count());
        Assert.Contains(paths, p => p.StartsWith("Lite/", StringComparison.Ordinal));
        Assert.Contains(paths, p => p.StartsWith("Darling/", StringComparison.Ordinal));
        Assert.Contains(paths, p => p.StartsWith("PerformanceMonitor.Alerting/", StringComparison.Ordinal));
    }
}
