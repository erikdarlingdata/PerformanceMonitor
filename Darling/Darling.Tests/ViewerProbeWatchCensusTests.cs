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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The census of the desktop viewer's data-start probes (#5022). A load that starts a <c>...DataStartAsync(</c> probe beside its read,
/// then awaits the read before the probe, leaves the probe unobserved when the read throws; a probe that fails after that reaches
/// <c>App.OnUnobservedTaskException</c> and is logged as an Error. <c>AwaitReadWatchingProbeAsync</c> prevents that, so every such
/// read must be awaited through it. A new probe site that awaits its read bare fails here, naming the file and method.
/// </summary>
public sealed class ViewerProbeWatchCensusTests
{
    /// <summary>Methods that start a probe and await a read bare on purpose, as "file::method" with the reason.</summary>
    private static readonly Dictionary<string, string> Allowed = new(StringComparer.Ordinal)
    {
        // None today: every site on dev routes its read through the helper. Add "File.cs::Method" => "why" for a justified exception.
    };

    private static readonly Regex MethodHeader = new(
        @"^ {4,8}(?:(?:private|internal|public|protected|static|async|override|virtual)\s+)+[\w<>\[\],\.\(\)\? ]+?\s+(\w+)\s*(?:<[^>]*>)?\(",
        RegexOptions.Multiline);

    private static readonly Regex ProbeStart = new(
        @"(?:\bvar|\bTask\s*<\s*DateTime\s*\?\s*>)\s+(\w+)\s*=\s*[^;]*?\.Get\w*(?:DataStart|WindowFloor)Async\(",
        RegexOptions.Compiled);

    private static string ViewerDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "Darling", "PerformanceMonitor.Darling.Viewer")))
            dir = dir.Parent;
        Assert.NotNull(dir);
        return Path.Combine(dir!.FullName, "Darling", "PerformanceMonitor.Darling.Viewer");
    }

    /// <summary>Every method that starts a probe and awaits a read bare before it awaits the probe, as "file::method".</summary>
    internal static (List<string> Offenders, int Sites) Scan(IEnumerable<(string File, string Source)> files)
    {
        var offenders = new List<string>();
        var sites = 0;
        foreach (var (file, raw) in files)
        {
            /* Comments and string literals blanked, so an "await" or a probe name in prose cannot decide a site. */
            var source = CSharpSourceWalker.StripCommentsAndStrings(raw);
            var headers = MethodHeader.Matches(source).ToList();
            for (var i = 0; i < headers.Count; i++)
            {
                var end = i + 1 < headers.Count ? headers[i + 1].Index : source.Length;
                var body = source[headers[i].Index..end];
                foreach (Match probe in ProbeStart.Matches(body))
                {
                    var name = probe.Groups[1].Value;

                    /* The first await after this probe starts. If it is the helper, the read is watched. If it names the probe, the probe is
                       awaited first (the banner step) and no read stood in front of it. Otherwise a read is awaited bare. */
                    var after = body[(probe.Index + probe.Length)..];
                    var awaitLine = after.Split('\n').FirstOrDefault(l => l.Contains("await ", StringComparison.Ordinal));
                    if (awaitLine is null) continue;
                    var watched = awaitLine.Contains("AwaitReadWatchingProbeAsync", StringComparison.Ordinal);
                    if (!watched && Regex.IsMatch(awaitLine, @"\b" + name + @"\b")) continue;
                    sites++;
                    if (watched) continue;
                    offenders.Add($"{file}::{headers[i].Groups[1].Value}");
                }
            }
        }
        return (offenders, sites);
    }

    private static List<(string File, string Source)> ViewerSources() =>
        Directory.GetFiles(ViewerDir(), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Select(f => (Path.GetFileName(f), File.ReadAllText(f)))
            .ToList();

    [Fact]
    public void EveryViewerLoadThatStartsAProbeBesideARead_AwaitsTheReadThroughTheHelper()
    {
        var (offenders, _) = Scan(ViewerSources());
        var bad = offenders.Where(o => !Allowed.ContainsKey(o)).ToList();
        Assert.True(
            bad.Count == 0,
            "These viewer methods start a ...DataStartAsync probe and await a read bare, so a read that throws leaves the probe unobserved "
            + "(an Error line in the log): route the read through AwaitReadWatchingProbeAsync, or allow-list the method with a reason: "
            + string.Join(", ", bad));
    }

    [Fact]
    public void TheCensusFindsTheProbeSitesItGuards()
    {
        /* A scan that matched nothing would pass forever: the viewer has two dozen such loads. */
        var (_, sites) = Scan(ViewerSources());
        Assert.True(sites >= 35, $"expected at least 35 probe-beside-read sites, found {sites}");
    }

    [Fact]
    public void TheCensus_FlagsABareReadBesideAProbe_AndPassesAWatchedOne()
    {
        const string Bare = "public partial class T\n{\n    private async Task LoadXAsync()\n    {\n        var dataStartTask = _dataService.GetXDataStartAsync(1);\n        var rows = await _dataService.GetXAsync(1);\n        await ShowEventDataStartAsync(b, dataStartTask, \"X\");\n    }\n}\n";
        const string Watched = "public partial class T\n{\n    private async Task LoadXAsync()\n    {\n        var dataStartTask = _dataService.GetXDataStartAsync(1);\n        var rowsTask = _dataService.GetXAsync(1);\n        await AwaitReadWatchingProbeAsync(rowsTask, dataStartTask, \"X\");\n        await ShowEventDataStartAsync(b, dataStartTask, \"X\");\n    }\n}\n";
        Assert.Equal(new[] { "T.cs::LoadXAsync" }, Scan(new[] { ("T.cs", Bare) }).Offenders);
        Assert.Empty(Scan(new[] { ("T.cs", Watched) }).Offenders);

        /* The WindowFloor probe shape, typed rather than var, and a conditional start. */
        const string BareFloor = "public partial class T\n{\n    private async Task LoadYAsync()\n    {\n        Task<DateTime?> floorTask = _dataService.GetYWindowFloorAsync(1);\n        var rows = await _dataService.GetYAsync(1);\n    }\n}\n";
        const string BareConditional = "public partial class T\n{\n    private async Task LoadZAsync()\n    {\n        var floorTask = on ? _dataService.GetZDataStartAsync(1) : Task.FromResult<DateTime?>(null);\n        var rows = await _dataService.GetZAsync(1);\n    }\n}\n";
        Assert.Equal(new[] { "T.cs::LoadYAsync" }, Scan(new[] { ("T.cs", BareFloor) }).Offenders);
        Assert.Equal(new[] { "T.cs::LoadZAsync" }, Scan(new[] { ("T.cs", BareConditional) }).Offenders);

        /* An await named only in a comment does not make a read look watched; every probe in a method is checked, not just the first. */
        const string Commented = "public partial class T\n{\n    private async Task LoadWAsync()\n    {\n        var dataStartTask = _dataService.GetWDataStartAsync(1);\n        // await AwaitReadWatchingProbeAsync(rows, dataStartTask, \"W\");\n        var rows = await _dataService.GetWAsync(1);\n    }\n}\n";
        Assert.Equal(new[] { "T.cs::LoadWAsync" }, Scan(new[] { ("T.cs", Commented) }).Offenders);
        const string TwoProbes = "public partial class T\n{\n    private async Task LoadVAsync()\n    {\n        var aTask = _d.GetADataStartAsync(1);\n        var rA = _d.GetAAsync(1);\n        await AwaitReadWatchingProbeAsync(rA, aTask, \"A\");\n        var bTask = _d.GetBWindowFloorAsync(1);\n        var rows = await _d.GetBAsync(1);\n    }\n}\n";
        Assert.Equal(new[] { "T.cs::LoadVAsync" }, Scan(new[] { ("T.cs", TwoProbes) }).Offenders);
    }
}
