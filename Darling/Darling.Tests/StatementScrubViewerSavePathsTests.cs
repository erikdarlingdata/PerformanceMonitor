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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, the Darling viewer's side of "what leaves the app". The viewer never opens a connection to a monitored
/// server: it reads rows the collectors already judged, and asks the service for anything live (the Active Queries
/// snapshot, a plan by sql_handle, an actual-plan re-run), whose handlers judge the text and the plans before they
/// answer. So a viewer save or copy of statement text or a plan carries what the screen shows. Pinned here, so a change
/// to that is noticed: no viewer file connects to SQL Server itself, every viewer site that writes a .sqlplan asks the
/// withheld guard first (Lite.Tests holds the same scan over Lite and the shared project), and the service handlers a live
/// answer comes from still judge it.
/// </summary>
public sealed class StatementScrubViewerSavePathsTests
{
    private static IEnumerable<string> ViewerSources() =>
        Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer"), "*.cs", SearchOption.AllDirectories)
            .Where(f =>
            {
                var relative = Path.GetRelativePath(RepoRoot(), f).Replace('\\', '/');
                return !relative.Contains("/obj/", StringComparison.Ordinal) && !relative.Contains("/bin/", StringComparison.Ordinal);
            });

    [Fact]
    public void NoViewerFile_OpensItsOwnConnectionToASqlServer()
    {
        var offenders = new List<string>();
        foreach (var file in ViewerSources())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            if (Regex.IsMatch(code, @"new\s+SqlConnection\s*\("))
            {
                offenders.Add(Path.GetFileName(file));
            }
        }

        Assert.Empty(offenders);
    }

    [Fact]
    public void EveryViewerPlanSaveSite_RefusesTheWithheldMarkerFirst()
    {
        var problems = new List<string>();
        var sites = 0;
        foreach (var file in ViewerSources())
        {
            var text = File.ReadAllText(file);
            if (!text.Contains("new SaveFileDialog", StringComparison.Ordinal) || !text.Contains(".sqlplan\"", StringComparison.Ordinal))
            {
                continue;
            }

            sites++;
            if (!text.Contains("WithheldPlanGuard.RefuseSave(", StringComparison.Ordinal))
            {
                problems.Add(Path.GetFileName(file) + " writes a .sqlplan without WithheldPlanGuard.RefuseSave");
            }
        }

        /* The three history windows. The tab's own plan saves go through the shared FileSaveHelper, which Lite.Tests scans. */
        Assert.True(sites >= 3, "expected at least 3 viewer plan save sites, found " + sites);
        Assert.Empty(problems);
    }

    [Fact]
    public void TheServiceHandlers_ThatAnswerALiveRead_StillJudgeIt()
    {
        var collector = File.ReadAllText(Path.Combine(RepoRoot(), "PerformanceMonitor.Collectors", "QuerySnapshotsCollector.cs"));
        var start = collector.IndexOf("public override async ValueTask<List<Row>> ReadAsync", StringComparison.Ordinal);
        Assert.True(start > 0);
        var body = collector[start..Math.Min(collector.Length, start + 3500)];
        Assert.Contains("BeginStatementScrub()", body, StringComparison.Ordinal);
        Assert.Contains("scrub.Text(reader.GetString(3))", body, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(body, @"scrub\.Xml\(").Count);

        var worker = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var outcome = worker.IndexOf("internal static CommandOutcome PlanResultOutcome", StringComparison.Ordinal);
        Assert.True(outcome > 0);
        Assert.Contains("SensitiveStatements.Xml(planXml)", worker[outcome..Math.Min(worker.Length, outcome + 900)], StringComparison.Ordinal);
        Assert.Contains("PlanResultOutcome(\"actual plan captured\"", worker, StringComparison.Ordinal);
        Assert.Contains("PlanResultOutcome(\"plan fetched\"", worker, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("repository root not found");
    }
}
