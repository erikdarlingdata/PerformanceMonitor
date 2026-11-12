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
[Trait("Stage", "Guard")] // #5798: walks the repo tree, so the guard stage runs it
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
        foreach (var file in ViewerSources())
        {
            var text = File.ReadAllText(file);
            if (!text.Contains("new SaveFileDialog", StringComparison.Ordinal) || !text.Contains(".sqlplan\"", StringComparison.Ordinal))
            {
                continue;
            }

            if (!text.Contains("WithheldPlanGuard.RefuseSave(", StringComparison.Ordinal))
            {
                problems.Add(Path.GetFileName(file) + " writes a .sqlplan without WithheldPlanGuard.RefuseSave");
            }
        }

        /* Since #7228 the three history windows and the tab's plan saves all go through the shared FileSaveHelper, which
           StatementScrubViewerPlanWithheldTests scans, so the Viewer project holds no site of its own. A new one must still guard. */
        Assert.Empty(problems);
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
