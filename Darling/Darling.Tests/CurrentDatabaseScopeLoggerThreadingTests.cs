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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 review: <c>CurrentDatabaseScope</c> fails open (a monitoring read never fails on the dropped-database lookup) and logs the
/// failure at Warning, but only when the caller hands it a logger. These pins hold the two FinOps tools that were left passing none
/// (get_finops index_analysis and get_finops_recommendations) to the same shape as get_wait_stats (<c>ILogger? logger = null</c> just
/// before the cancellation token), and hold that no non-test caller of a logger-taking <c>CurrentDatabaseScope</c> method leaves the
/// logger off, so a new caller cannot reopen the gap. The Viewer has no host logger: it reaches most of these through the readers, whose
/// logger parameter is optional, and the one direct call (the regressions grid, #6654) passes its own ViewerStoreLogger.
/// </summary>
[Trait("Stage", "Guard")] // #5798: walks the repo tree, so the guard stage runs it
public sealed class CurrentDatabaseScopeLoggerThreadingTests
{
    [Fact]
    public void EveryNonTestCaller_OfACurrentDatabaseScopeLookup_PassesALogger()
    {
        var call = new Regex(@"CurrentDatabaseScope\.(ReadAsync|ReadSnapshotAsync|ReadDroppedAsync|NarrowAsync|OmitDroppedAsync)\(", RegexOptions.CultureInvariant);
        var offenders = new List<string>();
        var checkedCalls = 0;
        var darling = Path.Combine(RepoRoot(), "Darling");
        foreach (var file in Directory.EnumerateFiles(darling, "*.cs", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(darling, file).Replace('\\', '/');
            if (relative.StartsWith("Darling.Tests/", StringComparison.Ordinal) || relative.Contains("/obj/", StringComparison.Ordinal) || relative.Contains("/bin/", StringComparison.Ordinal))
            {
                continue;
            }

            var source = File.ReadAllText(file);
            foreach (Match m in call.Matches(source))
            {
                checkedCalls++;
                var end = source.IndexOf(';', m.Index);
                var expression = source.Substring(m.Index, end - m.Index);
                if (!expression.Contains("logger", StringComparison.Ordinal))
                {
                    offenders.Add($"{relative}: {m.Groups[1].Value}");
                }
            }
        }

        Assert.True(checkedCalls >= 5, "the scan found only " + checkedCalls + " callers; the pattern or the root is wrong");
        Assert.True(offenders.Count == 0, "CurrentDatabaseScope callers with no logger: " + string.Join("; ", offenders));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile);
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
