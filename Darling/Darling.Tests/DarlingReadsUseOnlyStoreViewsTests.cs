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
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Every <c>v_</c> name that a Darling read uses as a <c>FROM</c> or <c>JOIN</c> target must be a view the Darling store creates.
/// Lite's reads query <c>v_server_properties</c>, which has no Darling twin, and a read ported from Lite that keeps the name fails
/// on PostgreSQL with 42P01 (the relation does not exist). Only the live tests, which need a database, see that failure. This
/// pin reads the source text of every Darling project that holds reads, with the comments removed, so it fails without one. The
/// projects are <c>PerformanceMonitor.Darling.Service</c>, <c>PerformanceMonitor.Darling.Viewer</c>,
/// <c>PerformanceMonitor.Darling.Analysis</c> and <c>PerformanceMonitor.Darling.Storage</c>.
/// The store's views are <see cref="PgSchemaGenerator.AllPassthroughViews"/> and <see cref="PgSchemaGenerator.PayloadResolvingViews"/>.
/// </summary>
public sealed class DarlingReadsUseOnlyStoreViewsTests
{
    private static readonly Regex BlockComment = new(@"/\*.*?\*/", RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex LineComment = new(@"//[^\r\n]*", RegexOptions.Compiled);
    /// <summary>A <c>FROM</c> or <c>JOIN</c> followed by a <c>v_</c> name. A <c>FROM</c> that follows <c>DISTINCT</c> is the comparison
    /// operator in <c>IS [NOT] DISTINCT FROM v_old_x</c> (a variable named <c>v_old_x</c>, not a table source), so the lookbehind skips it (#5240).</summary>
    private static readonly Regex ViewTarget = new(@"(?<!\bDISTINCT\s+)\b(?:FROM|JOIN)\s+(v_[a-z0-9_]+)", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    [Theory]
    [InlineData("WHERE a IS DISTINCT FROM v_x", false)]
    [InlineData("WHERE a IS NOT DISTINCT FROM v_x", false)]
    [InlineData("WHERE a IS DISTINCT   FROM v_x", false)]
    [InlineData("SELECT 1 FROM v_bogus", true)]
    [InlineData("SELECT 1 FROM t JOIN v_bogus ON 1 = 1", true)]
    [InlineData("SELECT 1 WHERE a IS DISTINCT FROM v_x AND EXISTS (SELECT 1 FROM v_bogus)", true)]
    public void ViewTargetPattern_SkipsIsDistinctFrom_ButStillFlagsRealFromAndJoinTargets(string sql, bool flagged)
    {
        Assert.Equal(flagged, ViewTarget.IsMatch(sql));
    }

    [Fact]
    public void EveryViewNamedAsAFromOrJoinTarget_InTheServiceViewerAnalysisAndStorageSql_IsOneTheStoreCreates()
    {
        var storeViews = new HashSet<string>(
            PgSchemaGenerator.AllPassthroughViews.Concat(PgSchemaGenerator.PayloadResolvingViews), StringComparer.OrdinalIgnoreCase);
        var unknown = new SortedSet<string>(StringComparer.Ordinal);
        var scanned = 0;

        foreach (var project in new[]
        {
            "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Viewer",
            "PerformanceMonitor.Darling.Analysis", "PerformanceMonitor.Darling.Storage",
        })
        {
            foreach (var file in Directory.EnumerateFiles(PathTo("Darling", project), "*.cs", SearchOption.AllDirectories))
            {
                var segments = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                if (segments.Contains("bin") || segments.Contains("obj"))
                    continue;

                var sqlAndCode = LineComment.Replace(BlockComment.Replace(File.ReadAllText(file), " "), string.Empty);
                scanned++;
                foreach (Match target in ViewTarget.Matches(sqlAndCode))
                {
                    if (!storeViews.Contains(target.Groups[1].Value))
                        unknown.Add($"{target.Groups[1].Value} in {Path.GetFileName(file)}");
                }
            }
        }

        Assert.True(scanned > 20, $"Only {scanned} source files were scanned, so this pin is not reading the code it guards.");
        Assert.True(
            unknown.Count == 0,
            "These views are not views the Darling store creates (read the base table instead): " + string.Join(", ", unknown));
    }
}
