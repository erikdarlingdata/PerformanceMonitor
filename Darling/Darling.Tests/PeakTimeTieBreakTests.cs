/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license text.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4731: a tile's peak time must not depend on which of several tied rows the engine reaches first, and must
/// not be the time of a sample that has no value. Every peak-time sort - the ORDER BY of a per-tile
/// <c>array_agg(collection_time ORDER BY ...)[1]</c> and of a <c>(SELECT collection_time ... LIMIT 1)</c>
/// subquery - orders by the value <c>DESC NULLS LAST</c>, then by <c>collection_time DESC</c>: two rows tied on
/// the peak report the later collection time on every run, and a NULL value (which PostgreSQL sorts first under
/// <c>DESC</c>) never wins, just as it never wins the tile's <c>MAX</c>, which ignores NULLs. The census reads
/// every <c>*AnomalyDetector*.cs</c> file under the analysis project, so a new detector file, or a new sort in
/// an old one, is held to both halves without anyone registering it; a sort added without either half fails
/// here rather than showing a different time on a rerun.
/// </summary>
public sealed class PeakTimeTieBreakTests
{
    private const string AnalysisProject = "PerformanceMonitor.Darling.Analysis";

    /// <summary>What every peak-time ORDER BY ends with: the value's NULLs last, then the later collection time.</summary>
    private const string PeakOrderSuffix = " DESC NULLS LAST, collection_time DESC";

    private const RegexOptions SqlScan = RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled;

    /* Each pattern reads SQL in any case and across line breaks (\s covers the newline; [\s\S] spans lines), and each
       is tempered so a match cannot run on into the NEXT ORDER BY: a sort that lacks its tie-break must show up as
       itself, not weld itself to a later site's text and hide. */

    /// <summary>Shape one: the per-tile aggregate, <c>(array_agg(collection_time ORDER BY ...))[1]</c>. Any ORDER BY.</summary>
    private static readonly Regex AggregateShape = new(
        @"\barray_agg\s*\(\s*collection_time\s+ORDER\s+BY\s+(?<order>(?:(?!ORDER\s+BY|\barray_agg\b)[\s\S])*?)\s*\)\s*\)\s*\[\s*1\s*\]",
        SqlScan);

    /// <summary>Shape two: the correlated subquery, <c>(SELECT collection_time FROM ... ORDER BY ... LIMIT 1)</c>. Any ORDER BY.</summary>
    private static readonly Regex SubqueryShape = new(
        @"\(\s*SELECT\s+collection_time\s+FROM\b(?:(?!ORDER\s+BY|\bSELECT\b)[\s\S])*?ORDER\s+BY\s+(?<order>(?:(?!\bLIMIT\b|ORDER\s+BY)[\s\S])*?)\s+LIMIT\s+1\s*\)",
        SqlScan);

    /// <summary>Any other ORDER BY that already carries the tie-break (<c>... DESC, collection_time DESC</c>), so a
    /// peak sort in a third shape - the peak sample's reads, a window function - is held to the same suffix.</summary>
    private static readonly Regex TieBrokenShape = new(
        @"ORDER\s+BY\s+(?<order>(?:(?!ORDER\s+BY)[\s\S])*?\bDESC\b(?:\s+NULLS\s+(?:FIRST|LAST))?\s*,\s*collection_time\s+DESC\b)",
        SqlScan);

    private sealed record PeakSort(string File, int Line, string Shape, string Order);

    /// <summary>The ORDER BY of every peak-time sort in one piece of SQL: its offset, which shape found it, and its text.</summary>
    private static List<(int Index, string Shape, string Order)> PeakTimeOrderBys(string sql)
    {
        var found = new SortedDictionary<int, (string Shape, string Order)>();

        /* The two anchored shapes go first, so a sort with no tie-break at all is found, and the same clause found
           again by the tie-broken shape keeps the anchored shape's name. */
        foreach (var (shape, name) in new[] { (AggregateShape, "array_agg"), (SubqueryShape, "LIMIT 1 subquery"), (TieBrokenShape, "tie-broken sort") })
        {
            foreach (Match match in shape.Matches(sql))
            {
                var order = match.Groups["order"];
                found.TryAdd(order.Index, (name, order.Value));
            }
        }

        return found.Select(pair => (pair.Key, pair.Value.Shape, pair.Value.Order)).ToList();
    }

    /// <summary>The ORDER BY with every run of whitespace, line breaks included, folded to one space.</summary>
    private static string Normalise(string order) =>
        Regex.Replace(Regex.Replace(order, @"\s+", " "), @" ?, ?", ", ").Trim();

    /// <summary>Every <c>*AnomalyDetector*.cs</c> under the analysis project (relative to it), found by scanning the
    /// directory and never by a list, so a detector file added later is read without anyone registering it.</summary>
    private static List<string> DetectorSources()
    {
        var project = RepoFile.PathTo("Darling", AnalysisProject);

        return Directory.EnumerateFiles(project, "*AnomalyDetector*.cs", SearchOption.AllDirectories)
            .Select(path => Path.GetRelativePath(project, path))
            .Where(relative => !relative.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(part => part is "bin" or "obj"))
            .OrderBy(relative => relative, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>Every peak-time sort in the string literals of every detector source. Literals only, so a comment
    /// that quotes an old ORDER BY is not a site.</summary>
    private static List<PeakSort> AllPeakTimeSorts()
    {
        var sorts = new List<PeakSort>();

        foreach (var file in DetectorSources())
        {
            var source = RepoFile.ReadRepoFile("Darling", AnalysisProject, file);

            foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(source))
            {
                foreach (var (index, shape, order) in PeakTimeOrderBys(body))
                {
                    var line = 1 + source.AsSpan(0, start + index).Count('\n');
                    sorts.Add(new PeakSort(file.Replace('\\', '/'), line, shape, Normalise(order)));
                }
            }
        }

        return sorts;
    }

    [Fact]
    public void EveryPeakTimeSort_OrdersByTheValue_ThenByTheLaterCollectionTime()
    {
        var offenders = AllPeakTimeSorts()
            .Where(sort => !sort.Order.EndsWith(", collection_time DESC", StringComparison.Ordinal))
            .Select(sort => $"{sort.File}:{sort.Line}: the {sort.Shape} ORDER BY '{sort.Order}' leaves tied rows to the engine - end it with ', collection_time DESC'")
            .ToList();

        Assert.True(offenders.Count == 0, string.Join(Environment.NewLine, offenders));
    }

    [Fact]
    public void EveryPeakTimeSort_SortsASampleWithNoValueLast_SoThePeakTimeIsARowThePeakValueCounted()
    {
        /* PostgreSQL sorts NULLs FIRST under DESC. The tile's peak value is a MAX, which ignores NULLs, so an ORDER BY
           without NULLS LAST can report the time of a sample that has no value while the value beside it is another
           row's. Every site says so, not only the ones whose value can be NULL today, so there is one shape. */
        var offenders = AllPeakTimeSorts()
            .Where(sort => !sort.Order.EndsWith(PeakOrderSuffix, StringComparison.OrdinalIgnoreCase))
            .Select(sort => $"{sort.File}:{sort.Line}: the {sort.Shape} ORDER BY '{sort.Order}' lets a sample with no value sort first - end it with '{PeakOrderSuffix.TrimStart()}'")
            .ToList();

        Assert.True(offenders.Count == 0, string.Join(Environment.NewLine, offenders));
    }

    [Fact]
    public void TheCensus_ReadsEveryDetectorSource_AndFindsBothShapesInThem()
    {
        var files = DetectorSources();
        Assert.Contains("PgAnomalyDetector.cs", files);
        Assert.Contains("PgTargetAnomalyDetector.cs", files);
        Assert.Contains("PgTargetAnomalyDetector.Plans.cs", files);
        Assert.Contains("PgTargetAnomalyDetector.Growth.cs", files);

        /* A census that quietly stops matching passes on nothing. Today there are 15 aggregates and 3 subqueries. */
        var sorts = AllPeakTimeSorts();
        Assert.True(sorts.Count(sort => sort.Shape == "array_agg") >= 15, "the census found fewer per-tile aggregates than the detectors hold - its pattern has stopped matching");
        Assert.True(sorts.Count(sort => sort.Shape == "LIMIT 1 subquery") >= 3, "the census found fewer LIMIT 1 subqueries than the detectors hold - its pattern has stopped matching");
    }

    [Fact]
    public void TheCensus_FindsEitherShapeInAnyCaseAcrossLineBreaks_AndNothingThatIsNotAPeakTimeSort()
    {
        const string sql = @"
WITH snaps AS (SELECT DISTINCT collection_time FROM t WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 2)
SELECT (ARRAY_AGG(collection_time
                  ORDER BY mean_ms DESC,
                           collection_time DESC)) [1] AS spelled_out,
       (select collection_time
        from per_collection
        order by mean_ms desc nulls last, collection_time desc
        limit 1) AS lower_case,
       (SELECT collection_time FROM rated ORDER BY bytes_per_day DESC LIMIT 1) AS no_tie_break,
       (array_agg(collection_time ORDER BY v DESC NULLS LAST))[1] AS aggregate_no_tie_break,
       (array_agg(reads ORDER BY ms_per_read DESC NULLS LAST, collection_time DESC))[1] AS other_column
FROM x
GROUP BY k
ORDER BY local_hour DESC
LIMIT 1";

        var found = PeakTimeOrderBys(sql).Select(hit => (hit.Shape, Order: Normalise(hit.Order))).ToList();

        Assert.Equal(
            new (string, string)[]
            {
                ("array_agg", "mean_ms DESC, collection_time DESC"),
                ("LIMIT 1 subquery", "mean_ms desc nulls last, collection_time desc"),
                ("LIMIT 1 subquery", "bytes_per_day DESC"),
                ("array_agg", "v DESC NULLS LAST"),
                ("tie-broken sort", "ms_per_read DESC NULLS LAST, collection_time DESC"),
            },
            found);
    }

    [Fact]
    public void TheIoPeakSample_AndItsReadCount_PickTheSameTiedRow()
    {
        /* Two arrays ride one row: the peak sample's time and that sample's reads. They must share one ORDER BY,
           tie-break and NULLS LAST included, or a tie (or a sample with no latency) can pair one row's time with
           another row's reads. */
        var io = RepoFile.ReadRepoFile("Darling", AnalysisProject, "PgTargetAnomalyDetector.Io.cs");
        Assert.Contains("(array_agg(collection_time ORDER BY ms_per_read DESC NULLS LAST, collection_time DESC))[1] AS peak_sample", io, StringComparison.Ordinal);
        Assert.Contains("(array_agg(reads ORDER BY ms_per_read DESC NULLS LAST, collection_time DESC))[1]", io, StringComparison.Ordinal);
    }
}
