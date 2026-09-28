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
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the set of source files that write <c>query_store_stats</c> (#4661). The per-database watermark cache is
/// exact only while every writer either advances it (the live COPY) or invalidates it (the backfill), or cannot
/// touch a row newer than the read floor (retention). The real writers build the table name at run time
/// (<c>{table}</c>, <c>collect.{Table}</c>, the collector's <c>TargetTable</c>), so a search for the literal table
/// name finds none of them; <see cref="FindWriters"/> keys on the spellings the writers actually use.
/// </summary>
public sealed class QueryStoreWriterSetPinTests
{
    /// <summary>How a source names the table: the literal, a run-time spelling, or a sweep over every collector's
    /// <c>definition.TargetTable</c> (which includes this one).</summary>
    private static readonly Regex TableReference = new(
        @"query_store_stats\b|QueryStoreCollector\.Instance\b|QueryStoreSliceRepair\.Table\b|\bdefinition\.TargetTable\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>A write verb in SQL text aimed straight at the table, spelled as the literal or as a run-time hole.</summary>
    private static readonly Regex SqlWriteAtTable = new(
        @"\b(DELETE\s+FROM|INSERT\s+INTO|UPDATE|TRUNCATE(\s+TABLE)?|COPY)\s+(collect\.)?(query_store_stats\b|\{[^}]*(?-i:TargetTable|\bTable\b)[^}]*\})",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    /// <summary>A call that writes: it names its table by argument or by definition, not in the SQL it sits beside.</summary>
    private static readonly Regex WriterCall = new(
        @"drop_chunks|TimeSlicedDeleteSql\(|DropChunksSqlFor\(|BeginBinaryImport|WriteBackfillBatchAsync\(|WriteBatchAsync\(",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The files whose code (comments removed, string literals kept because the SQL lives in them) writes the
    /// table: a SQL write verb aimed at it, or a writer call in a file that references it.
    /// </summary>
    internal static IReadOnlyList<string> FindWriters(IEnumerable<(string File, string Text)> sources)
    {
        var found = new List<string>();
        foreach (var (file, text) in sources)
        {
            var code = WithoutComments(text);
            if (SqlWriteAtTable.IsMatch(code) || (TableReference.IsMatch(code) && WriterCall.IsMatch(code)))
            {
                found.Add(file);
            }
        }

        return found;
    }

    private static string WithoutComments(string text)
    {
        var mask = CSharpSourceWalker.CodeMask(text);
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(text))
        {
            for (var i = start; i < start + body.Length && i < mask.Length; i++)
            {
                mask[i] = true;
            }
        }

        var chars = text.ToCharArray();
        for (var i = 0; i < chars.Length; i++)
        {
            if (!mask[i] && chars[i] != '\n')
            {
                chars[i] = ' ';
            }
        }

        return new string(chars);
    }

    [Fact]
    public void Detector_FindsARuntimeBuiltDelete_ThroughTheCollectorsTargetTable()
    {
        var writers = FindWriters(new[]
        {
            ("Fake1.cs", "var sql = $\"DELETE FROM {QueryStoreCollector.Instance.TargetTable} WHERE collection_time < $1\";"),
        });
        Assert.Equal(new[] { "Fake1.cs" }, writers);
    }

    [Fact]
    public void Detector_FindsALiteralInsert()
    {
        var writers = FindWriters(new[]
        {
            ("Fake2.cs", "var sql = \"INSERT INTO collect.query_store_stats (a) VALUES (1)\";"),
        });
        Assert.Equal(new[] { "Fake2.cs" }, writers);
    }

    [Fact]
    public void Detector_FindsARepairStyleWriter_AndARetentionStyleCall()
    {
        var writers = FindWriters(new[]
        {
            ("Fake3.cs", "var sql = $\"DELETE FROM collect.{QueryStoreSliceRepair.Table}\";"),
            ("Fake4.cs", "var sql = TimeSlicedDeleteSql(QueryStoreCollector.Instance.TargetTable, \"c\", cutoff);"),
        });
        Assert.Equal(new[] { "Fake3.cs", "Fake4.cs" }, writers);
    }

    [Fact]
    public void Detector_IgnoresAFileThatOnlySelects_AndOneThatOnlyMentionsItInAComment()
    {
        var writers = FindWriters(new[]
        {
            ("Reader.cs", "var sql = \"SELECT max(last_execution_time) FROM collect.query_store_stats WHERE database_name = $1\";"),
            ("Prose.cs", "// DELETE FROM collect.query_store_stats is done elsewhere\r\nvar x = 1;"),
        });
        Assert.Empty(writers);
    }

    /// <summary>Every writer the cache's assumption names, each with why it is safe.</summary>
    private static readonly SortedDictionary<string, string> Allowed = new(StringComparer.Ordinal)
    {
        ["PerformanceMonitor.Darling.Service/DarlingCollectorRunner.cs"] =
            "the live COPY chokepoint; it advances the cache after commit, or invalidates on a fault or foreign row",
        ["PerformanceMonitor.Darling.Service/QueryStoreBackfill.cs"] =
            "writes through WriteBackfillBatchAsync, which invalidates the cache in a finally",
        ["PerformanceMonitor.Darling.Service/DarlingRetention.cs"] =
            "purges whole days, at least one day old, never a row inside the three-hour read floor",
        ["PerformanceMonitor.Darling.Storage/QueryStoreSliceRepair.cs"] =
            "replaces a slice in one transaction and writes back the same collection_time and MAX, so it keeps the maximum unchanged",
    };

    [Fact]
    public void TheWritersOfQueryStoreStats_AreExactlyTheKnownSet()
    {
        var sources = new List<(string File, string Text)>();
        foreach (var project in new[] { "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Storage" })
        {
            var dir = RepoFile.PathTo("Darling", project);
            foreach (var path in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
            {
                var rel = Path.GetRelativePath(dir, path).Replace('\\', '/');
                if (rel.StartsWith("obj/", StringComparison.Ordinal) || rel.StartsWith("bin/", StringComparison.Ordinal))
                {
                    continue;
                }

                sources.Add((project + "/" + rel, File.ReadAllText(path)));
            }
        }

        var found = FindWriters(sources).OrderBy(f => f, StringComparer.Ordinal).ToList();
        Assert.True(found.SequenceEqual(Allowed.Keys),
            "a writer of query_store_stats must invalidate or advance DatabaseWatermarkCache, then be added here with its reason. Found: "
            + string.Join(", ", found));
    }

    [Fact]
    public void WriteBackfillBatchAsync_HasExactlyOneCaller_TheBackfill()
    {
        var callers = new List<string>();
        var dir = RepoFile.PathTo("Darling", "PerformanceMonitor.Darling.Service");
        foreach (var path in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
            var n = Regex.Matches(code, @"\bWriteBackfillBatchAsync\(").Count;
            var declarations = Regex.Matches(code, @"Task<int>\s+WriteBackfillBatchAsync<").Count;
            if (n - declarations > 0)
            {
                callers.Add(Path.GetFileName(path) + " x" + (n - declarations));
            }
        }

        Assert.Equal(new[] { "QueryStoreBackfill.cs x1" }, callers);
    }
}
