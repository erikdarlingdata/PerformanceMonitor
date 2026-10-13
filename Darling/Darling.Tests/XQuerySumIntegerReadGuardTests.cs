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
/// #5754: collector SQL must not read an XQuery <c>sum()</c> or <c>avg()</c> straight into an integer SQL type. Both return
/// <c>xs:double</c> over untyped attributes, and <c>xml.value()</c> reaches an integer type through the value's text (<c>0E0</c>,
/// <c>1.2E1</c>), which fails with "Error converting data type nvarchar to bigint" (the Client Timeouts collector, on every run).
/// The safe form reads <c>float</c> and CONVERTs. Narrow on purpose: only <c>value('sum(...)', 'int-type')</c> and
/// <c>value('avg(...)', 'int-type')</c>. <c>count()</c> returns xs:integer and is fine.
///
/// <para>Two bounds. (1) <c>[^']*'</c> ends the XQuery at its first single quote, so an XQuery that itself contains a T-SQL-escaped
/// <c>''</c> literal, such as <c>.value('sum(/a/b[@n=''x'']/@c)', 'bigint')</c>, is not matched: the scan stops at the <c>''</c> and the
/// <c>\s*,</c> after it cannot match. No such read exists today (<c>AttentionCollector</c> holds the only <c>value('sum(</c> in the live
/// tree), so this is a documentation gap, not a hole. (2) <c>ScannedTrees</c> covers every shipped <c>.value('...')</c> today: all of them
/// are in <c>PerformanceMonitor.Collectors</c> (<c>LongQueryCompletionsCollector</c>, <c>DeadlocksCollector</c>, <c>AttentionCollector</c>,
/// <c>BlockedProcessReportCollector</c>, <c>SystemHealthEventsCollector</c>, <c>MemoryPressureEventsCollector</c>,
/// <c>IndexObjectStatsCollector</c>, <c>CpuUtilizationCollector</c>). The other four trees are belt and braces; the only <c>.value(</c>
/// outside them is in <c>install/</c> and <c>deprecated/</c>, the retired SKUs that <c>TsqlConventionGuardTests</c> also treats as out of scope.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class XQuerySumIntegerReadGuardTests
{
    /// <summary>A value() whose XQuery is a sum or avg and whose SQL type is tinyint, smallint, int, integer or bigint. A single quote ends the query text, so the doubled quotes of a verbatim C# string do not hide it.</summary>
    private static readonly Regex Offender = new(
        @"\.value\s*\(\s*'\s*(sum|avg)\s*\([^']*'\s*,\s*'\s*(tinyint|smallint|int|integer|bigint)\s*'\s*\)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly string[] ScannedTrees =
    {
        "PerformanceMonitor.Collectors",
        "PerformanceMonitor.Alerting",
        "PerformanceMonitor.Common",
        "Lite/Services",
        "Darling/PerformanceMonitor.Darling.Service",
    };

    internal static IReadOnlyList<string> Offenders(string source) =>
        Offender.Matches(source).Select(m => m.Value).ToList();

    [Fact]
    public void TheScan_CatchesThe5714Read_AndTheAvgTwin()
    {
        /* The exact line #5714 shipped (a C# verbatim string, so the quotes inside the XQuery are doubled). */
        const string shipped =
            "    @event_count = @target_data.value('sum(/CounterTarget/Packages/Package/Event[@name=\"\"attention\"\"]/@count)', 'bigint');";
        Assert.Single(Offenders(shipped));

        Assert.Single(Offenders("x.value('avg(/a/b/@c)', 'int')"));
        Assert.Single(Offenders("x.value( 'sum(/a/b/@c)' , 'integer' )"));
    }

    [Fact]
    public void TheScan_AllowsTheSafeForms()
    {
        Assert.Empty(Offenders("CONVERT(bigint, @t.value('sum(/a/b/@c)', 'float'))"));
        Assert.Empty(Offenders("@t.value('count(/a/b)', 'int')"));
        Assert.Empty(Offenders("@t.value('(/a/b/@c)[1]', 'bigint')"));
        Assert.Empty(Offenders("@t.value('sum(/a/b/@c)', 'decimal(38,0)')"));
    }

    [Fact]
    public void CollectorSql_NeverReadsASumOrAvgStraightIntoAnInteger()
    {
        var root = RepoRoot();
        var found = new List<string>();
        foreach (var tree in ScannedTrees)
        {
            var dir = Path.Combine(root, tree);
            if (!Directory.Exists(dir))
            {
                continue;
            }

            foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                    && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)))
            {
                foreach (var hit in Offenders(File.ReadAllText(file)))
                {
                    found.Add(Path.GetRelativePath(root, file) + ": " + hit);
                }
            }
        }

        Assert.True(found.Count == 0,
            "An XQuery sum()/avg() is read straight into an integer type (it fails on the double's text, #5754). Read it as 'float' and CONVERT:\n"
            + string.Join("\n", found));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
