/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6850: the words a missing Resource Use By Tag source is named in come from one table both apps use. These cover every section
/// Lite defines, so a section added without words fails here and does not read as another section.
/// </summary>
public sealed class LiteQueryTagSourceWordsTests
{
    /// <summary>The sentence per section, the same ones the Darling Viewer's tests pin.</summary>
    internal static readonly IReadOnlyDictionary<string, string> Expected = new Dictionary<string, string>
    {
        ["plan_cache"] = "This store has no query stats data yet.",
        ["query_store"] = "This store has no Query Store data yet.",
        ["procedures"] = "This store has no procedure stats data yet.",
        ["blocking_reports"] = "This store has no blocked process report data yet.",
        ["blocking_snapshots"] = "This store has no blocking snapshot data yet.",
        ["deadlocks"] = "This store has no deadlock data yet.",
    };

    internal static IReadOnlyList<QueryTagRollupSection> Sections { get; } =
        [.. typeof(QueryTagRollupSection)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(QueryTagRollupSection))
            .Select(f => (QueryTagRollupSection)f.GetValue(null)!)];

    [Fact]
    public void EverySection_HasWords()
    {
        Assert.NotEmpty(Sections);
        foreach (var section in Sections)
        {
            Assert.True(Expected.ContainsKey(section.Name), $"section {section.Name} has no expected sentence in this test");
            Assert.Equal(Expected[section.Name], QueryTagSourceWords.Missing(section.Name));
        }

        Assert.Equal(Expected.Keys.OrderBy(k => k, StringComparer.Ordinal), Sections.Select(s => s.Name).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Equal(Expected.Keys.OrderBy(k => k, StringComparer.Ordinal), QueryTagSourceWords.SectionNames.OrderBy(n => n, StringComparer.Ordinal));
    }

    [Fact]
    public void ASectionWithNoWords_Throws_AndIsNeverReadAsDeadlock()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => QueryTagSourceWords.For("a_section_nobody_named"));
    }
}
