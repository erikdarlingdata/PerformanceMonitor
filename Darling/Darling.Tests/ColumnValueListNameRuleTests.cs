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
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The "no value list" column-name rule (#5565) lives twice: <c>ColumnValueListColumns</c> for the desktop and
/// <c>NO_LIST_SUFFIXES</c> / <c>NO_LIST_FRAGMENTS</c> in <c>grid-value-filter.js</c> for the web page. A statement,
/// plan or message column that gets a list on one half and not the other keeps its text in the browser or the
/// settings folder, so these tests hold the two lists to be the same and run the fixture's desktop/web name pairs
/// through the desktop half (the web half runs in <see cref="GridValueFilterBehaviourTests"/>).
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ColumnValueListNameRuleTests
{
    private static string[] JsArray(string source, string name)
    {
        var m = Regex.Match(source, @"export const " + name + @" = \[(?<items>.*?)\];", RegexOptions.Singleline);
        Assert.True(m.Success, name + " was not found in grid-value-filter.js");
        return Regex.Matches(m.Groups["items"].Value, "\"(?<s>[^\"]*)\"").Select(x => x.Groups["s"].Value).ToArray();
    }

    private static string WebSource() => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "grid-value-filter.js");

    [Fact]
    public void The_web_and_desktop_suffix_lists_are_the_same()
    {
        var web = JsArray(WebSource(), "NO_LIST_SUFFIXES").OrderBy(s => s, StringComparer.Ordinal).ToArray();
        var desktop = ColumnValueListColumns.NameSuffixes.Select(s => s.ToLowerInvariant()).OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.NotEmpty(desktop);
        Assert.Equal(desktop, web);
    }

    [Fact]
    public void The_web_and_desktop_fragment_lists_are_the_same()
    {
        var web = JsArray(WebSource(), "NO_LIST_FRAGMENTS").OrderBy(s => s, StringComparer.Ordinal).ToArray();
        var desktop = ColumnValueListColumns.NameFragments.Select(s => s.ToLowerInvariant()).OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.NotEmpty(desktop);
        Assert.Equal(desktop, web);
    }

    private static JsonElement Pairs()
    {
        var path = PathTo("Darling", "Darling.Tests", "Fixtures", "column-value-filter-cases.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        return Find(doc.RootElement, "noListColumnPairs").Clone();
    }

    private static JsonElement Find(JsonElement e, string name)
    {
        if (e.ValueKind == JsonValueKind.Object)
        {
            foreach (var p in e.EnumerateObject())
            {
                if (p.Name == name)
                {
                    return p.Value;
                }

                if (p.Value.ValueKind == JsonValueKind.Object)
                {
                    var inner = Find(p.Value, name);
                    if (inner.ValueKind != JsonValueKind.Undefined)
                    {
                        return inner;
                    }
                }
            }
        }

        return default;
    }

    [Fact]
    public void The_desktop_gives_the_fixture_s_answer_for_every_column_name_pair()
    {
        var pairs = Pairs();
        var excluded = pairs.GetProperty("excluded").EnumerateArray().Select(p => p.GetProperty("desktop").GetString()!).ToArray();
        var listed = pairs.GetProperty("listed").EnumerateArray().Select(p => p.GetProperty("desktop").GetString()!).ToArray();
        Assert.NotEmpty(excluded);
        Assert.NotEmpty(listed);
        Assert.All(excluded, n => Assert.True(ColumnValueListColumns.IsExcluded(n), n + " must get no list"));
        Assert.All(listed, n => Assert.False(ColumnValueListColumns.IsExcluded(n), n + " must get a list"));
    }

    [Fact]
    public void Every_pair_names_a_web_key_that_snake_cases_the_desktop_name()
    {
        // A pair whose two halves name different columns would test nothing: the web key is the desktop name in snake_case,
        // allowing the few real columns whose key differs (listed in the exceptions).
        var exceptions = new HashSet<string> { "SqlText", "Command", "Preview" };
        var pairs = Pairs();
        foreach (var list in new[] { "excluded", "listed" })
        {
            foreach (var p in pairs.GetProperty(list).EnumerateArray())
            {
                var d = p.GetProperty("desktop").GetString()!;
                var w = p.GetProperty("web").GetString()!;
                var snake = Regex.Replace(d, "(?<=[a-z0-9])(?=[A-Z])", "_").ToLowerInvariant();
                Assert.True(exceptions.Contains(d) || snake == w, d + " vs " + w);
            }
        }
    }
}
