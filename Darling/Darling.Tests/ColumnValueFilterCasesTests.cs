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
using PerformanceMonitor.Common;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The value-list column filter (#5565) through the shared Common code: every case in
/// <c>Fixtures/column-value-filter-cases.json</c> (which the web page tests read too) is driven step by step, and the
/// list, the stored state and the rows shown are checked after each step that says what it expects.
/// </summary>
public sealed class ColumnValueFilterCasesTests
{
    private sealed class Row
    {
        public string? Login { get; set; }
    }

    private static JsonElement Cases()
    {
        var path = PathTo("Darling", "Darling.Tests", "Fixtures", "column-value-filter-cases.json");
        return JsonDocument.Parse(File.ReadAllText(path)).RootElement.GetProperty("cases").Clone();
    }

    public static IEnumerable<object[]> CaseIds() =>
        Cases().EnumerateArray().Select(c => new object[] { c.GetProperty("id").GetInt32() });

    private static List<string?> ReadRows(JsonElement rows)
    {
        if (rows.ValueKind == JsonValueKind.Object)
        {
            var gen = rows.GetProperty("generate");
            var prefix = gen.GetProperty("prefix").GetString()!;
            return Enumerable.Range(0, gen.GetProperty("count").GetInt32()).Select(i => (string?)(prefix + i)).ToList();
        }
        return rows.EnumerateArray().Select(r => r.ValueKind == JsonValueKind.Null ? null : r.GetString()).ToList();
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void The_fixture_has_the_spec_cases()
    {
        Assert.True(Cases().GetArrayLength() >= 8);
    }

    [Theory]
    [MemberData(nameof(CaseIds))]
    public void A_fixture_case_runs_through_the_shared_code(int id)
    {
        var testCase = Cases().EnumerateArray().Single(c => c.GetProperty("id").GetInt32() == id);
        var rows = ReadRows(testCase.GetProperty("rows"));
        var state = new ColumnFilterState { ColumnName = nameof(Row.Login) };
        var search = string.Empty;
        var stepNo = 0;

        foreach (var step in testCase.GetProperty("steps").EnumerateArray())
        {
            stepNo++;
            var at = $"case {id} step {stepNo} ({step.GetProperty("action").GetString()})";
            var action = step.GetProperty("action").GetString();
            switch (action)
            {
                case "open":
                    break;
                case "search":
                    search = step.GetProperty("text").GetString() ?? string.Empty;
                    break;
                case "untick":
                case "tick":
                case "tickOnly":
                case "selectAll":
                {
                    var selection = new ColumnValueSelection(ColumnValueCatalog.Build(rows), state);
                    var blank = step.TryGetProperty("blank", out var b) && b.GetBoolean();
                    var values = step.TryGetProperty("values", out var v) ? Strings(v) : Array.Empty<string>();
                    if (action == "untick")
                    {
                        foreach (var value in values) selection.SetTicked(value, false);
                        if (blank) selection.SetBlankTicked(false);
                    }
                    else if (action == "tick")
                    {
                        foreach (var value in values) selection.SetTicked(value, true);
                        if (blank) selection.SetBlankTicked(true);
                    }
                    else if (action == "tickOnly")
                    {
                        selection.TickOnly(values, blank);
                    }
                    else
                    {
                        selection.SetAll(ColumnValueCatalog.Build(rows).List(search), step.GetProperty("ticked").GetBoolean());
                    }
                    selection.ApplyTo(state);
                    break;
                }
                case "refresh":
                    if (step.TryGetProperty("rows", out var replace))
                        rows = ReadRows(replace);
                    if (step.TryGetProperty("addRows", out var add))
                        rows.AddRange(ReadRows(add));
                    break;
                case "textMatch":
                    state.Operator = Enum.Parse<FilterOperator>(step.GetProperty("operator").GetString()!);
                    state.Value = step.GetProperty("text").GetString() ?? string.Empty;
                    break;
                default:
                    Assert.Fail($"{at}: unknown action");
                    break;
            }

            if (!step.TryGetProperty("expect", out var expect))
                continue;

            var catalog = ColumnValueCatalog.Build(rows);
            var listing = catalog.List(search);
            var shown = rows.Where(r => ColumnFilterMatcher.MatchesFilter(new Row { Login = r }, state)).ToList();

            if (expect.TryGetProperty("listBlank", out var listBlank))
                Assert.True(listBlank.GetBoolean() == listing.HasBlankEntry, $"{at}: (Blanks) entry");
            if (expect.TryGetProperty("listValues", out var listValues))
                Assert.True(Strings(listValues).SequenceEqual(listing.Values), $"{at}: listed values {string.Join("|", listing.Values.Take(15))}");
            if (expect.TryGetProperty("listCount", out var listCount))
                Assert.True(listCount.GetInt32() == listing.Count, $"{at}: listed {listing.Count}");
            if (expect.TryGetProperty("note", out var note))
                Assert.True(note.GetString() == (listing.CapNote ?? (state.ShowsNoValues ? ColumnValueCatalog.NoTicksNote : null)), $"{at}: note");
            if (expect.TryGetProperty("state", out var expectedState))
            {
                Assert.True(Enum.Parse<ColumnValueMode>(expectedState.GetProperty("mode").GetString()!) == state.ValueMode, $"{at}: mode {state.ValueMode}");
                Assert.True(expectedState.GetProperty("blank").GetBoolean() == state.ValueBlank, $"{at}: blank flag");
                Assert.True(Strings(expectedState.GetProperty("values")).ToHashSet(StringComparer.OrdinalIgnoreCase).SetEquals(state.Values), $"{at}: state values {string.Join("|", state.Values.Take(15))}");
            }
            if (expect.TryGetProperty("shown", out var expectedShown))
            {
                var want = expectedShown.EnumerateArray().Select(e => e.ValueKind == JsonValueKind.Null ? null : e.GetString()).ToList();
                Assert.True(want.SequenceEqual(shown), $"{at}: shown rows {string.Join("|", shown.Take(15).Select(s => s ?? "<null>"))}");
            }
            if (expect.TryGetProperty("shownCount", out var shownCount))
                Assert.True(shownCount.GetInt32() == shown.Count, $"{at}: shown {shown.Count} rows");
        }
    }
}
