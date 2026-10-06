/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5244 PR4 review L2 and L3: the empty answers of the tools that take <c>database_name</c> echo it the way their answers
/// with rows do (the name for one database, "the chosen databases" for two or more, null for all), and a long-query answer
/// that a filter emptied no longer sends the reader to the opt-in switch. The behaviour of the shared helper is run; the
/// tools' empty paths need a store, so their wiring is pinned from source, and Lite's twin tests run the same paths for real.
/// </summary>
public sealed class EmptyAnswerDatabaseEchoTests
{
    private static string Tool(string file) =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", file);

    [Theory]
    [InlineData(null)]
    [InlineData("DbA")]
    [InlineData("the chosen databases")]
    public void StatusForDatabase_WritesTheEchoBesideStatusAndMessage_NullIncluded(string? echo)
    {
        var root = JsonDocument.Parse(McpHelpers.StatusForDatabase("empty", "nothing here", echo)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal("nothing here", root.GetProperty("message").GetString());
        Assert.Equal(echo, root.GetProperty("database_name").ValueKind == JsonValueKind.Null ? null : root.GetProperty("database_name").GetString());
        Assert.False(root.TryGetProperty("hints", out _), "no hints argument, no hints key");
    }

    [Fact]
    public void StatusForDatabase_KeepsTheHints_AfterTheEcho()
    {
        var json = McpHelpers.StatusForDatabase("empty", "m", "DbA", new { effective_start = "2026-01-01" });

        Assert.Contains("\"database_name\":\"DbA\",\"hints\":{", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// L2, trends: <c>EmptyStatus</c> takes the filter and writes the echo, and every call passes it: a call that left it off
    /// would not compile, so the count of calls is the count of answers.
    /// </summary>
    [Fact]
    public void TrendEmptyStatus_WritesTheEcho_AndEveryCallPassesTheFilter()
    {
        var source = Tool("DarlingMcpTrendTools.cs");

        Assert.Contains("string message, TrendDisclosure disclosure, DatabaseFilter databases)", source, StringComparison.Ordinal);
        var body = source[source.IndexOf("private static string EmptyStatus(", StringComparison.Ordinal)..];
        body = body[..body.IndexOf("disclosure.WriteTo(envelope);", StringComparison.Ordinal)];
        Assert.Contains("[\"database_name\"] = databases.Describe()", body, StringComparison.Ordinal);

        var calls = Regex.Matches(source, @"EmptyStatus\(\s*""(empty|unavailable)""", RegexOptions.Singleline).Count;
        var withFilter = Regex.Matches(source, @"disclosure, databases\);", RegexOptions.Singleline).Count;
        Assert.True(calls >= 8, $"expected the trend tools' eight empty answers, found {calls}");
        Assert.Equal(calls, withFilter);
    }

    /// <summary>L2, the other tools: no empty answer of theirs is built without the echo.</summary>
    [Theory]
    [InlineData("DarlingMcpLongQueryTools.cs", 1)]
    [InlineData("DarlingMcpPlanCorrectionTools.cs", 1)]
    [InlineData("DarlingMcpQueryStoreClutterTools.cs", 2)]
    public void EmptyAnswers_GoThroughStatusForDatabase(string file, int expected)
    {
        var source = Tool(file);

        Assert.Equal(expected, Regex.Matches(source, @"McpHelpers\.StatusForDatabase\(\s*""(empty|unavailable)""", RegexOptions.Singleline).Count);
        Assert.DoesNotMatch(@"McpHelpers\.Status\(\s*""(empty|unavailable)""", source);
    }

    /// <summary>
    /// L3: the long-query empty answer names the opt-in switch only when no filter is chosen. Under a filter it ends after the
    /// database clause, as get_plan_corrections does.
    /// </summary>
    [Fact]
    public void LongQueryEmpty_BlamesTheOptInSwitch_OnlyWhenNoFilterIsChosen()
    {
        var source = Tool("DarlingMcpLongQueryTools.cs");

        /* Round 2: the sentence follows the (unfiltered) window-notice probe, so a filtered answer keeps it only when the store holds no row. */
        Assert.Contains("var collectorMayBeOff = databaseFilter.IsAll || (!emptyNotice.IsUnavailable && emptyNotice.EffectiveStart is null);", source, StringComparison.Ordinal);
        Assert.Contains("(collectorMayBeOff ? \". The long_query_completions collector is opt-in (default OFF)", source, StringComparison.Ordinal);
    }
}
