/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Mcp;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9245: Lite's update_query_tag names the line and byte where changes_json broke, and nothing from the input.
/// </summary>
public sealed class LiteJsonParseErrorTests
{
    private const string Distinct = "Zq7-distinct-9";

    [Fact]
    public void QueryTagChangesJson_BadLiteral_NamesThePosition_NotTheInput()
    {
        var (edit, error, _) = McpQueryTagTools.ParseTagChanges("{\"name\":nil, \"x\":\"" + Distinct + "\"}");

        Assert.Null(edit);
        Assert.NotNull(error);
        Assert.Matches(@"^changes_json is not valid JSON \(line 1, byte 10\)\.$", error);
        Assert.DoesNotContain(Distinct, error!, System.StringComparison.Ordinal);
        Assert.DoesNotContain("nil", error!, System.StringComparison.Ordinal);
    }

    [Fact]
    public void QueryTagChangesJson_UnpairedSurrogate_GetsTheFixedText()
    {
        var (edit, error, _) = McpQueryTagTools.ParseTagChanges("{\"name\":\"\uD800\"}");

        Assert.Null(edit);
        Assert.Equal("changes_json is not valid JSON.", error);
    }
}
