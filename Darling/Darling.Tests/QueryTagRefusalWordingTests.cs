/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8226: the refusal delete_query_tag gives for a tag with a subtree counts each part the way a person says it, through the
/// same rule the apps' count labels use, so no "(s)" reaches a reader.
/// </summary>
public sealed class QueryTagRefusalWordingTests
{
    [Fact]
    public void TheTagInUseRefusal_CountsEachPartAsAPersonSaysIt()
    {
        var message = DarlingMcpQueryTagTools.TagInUseMessage(7, new QueryTagDeleteImpact(1, 3, 0));

        Assert.Equal(
            "Tag 7 has 1 child tag, 3 hand assignments and 0 rules in its subtree, and deleting it deletes them all. Nothing was deleted. Repeat with confirm=true to proceed.",
            message);
    }

    [Fact]
    public void TheTagInUseRefusal_SeparatesThousands_LikeEveryOtherCount()
    {
        var message = DarlingMcpQueryTagTools.TagInUseMessage(2, new QueryTagDeleteImpact(12, 1500, 1));

        Assert.Contains("12 child tags, 1,500 hand assignments and 1 rule", message, System.StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", message, System.StringComparison.Ordinal);
    }
}
