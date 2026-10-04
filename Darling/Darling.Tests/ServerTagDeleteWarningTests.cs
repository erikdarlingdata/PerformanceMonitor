/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>The sentence both tag-delete dialogs append when custom alert rules are scoped to the tag or a child.</summary>
public sealed class ServerTagDeleteWarningTests
{
    private static List<TagScopedRule> Rules(int n) =>
        Enumerable.Range(1, n).Select(i => new TagScopedRule(10 + i, "Rule" + i, true, 3)).ToList();

    [Fact]
    public void NoRules_AddNothing() =>
        Assert.Equal(string.Empty, ServerTagCoverage.DeleteWarning(Rules(0)));

    [Fact]
    public void OneRule_NamesItWithItsId() =>
        Assert.Equal(
            " Custom alert rules scoped to this tag or a child tag will stop matching any server: 'Rule1' (#11)",
            ServerTagCoverage.DeleteWarning(Rules(1)));

    [Fact]
    public void FiveRules_NameAllFive_WithNoMoreSuffix()
    {
        var text = ServerTagCoverage.DeleteWarning(Rules(5));

        Assert.EndsWith("'Rule4' (#14), 'Rule5' (#15)", text);
        Assert.DoesNotContain("more", text);
    }

    [Fact]
    public void SevenRules_NameFive_ThenSayTwoMore()
    {
        var text = ServerTagCoverage.DeleteWarning(Rules(7));

        Assert.EndsWith("'Rule5' (#15), and 2 more", text);
        Assert.DoesNotContain("Rule6", text);
    }
}
