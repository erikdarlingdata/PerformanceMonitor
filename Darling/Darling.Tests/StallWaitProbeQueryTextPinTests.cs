/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The stall probe's idle-wait list (#5267) against the shared <c>IgnoredWaitDefaults.All</c> set. The list is
/// rendered into the probe's T-SQL as literals, and a name that is not a plain wait name is refused. Lite.Tests
/// pins the same facts, but CI does not run Lite.Tests on a Darling-only change, so this class is what stops a
/// bad entry added to the shared set from reaching a release.
/// </summary>
public sealed class StallWaitProbeQueryTextPinTests
{
    /// <summary>
    /// Every name in the shared set renders: it is a plain upper-case wait name, and the rendered text holds it
    /// as a literal exactly once, because the list is rendered a single time.
    /// </summary>
    [Fact]
    public void EveryIgnoredWaitName_IsAPlainWaitName_AndIsRenderedIntoTheProbeText()
    {
        var sql = StallWaitProbePolicy.QueryText;

        /* The list is the leading derived set; the scheduler filter further down has a literal of its own. */
        var list = sql.Substring(0, sql.IndexOf("SELECT /* PerformanceMonitorDarling", StringComparison.Ordinal));

        Assert.NotEmpty(IgnoredWaitDefaults.All);
        Assert.All(IgnoredWaitDefaults.All, name => Assert.Matches("^[A-Z0-9_]+$", name));

        foreach (var name in IgnoredWaitDefaults.All)
        {
            Assert.Single(Regex.Matches(sql, "N'" + Regex.Escape(name) + "'"));
            Assert.Contains("N'" + name + "'", list, StringComparison.Ordinal);
        }

        Assert.Equal(
            IgnoredWaitDefaults.All.OrderBy(n => n, StringComparer.Ordinal).ToList(),
            Regex.Matches(list, "N'([^']*)'").Select(m => m.Groups[1].Value).ToList());
    }

    /// <summary>
    /// A bad entry fails the BUILDER, with the documented exception, for a quote, a lower-case name, a trailing
    /// space, an empty name, a null and an empty list.
    /// </summary>
    [Theory]
    [InlineData("BAD'; DROP")]
    [InlineData("lower_case")]
    [InlineData("TRAILING_SPACE ")]
    [InlineData("")]
    [InlineData(null)]
    public void ABadEntry_IsRefusedByTheBuilder_WithTheDocumentedException(string? bad)
    {
        Assert.Throws<InvalidOperationException>(() => StallWaitProbePolicy.BuildQueryText(new[] { "OK_WAIT", bad! }));
        Assert.Throws<InvalidOperationException>(() => StallWaitProbePolicy.BuildQueryText(Array.Empty<string>()));
    }

    /// <summary>
    /// The refusal reaches only the probe-text path. The policy's own members, which every budgeted collector
    /// uses, do not render the list, so a bad entry cannot make them throw. This is the property the text
    /// being a lazy holder (and not a static field of the policy) exists to give.
    /// </summary>
    [Fact]
    public void ABadEntry_FailsOnlyTheTextPath_NotThePolicy()
    {
        /* The builder throws... */
        Assert.Throws<InvalidOperationException>(() => StallWaitProbePolicy.BuildQueryText(new[] { "bad name" }));

        /* ...and the policy is still usable beside it: nothing but QueryText calls the builder with the shared
           set, and QueryText is read inside the probe's try block. */
        Assert.True(StallWaitProbePolicy.HardBudget > TimeSpan.Zero);
        Assert.NotEmpty(StallWaitProbePolicy.Outcomes);
        Assert.Contains(StallWaitProbePolicy.OutcomeQueryFailed, StallWaitProbePolicy.Outcomes);

        /* Structurally: the policy's type initialiser never calls the builder, so no entry in the shared set
           can fail it. A static field initialised with BuildQueryText() would put the call back in it. */
        Assert.False(TypeInitialiserCallsBuilder(), "the policy's type initialiser renders the query text");
    }

    /// <summary>
    /// The tool text names the three generations of stall-probe row (#5267): the guide tail, which tools/list
    /// does not serve, says a tail without the idle part is from a build that still ranked idle waits. The head
    /// (the part tools/list serves) is pinned in length by McpToolsListBudgetTests and here by content.
    /// </summary>
    [Fact]
    public void TheGuideTail_SaysARowWithoutTheIdlePart_RankedIdleWaits_AndTheHeadIsUntouched()
    {
        var method = typeof(DarlingMcpStallProbeTools)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_collector_stall_probes");
        var (head, tail) = McpToolGuide.Split(method.GetCustomAttribute<DescriptionAttribute>()!.Description);

        Assert.NotNull(tail);
        Assert.Contains("A tail without 'idle K tasks/J types' is from a build that still ranked idle waits.", tail, StringComparison.Ordinal);

        /* The head is byte-identical to dev's: same length (the budget file pins 595 with the schema) and the
           same text, and it carries nothing about idle waits. */
        Assert.StartsWith("Out-of-band DMV samples taken when this service's own collector stalls", head, StringComparison.Ordinal);
        Assert.EndsWith("Unbanded, untrended: one row is evidence about a moment.", head, StringComparison.Ordinal);
        Assert.Equal(564, head.Length);
        Assert.DoesNotContain("idle", head, StringComparison.OrdinalIgnoreCase);
    }

    private static bool TypeInitialiserCallsBuilder()
    {
        var builder = typeof(StallWaitProbePolicy).GetMethod(
            "BuildQueryText",
            BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public);
        Assert.NotNull(builder);

        var il = typeof(StallWaitProbePolicy).TypeInitializer!.GetMethodBody()!.GetILAsByteArray()!;
        var token = BitConverter.GetBytes(builder!.MetadataToken);

        for (var i = 0; i <= il.Length - token.Length; i++)
        {
            if (il.AsSpan(i, token.Length).SequenceEqual(token))
            {
                return true;
            }
        }

        return false;
    }
}
