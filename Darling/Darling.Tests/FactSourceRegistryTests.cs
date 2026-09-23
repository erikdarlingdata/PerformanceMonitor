/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3541 A13: <c>get_analysis_facts</c>' <c>source</c> filter documented four of the engine's fifteen sources
/// and applied an unknown one as an equality filter, so a caller who typed any of the other eleven read
/// <c>[]</c> as "no facts of that kind" for a value that could never have matched. The accepted set is now
/// <see cref="FactScorer.KnownSources"/>, published in both SKUs' descriptions and enforced by refusal.
///
/// <para>A registry is only the truth if nothing can emit a source it does not list, so the register is
/// pinned three ways: against every <c>Source = "..."</c> literal in the three fact-collector assemblies
/// (exact set equality — a new source that lands in a collector without landing here fails HERE, not in an
/// agent's empty result), against the scorer's own switch arms (a subset — two sources carry context and
/// are deliberately not scored), and against the two tools' descriptions and refusals.</para>
/// </summary>
public sealed class FactSourceRegistryTests
{
    /// <summary>A source literal wherever it is stamped: <c>Source = "waits"</c> on a fact, or the one named
    /// constant (<c>AnalysisContext.FactSource = "coverage"</c>) the coverage fact is stamped from.</summary>
    private static readonly Regex SourceLiteral = new(@"Source\s*=\s*""([a-z_]+)""", RegexOptions.Compiled);

    /// <summary>The three assemblies whose collectors stamp <c>Fact.Source</c>. The frozen Dashboard is not
    /// swept: it is on bug-fix support and its collectors are a copy of Lite's.</summary>
    private static readonly string[] CollectorDirectories =
    {
        "PerformanceMonitor.Analysis",
        "Lite/Analysis",
        "Darling/PerformanceMonitor.Darling.Analysis",
    };

    [Fact]
    public void TheRegistry_IsExactlyTheSourcesTheCollectorsEmit()
    {
        var emitted = new SortedSet<string>(StringComparer.Ordinal);
        var filesSeen = 0;
        foreach (var directory in CollectorDirectories)
        {
            var root = RepoFile.PathTo(directory);
            foreach (var file in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
            {
                filesSeen++;
                foreach (Match m in SourceLiteral.Matches(File.ReadAllText(file)))
                    emitted.Add(m.Groups[1].Value);
            }
        }

        Assert.True(filesSeen >= 30, $"only {filesSeen} collector sources were swept; the directories have moved");
        Assert.Equal(emitted.ToArray(), FactScorer.KnownSources.ToArray());
    }

    [Fact]
    public void TheRegistry_IsSorted_AndLowercaseSnakeCase()
    {
        Assert.Equal(FactScorer.KnownSources.Order(StringComparer.Ordinal).ToArray(), FactScorer.KnownSources.ToArray());
        Assert.Equal(FactScorer.KnownSources.Distinct(StringComparer.Ordinal).Count(), FactScorer.KnownSources.Count);
        Assert.All(FactScorer.KnownSources, s => Assert.Matches("^[a-z_]+$", s));
        /* The count the campaign wrote down was fourteen; coverage (#3538) made fifteen; the eleven pg_
           sources of the PostgreSQL-target vocabulary (#3542 D2) made twenty-six; the three v2 families (#3691:
           pg_io, pg_replication, pg_bloat) made twenty-nine; the wave-3 blocking family's stub source (#3691
           between waves: pg_blocking) made thirty; the three v3 families' stub sources (#3691 v3 plumbing:
           pg_plans, pg_kernel, pg_memory) made thirty-three; lane 38's object-growth source (pg_growth, declared with its
           family — no stub) made thirty-four. A moved count is a moved contract, and the description on both SKUs spells
           the list out. */
        Assert.Equal(34, FactScorer.KnownSources.Count);

        /* And the pg_ members are exactly PgTargetSources.All — declared once, registered once. */
        Assert.Equal(
            PgTargetSources.All.ToArray(),
            FactScorer.KnownSources.Where(PgTargetSources.IsPgSource).ToArray());
    }

    /// <summary>Every source the scorer's Layer-1 switch scores is registered. The reverse is deliberately
    /// NOT asserted: <c>coverage</c> and <c>sessions</c> are emitted as context with base severity 0.</summary>
    [Fact]
    public void EveryScoredSource_IsRegistered()
    {
        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "FactScorer.cs");
        var switchStart = scorer.IndexOf("fact.BaseSeverity = fact.Source switch", StringComparison.Ordinal);
        Assert.True(switchStart >= 0, "the scorer's source switch has moved");
        var switchEnd = scorer.IndexOf("};", switchStart, StringComparison.Ordinal);
        var arms = Regex.Matches(scorer[switchStart..switchEnd], @"""([a-z_]+)""\s*=>").Select(m => m.Groups[1].Value).ToArray();
        Assert.True(arms.Length >= 10, "the switch-arm scan found too few arms");
        Assert.All(arms, arm => Assert.Contains(arm, FactScorer.KnownSources));
    }

    /// <summary>
    /// Both SKUs' <c>source</c> descriptions spell out the registry verbatim, in its order — an attribute
    /// argument must be a constant, so the pin is what ties the constant to the list.
    /// </summary>
    [Fact]
    public void BothDescriptions_SpellOutTheRegistry()
    {
        var expected = string.Join(", ", FactScorer.KnownSources);

        /* #3898: get_analysis_facts converted, and D2 caps a converted tool's parameter descriptions at 200
           characters, so source's own description no longer spells out the registry — it now states the
           guardrail (an unrecognized value is refused) and points at the full list, which rides on the tool's
           own tail (get_tool_guide) as an appended sentence instead of being lost. Re-pointed here: the
           refusal guardrail stays on the parameter description, the full list moves to the tail pin. */
        var darlingParam = typeof(DarlingMcpTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_analysis_facts")
            .GetParameters().Single(p => p.Name == "source")
            .GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("refused", darlingParam, StringComparison.Ordinal);

        var darlingTail = McpToolGuideTests.Served("get_analysis_facts").Tail!;
        Assert.Contains(expected, darlingTail, StringComparison.Ordinal);

        /* Lite's, from source: this project does not reference the desktop app. The full list still appears
           somewhere in the file (now in the tool's tail rather than the parameter), so this coarser
           whole-file check needs no change. */
        var lite = RepoFile.ReadRepoFile("Lite", "Mcp", "McpAnalysisTools.cs");
        Assert.Contains(expected, lite, StringComparison.Ordinal);
        Assert.Contains("FactSourceFilterDescription", lite, StringComparison.Ordinal);
        Assert.Contains("McpHelpers.ValidateChoice(source, FactScorer.KnownSources, \"source\")", lite, StringComparison.Ordinal);
        Assert.DoesNotContain("Filter to a specific source category: waits, blocking, config, memory", lite, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnknownSource_IsRefusedWithTheWholeSet_AndAKnownOneInAnyCasePasses()
    {
        var refusal = McpHelpers.ValidateChoice("perfmon", FactScorer.KnownSources, "source");
        Assert.NotNull(refusal);
        Assert.StartsWith("Invalid source value 'perfmon'", McpHelpers.ErrorMessageOf(refusal!), StringComparison.Ordinal);
        Assert.Contains(string.Join(", ", FactScorer.KnownSources), refusal, StringComparison.Ordinal);

        Assert.Null(McpHelpers.ValidateChoice("waits", FactScorer.KnownSources, "source"));
        Assert.Null(McpHelpers.ValidateChoice("Bad_Actor", FactScorer.KnownSources, "source"));
        Assert.Null(McpHelpers.ValidateChoice(null, FactScorer.KnownSources, "source"));
        Assert.Null(McpHelpers.ValidateChoice("  ", FactScorer.KnownSources, "source"));
    }
}
