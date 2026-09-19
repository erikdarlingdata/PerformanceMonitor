/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 (from #3541): the eleven-rule MCP payload contract, the four rules that were sentences rather than
/// tests, and what a census can honestly say about each. Five rules were already pinned when this file was
/// written — the page dialect (<see cref="McpPageContractTests"/>), percent denominators
/// (<c>DarlingMcpPgPercentDenominatorTests</c>), latest-is-a-time (<see cref="McpLatestSnapshotStampTests"/>),
/// zero-is-a-measurement (<c>McpZeroIsAMeasurementTests</c>), filters-in-the-query
/// (<c>McpFilterSemanticsLivePostgresTests</c> and the A13 region of the page census) — and "writes report
/// what happened" is pinned per write tool by #3615 (<c>DarlingMcpServerAdminToolsSurfaceTests</c>:
/// <c>EveryAddStatus_HasExactlyOneSummaryCounter_AndNoCounterIsUnnamed</c>,
/// <c>Aggregate_CountsEveryResultOnce_AndTheCountersSumToRequested</c>). This file takes the remainder:
///
/// <list type="bullet">
/// <item><b>errors one shape</b> — census-able as "every catch returns through a SHARED shape, never an
/// ad-hoc one", and pinned that way; but the shared shapes are TWO (<c>McpHelpers.FormatError</c> is a bare
/// string, <c>McpHelpers.Status("error", …)</c> is a JSON envelope), so the rule itself is NOT met today and
/// the split is pinned as an inventory for the vocabulary lane to collapse.</item>
/// <item><b>refuse what you cannot honor</b> — census-able: every bounded parameter reaches a shared validator
/// (or one of four rostered inline refusals), and no parameter is clamped against a ceiling. The
/// <c>Math.Abs</c> arm is <c>McpPageContractTests.NoTool_ReadsAParameterAsItsAbsoluteValue_OnEitherSku</c>
/// and is not repeated here.</item>
/// <item><b>name = truth</b> — NOT mechanically pinnable in general; two greps approximate it and are pinned
/// as inventories: every <c>total_*</c> assigned from a <c>.Count</c> names a WHOLE set (rostered, each with
/// the set it counts), and every reader anchored on <c>MAX(collection_time)</c> projects the anchor so a
/// stamp is possible (two rostered exceptions). The rest of the rule — does the key say what the number
/// is — is a review-checklist item, and this file says so rather than pretending a regex reads meaning.</item>
/// <item><b>one vocabulary</b> — NOT collapsed here (that is the vocabulary lane's, after the other MCP lanes
/// land); the INVENTORY is pinned instead: every truncation-key dialect, every severity-word spelling, and
/// every statement-terminal literal <c>LIMIT</c>, each as an exact roster that fails when a dialect appears
/// or disappears without the roster moving. The lane that collapses them shrinks the rosters.</item>
/// </list>
///
/// <para>Every fact states the bound it rests on, in the idiom the sibling censuses use: the population it
/// sweeps, the floor that proves the marker still matches, and the roster it compares against. Rosters are
/// EQUALITIES, not subsets — a hit that vanishes without leaving the roster is as red as a new one, because a
/// roster nobody shrinks is a widened exemption (<see cref="McpLatestSnapshotStampTests"/>' rule). Darling's
/// tools are read from source rather than reflected because Lite's must be, and one method for both SKUs is
/// one scope for both.</para>
///
/// <para>Files whose names carry <c>ForcePlan</c> are outside every sweep here by standing decision, not by
/// allowance: the force-plan bot is not this campaign's to census.</para>
/// </summary>
public sealed class McpPayloadContractCensusTests
{
    /* ───────────────────────── the population ───────────────────────── */

    private const string DarlingTools = "Darling/PerformanceMonitor.Darling.Service/Mcp";
    private const string LiteTools = "Lite/Mcp";

    /// <summary>The floor under every both-SKU tool-body sweep: 244 tool methods at the time of writing. A
    /// sweep that examined fewer than this has a marker that stopped matching, and would pass for free.</summary>
    private const int ToolBodyFloor = 150;

    /// <summary>
    /// Every tool method on both SKUs: its file, its name, and its span from the <c>[McpServerTool]</c>
    /// attribute to the member's closing brace. The span ends at the METHOD, not at the next attribute, so a
    /// private helper below the last tool in a file (a wire-shape builder, a parse helper) is not read as
    /// part of a tool — the page census slices to the next attribute, which is right for a payload-key
    /// survey and wrong for a catch survey, where a helper's own catch would be charged to the tool above it.
    /// </summary>
    private static IEnumerable<(string File, string Tool, string Span, string Source)> ToolMethods()
    {
        foreach (var (file, source) in ToolSources())
        {
            var marks = Regex.Matches(source, @"\[McpServerTool\(Name = ""([a-z_0-9]+)""");
            for (var i = 0; i < marks.Count; i++)
            {
                var close = source.IndexOf("\n    }\n", marks[i].Index, StringComparison.Ordinal);
                var end = close < 0 ? source.Length : close + 6;
                yield return (file, marks[i].Groups[1].Value, source[marks[i].Index..end], source);
            }
        }
    }

    private static IEnumerable<(string File, string Source)> ToolSources()
    {
        foreach (var directory in new[] { DarlingTools, LiteTools })
        {
            var root = RepoFile.PathTo(directory);
            foreach (var file in Directory.EnumerateFiles(root, "*.cs").Order(StringComparer.Ordinal))
            {
                var name = Path.GetFileName(file);
                if (name.Contains("ForcePlan", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return (name, File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal));
            }
        }
    }

    /* ───────────────────────── rule: errors one shape ───────────────────────── */

    /// <summary>
    /// The one catch in a tool method whose return is neither shared shape. <c>list_servers</c> returns an
    /// interpolated sentence of its own. Shrink-only: the file is another lane's tonight, and the fix is one
    /// line through either shared helper.
    /// </summary>
    public static readonly (string File, string Tool)[] AdHocErrorReturns =
    [
        ("DarlingMcpDataTools.cs", "list_servers"),
    ];

    /// <summary>
    /// The files whose tool catches return the JSON envelope (<c>McpHelpers.Status("error", …)</c>) rather
    /// than the bare string (<c>McpHelpers.FormatError</c>). Every one is a PostgreSQL tool file plus the
    /// collector-cost tool; every Lite file and every SQL Server-family Darling file uses the bare string.
    /// This is the INVENTORY of the split, pinned at file grain so a new tool in an existing file that keeps
    /// its file's dialect changes nothing, while a new file must declare which side it is on — and the lane
    /// that collapses the two shapes into one empties this roster (or fills it with every file) and says
    /// which shape won.
    /// </summary>
    public static readonly string[] FilesReturningTheJsonErrorEnvelope =
    [
        "DarlingMcpCollectorCostTools.cs",
        "DarlingMcpPgAutovacuumTools.cs",
        "DarlingMcpPgBlockingTools.cs",
        "DarlingMcpPgCpuUtilizationTools.cs",
        "DarlingMcpPgDeadlockTools.cs",
        "DarlingMcpPgIndexTools.cs",
        "DarlingMcpPgIoTools.cs",
        "DarlingMcpPgKernelStatsTools.cs",
        "DarlingMcpPgLogEventTools.cs",
        "DarlingMcpPgLoggingAuditTools.cs",
        "DarlingMcpPgPlanTools.cs",
        "DarlingMcpPgPredicateTools.cs",
        "DarlingMcpPgReplicationStatsTools.cs",
        "DarlingMcpPgServerStateTools.cs",
        "DarlingMcpPgSlotTools.cs",
        "DarlingMcpPgStatementTools.cs",
        "DarlingMcpPgTrendTools.cs",
        "DarlingMcpPgWaitSamplingTools.cs",
        "DarlingMcpPgWaitTools.cs",
        "DarlingMcpPgWraparoundTools.cs",
        "DarlingMcpPgXminTools.cs",
    ];

    /// <summary>
    /// Every <c>return</c> inside a <c>catch</c> in a tool method, on both SKUs, is one of the shared shapes:
    /// the bare-string helper (directly or through <c>Task.FromResult</c> on the synchronous tools), the JSON
    /// envelope, the write tools' <c>Outcome("invalid", …)</c> for a body that would not parse (#3615's
    /// write contract), or a gate computed above the catch (<c>gated</c> / <c>capability</c> /
    /// <c>precondition</c> — a <c>Status</c> envelope already built). Anything else is an ad-hoc error and
    /// must be in <see cref="AdHocErrorReturns"/> exactly.
    ///
    /// <para>Bound: catch blocks are located on the comments-and-strings-stripped text (a description that
    /// says "catch-all rows" is prose, not a keyword), brace-balanced there, and the return expressions read
    /// off the same span of the real source with comments removed. 254 catch returns at the time of writing;
    /// the floor is 200.</para>
    /// </summary>
    [Fact]
    public void EveryToolCatch_ReturnsThroughASharedErrorShape_AndTheAdHocRosterIsExact()
    {
        var returns = 0;
        var adHoc = new List<(string File, string Tool)>();
        var offenders = new List<string>();
        var envelopeFiles = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (file, tool, span, _) in ToolMethods())
        {
            foreach (var expression in CatchReturns(span))
            {
                returns++;
                var shape = ClassifyErrorReturn(expression);
                if (shape == ErrorShape.JsonEnvelope)
                {
                    envelopeFiles.Add(file);
                }

                if (shape == ErrorShape.AdHoc)
                {
                    adHoc.Add((file, tool));
                    offenders.Add($"{file} {tool}: return {expression}");
                }
            }
        }

        Assert.True(returns >= 200, $"only {returns} catch returns were examined across both SKUs; the catch marker has stopped matching");

        Assert.True(
            AdHocErrorReturns.ToHashSet().SetEquals(adHoc.Distinct()),
            "the ad-hoc error roster no longer matches the tree — new ad-hoc: ["
            + string.Join("; ", offenders.Where(o => !AdHocErrorReturns.Any(r => o.StartsWith($"{r.File} {r.Tool}:", StringComparison.Ordinal))))
            + "]; rostered but gone (shrink the roster): ["
            + string.Join(", ", AdHocErrorReturns.Except(adHoc).Select(r => $"{r.File} {r.Tool}"))
            + "]");

        Assert.True(
            FilesReturningTheJsonErrorEnvelope.ToHashSet(StringComparer.Ordinal).SetEquals(envelopeFiles),
            "the JSON-envelope inventory no longer matches the tree — files now returning Status(\"error\") from a tool catch that are not rostered: ["
            + string.Join(", ", envelopeFiles.Except(FilesReturningTheJsonErrorEnvelope).Order())
            + "]; rostered files that no longer do (shrink the roster): ["
            + string.Join(", ", FilesReturningTheJsonErrorEnvelope.Except(envelopeFiles).Order())
            + "]");
    }

    /// <summary>
    /// The two shared shapes are two WIRE shapes, executed: one is a sentence, the other is JSON an agent can
    /// branch on. That difference is the finding the inventory above exists to carry to the vocabulary lane
    /// — 200-odd tools answer an exception with prose and 30-odd with <c>{"status":"error"}</c>, and a client
    /// that keys on <c>status</c> sees the first group's failures as successful text.
    /// </summary>
    [Fact]
    public void TheTwoSharedErrorShapes_AreASentenceAndAnEnvelope()
    {
        var sentence = McpHelpers.FormatError("reading x", new InvalidOperationException("boom"));
        Assert.Equal("Error during reading x: boom", sentence);
        Assert.ThrowsAny<JsonException>(() => JsonDocument.Parse(sentence));

        using var envelope = JsonDocument.Parse(McpHelpers.Status("error", "boom"));
        Assert.Equal("error", envelope.RootElement.GetProperty("status").GetString());
        Assert.Equal("boom", envelope.RootElement.GetProperty("message").GetString());
    }

    /// <summary>The classifier, witnessed against each shape as it appears in the tree.</summary>
    [Theory]
    [InlineData("McpHelpers.FormatError(\"get_x\", ex)", ErrorShape.Sentence)]
    [InlineData("Task.FromResult(McpHelpers.FormatError(\"validate_custom_view\", ex))", ErrorShape.Sentence)]
    [InlineData("McpHelpers.Status(\"error\", $\"Reading PostgreSQL deadlocks failed: {ex.Message}\")", ErrorShape.JsonEnvelope)]
    [InlineData("Outcome(\"invalid\", $\"settings_json is not valid JSON: {ex.Message}\")", ErrorShape.WriteOutcome)]
    [InlineData("gated", ErrorShape.Gate)]
    [InlineData("$\"Could not read the servers registry from the Postgres store: {ex.Message}\"", ErrorShape.AdHoc)]
    [InlineData("JsonSerializer.Serialize(new { error = ex.Message })", ErrorShape.AdHoc)]
    [InlineData("McpHelpers.Status(\"empty\", \"nothing\")", ErrorShape.AdHoc)]
    public void TheErrorShapeClassifier_NamesEachShape(string expression, ErrorShape expected) =>
        Assert.Equal(expected, ClassifyErrorReturn(expression));

    public enum ErrorShape { Sentence, JsonEnvelope, WriteOutcome, Gate, AdHoc }

    private static ErrorShape ClassifyErrorReturn(string expression)
    {
        if (expression.StartsWith("McpHelpers.FormatError(", StringComparison.Ordinal)
            || expression.StartsWith("Task.FromResult(McpHelpers.FormatError(", StringComparison.Ordinal))
        {
            return ErrorShape.Sentence;
        }

        if (expression.StartsWith("McpHelpers.Status(\"error\"", StringComparison.Ordinal))
        {
            return ErrorShape.JsonEnvelope;
        }

        if (expression.StartsWith("Outcome(\"invalid\"", StringComparison.Ordinal))
        {
            return ErrorShape.WriteOutcome;
        }

        if (expression is "gated" or "capability" or "precondition")
        {
            return ErrorShape.Gate;
        }

        return ErrorShape.AdHoc;
    }

    /// <summary>Each <c>return …;</c> expression inside each <c>catch</c> block of <paramref name="span"/>,
    /// whitespace collapsed. Structure is read off the stripped text so a literal cannot open a block.</summary>
    private static IEnumerable<string> CatchReturns(string span)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(span);
        foreach (Match keyword in Regex.Matches(code, @"\bcatch\b\s*(\([^)]*\))?\s*(when\s*\([^)]*\))?\s*\{"))
        {
            var open = keyword.Index + keyword.Length - 1;
            var block = CSharpSourceWalker.BraceBalanced(code, open);
            var body = StripComments(span.Substring(open, block.Length));
            foreach (Match ret in Regex.Matches(body, @"\breturn\s+(.+?);", RegexOptions.Singleline))
            {
                yield return Regex.Replace(ret.Groups[1].Value, @"\s+", " ").Trim();
            }
        }
    }

    /* ───────────────────────── rule: refuse what you cannot honor ───────────────────────── */

    /// <summary>The parameters whose values a tool can fail to honor: a span, a cap, an anchor.</summary>
    private static readonly string[] BoundedParameters = ["hours_back", "limit", "top", "days_back", "as_of"];

    /// <summary>
    /// The tools that refuse a bad <c>days_back</c> INLINE (<c>if (days_back &lt;= 0 || days_back &gt; Max…)
    /// return "Invalid days_back …"</c>) rather than through a <c>McpHelpers</c> validator — because none
    /// exists for a day-grained span. They refuse; they do not clamp. Rostered so the rule "every bounded
    /// parameter reaches the shared vocabulary" is honest about its five exceptions, and so a
    /// <c>ValidateDaysBack</c> that lands later shrinks this to nothing. Three of the five refuse with a bare
    /// sentence rather than an envelope — the errors-one-shape split, one layer down.
    /// </summary>
    public static readonly (string File, string Tool)[] InlineDaysBackRefusals =
    [
        ("DarlingMcpCollectorCostTools.cs", "get_collector_cost"),
        ("DarlingMcpHealthTools.cs", "get_daily_summary_range"),
        ("DarlingMcpStallProbeTools.cs", "get_collector_stall_probes"),
        ("DarlingMcpStoreMetricsTools.cs", "get_store_metrics"),
        ("McpHealthTools.cs", "get_daily_summary_range"),
    ];

    private static readonly Regex SharedValidatorCall = new(
        @"\bMcpHelpers\.(ValidateWindow|ValidateUncappedWindow|ValidateHoursBack|ValidateTop|ResolveAsOf|ParseSummaryDate)\(",
        RegexOptions.Compiled);

    private static readonly Regex InlineDaysBackRefusal = new(
        @"if \(days_back <= 0 \|\| days_back > [\w.]+\)", RegexOptions.Compiled);

    /// <summary>
    /// Every tool method that declares a bounded parameter validates it through the shared validators —
    /// in its own span, or in a same-file private helper the span calls (the nine health-parser tools share
    /// one <c>CollectAsync</c>) — or is one of the four inline <c>days_back</c> refusals, exactly.
    ///
    /// <para>Bound: parameters are read off the method signature (the text before the first member-indent
    /// brace), matched by declared name; a helper counts when the span names it and its own body carries a
    /// validator call. 156 tool methods declare a bounded parameter at the time of writing; the floor is 100.</para>
    /// </summary>
    [Fact]
    public void EveryBoundedParameter_ReachesASharedValidator_OrARosteredInlineRefusal()
    {
        var examined = 0;
        var unvalidated = new List<string>();
        var inlineSeen = new List<(string File, string Tool)>();

        foreach (var (file, tool, span, source) in ToolMethods())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(span);
            var brace = code.IndexOf("\n    {", StringComparison.Ordinal);
            var signature = brace < 0 ? code : code[..brace];
            var declared = BoundedParameters
                .Where(p => Regex.IsMatch(signature, @"\b(int|string\?|int\?)\s+" + p + @"\b"))
                .ToList();
            if (declared.Count == 0)
            {
                continue;
            }

            examined++;
            /* Recorded BEFORE the validator test, because get_daily_summary_range refuses days_back inline AND
               resolves as_of through the shared parser — an early continue would hide the inline site. */
            var inline = InlineDaysBackRefusal.IsMatch(code);
            if (inline)
            {
                inlineSeen.Add((file, tool));
            }

            if (inline || SharedValidatorCall.IsMatch(code) || CallsAValidatingHelper(code, source))
            {
                continue;
            }

            unvalidated.Add($"{file} {tool} ({string.Join(", ", declared)})");
        }

        Assert.True(examined >= 100, $"only {examined} tool methods declare a bounded parameter; the signature marker has stopped matching");
        Assert.True(unvalidated.Count == 0,
            "these tools take a bounded parameter and neither validate it through McpHelpers nor refuse it inline — a value they cannot honor must be refused, never clamped or ignored: "
            + string.Join("; ", unvalidated));
        Assert.True(InlineDaysBackRefusals.ToHashSet().SetEquals(inlineSeen),
            "the inline days_back refusal roster no longer matches the tree: found [" + string.Join(", ", inlineSeen.Select(t => $"{t.File} {t.Tool}"))
            + "], rostered [" + string.Join(", ", InlineDaysBackRefusals.Select(t => $"{t.File} {t.Tool}")) + "]");
    }

    /// <summary>A same-file <c>private static</c> member the span calls whose own body carries a validator call.</summary>
    private static bool CallsAValidatingHelper(string toolCode, string source)
    {
        var fileCode = CSharpSourceWalker.StripCommentsAndStrings(source);
        foreach (Match helper in Regex.Matches(fileCode, @"\n    private static [\w<>\[\]?,. ]+\s+(\w+)(?:<\w+>)?\("))
        {
            var name = helper.Groups[1].Value;
            if (!Regex.IsMatch(toolCode, @"\b" + Regex.Escape(name) + @"\s*(<[^>]+>)?\("))
            {
                continue;
            }

            var close = fileCode.IndexOf("\n    }\n", helper.Index + 1, StringComparison.Ordinal);
            var body = close < 0 ? fileCode[helper.Index..] : fileCode[helper.Index..close];
            if (SharedValidatorCall.IsMatch(body))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// A parameter clamped against a ceiling or a literal — <c>Math.Clamp(limit, 1, MaxTop)</c>,
    /// <c>Math.Min(hours_back, 168)</c>, <c>limit = Math.Max(1, limit)</c> — which answers a request the
    /// tool cannot honor with a quietly different one. NOT <c>Math.Min(rows.Count, limit)</c>: that is the
    /// page's count, a fact about what came back, and it is matched as such in the witness below.
    /// </summary>
    private static readonly Regex ClampOfParameter = new(
        @"\b(?:limit|top|hours_back|days_back|minutes_back)\s*=\s*Math\.(?:Clamp|Min|Max)\("
        + @"|\bMath\.(?:Clamp|Min|Max)\(\s*(?:limit|top|hours_back|days_back|minutes_back)\s*,\s*(?:\d+|[\w.]*Max\w*)\b"
        + @"|\bMath\.(?:Clamp|Min|Max)\(\s*(?:\d+|[\w.]*Max\w*)\s*,\s*(?:limit|top|hours_back|days_back|minutes_back)\b",
        RegexOptions.Compiled);

    /// <summary>
    /// No tool on either SKU clamps a bounded parameter. Zero hits at the time of writing, so this fact is a
    /// negative census and carries its own controls: the population floor, and the witness below that proves
    /// the matcher fires on the defect shapes and not on the page-count idiom that shares its tokens.
    /// </summary>
    [Fact]
    public void NoTool_ClampsABoundedParameter_OnEitherSku()
    {
        var examined = 0;
        var offenders = new List<string>();
        foreach (var (file, tool, span, _) in ToolMethods())
        {
            examined++;
            var hit = ClampOfParameter.Match(CSharpSourceWalker.StripCommentsAndStrings(span));
            if (hit.Success)
            {
                offenders.Add($"{file} {tool}: {hit.Value}");
            }
        }

        Assert.True(examined >= ToolBodyFloor, $"only {examined} tool methods were examined across both SKUs; the marker has stopped matching");
        Assert.True(offenders.Count == 0, "these tools clamp a parameter instead of refusing it: " + string.Join("; ", offenders));
    }

    [Fact]
    public void TheClampDiscriminator_FlagsTheDefectShapes_AndPassesThePageCount()
    {
        Assert.Matches(ClampOfParameter, "            limit = Math.Clamp(limit, 1, McpHelpers.MaxTop);");
        Assert.Matches(ClampOfParameter, "            var hours = Math.Min(hours_back, 168);");
        Assert.Matches(ClampOfParameter, "            var effective = Math.Min(McpHelpers.MaxTop, top);");
        Assert.Matches(ClampOfParameter, "            top = Math.Max(1, top);");
        /* The page's count is a measurement of the page, not a rewrite of the request. */
        Assert.DoesNotMatch(ClampOfParameter, "                shown = Math.Min(rows.Count, limit),");
        Assert.DoesNotMatch(ClampOfParameter, "                withheld = Math.Max(0, databasesTotal - perDatabase.Count),");
        Assert.DoesNotMatch(ClampOfParameter, "                    bucket_label = labels[Math.Clamp(c.BucketIndex, 0, HeatmapBucketCount - 1)],");
    }

    /* ───────────────────────── rule: name = truth — the two greps ───────────────────────── */

    /// <summary>
    /// Every <c>total_*</c> key assigned from a <c>.Count</c> or <c>.Length</c> in a tool body on either SKU,
    /// with the set it counts. Each one is a WHOLE set — an uncapped read, or the collected fact list — so
    /// the name is true; a page count under this prefix is the #3594 defect and belongs in
    /// <see cref="McpPageContractTests.NoPagedTool_PublishesAPageCountAsATotal_OnEitherSku"/>'s population,
    /// not here. The roster is exact so a new <c>total_* = x.Count</c> has to prove its set is whole by
    /// joining it with a reason, and so a renamed one (<c>*_returned</c>) shrinks it.
    /// </summary>
    public static readonly (string File, string Tool, string Assignment, string Set)[] TotalsAssignedFromAWholeSetCount =
    [
        ("DarlingMcpAlertTools.cs", "get_mute_rules", "total_count = list.Count", "every mute rule; the read has no cap"),
        ("DarlingMcpDefaultTraceTools.cs", "get_default_trace_events", "total_events = significant.Count", "every significant event in the window; the read has no cap and the page is Take(limit) beside it as shown"),
        ("DarlingMcpHealthParserTools.cs", "get_health_parser_system_health", "total_entries = c.Rows.Count", "every shredded entry in the window; shredded in C# from every XML in the window, the page is Take(limit) beside it as shown"),
        ("DarlingMcpTools.cs", "get_analysis_facts", "total_facts = facts.Count", "every collected fact; shown is the filtered count beside it"),
        ("DarlingMcpTools.cs", "get_analysis_findings", "total_occurrences = findings.Count", "every retained occurrence of the finding; shown beside it"),
        ("McpAlertTools.cs", "get_mute_rules", "total_count = rules.Count", "every mute rule; the read has no cap"),
        ("McpAnalysisTools.cs", "get_analysis_facts", "total_facts = facts.Count", "every collected fact; shown is the filtered count beside it"),
        ("McpAnalysisTools.cs", "get_analysis_findings", "total_occurrences = findings.Count", "every retained occurrence of the finding; shown beside it"),
        ("McpDefaultTraceTools.cs", "get_default_trace_events", "total_events = rows.Count", "every significant event in the window; the read has no cap and the page is Take(limit) beside it as shown"),
        ("McpHealthParserTools.cs", "get_health_parser_system_health", "total_entries = rows.Count", "every shredded entry in the window; the page is Take(limit) beside it as shown"),
    ];

    /// <summary>A <c>total_*</c> key assigned a collection's count — not a comparison on one
    /// (<c>durations.Count &gt; 0 ? …</c>) and not a predicate count (<c>.Count(r =&gt; …)</c>).</summary>
    private static readonly Regex TotalFromCount = new(
        @"\btotal_\w+\s*=\s*[\w.]+\.(?:Count|Length)\b(?!\s*[><=!(])", RegexOptions.Compiled);

    [Fact]
    public void EveryTotalAssignedFromACount_IsInTheWholeSetInventory()
    {
        var examined = 0;
        var found = new List<(string File, string Tool, string Assignment)>();
        foreach (var (file, tool, span, _) in ToolMethods())
        {
            examined++;
            foreach (Match hit in TotalFromCount.Matches(StripComments(span)))
            {
                found.Add((file, tool, Regex.Replace(hit.Value, @"\s+", " ")));
            }
        }

        var rostered = TotalsAssignedFromAWholeSetCount.Select(t => (t.File, t.Tool, t.Assignment)).ToHashSet();
        Assert.True(examined >= ToolBodyFloor, $"only {examined} tool methods were examined across both SKUs; the marker has stopped matching");
        Assert.True(rostered.SetEquals(found),
            "the total_*-from-Count inventory no longer matches the tree — new (prove the set is whole, or rename *_returned): ["
            + string.Join("; ", found.Except(rostered).Select(f => $"{f.File} {f.Tool}: {f.Assignment}"))
            + "]; rostered but gone (shrink the roster): ["
            + string.Join("; ", rostered.Except(found).Select(f => $"{f.File} {f.Tool}: {f.Assignment}")) + "]");
    }

    [Fact]
    public void TheTotalDiscriminator_FlagsACountUnderATotalName_AndPassesAComparisonAndAPredicate()
    {
        Assert.Matches(TotalFromCount, "                total_events = rows.Count,");
        Assert.Matches(TotalFromCount, "                total_count = list.Count,");
        Assert.DoesNotMatch(TotalFromCount, "            total_duration_ms = durations.Count > 0 ? durations.Sum() : (long?)null,");
        Assert.DoesNotMatch(TotalFromCount, "                total_answered = rows.Count(r => r.SkippedReason is null),");
        Assert.DoesNotMatch(TotalFromCount, "                events_returned = page.Count,");
    }

    /// <summary>
    /// The Darling reader SQL constants whose outer statement anchors on <c>= (SELECT MAX(collection_time |
    /// capture_time) …)</c> — a latest-snapshot read — and do NOT project the anchor column, so the tool above
    /// them cannot stamp the snapshot's time (#3541 A10, "latest is a time"). Two, both known:
    /// <c>IndexUsageSql</c> is the object-stats residual <see cref="McpLatestSnapshotStampTests.UnstampedLatestReadsPendingA10"/>
    /// already holds; <c>IndexUsageMatchCountSql</c> is a <c>COUNT(*)</c> over the same anchor with no row to
    /// stamp. Shrink-only: stamping the trio is the A10 lane's.
    /// </summary>
    public static readonly string[] LatestAnchoredReadsWithoutTheirStamp =
    [
        "DarlingObjectStatsReader.cs IndexUsageMatchCountSql",
        "DarlingObjectStatsReader.cs IndexUsageSql",
    ];

    private static readonly Regex LatestAnchor = new(
        @"=\s*\(\s*SELECT\s+MAX\((?:\w+\.)?(collection_time|capture_time)\)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Every reader constant on both SKUs whose OUTER statement is anchored on the newest capture projects
    /// the anchor column, or is rostered. This is the SQL-side approach to the rule
    /// <see cref="McpLatestSnapshotStampTests"/> pins from the tool side (by reader-method name): a stamp
    /// that is not on the row statement cannot be published without a second read that could stamp the
    /// next capture.
    ///
    /// <para>Bound: constants are <c>const string</c> / <c>static readonly string</c> raw or verbatim literals
    /// in <c>Darling/PerformanceMonitor.Darling.Storage</c>, the Darling service's <c>Mcp/*Reader*.cs</c>, and
    /// <c>Lite/Services/LocalDataService*.cs</c>. Only an anchor at parenthesis depth zero of the constant
    /// counts as the statement's own — an anchor inside a CTE or a subquery picks a SET (the PVS trend's
    /// top five, the store log's newest capture in a summary) and the rows it feeds are not a snapshot. The
    /// anchor's owning <c>SELECT</c> list is the last depth-zero <c>SELECT</c> before it up to the next
    /// depth-zero <c>FROM</c>. 15 anchored constants at the time of writing; the floor is 10.</para>
    /// </summary>
    [Fact]
    public void EveryLatestAnchoredRead_ProjectsItsAnchorColumn_OrIsRostered()
    {
        var anchored = 0;
        var unstamped = new List<string>();

        foreach (var (file, name, sql) in ReaderSqlConstants())
        {
            var clean = Regex.Replace(Regex.Replace(sql, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"--[^\n]*", string.Empty);
            var anchor = LatestAnchor.Match(clean);
            if (!anchor.Success || Depth(clean, anchor.Index) != 0)
            {
                continue;
            }

            anchored++;
            var column = anchor.Groups[1].Value.ToLowerInvariant();
            var selectList = OwningSelectList(clean, anchor.Index);
            if (selectList is null || !Regex.IsMatch(selectList, @"\b" + column + @"\b", RegexOptions.IgnoreCase))
            {
                unstamped.Add($"{file} {name}");
            }
        }

        Assert.True(anchored >= 10, $"only {anchored} latest-anchored reader constants were found; the anchor pattern has stopped matching");
        Assert.True(LatestAnchoredReadsWithoutTheirStamp.ToHashSet(StringComparer.Ordinal).SetEquals(unstamped),
            "the unstamped latest-read roster no longer matches the tree — new (project the anchor column so the tool can stamp): ["
            + string.Join(", ", unstamped.Except(LatestAnchoredReadsWithoutTheirStamp).Order())
            + "]; rostered but now stamped (shrink the roster): ["
            + string.Join(", ", LatestAnchoredReadsWithoutTheirStamp.Except(unstamped).Order()) + "]");
    }

    [Fact]
    public void TheAnchorWalker_FindsTheOwningSelectList_AndIgnoresASubqueryAnchor()
    {
        const string Stamped = """
            SELECT collection_time, name, setting
            FROM pg_server_config AS c
            WHERE c.server_id = $1
            AND   c.collection_time = (SELECT MAX(collection_time) FROM pg_server_config WHERE server_id = $1)
            """;
        var anchor = LatestAnchor.Match(Stamped);
        Assert.True(anchor.Success);
        Assert.Equal(0, Depth(Stamped, anchor.Index));
        Assert.Contains("collection_time, name, setting", OwningSelectList(Stamped, anchor.Index), StringComparison.Ordinal);

        const string Unstamped = """
            SELECT name, setting
            FROM pg_server_config
            WHERE collection_time = (SELECT MAX(collection_time) FROM pg_server_config WHERE server_id = $1)
            """;
        var list = OwningSelectList(Unstamped, LatestAnchor.Match(Unstamped).Index)!;
        Assert.DoesNotMatch(new Regex(@"\bcollection_time\b"), list);

        const string Subquery = """
            SELECT collection_time, database_name, pvs_size_mb
            FROM v_pvs_stats
            WHERE database_name IN (
                SELECT database_name FROM v_pvs_stats
                WHERE collection_time = (SELECT MAX(collection_time) FROM v_pvs_stats WHERE server_id = $1)
                LIMIT 5)
            """;
        Assert.Equal(1, Depth(Subquery, LatestAnchor.Match(Subquery).Index));
    }

    /* ───────────────────────── rule: refuse — the timestamp arm (non-Exact TryParse) ───────────────────────── */

    /// <summary>
    /// The general (non-<c>Exact</c>) <c>DateTime.TryParse</c> calls left in either SKU's tool sources. Both
    /// carry <c>InvariantCulture</c> and <c>AdjustToUniversal | AssumeUniversal</c>, so they read a UTC
    /// instant deterministically — the #3641 defect was a bare <c>TryParse</c> under the host culture — but
    /// they accept every shape the framework parser does rather than the <c>AsOfFormats</c> allowlist
    /// <c>McpHelpers.ResolveAsOf</c> refuses against. Shrink-only for the vocabulary lane: the file is another
    /// lane's tonight, and the fix is the allowlist.
    /// </summary>
    public static readonly string[] GeneralTimestampParsesWithUtcStyles =
    [
        "DarlingMcpAlertTools.cs BuildMuteRuleUpdate",
        "DarlingMcpAlertTools.cs CreateMuteRule",
    ];

    private static readonly Regex GeneralTimestampParse = new(
        @"\bDateTime(?:Offset)?\.(?:Try)?Parse\(", RegexOptions.Compiled);

    /// <summary>
    /// No tool source on either SKU parses a timestamp with the general parser under the host culture, and
    /// the general parses that DO carry the UTC styles are exactly the roster. Bound: whole tool files
    /// (parse helpers live below the tools), strings and comments stripped for the call, the call's own
    /// argument list read for the styles; the site is named by the enclosing tool or member.
    /// </summary>
    [Fact]
    public void NoToolParsesATimestampUnderTheHostCulture_AndTheGeneralParseRosterIsExact()
    {
        var found = new List<string>();
        var offenders = new List<string>();
        var files = 0;

        foreach (var (file, source) in ToolSources())
        {
            files++;
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);
            foreach (Match call in GeneralTimestampParse.Matches(code))
            {
                var arguments = CSharpSourceWalker.BraceBalanced(code.Replace('(', '{').Replace(')', '}'), call.Index + call.Length - 1);
                var site = $"{file} {EnclosingMember(code, call.Index)}";
                found.Add(site);
                if (!arguments.Contains("DateTimeStyles.AdjustToUniversal", StringComparison.Ordinal)
                    || !arguments.Contains("DateTimeStyles.AssumeUniversal", StringComparison.Ordinal)
                    || !arguments.Contains("CultureInfo.InvariantCulture", StringComparison.Ordinal))
                {
                    offenders.Add(site);
                }
            }
        }

        Assert.True(files >= 60, $"only {files} tool files were read across both SKUs");
        Assert.True(offenders.Count == 0,
            "these sites parse a timestamp under the host culture or without the UTC styles — 01/02/2026 reads as two different days on two hosts: " + string.Join("; ", offenders));
        Assert.True(GeneralTimestampParsesWithUtcStyles.ToHashSet(StringComparer.Ordinal).SetEquals(found),
            "the general-TryParse roster no longer matches the tree: found [" + string.Join(", ", found.Order())
            + "], rostered [" + string.Join(", ", GeneralTimestampParsesWithUtcStyles) + "]");
    }

    /// <summary>The shared parsers are the allowlisted shape: <c>ResolveAsOf</c> refuses what the framework
    /// parser would accept, which is the whole point of an allowlist.</summary>
    [Fact]
    public void TheSharedTimestampParsers_RefuseWhatTheGeneralParserAccepts()
    {
        Assert.Null(McpHelpers.ResolveAsOf("2026-08-18T14:30:00Z", out var accepted));
        Assert.Equal(new DateTime(2026, 8, 18, 14, 30, 0, DateTimeKind.Utc), accepted);
        Assert.NotNull(McpHelpers.ResolveAsOf("08/18/2026 14:30", out _));
        Assert.NotNull(McpHelpers.ResolveAsOf("18 Aug 2026", out _));
        Assert.True(DateTime.TryParse("08/18/2026 14:30", CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out _),
            "the control: the general parser under invariant UTC styles accepts the shape the allowlist refuses — that acceptance is what the roster above inventories");
    }

    /* ───────────────────────── rule: one vocabulary — the inventories ───────────────────────── */

    /// <summary>
    /// Every payload key on either SKU that speaks about a cut, by name. Fourteen spellings. Some are
    /// different FACTS and not dialects of one — <c>query_text_may_be_truncated</c> is about text cut at
    /// collection, <c>capture_was_truncated</c> about the collector's per-capture cap, <c>scan_truncated</c>
    /// about a fingerprint scan ceiling, <c>*_withheld</c> about a reach verdict — and the vocabulary lane
    /// decides which are which. The page-cut dialects proper are <c>truncated</c> (the #3594 spelling),
    /// <c>limit_reached</c> (five PostgreSQL tools; true by construction under <c>&gt;= limit</c>, so weaker
    /// than an observation), <c>is_partial</c>, <c>partial_count</c>, <c>history_capped</c>, and the
    /// <c>shown</c>-beside-a-total pair that carries no flag at all. Pinned at KEY grain: another tool joining
    /// an existing dialect changes nothing here; a fifteenth spelling, or the collapse of one, moves the roster.
    /// Swept over whole tool sources rather than tool-method spans, because five of the fourteen are emitted
    /// from wire-shape builders below the tools (<c>get_pg_blocking</c>'s chain keys, the autovacuum tool's
    /// <c>history_capped</c> — itself a <c>runs.Count &gt;= RunsReadPerTable</c> against a collector constant,
    /// the <c>limit_reached</c> shape one layer down — and the logging audit's <c>partial_count</c>).
    /// </summary>
    public static readonly string[] TruncationKeyDialects =
    [
        "answered_rows_withheld",
        "capture_was_truncated",
        "chain_may_be_truncated",
        "chain_truncation_note",
        "created_rows_withheld",
        "history_capped",
        "is_partial",
        "limit_reached",
        "partial_count",
        "query_text_may_be_truncated",
        "scan_truncated",
        "shown",
        "truncated",
        "truncation_note",
    ];

    /// <summary>A payload key at initializer indent: <c>name = …</c>, or the shorthand <c>name,</c> alone on
    /// its line. Keys only — the value side is not read — so a local named <c>truncated</c> assigned in the
    /// body is counted once, where it is emitted.</summary>
    private static readonly Regex PayloadKey = new(@"^\s+([a-z][a-z0-9_]*)\s*(?:=(?!=)|,\s*$)", RegexOptions.Compiled | RegexOptions.Multiline);

    private static readonly Regex AboutACut = new(
        @"trunc|^limit_reached$|^is_partial$|^partial_count$|^partial$|_capped$|^shown$|^more_available$|^has_more$|_withheld$|^omitted$",
        RegexOptions.Compiled);

    [Fact]
    public void TheTruncationKeyInventory_IsExact_OnBothSkus()
    {
        var files = 0;
        var found = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);
        foreach (var (file, source) in ToolSources())
        {
            files++;
            foreach (Match key in PayloadKey.Matches(StripComments(source)))
            {
                var name = key.Groups[1].Value;
                if (!AboutACut.IsMatch(name))
                {
                    continue;
                }

                if (!found.TryGetValue(name, out var sites))
                {
                    found[name] = sites = new SortedSet<string>(StringComparer.Ordinal);
                }

                sites.Add(file);
            }
        }

        Assert.True(files >= 60, $"only {files} tool sources were read across both SKUs");
        /* The spelling every other census keys on must be in the inventory on most tool files, or the key
           matcher is dead. */
        Assert.True(found.TryGetValue("truncated", out var truncated) && truncated.Count >= 25, "the key matcher no longer finds `truncated` across twenty-odd tool files");
        Assert.True(TruncationKeyDialects.ToHashSet(StringComparer.Ordinal).SetEquals(found.Keys),
            "the truncation-key inventory no longer matches the tree — new dialect(s): ["
            + string.Join("; ", found.Keys.Except(TruncationKeyDialects).Order().Select(k => $"{k} on {string.Join(", ", found[k])}"))
            + "]; rostered but gone (the collapse landed — shrink the roster): ["
            + string.Join(", ", TruncationKeyDialects.Except(found.Keys).Order()) + "]");
    }

    /// <summary>
    /// Every spelling of a severity word that appears as a whole string literal in a source under either
    /// SKU's Mcp directory (tools and the Darling service's own readers alike), with the files that spell
    /// it. Three casings: Pascal (<c>Healthy</c>/<c>Warning</c>/<c>Critical</c>/<c>Unknown</c> — the
    /// <c>HealthSeverity</c> vocabulary written out by the three PostgreSQL object tools and the AG reader's
    /// label switch), lower (<c>warning</c> is <c>audit_config</c>'s recommendation status on both SKUs and
    /// the wraparound tool's band; <c>unknown</c> is a wait-instrument token, the logging audit's verdict
    /// constant and Lite's version fallback), and UPPER (<c>CRITICAL</c> is the plan-cache reader's band;
    /// <c>HEALTHY</c> is SQL Server's own <c>synchronization_health_desc</c> token, parsed on the way IN by
    /// the AG and fleet readers rather than emitted). Not all are severities and not all are outputs, but
    /// they are the same words on the same surface, which is what a vocabulary lane has to know before it
    /// collapses anything. Enum vocabularies that reach the wire through row types (<c>DailyHealthBand</c>,
    /// <c>HealthSeverity</c>) are not literals here and are pinned by their named instance below rather than
    /// swept — a sweep over <c>.ToString()</c> would have to resolve types.
    /// </summary>
    public static readonly (string Spelling, string[] Files)[] SeverityWordSpellings =
    [
        ("CRITICAL", ["DarlingPlanCacheSchedulerReader.cs"]),
        ("Critical", ["DarlingAgReader.cs", "DarlingMcpPgIndexUsageTools.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs"]),
        ("HEALTHY", ["DarlingAgReader.cs", "DarlingFleetReader.cs"]),
        ("Healthy", ["DarlingAgReader.cs", "DarlingMcpPgIndexUsageTools.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs"]),
        ("Unknown", ["DarlingAgReader.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs", "DarlingMcpTools.cs", "McpAnalysisTools.cs"]),
        ("Warning", ["DarlingAgReader.cs", "DarlingMcpPgIndexUsageTools.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs"]),
        ("unknown", ["DarlingMcpPgWaitSamplingTools.cs", "DarlingPgLoggingAudit.cs", "McpHealthTools.cs"]),
        ("warning", ["DarlingMcpPgWraparoundTools.cs", "DarlingMcpTools.cs", "McpAnalysisTools.cs"]),
    ];

    private static readonly Regex SeverityWord = new(
        @"^(?:healthy|warning|critical|unknown|nodata|no data|info|elevated)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    [Fact]
    public void TheSeverityWordInventory_IsExact_OnBothSkus()
    {
        var found = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);
        var literals = 0;
        foreach (var (file, source) in ToolSources())
        {
            foreach (var (_, text) in CSharpSourceWalker.StringLiteralBodies(source))
            {
                literals++;
                if (!SeverityWord.IsMatch(text))
                {
                    continue;
                }

                if (!found.TryGetValue(text, out var files))
                {
                    found[text] = files = new SortedSet<string>(StringComparer.Ordinal);
                }

                files.Add(file);
            }
        }

        Assert.True(literals >= 5000, $"only {literals} string literals were read across both SKUs' tool sources; the literal walker has stopped matching");
        var expected = SeverityWordSpellings.ToDictionary(s => s.Spelling, s => s.Files.Order(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal);
        var actual = found.ToDictionary(f => f.Key, f => f.Value.ToArray(), StringComparer.Ordinal);
        Assert.True(
            expected.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(actual.Keys)
            && expected.All(e => e.Value.SequenceEqual(actual[e.Key], StringComparer.Ordinal)),
            "the severity-word inventory no longer matches the tree: found ["
            + string.Join("; ", actual.OrderBy(a => a.Key, StringComparer.Ordinal).Select(a => $"{a.Key} in {string.Join(", ", a.Value)}"))
            + "]");
    }

    /// <summary>
    /// The named instance behind "NoData and No Data in one payload": <c>get_daily_summary</c> on both SKUs
    /// publishes <c>health_band = HealthBand.ToString()</c> (<c>NoData</c>) beside <c>overall_health =
    /// DailyHealthBandCalculator.Label(…)</c> (<c>No Data</c>) — the same band under two spellings in one
    /// object. Pinned as the mechanism, executed against the shared calculator, so the lane that collapses
    /// the pair has the two sites and the two spellings by name and this fact to delete.
    /// </summary>
    [Fact]
    public void TheDailySummary_CarriesTheSameBandUnderTwoSpellings_OnBothSkus()
    {
        Assert.Equal("NoData", DailyHealthBand.NoData.ToString());
        Assert.Equal("No Data", DailyHealthBandCalculator.Label(DailyHealthBand.NoData));
        Assert.Equal("Healthy", DailyHealthBandCalculator.Label(DailyHealthBand.Healthy));

        foreach (var (file, tool) in new[] { ("DarlingMcpHealthTools.cs", "get_daily_summary"), ("McpHealthTools.cs", "get_daily_summary") })
        {
            var span = ToolMethods().Single(t => t.File == file && t.Tool == tool).Span;
            var code = StripComments(span);
            Assert.Contains("health_band = row.HealthBand.ToString()", code, StringComparison.Ordinal);
            Assert.Contains("overall_health = row.OverallHealth", code, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Every literal <c>LIMIT n</c> (<c>n &gt; 1</c>) that ENDS a reader statement on either SKU — the shape
    /// #3541 A3 and #3659 found behind tools that advertised <c>limit</c>. Seven remain, all in Lite's
    /// service layer, none behind a tool that takes a <c>limit</c>: six are viewer-only reads and one
    /// (<c>GetPlanCacheSnapshotAsync</c>, behind <c>get_plan_cache_bloat</c>, which takes no cap) is a
    /// ceiling of 30 over a population of a dozen cache types. <c>LIMIT 1</c> is the latest-row idiom and is
    /// not a page. Pinned so a new terminal literal has to say what it is, and so the Lite twin of
    /// <see cref="McpPageContractTests.EveryPagedRead_BindsItsCapAsAParameter_NeverALiteral"/> has a
    /// population to shrink.
    /// </summary>
    public static readonly (string File, string Member, int Limit)[] StatementTerminalLiteralLimits =
    [
        ("LocalDataService.Blocking.cs", "GetBlockingPairRowsAsync", 5000),
        ("LocalDataService.FinOps.Recommendations.cs", "GetRecommendationsAsync", 10),
        ("LocalDataService.LongQueries.cs", "GetRecentLongQueryCompletionsAsync", 200),
        ("LocalDataService.PlanCache.cs", "GetPlanCacheSnapshotAsync", 30),
        ("LocalDataService.RunningJobs.cs", "GetAnomalousJobsAsync", 5),
        ("LocalDataService.WaitStats.cs", "GetAllQuerySnapshotsInRangeAsync", 2000),
        ("LocalDataService.WaitStats.cs", "GetQuerySnapshotsByWaitTypeAsync", 500),
    ];

    private static readonly Regex TerminalLiteralLimit = new(
        @"\bLIMIT\s+(\d+)\s*;?\s*(?:""""""|""\s*;|""\s*\)|""\s*,)", RegexOptions.Compiled);

    [Fact]
    public void EveryStatementTerminalLiteralLimit_IsInTheInventory_OnBothSkus()
    {
        var found = new List<(string File, string Member, int Limit)>();
        var files = 0;
        foreach (var (file, source) in ReaderSources())
        {
            files++;
            var code = CSharpSourceWalker.StripCommentsAndStrings(source);
            foreach (Match hit in TerminalLiteralLimit.Matches(source))
            {
                var n = int.Parse(hit.Groups[1].Value, CultureInfo.InvariantCulture);
                if (n <= 1)
                {
                    continue;
                }

                found.Add((file, EnclosingMember(code, hit.Index), n));
            }
        }

        Assert.True(files >= 60, $"only {files} reader sources were read across both SKUs");
        var rostered = StatementTerminalLiteralLimits.ToHashSet();
        Assert.True(rostered.SetEquals(found),
            "the terminal literal-LIMIT inventory no longer matches the tree — new (bind the cap, or say what the ceiling is): ["
            + string.Join("; ", found.Except(rostered).Select(f => $"{f.File} {f.Member} LIMIT {f.Limit}"))
            + "]; rostered but gone (shrink the roster): ["
            + string.Join("; ", rostered.Except(found).Select(f => $"{f.File} {f.Member} LIMIT {f.Limit}")) + "]");
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    /// <summary>Reader SQL sources on both SKUs: the Darling storage readers, the Darling service's own readers,
    /// and Lite's service layer. <c>ForcePlan</c> files excluded by standing decision.</summary>
    private static IEnumerable<(string File, string Source)> ReaderSources()
    {
        foreach (var (directory, pattern) in new[]
        {
            ("Darling/PerformanceMonitor.Darling.Storage", "*.cs"),
            ("Darling/PerformanceMonitor.Darling.Service/Mcp", "*Reader*.cs"),
            ("Lite/Services", "LocalDataService*.cs"),
        })
        {
            var root = RepoFile.PathTo(directory);
            foreach (var file in Directory.EnumerateFiles(root, pattern).Order(StringComparer.Ordinal))
            {
                var name = Path.GetFileName(file);
                if (name.Contains("ForcePlan", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return (name, File.ReadAllText(file).Replace("\r\n", "\n", StringComparison.Ordinal));
            }
        }
    }

    /// <summary>Every named string constant holding SQL in the reader sources: raw (<c>"""</c>) or verbatim
    /// (<c>@"</c>) literals declared <c>const string</c> or <c>static readonly string</c>.</summary>
    private static IEnumerable<(string File, string Name, string Sql)> ReaderSqlConstants()
    {
        var declaration = new Regex(
            @"(?:public|internal|private)\s+(?:static\s+readonly\s+string|const\s+string)\s+(\w+)\s*=\s*(?:\$?""""""(.*?)""""""|@""((?:[^""]|"""")*)"")\s*;",
            RegexOptions.Singleline);
        foreach (var (file, source) in ReaderSources())
        {
            foreach (Match m in declaration.Matches(source))
            {
                yield return (file, m.Groups[1].Value, m.Groups[2].Success ? m.Groups[2].Value : m.Groups[3].Value);
            }
        }
    }

    /// <summary>Parenthesis depth of <paramref name="position"/> within <paramref name="sql"/>.</summary>
    private static int Depth(string sql, int position)
    {
        var depth = 0;
        for (var i = 0; i < position; i++)
        {
            if (sql[i] == '(') depth++;
            else if (sql[i] == ')') depth--;
        }

        return depth;
    }

    /// <summary>The select list owning the anchor at <paramref name="anchor"/>: from the last depth-zero
    /// <c>SELECT</c> before it to the next depth-zero <c>FROM</c>; null when no depth-zero SELECT precedes it.</summary>
    private static string? OwningSelectList(string sql, int anchor)
    {
        var tokens = Regex.Matches(sql, @"\(|\)|\bSELECT\b|\bFROM\b", RegexOptions.IgnoreCase);
        var depth = 0;
        var select = -1;
        foreach (Match t in tokens)
        {
            if (t.Index >= anchor) break;
            if (t.Value == "(") depth++;
            else if (t.Value == ")") depth--;
            else if (depth == 0 && t.Value.Equals("SELECT", StringComparison.OrdinalIgnoreCase)) select = t.Index;
        }

        if (select < 0) return null;
        depth = 0;
        foreach (Match t in tokens)
        {
            if (t.Index <= select) continue;
            if (t.Value == "(") depth++;
            else if (t.Value == ")") depth--;
            else if (depth == 0 && t.Value.Equals("FROM", StringComparison.OrdinalIgnoreCase)) return sql[select..t.Index];
        }

        return sql[select..];
    }

    /// <summary>The tool name or member name enclosing <paramref name="position"/> in stripped code: the last
    /// <c>[McpServerTool(Name = "…")]</c> marker or member-indent declaration before it.</summary>
    private static string EnclosingMember(string code, int position)
    {
        var best = "(file scope)";
        var bestIndex = -1;
        foreach (Match m in Regex.Matches(code, @"\n    (?:public|internal|private)\s+(?:static\s+)?(?:async\s+)?[\w<>\[\]?,. ()]+?\s+(\w+)\s*\("))
        {
            if (m.Index < position && m.Index > bestIndex)
            {
                bestIndex = m.Index;
                best = m.Groups[1].Value;
            }
        }

        return best;
    }

    /// <summary>Comments removed, string literals kept — the page census's <c>Strip</c>.</summary>
    private static string StripComments(string source) =>
        Regex.Replace(Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);
}
