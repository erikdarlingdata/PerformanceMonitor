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
using PerformanceMonitor.Darling.Service.Mcp;
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
/// ad-hoc one", and pinned that way (the one ad-hoc site, <c>list_servers</c>, went through <c>FormatError</c>
/// in #3653's vocabulary lane, so the ad-hoc roster is empty); but the shared shapes are TWO
/// (<c>McpHelpers.FormatError</c> is a bare string, <c>McpHelpers.Status("error", …)</c> is a JSON envelope),
/// so the rule itself is NOT met today. Collapsing them is one helper change with a fleet-wide wire effect
/// (~212 tools' failures become an envelope) and is held for the maintainer's ruling (#3653 Q11); the split
/// stays pinned as a file-grain inventory until then.</item>
/// <item><b>refuse what you cannot honor</b> — census-able: every bounded parameter reaches a shared validator
/// (<c>McpHelpers.ValidateDaysBack</c> now covers the day-grained span, so the inline-refusal roster is
/// empty), and no parameter is clamped against a ceiling. The <c>Math.Abs</c> arm is
/// <c>McpPageContractTests.NoTool_ReadsAParameterAsItsAbsoluteValue_OnEitherSku</c> and is not repeated here.</item>
/// <item><b>name = truth</b> — NOT mechanically pinnable in general; two greps approximate it and are pinned
/// as inventories: every <c>total_*</c> assigned from a <c>.Count</c> names a WHOLE set (rostered, each with
/// the set it counts), and every reader anchored on <c>MAX(collection_time)</c> projects the anchor so a
/// stamp is possible (two rostered exceptions). The rest of the rule — does the key say what the number
/// is — is a review-checklist item, and this file says so rather than pretending a regex reads meaning.</item>
/// <item><b>one vocabulary</b> — collapsed and CLASSIFIED (#3653 A15/A16, the vocabulary lane): the page cut
/// has one spelling (<c>truncated</c>, observed) and the keys that are not page cuts are rostered by class
/// with the reason each keeps its name (a second bound, a source-side cut, the withheld summary, a homonym);
/// the band canon is one spelling (PascalCase) and every other severity-word literal is rostered with the
/// vocabulary it actually belongs to; and every statement-terminal literal <c>LIMIT</c> is inventoried. Each
/// roster is an equality that fails when a spelling appears or disappears without the roster moving.
/// <b>Residue, stated rather than hidden:</b> <c>truncated</c> carries two facts — the page cut, and on the #2364
/// trend family and <c>get_query_store_top</c> the WINDOW floor (the store's retention did not reach the whole
/// window) — and separating them is a rename across a file another lane holds; the PostgreSQL severity-token
/// ladder (<c>ok / info_* / warning[_*] / critical_*</c>) is a different vocabulary from the band, and folding
/// it in is a reshape (band + reason) for the maintainer; <c>shown</c> survives beside an honest total because
/// four <c>PgTarget*</c> tests read it and that fence stands.</item>
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
    /// The catches in a tool method whose return is neither shared shape. Empty: the one there was
    /// (<c>list_servers</c> returned an interpolated sentence of its own) went through <c>McpHelpers.FormatError</c>
    /// in #3653's vocabulary lane. Kept as a roster rather than a bare "zero" so a new ad-hoc return fails
    /// by name and a deliberate exception has somewhere to state its reason.
    /// </summary>
    public static readonly (string File, string Tool)[] AdHocErrorReturns = [];

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
    /// return "Invalid days_back …"</c>) rather than through a <c>McpHelpers</c> validator. Empty since #3653's
    /// vocabulary lane added <c>McpHelpers.ValidateDaysBack(daysBack, maxDaysBack)</c> and moved the five sites
    /// onto it — each keeping its own ceiling (60 / 90 / 366 / 400 days: the retention of the series it reads).
    /// Kept as a roster so a new inline refusal fails by name.
    /// </summary>
    public static readonly (string File, string Tool)[] InlineDaysBackRefusals = [];

    private static readonly Regex SharedValidatorCall = new(
        @"\bMcpHelpers\.(ValidateWindow|ValidateUncappedWindow|ValidateHoursBack|ValidateDaysBack|ValidateTop|ResolveAsOf|ParseSummaryDate)\(",
        RegexOptions.Compiled);

    private static readonly Regex InlineDaysBackRefusal = new(
        @"if \(days_back <= 0 \|\| days_back > [\w.]+\)", RegexOptions.Compiled);

    /// <summary>
    /// Every tool method that declares a bounded parameter validates it through the shared validators —
    /// in its own span, or in a same-file private helper the span calls (the nine health-parser tools share
    /// one <c>CollectAsync</c>) — or is a rostered inline <c>days_back</c> refusal, exactly (none today).
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

    /// <summary>The day-grained validator, executed at its boundaries: it refuses zero, negatives and one past
    /// the ceiling it is handed, accepts the ceiling itself, and speaks the first sentence of
    /// <c>ValidateHoursBack</c>'s refusal — the sentence the two range-tool tests
    /// (<c>DarlingDailySummaryRangeTests</c>, Lite's <c>DailySummaryRangeToolTests</c>) pin on the wire.</summary>
    [Fact]
    public void ValidateDaysBack_RefusesOutsideTheCeilingItIsHanded_InTheSharedSentence()
    {
        Assert.Null(McpHelpers.ValidateDaysBack(1, 60));
        Assert.Null(McpHelpers.ValidateDaysBack(60, 60));
        Assert.Equal("Invalid days_back value '0'. Must be a positive integer (1-60).", McpHelpers.ValidateDaysBack(0, 60));
        Assert.Equal("Invalid days_back value '-7'. Must be a positive integer (1-60).", McpHelpers.ValidateDaysBack(-7, 60));
        Assert.Equal("Invalid days_back value '61'. Must be a positive integer (1-60).", McpHelpers.ValidateDaysBack(61, 60));
        Assert.Equal("Invalid days_back value '367'. Must be a positive integer (1-366).", McpHelpers.ValidateDaysBack(367, McpHelpers.MaxDailySummaryDaysBack));
        /* The sentence shape is ValidateHoursBack's first sentence, word for word but for the parameter. */
        Assert.StartsWith("Invalid hours_back value '0'. Must be a positive integer (1-", McpHelpers.ValidateHoursBack(0), StringComparison.Ordinal);
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

    /* ───────────────────────── rule: one vocabulary — the classification ───────────────────────── */

    /// <summary>
    /// <b>The cut vocabulary, classified (#3653 A15/A16).</b> #3699 inventoried fourteen payload keys that speak
    /// about a cut and left the question of which were dialects of one fact and which were different facts to
    /// this lane. Read from the source, they are five things, and the rosters below say which is which so the
    /// census can assert the CLASSIFICATION rather than a flat list:
    ///
    /// <list type="bullet">
    /// <item><b>The page cut</b> — the tool returned fewer rows than exist because of the caller's cap (or a
    /// read cap of its own). ONE spelling: <c>truncated</c>, OBSERVED off a <c>cap + 1</c> fetch
    /// (<c>McpHelpers.BoundPage</c>), beside the page's count under a <c>*_returned</c> name (#3594). Four
    /// PostgreSQL tools spelled it <c>limit_reached = x.Count &gt;= limit</c> (an inference, not an
    /// observation — a population of exactly <c>limit</c> read as cut) and the autovacuum tool's run history
    /// spelled it <c>history_capped = runs.Count &gt;= RunsReadPerTable</c>, the same inference against a
    /// collector constant; <c>get_analysis_findings</c> on both SKUs inferred its <c>truncation_note</c> from
    /// <c>&gt;= WindowCoveringLimit</c>. All six now fetch one past the cap and publish <c>truncated</c>; the
    /// two retired spellings are named in <see cref="RetiredCutSpellings"/> so their return is a specific
    /// failure, not a generic one.</item>
    /// <item><b>A second bound in the same payload</b> — <see cref="SecondBoundCutKeys"/>: <c>get_blocking</c> /
    /// <c>get_deadlocks</c> under a <c>dedup_key</c> scan the window up to a stated ceiling BEFORE the page is
    /// cut, and the two cuts are two facts a caller acts on differently (raise <c>limit</c>, or narrow the
    /// window). The second is spelled <c>&lt;bound&gt;_truncated</c>, observed the same way.</item>
    /// <item><b>The prose beside the flag</b> — <see cref="CutNoteKeys"/>: <c>truncation_note</c> is the
    /// <c>*_note</c> idiom every disclosure on the surface uses, null when nothing was cut. Three tools carry
    /// it; on <c>get_query_store_top</c> the <c>truncated</c> it explains is the #2364 WINDOW floor (the store's
    /// raw retention did not reach the whole window) — the same key, a different fact, and the residue this
    /// classification leaves (see the class summary).</item>
    /// <item><b>Source-side cuts</b> — <see cref="SourceSideCutKeys"/>: the capture, the chain or the text was
    /// cut BEFORE the store, by the collector. They keep their names because they are true and different: a
    /// caller can do nothing about them by re-paging, and folding them into <c>truncated</c> would tell that
    /// caller to raise a limit that changes nothing.</item>
    /// <item><b>The withheld summary</b> — <see cref="WithheldSummaryKeys"/>: #3594's own vocabulary for a
    /// reach verdict that withholds a figure rather than publishing a page's count under a whole's name.</item>
    /// </list>
    ///
    /// <para>Two more keys the inventory's regex caught are NOT about a cut at all and are excluded by name
    /// with the reason (<see cref="CutHomonyms"/>): <c>is_partial</c> is a PARTIAL INDEX (<c>CREATE INDEX …
    /// WHERE</c>), <c>partial_count</c> counts logging-audit facets whose verdict is <c>partial</c>. And one
    /// page count survives under a neutral noun (<see cref="PageCountsUnderANeutralNoun"/>): <c>shown</c>
    /// beside an honest whole <c>total_*</c> on the health-parser, default-trace and analysis-facts tools —
    /// the cut is exact (<c>total − shown</c>) and disclosed by the pair, and its rename to <c>*_returned</c> +
    /// <c>truncated</c> is fenced tonight: four <c>PgTarget*</c> test files read <c>shown</c> off
    /// <c>get_analysis_facts</c>, and that fence is a standing decision, not this lane's to cross.</para>
    ///
    /// <para>Every roster is an EQUALITY on both the key and the files that emit it. A fifteenth spelling
    /// fails with the class it should have joined; a rostered key that leaves a file fails until the roster
    /// follows it. Swept over whole tool sources, because several are emitted from wire-shape builders below
    /// the last tool.</para>
    /// </summary>
    public static readonly (string Key, string[] Files, string WhatWasCut)[] SecondBoundCutKeys =
    [
        ("scan_truncated", ["DarlingMcpBlockingTools.cs"],
            "the dedup_key fingerprint scan's ceiling (FingerprintScanCeiling), observed off a ceiling + 1 fetch, beside the page's own truncated — two bounds in one payload, the second spelled <bound>_truncated"),
    ];

    public static readonly (string Key, string[] Files, string WhatItExplains)[] CutNoteKeys =
    [
        ("truncation_note", ["DarlingMcpDataTools.cs", "DarlingMcpTools.cs", "McpAnalysisTools.cs"],
            "the prose beside truncated: on get_analysis_findings (both SKUs) the WindowCoveringLimit read cap, observed off a cap + 1 fetch; on get_query_store_top the #2364 window floor — the store's raw retention did not reach the whole requested window"),
    ];

    public static readonly (string Key, string[] Files, string WhatWasCut)[] SourceSideCutKeys =
    [
        ("capture_was_truncated", ["DarlingMcpPgSessionStatesTools.cs"],
            "the pg_session_states collector's per-capture row cap bit at COLLECTION: the stored rows for that capture are a worst-first sample of the instance's sessions"),
        ("chain_may_be_truncated", ["DarlingMcpPgBlockingTools.cs"],
            "the pg_blocking collector's chain-walk depth cap at COLLECTION: the blocking chain may continue past the last edge stored"),
        ("chain_truncation_note", ["DarlingMcpPgBlockingTools.cs"],
            "the prose beside chain_may_be_truncated — what the depth cap is and why the root shown may not be the root"),
        ("query_text_may_be_truncated", ["DarlingMcpPgBlockingTools.cs"],
            "the statement text cut at COLLECTION to the collector's per-row text cap (track_activity_query_size on the target is the other cutter) — the store never held the rest"),
    ];

    public static readonly (string Key, string[] Files, string WhatIsWithheld)[] WithheldSummaryKeys =
    [
        ("answered_rows_withheld", ["DarlingMcpPgIndexTools.cs"], "#3594's reach verdict: the answered-index summary is withheld rather than published as a count of the page"),
        ("created_rows_withheld", ["DarlingMcpPgServerStateTools.cs"], "#3594's reach verdict: the created-slot summary is withheld rather than published as a count of the page"),
    ];

    public static readonly (string Key, string[] Files, string WhyItSurvives)[] PageCountsUnderANeutralNoun =
    [
        ("shown", ["DarlingMcpDefaultTraceTools.cs", "DarlingMcpHealthParserTools.cs", "DarlingMcpTools.cs", "McpAnalysisTools.cs", "McpDefaultTraceTools.cs", "McpHealthParserTools.cs"],
            "the page's count beside an honest WHOLE total (total_entries / total_events / total_facts, or a <noun>_count over the whole in-memory set) — the cut is exact and disclosed by the pair; the #3594 spelling is *_returned + truncated, and the rename is fenced tonight because four PgTarget* test files read shown off get_analysis_facts"),
    ];

    public static readonly (string Key, string[] Files, string WhatItActuallyIs)[] CutHomonyms =
    [
        ("is_partial", ["DarlingMcpPgIndexUsageTools.cs"], "a PARTIAL INDEX (CREATE INDEX … WHERE) — an index property the collector reads off pg_index, not a cut"),
        ("partial_count", ["DarlingMcpPgLoggingAuditTools.cs"], "the number of logging-audit facets whose verdict is `partial` (DarlingPgLoggingAudit.Partial) — a verdict tally, not a cut"),
    ];

    /// <summary>The spellings this lane retired, named so their return fails as THE regression rather than as
    /// an unclassified key: both were <c>x.Count &gt;= cap</c> inferences, and the fix is <c>cap + 1</c> +
    /// <c>McpHelpers.BoundPage</c> + <c>truncated</c>.</summary>
    public static readonly string[] RetiredCutSpellings = ["limit_reached", "history_capped"];

    /// <summary>A payload key at initializer indent: <c>name = …</c>, or the shorthand <c>name,</c> alone on
    /// its line. Keys only — the value side is not read — so a local named <c>truncated</c> assigned in the
    /// body is counted once, where it is emitted.</summary>
    private static readonly Regex PayloadKey = new(@"^\s+([a-z][a-z0-9_]*)\s*(?:=(?!=)|,\s*$)", RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>Every spelling a cut has been given or could plausibly be given next: the fourteen the
    /// inventory found, plus the shapes a new page cut would most likely take (<c>*_reached</c>, <c>*_capped</c>,
    /// <c>cap_hit</c> / <c>limit_hit</c>, <c>has_more</c>, <c>more_available</c>, <c>omitted</c>). Widened on purpose beyond what
    /// the tree contains: a negative census is only as good as the shapes it looks for.</summary>
    private static readonly Regex AboutACut = new(
        @"trunc|_reached$|^capped$|_capped$|^(?:cap|limit|ceiling)_hit$|^is_partial$|^partial_count$|^partial$|^shown$|^more_available$|^has_more$|_withheld$|^omitted$",
        RegexOptions.Compiled);

    private static IEnumerable<(string Key, string[] Files)> ClassifiedCutKeys() =>
        SecondBoundCutKeys.Select(k => (k.Key, k.Files))
            .Concat(CutNoteKeys.Select(k => (k.Key, k.Files)))
            .Concat(SourceSideCutKeys.Select(k => (k.Key, k.Files)))
            .Concat(WithheldSummaryKeys.Select(k => (k.Key, k.Files)))
            .Concat(PageCountsUnderANeutralNoun.Select(k => (k.Key, k.Files)))
            .Concat(CutHomonyms.Select(k => (k.Key, k.Files)));

    [Fact]
    public void EveryCutKey_IsThePageDialect_OrClassified_OnBothSkus()
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
        /* The page dialect must be on most tool files, or the key matcher is dead. */
        Assert.True(found.TryGetValue("truncated", out var truncated) && truncated.Count >= 25, "the key matcher no longer finds `truncated` across twenty-odd tool files");

        var classified = ClassifiedCutKeys().ToDictionary(k => k.Key, k => k.Files.Order(StringComparer.Ordinal).ToArray(), StringComparer.Ordinal);
        Assert.True(classified.Keys.Distinct(StringComparer.Ordinal).Count() == classified.Count, "a key is classified twice; every cut key belongs to exactly one class");

        var retired = RetiredCutSpellings.Where(found.ContainsKey).ToList();
        Assert.True(retired.Count == 0,
            "a retired cut spelling is back — it was an `x.Count >= cap` inference; fetch cap + 1, bind through McpHelpers.BoundPage and publish `truncated`: "
            + string.Join("; ", retired.Select(k => $"{k} on {string.Join(", ", found[k])}")));

        var unclassified = found.Keys.Where(k => k != "truncated" && !classified.ContainsKey(k)).Order(StringComparer.Ordinal).ToList();
        Assert.True(unclassified.Count == 0,
            "a cut spelling with no class — a page cut is spelled `truncated` (observed, beside *_returned); a second bound joins SecondBoundCutKeys as <bound>_truncated; a cut made before the store joins SourceSideCutKeys with what was cut; a homonym joins CutHomonyms with what it actually is: "
            + string.Join("; ", unclassified.Select(k => $"{k} on {string.Join(", ", found[k])}")));

        var gone = classified.Keys.Where(k => !found.ContainsKey(k)).Order(StringComparer.Ordinal).ToList();
        Assert.True(gone.Count == 0, "rostered cut keys no longer emitted anywhere (the collapse reached them — shrink the roster): " + string.Join(", ", gone));

        var moved = classified
            .Where(c => !c.Value.SequenceEqual(found[c.Key], StringComparer.Ordinal))
            .Select(c => $"{c.Key}: rostered on [{string.Join(", ", c.Value)}], emitted on [{string.Join(", ", found[c.Key])}]")
            .ToList();
        Assert.True(moved.Count == 0, "a classified cut key is emitted from a different set of files than its roster says: " + string.Join("; ", moved));
    }

    /// <summary>The cut matcher fires on every spelling the tree has used and the shapes a new one would take,
    /// and not on the neighbours that merely share a token.</summary>
    [Theory]
    [InlineData("truncated", true)]
    [InlineData("scan_truncated", true)]
    [InlineData("limit_reached", true)]
    [InlineData("history_capped", true)]
    [InlineData("cap_hit", true)]
    [InlineData("has_more", true)]
    [InlineData("shown", true)]
    [InlineData("answered_rows_withheld", true)]
    [InlineData("is_partial", true)]
    [InlineData("events_returned", false)]
    [InlineData("total_events", false)]
    [InlineData("limit", false)]
    [InlineData("partial_index_count", false)]
    /* Buffer-cache hits share the token a cap hit would use; the arm is anchored to the cap words. */
    [InlineData("shared_blks_hit", false)]
    [InlineData("blocks_hit", false)]
    public void TheCutMatcher_FiresOnEverySpellingAndTheLikelyNextOnes_AndNotOnNeighbours(string key, bool expected) =>
        Assert.Equal(expected, AboutACut.IsMatch(key));

    /// <summary>
    /// The six sites this lane moved onto the page dialect, read back: each fetches one past its cap and binds
    /// through <c>McpHelpers.BoundPage</c>, none infers, and the four caller-capped ones publish their page count
    /// under a <c>*_returned</c> name. The page census (<see cref="McpPageContractTests.PgPagedToolsThroughTheHelper"/>)
    /// carries the four tools' full contract; this is the vocabulary half: the spelling is <c>truncated</c>.
    /// </summary>
    [Theory]
    [InlineData("DarlingMcpPgAutovacuumTools.cs", "get_pg_autovacuum_health", "limit + 1", "BoundPage(fetched, limit)", "tables_returned")]
    [InlineData("DarlingMcpPgSessionStatesTools.cs", "get_pg_session_states", "limit + 1", "BoundPage(fetched, limit)", "sessions_returned")]
    [InlineData("DarlingMcpPgIndexUsageTools.cs", "get_pg_index_usage", "limit + 1", "BoundPage(fetched, limit)", "indexes_returned")]
    [InlineData("DarlingMcpPgTableBloatTools.cs", "get_pg_table_bloat", "limit + 1", "BoundPage(fetched, limit)", "tables_returned")]
    [InlineData("DarlingMcpTools.cs", "get_analysis_findings", "WindowCoveringLimit + 1", "BoundPage(fetched, FindingOccurrences.WindowCoveringLimit)", null)]
    [InlineData("McpAnalysisTools.cs", "get_analysis_findings", "WindowCoveringLimit + 1", "BoundPage(fetched, FindingOccurrences.WindowCoveringLimit)", null)]
    public void TheSixMovedSites_FetchOnePastTheCap_AndObserveThroughBoundPage(string file, string tool, string fetch, string bind, string? pageCount)
    {
        var span = StripComments(ToolMethods().Single(t => t.File == file && t.Tool == tool).Span);
        Assert.Contains(fetch, span, StringComparison.Ordinal);
        Assert.Contains("McpHelpers." + bind, span, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"\.Count\s*>=\s*(limit|\w*Limit\w*|\w*PerTable)\b"), span);
        Assert.Matches(new Regex(@"\n\s+truncated,"), span);
        if (pageCount is not null)
        {
            Assert.Contains(pageCount + " = ", span, StringComparison.Ordinal);
        }
    }

    /// <summary>The run-history block one layer down: <c>RecentRuns</c> binds to <c>RunsReadPerTable</c> and the
    /// tool asks the log for one more per relation, so the flag it publishes is the same observed
    /// <c>truncated</c>.</summary>
    [Fact]
    public void TheAutovacuumRunHistory_BindsToItsCapAndObserves()
    {
        var source = StripComments(ToolSources().Single(s => s.File == "DarlingMcpPgAutovacuumTools.cs").Source);
        Assert.Contains("RunsReadPerTable + 1);", source, StringComparison.Ordinal);
        Assert.Contains("McpHelpers.BoundPage(runs, RunsReadPerTable)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("history_capped", source, StringComparison.Ordinal);
        Assert.DoesNotContain(">= RunsReadPerTable", source, StringComparison.Ordinal);
    }

    /* ───────────────────────── rule: one vocabulary — severity words ───────────────────────── */

    /// <summary>
    /// <b>The band canon.</b> One spelling for a health band on the wire: the PascalCase enum token —
    /// <c>Healthy</c> / <c>Warning</c> / <c>Critical</c> / <c>Unknown</c> (<c>HealthSeverity</c>,
    /// <c>AgTopology.SeverityLabel</c>, <c>ServerHealthClassifier.BandLabel</c>) and <c>NoData</c> for the daily
    /// band's no-verdict (<c>DailyHealthBand</c>). It is the spelling the web client keys its CSS classes on
    /// (<c>band-Warning</c>, <c>sev-Critical</c>). These are the files that write a band out as a string literal;
    /// every other band reaches the wire through an enum's <c>ToString()</c> and cannot drift by spelling.
    /// </summary>
    public static readonly (string Spelling, string[] Files)[] BandCanonSpellings =
    [
        ("Critical", ["DarlingAgReader.cs", "DarlingMcpPgIndexUsageTools.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs"]),
        ("Healthy", ["DarlingAgReader.cs", "DarlingMcpPgIndexUsageTools.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs"]),
        ("Unknown", ["DarlingAgReader.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs"]),
        ("Warning", ["DarlingAgReader.cs", "DarlingMcpPgIndexUsageTools.cs", "DarlingMcpPgSessionStatesTools.cs", "DarlingMcpPgTableBloatTools.cs"]),
    ];

    /// <summary>
    /// <b>Every other literal that spells a severity word</b> under either SKU's Mcp directory, with what it
    /// actually is. #3699 counted eight spellings of four words; read from the source, six of the eight are
    /// not bands at all — they are other vocabularies that happen to use the same word, or the word on the way
    /// IN — and moving them to the canon would either break a scale whose other rungs are not severity words or
    /// rewrite what a DMV says. Each is rostered by (spelling, file) with the vocabulary it belongs to, so a
    /// NEW lower- or upper-case severity literal in any other file fails as a band spelled wrong, and a rostered
    /// one that disappears fails until the roster follows it.
    ///
    /// <para>The one of these that is a severity by any reading — the PostgreSQL tools' severity-TOKEN ladder,
    /// <c>ok / info_* / warning[_*] / critical_*</c> (autovacuum, replication slots, wraparound; the web client
    /// renders the token untranslated by design, <c>server-tabs.js</c>) — is a different vocabulary from the
    /// band, not a case variant of it: the tier is a prefix and the REASON is the rest of the token. Collapsing
    /// it into the canon means splitting <c>severity</c> into a band plus a <c>reason</c> on three tools, which
    /// is a payload reshape and a maintainer's call, not a spelling. The bare <c>warning</c> on the wraparound
    /// ladder is that family's tier with no qualifier, and <see cref="TheLadderTier_IsAPrefixOfATokenFamily_NotABand"/>
    /// executes the ladder around it.</para>
    /// </summary>
    public static readonly (string Spelling, string File, string WhatItIs)[] SeverityHomonyms =
    [
        ("CRITICAL", "DarlingPlanCacheSchedulerReader.cs",
            "bloat_level's top rung of the Dashboard's NORMAL / MEDIUM / HIGH / CRITICAL level scale, reproduced verbatim on both SKUs (Lite: LocalDataService.ClassifyPlanCacheBloat) and coloured by both viewers' BloatLevelBrush — a level scale whose other three rungs are not severity words"),
        ("HEALTHY", "DarlingAgReader.cs",
            "SQL Server's own synchronization_health_desc token, PARSED on the way in and banded to HealthSeverity — input vocabulary, never emitted"),
        ("HEALTHY", "DarlingFleetReader.cs",
            "the collector-health row status (the shared NEVER_RUN / NO_PERMISSIONS / FAILING / STALE / WARNING / HEALTHY vocabulary), compared on the way in to count a server's healthy collectors — input vocabulary"),
        ("Unknown", "DarlingMcpTools.cs",
            "audit_config's edition-NAME fallback (Enterprise / Standard / … / Unknown) — a name, spelled as the canon by coincidence"),
        ("Unknown", "McpAnalysisTools.cs",
            "audit_config's edition-NAME fallback — the Lite twin of the above"),
        ("unknown", "DarlingMcpPgWaitSamplingTools.cs",
            "the wait-instrument token for an arm this build does not know (PgWaitInstrument's service_sampled / pg_wait_sampling / … vocabulary) — an instrument, not a band"),
        ("unknown", "DarlingPgLoggingAudit.cs",
            "the logging audit's verdict vocabulary (instrumented / partial / off / unknown) — a verdict, not a band"),
        ("unknown", "McpHealthTools.cs",
            "get_collection_health's version-string fallback when the assembly carries no version — a version, not a band"),
        ("warning", "DarlingMcpPgWraparoundTools.cs",
            "the bare tier of the PostgreSQL severity-TOKEN ladder (ok / info_anti_wraparound_vacuum_expected / warning / critical_failsafe_range / critical_wraparound_imminent) — a tier-prefixed token family, not a case variant of the band"),
        ("warning", "DarlingMcpTools.cs",
            "audit_config's per-recommendation status (ok / warning / review) — the lower-case status vocabulary every `status` key on the surface speaks, not a band"),
        ("warning", "McpAnalysisTools.cs",
            "audit_config's per-recommendation status — the Lite twin of the above"),
    ];

    private static readonly Regex SeverityWord = new(
        @"^(?:healthy|warning|critical|unknown|nodata|no data|info|elevated)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    /// <summary>
    /// Every severity-word literal under either SKU's Mcp directory is the band canon in a rostered file, or a
    /// rostered homonym — exactly. Bound: whole string-literal bodies (a description that contains the word
    /// in a sentence is not a literal that IS the word), on tool and reader sources alike; the literal
    /// walker's floor is 5,000 literals across both SKUs.
    /// </summary>
    [Fact]
    public void EverySeverityWordLiteral_IsTheBandCanon_OrARosteredHomonym_OnBothSkus()
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

        /* The canon is PascalCase by construction: a homonym may share the canon's spelling (the edition
           fallback does) but a canon spelling is never lower- or upper-case. */
        Assert.All(BandCanonSpellings, c => Assert.Matches(new Regex("^[A-Z][a-z]+$"), c.Spelling));

        var expected = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);
        foreach (var (spelling, files) in BandCanonSpellings)
        {
            expected[spelling] = new SortedSet<string>(files, StringComparer.Ordinal);
        }

        foreach (var (spelling, file, _) in SeverityHomonyms)
        {
            if (!expected.TryGetValue(spelling, out var files))
            {
                expected[spelling] = files = new SortedSet<string>(StringComparer.Ordinal);
            }

            Assert.True(files.Add(file), $"{spelling} in {file} is rostered twice");
        }

        var report = string.Join("; ", found.OrderBy(a => a.Key, StringComparer.Ordinal).Select(a => $"{a.Key} in {string.Join(", ", a.Value)}"));
        Assert.True(
            expected.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(found.Keys)
            && expected.All(e => e.Value.SetEquals(found[e.Key])),
            "the severity-word classification no longer matches the tree — a new lower- or upper-case literal is a band spelled wrong (the canon is Healthy / Warning / Critical / Unknown / NoData), unless it is another vocabulary, in which case it joins SeverityHomonyms with what it is. Found: [" + report + "]");
    }

    /// <summary>The canon, executed off its sources of truth: the enum tokens and the two label helpers agree
    /// on the spelling, and the daily band's no-verdict token has no space in it.</summary>
    [Fact]
    public void TheBandCanon_IsWhatTheSharedClassifiersSpell()
    {
        Assert.Equal("Warning", AgTopology.SeverityLabel(HealthSeverity.Warning));
        Assert.Equal("Critical", ServerHealthClassifier.BandLabel(FleetHealthBand.Critical));
        Assert.Equal("Healthy", HealthSeverity.Healthy.ToString());
        Assert.Equal("Unknown", HealthSeverity.Unknown.ToString());
        Assert.Equal("NoData", DailyHealthBand.NoData.ToString());
        Assert.Equal("Offline", FleetHealthBand.Offline.ToString());
    }

    /// <summary>
    /// <c>CRITICAL</c> in the plan-cache reader is a rung of a LEVEL scale, not a band: the same helper on Lite
    /// spells all four rungs identically (read from source, because this project does not reference the
    /// desktop app), and the Darling one is executed at each boundary. Moving one rung to <c>Critical</c>
    /// would leave <c>Critical / HIGH / MEDIUM / NORMAL</c> — a scale in two cases, which is the defect this
    /// file exists to prevent.
    /// </summary>
    [Fact]
    public void ThePlanCacheBloatLevel_IsOneScaleOnBothSkus_AndNotABand()
    {
        Assert.Equal("CRITICAL", DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(100, 51).Level);
        Assert.Equal("HIGH", DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(100, 31).Level);
        Assert.Equal("MEDIUM", DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(100, 21).Level);
        Assert.Equal("NORMAL", DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(100, 20).Level);
        Assert.Equal("NORMAL", DarlingPlanCacheSchedulerReader.ClassifyPlanCacheBloat(0, 0).Level);

        var lite = File.ReadAllText(RepoFile.PathTo("Lite/Services/LocalDataService.PlanCache.cs"));
        var liteHelper = lite[lite.IndexOf("ClassifyPlanCacheBloat(long totalPlans", StringComparison.Ordinal)..];
        liteHelper = liteHelper[..liteHelper.IndexOf("\n    }", StringComparison.Ordinal)];
        foreach (var rung in new[] { "\"CRITICAL\"", "\"HIGH\"", "\"MEDIUM\"", "\"NORMAL\"" })
        {
            Assert.Contains(rung, liteHelper, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// <c>warning</c> in the wraparound tool is a TIER of a token ladder whose neighbours carry the tier as a
    /// prefix and the reason as the rest — executed at each boundary — not a lower-case band. The same family
    /// on the autovacuum and slot tools spells every non-<c>ok</c> rung with a reason; the wraparound ladder's
    /// warning rung is the one bare tier in the family, and that is a rename for the maintainer's ruling, not
    /// a case fix.
    /// </summary>
    [Fact]
    public void TheLadderTier_IsAPrefixOfATokenFamily_NotABand()
    {
        Assert.Equal("ok", DarlingMcpPgWraparoundTools.Classify(10.0, 20.0));
        Assert.StartsWith("info_", DarlingMcpPgWraparoundTools.Classify(10.0, 100.0), StringComparison.Ordinal);
        Assert.Equal("warning", DarlingMcpPgWraparoundTools.Classify(60.0, 100.0));
        Assert.StartsWith("critical_", DarlingMcpPgWraparoundTools.Classify(80.0, 100.0), StringComparison.Ordinal);
        Assert.StartsWith("critical_", DarlingMcpPgWraparoundTools.Classify(99.0, 100.0), StringComparison.Ordinal);

        Assert.StartsWith("warning_", DarlingMcpPgAutovacuumTools.Classify(false, 2.5, false), StringComparison.Ordinal);
        Assert.StartsWith("critical_", DarlingMcpPgAutovacuumTools.Classify(false, 10.0, null), StringComparison.Ordinal);
        Assert.StartsWith("warning_", DarlingMcpPgSlotTools.Classify("extended", true, null), StringComparison.Ordinal);
        Assert.StartsWith("critical_", DarlingMcpPgSlotTools.Classify("lost", false, null), StringComparison.Ordinal);
    }

    /// <summary>
    /// The named instance behind "NoData and No Data in one payload", now closed: <c>get_daily_summary</c> on
    /// both SKUs publishes <c>overall_health</c> and <c>health_band</c> from the SAME expression — the enum
    /// token — so the two keys cannot disagree. The calculator's human label ("No Data") still exists, for the
    /// desktop viewers' tooltips, and the executed pair below is the proof that it differs from the token on
    /// exactly one value and that the token is what the wire now carries. <c>row.OverallHealth</c> (the label)
    /// no longer appears in either tool.
    /// </summary>
    [Fact]
    public void TheDailySummary_PublishesOneBandSpelling_OnBothSkus()
    {
        Assert.Equal("NoData", DailyHealthBand.NoData.ToString());
        Assert.Equal("No Data", DailyHealthBandCalculator.Label(DailyHealthBand.NoData));
        Assert.Equal("Healthy", DailyHealthBandCalculator.Label(DailyHealthBand.Healthy));

        foreach (var (file, tool) in new[]
        {
            ("DarlingMcpHealthTools.cs", "get_daily_summary"), ("DarlingMcpHealthTools.cs", "get_daily_summary_range"),
            ("McpHealthTools.cs", "get_daily_summary"), ("McpHealthTools.cs", "get_daily_summary_range"),
        })
        {
            var code = StripComments(ToolMethods().Single(t => t.File == file && t.Tool == tool).Span);
            Assert.Contains("health_band = row.HealthBand.ToString()", code, StringComparison.Ordinal);
            Assert.Contains("overall_health = row.HealthBand.ToString()", code, StringComparison.Ordinal);
            Assert.DoesNotContain("OverallHealth", code, StringComparison.Ordinal);
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
