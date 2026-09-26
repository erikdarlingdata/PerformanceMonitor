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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
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
/// <item><b>errors one shape</b> — census-able as "every catch returns through ONE helper", and pinned that
/// way: every <c>catch</c> in every tool body on both SKUs returns <c>McpHelpers.FormatError</c>, which since
/// #3653 Q11 is the <c>{"status":"error", message, hints}</c> JSON envelope (a WIRE CHANGE for the 214 tools
/// that answered with the bare sentence <c>Error during …</c> until then). The history this file used to
/// carry as an inventory: the shared shapes were TWO — the bare sentence on every Lite file and every SQL
/// Server-family Darling file, <c>McpHelpers.Status("error", "Reading X failed: …")</c> on 21 PostgreSQL-side
/// files — and a <c>status</c>-keyed client read the first group's failures as successful text. The one ad-hoc
/// site (<c>list_servers</c>) went through <c>FormatError</c> in the vocabulary lane; the ruling then routed
/// the 30 PostgreSQL-side catches through the same helper, so ONE helper owns the shape AND the grammar, the
/// file-grain inventory (<c>FilesReturningTheJsonErrorEnvelope</c>) is retired, and a direct
/// <c>Status("error", …)</c> inside a catch is now classified AD-HOC — it is the retired dialect coming back,
/// and it fails by name. The envelope is executed here as a positive control, not trusted from its
/// doc comment. <b>And the refusal is the fourth outcome, with its own word (#3739):</b> every value a tool
/// hands back through the <c>if (error != null) return error;</c> idiom — the resolver's miss, a validator's
/// refusal, roughly four hundred sites on both SKUs — is <c>McpHelpers.Refusal</c>'s <c>{"status":"invalid",
/// message, hints.parameter}</c> envelope, built in the handful of PRODUCERS rather than at the sites; the
/// census below walks every guarded pass-through to the value it passes and requires the producer to be a
/// shared one, walks every bare-string return in a tool body into an exact roster, and holds that the
/// failure word <c>error</c> has exactly one producer (<c>FormatError</c>) so a refusal can never wear it again
/// — which is how nine PostgreSQL refusals (the issue counted seven; two more split the literal across a line break) answered HTTP 500 for a bad <c>limit</c> between #3719 and #3739.</item>
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
/// roster is an equality that fails when a spelling appears or disappears without the roster moving. The
/// residue the vocabulary lane left — <c>truncated</c> carrying two facts, the page cut and, on the #2364 trend
/// family and <c>get_query_store_top</c>, the WINDOW floor (the store's retention did not reach the whole
/// window) — is resolved by #3653 item 17: the window floor is <c>window_truncated</c> on both SKUs, classified
/// below as the second bound it is, and <see cref="TheWindowFloor_IsSpelledWindowTruncated_BesideItsReach_AndNeverBareTruncated_OnBothSkus"/>
/// fails a bare <c>truncated</c> written beside <c>effective_hours_back</c> by file and line.
/// <b>Residue, stated rather than hidden:</b> the PostgreSQL severity-token
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
    /// The catches in a tool method whose return is not the one helper. Empty: the one there was
    /// (<c>list_servers</c> returned an interpolated sentence of its own) went through <c>McpHelpers.FormatError</c>
    /// in #3653's vocabulary lane, and the 30 PostgreSQL-side catches that called <c>McpHelpers.Status("error", …)</c>
    /// directly — the retired second dialect — went through it under #3653 Q11. Kept as a roster rather than a
    /// bare "zero" so a new ad-hoc return fails by name and a deliberate exception has somewhere to state its
    /// reason.
    /// </summary>
    public static readonly (string File, string Tool)[] AdHocErrorReturns = [];

    /// <summary>
    /// The floor under the one-helper count. 243 catch returns route through <c>FormatError</c> at the time of
    /// writing (the remaining seven are the write tools' <c>Outcome("invalid", …)</c> and the gates); a sweep
    /// that found fewer has a helper marker that stopped matching — a renamed helper would otherwise pass as
    /// "no ad-hoc returns" for free.
    /// </summary>
    private const int EnvelopeReturnFloor = 200;

    /// <summary>
    /// Every <c>return</c> inside a <c>catch</c> in a tool method, on both SKUs, is one of: the ONE error
    /// helper (<c>McpHelpers.FormatError</c>, directly or through <c>Task.FromResult</c> on the synchronous
    /// tools — the <c>{"status":"error", message, hints}</c> envelope since #3653 Q11), the write tools'
    /// <c>Outcome("invalid", …)</c> for a body that would not parse (#3615's write contract), or a gate
    /// computed above the catch (<c>gated</c> / <c>capability</c> / <c>precondition</c> — a <c>Status</c>
    /// envelope already built). Anything else is an ad-hoc error and must be in <see cref="AdHocErrorReturns"/>
    /// exactly — INCLUDING a direct <c>McpHelpers.Status("error", …)</c>, which reaches the same wire shape by
    /// a second path and is exactly how the "Reading X failed" grammar drifted away from "Error during X" the
    /// first time. One helper owns the shape and the grammar; the census holds the helper, not the shape.
    ///
    /// <para>Bound: catch blocks are located on the comments-and-strings-stripped text (a description that
    /// says "catch-all rows" is prose, not a keyword), brace-balanced there, and the return expressions read
    /// off the same span of the real source with comments removed. 250 catch returns at the time of writing;
    /// the floor is 200, and the helper-routed count has its own floor (<see cref="EnvelopeReturnFloor"/>) so
    /// the classifier's first arm cannot silently stop matching while the ad-hoc roster stays empty.</para>
    /// </summary>
    [Fact]
    public void EveryToolCatch_ReturnsThroughASharedErrorShape_AndTheAdHocRosterIsExact()
    {
        var returns = 0;
        var envelopes = 0;
        var adHoc = new List<(string File, string Tool)>();
        var offenders = new List<string>();

        foreach (var (file, tool, span, _) in ToolMethods())
        {
            foreach (var expression in CatchReturns(span))
            {
                returns++;
                var shape = ClassifyErrorReturn(expression);
                if (shape == ErrorShape.Envelope)
                {
                    envelopes++;
                }

                if (shape == ErrorShape.AdHoc)
                {
                    adHoc.Add((file, tool));
                    offenders.Add($"{file} {tool}: return {expression}");
                }
            }
        }

        Assert.True(returns >= 200, $"only {returns} catch returns were examined across both SKUs; the catch marker has stopped matching");
        Assert.True(envelopes >= EnvelopeReturnFloor, $"only {envelopes} catch returns route through McpHelpers.FormatError; the helper marker has stopped matching");

        Assert.True(
            AdHocErrorReturns.ToHashSet().SetEquals(adHoc.Distinct()),
            "the ad-hoc error roster no longer matches the tree — new ad-hoc (a catch that does not return McpHelpers.FormatError; a direct Status(\"error\") is the retired dialect): ["
            + string.Join("; ", offenders.Where(o => !AdHocErrorReturns.Any(r => o.StartsWith($"{r.File} {r.Tool}:", StringComparison.Ordinal))))
            + "]; rostered but gone (shrink the roster): ["
            + string.Join(", ", AdHocErrorReturns.Except(adHoc).Select(r => $"{r.File} {r.Tool}"))
            + "]");
    }

    /// <summary>
    /// The one error shape, EXECUTED rather than read off a doc comment: <c>FormatError</c>'s output parses as
    /// JSON, its <c>status</c> is <c>error</c>, its <c>message</c> is the unchanged sentence (so a log grep for
    /// "Error during" still finds every failure), its <c>hints.operation</c> is the tool name verbatim, and it is
    /// byte-for-byte what <c>Status("error", sentence, new { operation })</c> builds — the same serializer, the
    /// same key order — so there is one envelope, not a look-alike. Before #3653 Q11 this fact asserted the
    /// opposite (that the sentence would NOT parse), which is the wire change in one line.
    /// </summary>
    [Fact]
    public void TheOneErrorShape_IsTheEnvelope_ExecutedThroughFormatError()
    {
        var wire = McpHelpers.FormatError("reading x", new InvalidOperationException("boom"));

        using var envelope = JsonDocument.Parse(wire);
        Assert.Equal("error", envelope.RootElement.GetProperty("status").GetString());
        Assert.Equal("Error during reading x: boom", envelope.RootElement.GetProperty("message").GetString());
        Assert.Equal("reading x", envelope.RootElement.GetProperty("hints").GetProperty("operation").GetString());
        Assert.Equal(3, envelope.RootElement.EnumerateObject().Count());

        Assert.Equal(McpHelpers.Status("error", "Error during reading x: boom", new { operation = "reading x" }), wire);
        Assert.Equal("Error during reading x: boom", McpHelpers.ErrorSentence("reading x", new InvalidOperationException("boom")));
    }

    /// <summary>
    /// The recognizer the consumers branch on (the web surface's HTTP mapping, the test guards that used to
    /// read <c>StartsWith("Error during")</c>), executed against the producer: it fires on <c>FormatError</c>'s
    /// output (and on a hand-built <c>Status("error", …)</c>, which is exactly why the tree may no longer
    /// build one outside <c>FormatError</c> — see <see cref="TheFailureWord_HasOneProducer_OnBothSkus"/>),
    /// tolerates leading whitespace the way the web sniff does, and does NOT fire on a miss envelope, on the
    /// refusal envelope, on a data payload whose first key begins with <c>status</c>, or on the retired bare
    /// sentence. <c>ErrorMessageOf</c> unwraps the sentence from either status-word envelope and hands anything
    /// else back untouched.
    /// </summary>
    [Fact]
    public void TheErrorEnvelopeRecognizer_FiresOnTheProducer_AndNotOnItsNeighbours()
    {
        var wire = McpHelpers.FormatError("get_x", new InvalidOperationException("boom"));
        Assert.StartsWith(McpHelpers.ErrorEnvelopePrefix, wire, StringComparison.Ordinal);
        Assert.True(McpHelpers.IsErrorEnvelope(wire));
        Assert.True(McpHelpers.IsErrorEnvelope(McpHelpers.Status("error", "Invalid limit value '0'.")));
        Assert.True(McpHelpers.IsErrorEnvelope("  " + wire));

        Assert.False(McpHelpers.IsErrorEnvelope(McpHelpers.Status("empty", "nothing")));
        Assert.False(McpHelpers.IsErrorEnvelope(McpHelpers.Status("precondition", "Query Store is off", new { statement = "ALTER DATABASE" })));
        Assert.False(McpHelpers.IsErrorEnvelope(McpHelpers.Refusal("limit", "Invalid limit value '0'.")));
        Assert.False(McpHelpers.IsErrorEnvelope("{\"status_counts\":{\"error\":2}}"));
        Assert.False(McpHelpers.IsErrorEnvelope("{\"status\":\"error_count\",\"message\":\"x\"}"));
        Assert.False(McpHelpers.IsErrorEnvelope("Error during get_x: boom"));
        Assert.False(McpHelpers.IsErrorEnvelope(null));
        Assert.False(McpHelpers.IsErrorEnvelope(""));

        Assert.Equal("Error during get_x: boom", McpHelpers.ErrorMessageOf(wire));
        Assert.Equal("Invalid limit value '0'.", McpHelpers.ErrorMessageOf(McpHelpers.Status("error", "Invalid limit value '0'.")));
        Assert.Equal("Invalid limit value '0'.", McpHelpers.ErrorMessageOf(McpHelpers.Refusal("limit", "Invalid limit value '0'.")));
        Assert.Equal("Could not resolve server.", McpHelpers.ErrorMessageOf("Could not resolve server."));
        Assert.Equal("{\"status\":\"error\",oops", McpHelpers.ErrorMessageOf("{\"status\":\"error\",oops"));
        Assert.Equal("{\"status\":\"invalid\",oops", McpHelpers.ErrorMessageOf("{\"status\":\"invalid\",oops"));
    }

    /// <summary>The classifier, witnessed against each shape as it appears in the tree — and against the one
    /// that no longer does: a direct <c>Status("error", …)</c> in a catch is the retired PostgreSQL dialect and
    /// is AD-HOC now, so its return fails as THE regression rather than as a roster surprise.</summary>
    [Theory]
    [InlineData("McpHelpers.FormatError(\"get_x\", ex)", ErrorShape.Envelope)]
    [InlineData("McpHelpers.FormatError(\"get_pg_deadlocks\", ex)", ErrorShape.Envelope)]
    [InlineData("Task.FromResult(McpHelpers.FormatError(\"validate_custom_view\", ex))", ErrorShape.Envelope)]
    [InlineData("McpHelpers.Status(\"error\", $\"Reading PostgreSQL deadlocks failed: {ex.Message}\")", ErrorShape.AdHoc)]
    [InlineData("Outcome(\"invalid\", $\"settings_json is not valid JSON: {ex.Message}\")", ErrorShape.WriteOutcome)]
    [InlineData("gated", ErrorShape.Gate)]
    [InlineData("$\"Could not read the servers registry from the Postgres store: {ex.Message}\"", ErrorShape.AdHoc)]
    [InlineData("$\"Error during get_x: {ex.Message}\"", ErrorShape.AdHoc)]
    [InlineData("JsonSerializer.Serialize(new { error = ex.Message })", ErrorShape.AdHoc)]
    [InlineData("McpHelpers.Status(\"empty\", \"nothing\")", ErrorShape.AdHoc)]
    public void TheErrorShapeClassifier_NamesEachShape(string expression, ErrorShape expected) =>
        Assert.Equal(expected, ClassifyErrorReturn(expression));

    public enum ErrorShape { Envelope, WriteOutcome, Gate, AdHoc }

    private static ErrorShape ClassifyErrorReturn(string expression)
    {
        if (expression.StartsWith("McpHelpers.FormatError(", StringComparison.Ordinal)
            || expression.StartsWith("Task.FromResult(McpHelpers.FormatError(", StringComparison.Ordinal))
        {
            return ErrorShape.Envelope;
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

    /* ───────────────────────── rule: refusals one shape (#3739) ───────────────────────── */

    /// <summary>
    /// The bare STRING returns left in a tool body on either SKU — a <c>return "…";</c> or <c>return $"…";</c>
    /// whose value reaches the wire as prose where every other outcome is JSON — with the reason each
    /// survives. #3739 shaped every REFUSAL as <c>McpHelpers.Refusal</c>'s <c>invalid</c> envelope (in the
    /// producers: the validators, both resolvers, the web dispatch's missing-parameter arm, and the handful of
    /// tools that refuse a parameter of their own inline), so what is left here is not a refusal at all: three
    /// MISS sentences that predate the four miss words and belong to #3703's lane, not this one. The roster is
    /// exact so a new bare sentence fails by name — a refusal is <c>Refusal(parameter, sentence)</c>, a miss is
    /// <c>Status(word, sentence)</c>, and a tool body has no third way to say no.
    /// </summary>
    public static readonly (string File, string Tool, string Sentence, string WhyItSurvives)[] BareSentenceReturns =
    [
        ("DarlingMcpDataTools.cs", "list_servers", "No servers are registered yet.",
            "a MISS on an empty registry (nothing to list), not a refusal of the request — the miss-word lane's (#3703), where `empty` is its word"),
        ("McpDiscoveryTools.cs", "list_servers", "No servers are configured.",
            "the Lite twin of the above: a MISS on an empty ServerManager, the miss-word lane's (#3703)"),
        ("McpPlanTools.cs", "analyze_query_store_plan", "Could not find connection details for server",
            "a resolved server whose connection details are gone from the manager between resolve and read — the request was fine, so not `invalid`; an `unavailable` miss for #3703's lane"),
    ];

    /// <summary>The shared producers whose return a tool may pass through under <c>if (x != null) return x;</c>,
    /// in three classes. REFUSALS: the two resolvers and every <c>McpHelpers</c> validator (each of which builds
    /// <c>Refusal</c>), the fleet-sweep watch-state validator, and the trend-width resolvers
    /// (<c>TrendBuckets.Resolve</c> / <c>ValidateWidth</c> / <c>RequireWholeHours</c>, #3897), which do too. GATES: the engine-capability
    /// and runtime-precondition probes on both SKUs, whose answer is a MISS envelope (<c>not_collected</c> /
    /// <c>precondition</c>) computed above the read and passed through by the same idiom — not refusals, but
    /// shared builders, which is what the sweep is holding. CARRIED: the health-parser family's
    /// <c>CollectAsync</c>, whose <c>Collected.EarlyReturn</c> is a resolver or validator result carried through
    /// the nine-tool helper that <see cref="CallsAValidatingHelper"/> already holds to the shared validators.</summary>
    private static readonly Regex SharedPassThroughProducer = new(
        @"\b(?:DarlingServerResolver|ServerResolver)\.(?:ResolveOrError\w*|ResolveWithFingerprintNameAsync)\("
        + @"|\bMcpHelpers\.(?:ValidateWindow|ValidateUncappedWindow|ValidateHoursBack|ValidateDaysBack|ValidateTop|ResolveAsOf|ParseSummaryDate|ValidateChoice|ValidateMinMs|Refusal)\("
        + @"|\bDarlingFleetSweepEndpoints\.ValidateWatchState\("
        + @"|\bTrendBuckets\.(?:Resolve|ValidateWidth|RequireWholeHours)\("
        + @"|\b(?:Darling|Mcp)EngineCapability\.NotCollectedStatusAsync\("
        + @"|\b(?:Darling|Mcp)RuntimePrecondition\.(?:StatusAsync|GatedOffStatusAsync)\("
        + @"|\bCollectAsync(?:<[^>]+>)?\(",
        RegexOptions.Compiled);

    /// <summary>A guarded pass-through: <c>if (x != null) return x;</c> / <c>if (x is not null) { return x; }</c>,
    /// the idiom every resolve-and-bail and validate-and-bail site on both SKUs uses. The guard and the return
    /// must name the SAME value, which is what makes this a pass-through of a producer's answer rather than a
    /// tool's own early return.</summary>
    private static readonly Regex GuardedPassThrough = new(
        @"\bif\s*\(\s*([A-Za-z_]\w*(?:\.\w+)?)\s*(?:!=|is\s+not)\s*null\s*\)\s*\{?\s*return\s+\1\s*;",
        RegexOptions.Compiled);

    /// <summary>The floor under the guarded pass-through count: 463 at the time of writing across both SKUs
    /// (196 resolver bails, 218 validator bails, 49 gate and carried pass-throughs — the ~190 + ~210 the issue counted). A sweep that found fewer has a
    /// marker that stopped matching and would pass for free.</summary>
    private const int GuardedPassThroughFloor = 300;

    /// <summary>
    /// Every REFUSAL a tool body on either SKU returns is the shared shape, and the bare sentences left are
    /// exactly the rostered misses. Three sweeps over every tool span, each on the comments-and-strings-stripped
    /// text so prose cannot open or close anything:
    ///
    /// <list type="number">
    /// <item><b>Every guarded pass-through</b> (<c>if (x != null) return x;</c>) is walked back to the LAST
    /// assignment of <c>x</c> before it — <c>var x = …</c>, <c>x = …</c>, or the destructure <c>var (…, x) = …</c>
    /// — and the producer on its right-hand side must be one of <see cref="SharedPassThroughProducer"/>'s: a
    /// resolver, a <c>McpHelpers</c> validator, the watch-state validator, a gate probe, or the health-parser
    /// helper that carries one of those. A <c>??</c> chain counts only when EVERY operand does (#3897: a shared first operand used to vouch for a hand-built second one). A pass-through
    /// of anything else is a refusal built by hand somewhere the census cannot see, and fails by name.</item>
    /// <item><b>Every <c>return</c> whose expression begins a string literal</b> (<c>"</c>, <c>$"</c>, <c>@"</c>)
    /// is a bare sentence and must be in <see cref="BareSentenceReturns"/> — exactly, both ways. The rule reaches
    /// ONE call deep (#3962): a tool's <c>return Helper(…)</c> is read through that same-file method's return
    /// expressions, arm by arm (<see cref="HelperReturnArms"/>) — get_query_heatmap's refusals were bare sentences
    /// in private helpers until #3897, and this sweep could not see them.</item>
    /// <item><b>Every <c>return McpHelpers.Status("invalid", …)</c> and every hand-serialized
    /// <c>new { status = "invalid" … }</c></b> is a refusal built beside the builder rather than through it and
    /// fails: the word is right, the shape (no <c>hints.parameter</c>) is not, and one producer is the point.
    /// The write tools' <c>Outcome("invalid", …)</c> is NOT this — it is their own builder for a body that
    /// will not parse, same word, same bytes, and #3615's write contract owns it.</item>
    /// </list>
    ///
    /// <para>Bound: 244 tool methods; the guarded pass-through count has its own floor
    /// (<see cref="GuardedPassThroughFloor"/>) so the idiom regex cannot silently stop matching while the
    /// offender lists stay empty, and the resolver-sourced and validator-sourced counts are each floored at
    /// 100 so neither producer class can vanish from the sweep unnoticed. The helpers followed are floored at
    /// 50 (60 when #3962 landed) for the same reason.</para>
    /// </summary>
    [Fact]
    public void EveryRefusal_ReturnsThroughTheSharedShape_AndTheBareSentenceRosterIsExact()
    {
        var passThroughs = 0;
        var fromResolver = 0;
        var fromValidator = 0;
        var unshared = new List<string>();
        var bare = new List<(string File, string Tool, string Sentence)>();
        var handBuilt = new List<string>();
        var helpersFollowed = 0;

        foreach (var (file, tool, span, source) in ToolMethods())
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(span);

            foreach (Match guard in GuardedPassThrough.Matches(code))
            {
                passThroughs++;
                /* A member guard (`c.EarlyReturn`) is walked back through its ROOT (`c`), whose producer is the
                   helper that filled it. */
                var root = guard.Groups[1].Value.Split('.')[0];
                var producer = LastAssignmentOf(code, root, guard.Index);
                if (producer is null)
                {
                    unshared.Add($"{file} {tool}: return {guard.Groups[1].Value}; (no assignment found in the tool body)");
                    continue;
                }

                /* Every operand of a `??` chain, not just the first (#3897): the chain passes through whichever
                   operand answers, so a hand-built refusal behind a shared first operand is still hand-built —
                   get_query_heatmap's bucket_minutes answered a bare sentence that way on both SKUs until #3897. */
                var unsharedOperand = CoalesceOperands(producer).FirstOrDefault(o => !SharedPassThroughProducer.IsMatch(o));
                if (unsharedOperand is not null)
                {
                    unshared.Add($"{file} {tool}: {guard.Groups[1].Value} = {producer} (not shared: {unsharedOperand.Trim()})");
                    continue;
                }

                if (producer.Contains("ResolveOrError", StringComparison.Ordinal) || producer.Contains("ResolveWithFingerprintName", StringComparison.Ordinal))
                {
                    fromResolver++;
                }
                else if (producer.Contains("McpHelpers.", StringComparison.Ordinal))
                {
                    fromValidator++;
                }
            }

            /* The tool's own returns, then — one call deep, #3962 — what each `return Helper(…)` hands back. */
            var helperArms = HelperReturnArms(span, source).ToList();
            helpersFollowed += helperArms.Select(h => h.Helper).Distinct(StringComparer.Ordinal).Count();
            foreach (var (expression, via) in ReturnExpressions(span, code).Select(e => (e, "return "))
                .Concat(helperArms.Select(h => (h.Arm, $"return {h.Helper}(…) → "))))
            {
                if (StartsWithStringLiteral(expression))
                {
                    var opening = expression.IndexOf('"') + 1;
                    var closing = expression.IndexOf('"', opening);
                    var text = closing > opening ? expression[opening..closing] : expression[opening..];
                    bare.Add((file, tool, text));
                }
                else if (expression.StartsWith("McpHelpers.Status(\"invalid\"", StringComparison.Ordinal)
                    || Regex.IsMatch(expression, @"^JsonSerializer\.Serialize\(\s*new\s*\{\s*status\s*=\s*""invalid"""))
                {
                    handBuilt.Add($"{file} {tool}: {via}{expression}");
                }
            }
        }

        Assert.True(passThroughs >= GuardedPassThroughFloor, $"only {passThroughs} guarded pass-throughs were found across both SKUs; the idiom marker has stopped matching");
        Assert.True(fromResolver >= 100, $"only {fromResolver} pass-throughs trace to a resolver; the resolver marker has stopped matching");
        Assert.True(fromValidator >= 100, $"only {fromValidator} pass-throughs trace to a McpHelpers validator; the validator marker has stopped matching");
        Assert.True(helpersFollowed >= 50, $"only {helpersFollowed} `return Helper(…)` calls were followed into a same-file method; the helper walk has stopped resolving them");

        Assert.True(unshared.Count == 0,
            "these tools pass a value through `if (x != null) return x;` that no shared producer built — a refusal is McpHelpers.Refusal(parameter, sentence), built where the sentence is, so the wire carries `invalid` and hints.parameter: "
            + string.Join("; ", unshared));

        Assert.True(handBuilt.Count == 0,
            "these tools build the `invalid` envelope by hand beside McpHelpers.Refusal — route them through it so every refusal carries hints.parameter: "
            + string.Join("; ", handBuilt));

        var rostered = BareSentenceReturns.Select(r => (r.File, r.Tool, r.Sentence)).ToList();
        var newBare = bare.Where(b => !rostered.Any(r => r.File == b.File && r.Tool == b.Tool && b.Sentence.StartsWith(r.Sentence, StringComparison.Ordinal))).ToList();
        var gone = rostered.Where(r => !bare.Any(b => b.File == r.File && b.Tool == r.Tool && b.Sentence.StartsWith(r.Sentence, StringComparison.Ordinal))).ToList();
        Assert.True(newBare.Count == 0,
            "a tool body returns a bare sentence that is not a rostered miss — a refusal is McpHelpers.Refusal(parameter, sentence); a miss is McpHelpers.Status(word, sentence): ["
            + string.Join("; ", newBare.Select(b => $"{b.File} {b.Tool}: \"{b.Sentence}\"")) + "]");
        Assert.True(gone.Count == 0,
            "rostered bare sentences no longer returned (the miss lane reached them — shrink the roster): ["
            + string.Join("; ", gone.Select(g => $"{g.File} {g.Tool}: \"{g.Sentence}\"")) + "]");
    }

    /// <summary>
    /// The failure word has ONE producer. <c>McpHelpers.Status("error", …)</c> appears nowhere in either SKU's
    /// tool sources — not in a catch (the catch census holds that) and not above one either, which is where
    /// the nine PostgreSQL refusals sat and answered HTTP 500 for a bad <c>limit</c> — and exactly once in
    /// <c>McpHelpers.cs</c>, inside <c>FormatError</c>. Read off the strings-KEPT text because the literal is the
    /// evidence; a refusal that wants a status word has <c>Refusal</c>, and a catch has <c>FormatError</c>.
    /// </summary>
    [Fact]
    public void TheFailureWord_HasOneProducer_OnBothSkus()
    {
        var offenders = new List<string>();
        var files = 0;
        foreach (var (file, source) in ToolSources())
        {
            files++;
            foreach (Match hit in Regex.Matches(StripComments(source), @"\bStatus\(\s*""error"""))
            {
                offenders.Add($"{file}: {EnclosingMember(CSharpSourceWalker.StripCommentsAndStrings(source), hit.Index)}");
            }
        }

        Assert.True(files >= 60, $"only {files} tool sources were read across both SKUs");
        Assert.True(offenders.Count == 0,
            "the failure word is built by hand outside McpHelpers.FormatError — a caught exception is FormatError(tool, ex); a refusal is Refusal(parameter, sentence): " + string.Join("; ", offenders));

        /* And in the helper itself, each word is spelled into Status exactly once: FormatError's and Refusal's. */
        var helpers = StripComments(File.ReadAllText(RepoFile.PathTo("PerformanceMonitor.Common/Mcp/McpHelpers.cs")));
        Assert.Single(Regex.Matches(helpers, @"\bStatus\(\s*""error"""));
        Assert.Single(Regex.Matches(helpers, @"\bStatus\(\s*""invalid"""));
    }

    /// <summary>
    /// The one refusal shape, EXECUTED rather than read off a doc comment: <c>Refusal</c>'s output parses, its
    /// <c>status</c> is <c>invalid</c>, its <c>message</c> is the sentence untouched, its <c>hints.parameter</c> is
    /// the parameter verbatim, and it is byte-for-byte what <c>Status("invalid", sentence, new { parameter })</c>
    /// builds. Then every shared validator, driven past its bound, hands back exactly that envelope for exactly
    /// its parameter — so the four hundred pass-through sites the census above walks are carrying the shape
    /// this fact proves, not a look-alike. The recognizer fires on the producer and on the write tools'
    /// <c>Outcome("invalid", …)</c> bytes, and not on its neighbours.
    /// </summary>
    [Fact]
    public void TheOneRefusalShape_IsTheEnvelope_ExecutedThroughRefusal_AndEveryValidatorBuildsIt()
    {
        var wire = McpHelpers.Refusal("hours_back", "Invalid hours_back value '0'.");

        using var envelope = JsonDocument.Parse(wire);
        Assert.Equal("invalid", envelope.RootElement.GetProperty("status").GetString());
        Assert.Equal("Invalid hours_back value '0'.", envelope.RootElement.GetProperty("message").GetString());
        Assert.Equal("hours_back", envelope.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Equal(3, envelope.RootElement.EnumerateObject().Count());
        Assert.Equal(McpHelpers.Status("invalid", "Invalid hours_back value '0'.", new { parameter = "hours_back" }), wire);

        Assert.StartsWith(McpHelpers.InvalidEnvelopePrefix, wire, StringComparison.Ordinal);
        Assert.True(McpHelpers.IsRefusalEnvelope(wire));
        Assert.True(McpHelpers.IsRefusalEnvelope("  " + wire));
        Assert.True(McpHelpers.IsRefusalEnvelope(JsonSerializer.Serialize(new { status = "invalid", message = "rule_id is required." }, McpHelpers.JsonOptions)));
        Assert.False(McpHelpers.IsRefusalEnvelope(McpHelpers.FormatError("get_x", new InvalidOperationException("boom"))));
        Assert.False(McpHelpers.IsRefusalEnvelope(McpHelpers.Status("empty", "nothing")));
        Assert.False(McpHelpers.IsRefusalEnvelope("{\"status\":\"invalid_count\",\"message\":\"x\"}"));
        Assert.False(McpHelpers.IsRefusalEnvelope("Invalid hours_back value '0'."));
        Assert.False(McpHelpers.IsRefusalEnvelope(null));
        Assert.False(McpHelpers.IsRefusalEnvelope(""));

        /* Every validator, past its bound, IS Refusal(its parameter, its sentence) — and the sentence is the
           pre-#3739 text, so the fragments the tool tests pin survive inside message. */
        AssertRefusal(McpHelpers.ValidateHoursBack(0), "hours_back", "Invalid hours_back value '0'. Must be a positive integer (1-168).");
        AssertRefusal(McpHelpers.ValidateHoursBack(169), "hours_back", "hours_back value '169' exceeds maximum of 168 hours (7 days). Use a smaller value.");
        AssertRefusal(McpHelpers.ValidateDaysBack(0, 60), "days_back", "Invalid days_back value '0'. Must be a positive integer (1-60).");
        AssertRefusal(McpHelpers.ValidateTop(0), "limit", "Invalid limit value '0'. Must be a positive integer (1-1000).");
        AssertRefusal(McpHelpers.ValidateTop(1001, "top"), "top", "top value '1001' exceeds maximum of 1000. Use a smaller value.");
        AssertRefusal(McpHelpers.ValidateWindow(0, null, out _), "hours_back", "Invalid hours_back value '0'. Must be a positive integer (1-168).");
        AssertRefusal(McpHelpers.ValidateWindow(4, "last tuesday", out _), "as_of", "Invalid as_of value 'last tuesday'. Expected an ISO-8601 UTC instant: '2026-08-18T14:30:00Z', '2026-08-18T14:30:00' (read as UTC), '2026-08-18T16:30:00+02:00', or '2026-08-18' for midnight UTC.");
        AssertRefusal(McpHelpers.ValidateUncappedWindow(-24, null, out _), "hours_back", "Invalid hours_back value '-24'. Must be a positive integer — a negative or zero window has no meaning and is refused rather than read as its absolute value. This read has no upper bound on hours_back.");
        AssertRefusal(McpHelpers.ResolveAsOf("2099-01-01T00:00:00Z", out _), "as_of", "as_of value '2099-01-01T00:00:00Z' is in the future. A stored read cannot cover data that has not been collected yet; anchor at or before now (UTC).");
        AssertRefusal(McpHelpers.ParseSummaryDate("01/02/2026", out _), "summary_date", "Invalid summary_date value '01/02/2026'. Expected an ISO-8601 calendar date, yyyy-MM-dd (e.g. 2026-07-09), read as a UTC day. Other spellings — including 07/09/2026 — are refused rather than guessed at, because 01/02/2026 reads as two different days depending on who wrote it.");
        AssertRefusal(McpHelpers.ValidateChoice("perfmon", new[] { "dmv", "xe" }, "source"), "source", "Invalid source value 'perfmon'. Accepted values: dmv, xe. Omit it for all.");
        AssertRefusal(McpHelpers.ValidateMinMs(-1, "min_duration_ms"), "min_duration_ms", "Invalid min_duration_ms value '-1'. A duration floor cannot be negative — use 0 to admit every row, or omit it entirely.");

        /* And the producers outside McpHelpers that the census accepts as shared: the watch-state validator, the
           trend-width resolvers (#3897) — out of range, over the read's cap (naming the width that fits), and a
           width the hourly rollup cannot serve — and the resolver. */
        AssertRefusal(DarlingFleetSweepEndpoints.ValidateWatchState("garbage", "watch_state"), "watch_state", "Unknown state 'garbage'. Legal values: pending, open, carried, closed; omit for open + carried.");
        AssertRefusal(TrendBuckets.Resolve(24, 0, 1, TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), out _), "bucket_minutes", "Invalid bucket_minutes value '0'. Must be between 1 and 1440 (one day), or omitted to size the points to the window.");
        AssertRefusal(TrendBuckets.ValidateWidth(1441), "bucket_minutes", "Invalid bucket_minutes value '1441'. Must be between 1 and 1440 (one day), or omitted to size the points to the window.");
        AssertRefusal(TrendBuckets.Resolve(24, 1, 6, TrendBudget.Mcp(TrendBuckets.FileIoMaxPoints), out _), "bucket_minutes", "bucket_minutes 1 over 24 hour(s) across 6 series is up to 8646 points, over this read's 1000-point cap. Use bucket_minutes 9 or wider, or narrow hours_back (as_of moves the window) for finer points.");
        AssertRefusal(TrendBuckets.RequireWholeHours(30, 4), "bucket_minutes", "bucket_minutes 30 cannot be served for this window: it reaches past the raw tier's 4-day retention, so the hourly rollup answers it, and that rollup's points are whole hours. Use a multiple of 60, or a window inside the last 4 days for finer points.");
        var (_, miss) = DarlingServerResolver.ResolveOrError(new[] { new DarlingServerResolver.RegisteredServer(1, "box-a", null) }, "box-b", DarlingPeerDirectory.Snapshot.Empty);
        AssertRefusal(miss, "server_name", "Could not resolve server. Available servers:\nbox-a");
    }

    private static void AssertRefusal(string? wire, string parameter, string sentence)
    {
        Assert.NotNull(wire);
        Assert.Equal(McpHelpers.Refusal(parameter, sentence), wire);
        Assert.True(McpHelpers.IsRefusalEnvelope(wire));
        Assert.Equal(sentence, McpHelpers.ErrorMessageOf(wire!));
    }

    /// <summary>The guarded pass-through idiom, witnessed on the shapes the tree uses and not on its
    /// neighbours: the guard and the return must name the same value.</summary>
    [Theory]
    [InlineData("        if (error != null) return error;", true)]
    [InlineData("        if (validation is not null) return validation;", true)]
    [InlineData("        if (validation != null)\n        {\n            return validation;\n        }", true)]
    [InlineData("            if (c.EarlyReturn != null) return c.EarlyReturn;", true)]
    [InlineData("        if (limitError != null) return McpHelpers.Status(\"error\", limitError);", false)]
    [InlineData("        if (error != null) return new Collected<T>(error, 0, \"\", new List<T>(), 0, null);", false)]
    [InlineData("        if (rows.Count != 0) return rows;", false)]
    public void TheGuardedPassThroughMatcher_FiresOnTheIdiom_AndNotOnItsNeighbours(string code, bool expected) =>
        Assert.Equal(expected, GuardedPassThrough.IsMatch(code));

    /// <summary>The producer walk, witnessed: a destructured resolver call, a plain validator assignment, a
    /// re-assignment (the LAST one before the guard wins), a <c>??</c> chain, and a value no shared producer
    /// built.</summary>
    [Fact]
    public void TheProducerWalk_FindsTheLastAssignment_AndClassifiesIt()
    {
        const string Body = """
                var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
                if (error != null) return error;

                var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
                if (validation != null) return validation;
                validation = McpHelpers.ValidateTop(top, "top");
                if (validation != null) return validation;

                var chained = McpHelpers.ValidateWindow(hours_back, as_of, out var end) ?? McpHelpers.ValidateTop(limit);
                if (chained != null) return chained;

                var homemade = group_by == "x" ? null : "group_by must be x";
                if (homemade != null) return homemade;

                var mixed = McpHelpers.ValidateWindow(hours_back, as_of, out var anchor) ?? ValidateBucketMinutes(bucket_minutes);
                if (mixed != null) return mixed;
            """;
        var code = CSharpSourceWalker.StripCommentsAndStrings(Body);
        var guards = GuardedPassThrough.Matches(code);
        Assert.Equal(6, guards.Count);

        Assert.Matches(SharedPassThroughProducer, LastAssignmentOf(code, "error", guards[0].Index)!);
        Assert.Contains("ValidateWindow", LastAssignmentOf(code, "validation", guards[1].Index)!, StringComparison.Ordinal);
        Assert.Contains("ValidateTop", LastAssignmentOf(code, "validation", guards[2].Index)!, StringComparison.Ordinal);
        Assert.Matches(SharedPassThroughProducer, LastAssignmentOf(code, "chained", guards[3].Index)!);
        var homemade = LastAssignmentOf(code, "homemade", guards[4].Index);
        Assert.NotNull(homemade);
        Assert.DoesNotMatch(SharedPassThroughProducer, homemade!);

        /* The #3897 case: the whole chain matches the producer pattern on its shared first operand, and only the
           operand walk sees that its second one is hand-built. */
        var mixed = LastAssignmentOf(code, "mixed", guards[5].Index)!;
        Assert.Matches(SharedPassThroughProducer, mixed);
        var operands = CoalesceOperands(mixed);
        Assert.Equal(2, operands.Count);
        Assert.Matches(SharedPassThroughProducer, operands[0]);
        Assert.DoesNotMatch(SharedPassThroughProducer, operands[1]);
        Assert.All(CoalesceOperands(LastAssignmentOf(code, "chained", guards[3].Index)!), o => Assert.Matches(SharedPassThroughProducer, o));
    }

    /// <summary>A string literal opens the expression: <c>"</c>, <c>$"</c>, <c>@"</c>, <c>$@"</c> or <c>@$"</c>.</summary>
    private static bool StartsWithStringLiteral(string expression) =>
        Regex.IsMatch(expression, @"^(?:\$@|@\$|\$|@)?""");

    /// <summary>
    /// #3962: what a tool's <c>return Helper(…)</c> hands its caller, one call deep. For every such return in
    /// <paramref name="span"/> whose callee is a method DECLARED in the same file (<paramref name="source"/>), each
    /// of that method's return expressions — its expression body, or every <c>return</c> in its block body — split
    /// at depth-zero <c>?</c> / <c>:</c> into its ternary arms, because <c>cond ? null : $"…"</c> is the shape a bare
    /// sentence hid in (get_query_heatmap's bucket_minutes refusal, until #3897). Arms are read off the
    /// comments-blanked, literals-kept text at the offsets the strings-blanked code gives, whitespace collapsed.
    /// Same file only: a method in another file is a shared producer, which the pass-through sweep holds to its
    /// own list; and one level only, which is where every helper on the tree sits.
    /// </summary>
    private static IEnumerable<(string Helper, string Arm)> HelperReturnArms(string span, string source)
    {
        var spanCode = CSharpSourceWalker.StripCommentsAndStrings(span);
        var (sourceCode, sourceText) = Stripped(source);

        foreach (var name in Regex.Matches(spanCode, @"\breturn\s+(?:await\s+)?(?<name>[A-Za-z_]\w*)\s*\(")
                     .Select(m => m.Groups["name"].Value).Distinct(StringComparer.Ordinal))
        {
            var declaration = Regex.Match(
                sourceCode,
                @"\b(?:private|internal|public|protected)\s+(?:static\s+)?(?:async\s+)?[\w<>?,\[\]\s]+?\s" + Regex.Escape(name) + @"\s*\(");
            if (!declaration.Success)
            {
                continue;
            }

            var close = MatchingClose(sourceCode, declaration.Index + declaration.Length - 1);
            var after = close + 1;
            while (after < sourceCode.Length && char.IsWhiteSpace(sourceCode[after]))
            {
                after++;
            }

            var expressions = new List<(int Start, int End)>();
            if (sourceCode.AsSpan(after).StartsWith("=>", StringComparison.Ordinal))
            {
                expressions.Add((after + 2, EndOfStatement(sourceCode, after + 2)));
            }
            else if (after < sourceCode.Length && sourceCode[after] == '{')
            {
                var bodyEnd = MatchingClose(sourceCode, after);
                foreach (Match keyword in Regex.Matches(sourceCode[after..bodyEnd], @"\breturn\b"))
                {
                    var start = after + keyword.Index + keyword.Length;
                    expressions.Add((start, EndOfStatement(sourceCode, start)));
                }
            }

            foreach (var (start, end) in expressions)
            {
                foreach (var (armStart, armEnd) in ResultArms(sourceCode, start, end))
                {
                    yield return (name, Regex.Replace(sourceText[armStart..armEnd], @"\s+", " ").Trim());
                }
            }
        }
    }

    /// <summary>A file's strings-blanked code and its comments-blanked, literals-kept text, walked once per file text
    /// rather than once per tool the file declares: <see cref="ToolMethods"/> hands every tool of a file the same
    /// source string, so the walk is keyed on that instance.</summary>
    private static readonly ConditionalWeakTable<string, Tuple<string, string>> StrippedSources = new();

    private static (string Code, string Text) Stripped(string source)
    {
        var entry = StrippedSources.GetValue(source, text =>
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(text);
            return Tuple.Create(code, StripCommentsPreservingLength(text, code));
        });
        return (entry.Item1, entry.Item2);
    }

    /// <summary>The index of the bracket closing the one at <paramref name="open"/>, on strings-blanked code.</summary>
    private static int MatchingClose(string code, int open)
    {
        var depth = 0;
        for (var i = open; i < code.Length; i++)
        {
            if (code[i] is '(' or '[' or '{')
            {
                depth++;
            }
            else if (code[i] is ')' or ']' or '}' && --depth == 0)
            {
                return i;
            }
        }

        return code.Length - 1;
    }

    /// <summary>The <c>;</c> ending the statement that starts at <paramref name="start"/>, at bracket depth zero.</summary>
    private static int EndOfStatement(string code, int start)
    {
        var depth = 0;
        for (var i = start; i < code.Length; i++)
        {
            if (code[i] is '(' or '[' or '{')
            {
                depth++;
            }
            else if (code[i] is ')' or ']' or '}')
            {
                depth--;
            }
            else if (code[i] == ';' && depth == 0)
            {
                return i;
            }
        }

        return code.Length;
    }

    /// <summary>The RESULT arms of the expression between <paramref name="start"/> and <paramref name="end"/>: for
    /// <c>c ? a : b</c> the arms of <c>a</c> and of <c>b</c> (so a chain <c>c1 ? a : c2 ? b : d</c> gives a, b and d,
    /// and no condition is ever an arm); for anything else, the expression itself. Read at bracket depth zero on
    /// strings-blanked code, so a <c>?</c> or <c>:</c> inside a literal or a call cannot split it, and <c>?.</c>,
    /// <c>?[</c> and <c>??</c> are not the conditional operator.</summary>
    private static IEnumerable<(int Start, int End)> ResultArms(string code, int start, int end)
    {
        var question = -1;
        var depth = 0;
        for (var i = start; i < end && question < 0; i++)
        {
            depth += code[i] switch { '(' or '[' or '{' => 1, ')' or ']' or '}' => -1, _ => 0 };
            if (depth == 0 && IsConditionalQuestion(code, i, end))
            {
                question = i;
            }
        }

        /* The ':' that closes this '?', past any conditional nested in its first arm. */
        var colon = -1;
        var nested = 0;
        depth = 0;
        for (var i = question + 1; question >= 0 && i < end && colon < 0; i++)
        {
            depth += code[i] switch { '(' or '[' or '{' => 1, ')' or ']' or '}' => -1, _ => 0 };
            if (depth != 0)
            {
                continue;
            }

            if (IsConditionalQuestion(code, i, end))
            {
                nested++;
            }
            else if (code[i] == ':' && code[i - 1] != ':' && (i + 1 >= end || code[i + 1] != ':'))
            {
                if (nested == 0)
                {
                    colon = i;
                }
                else
                {
                    nested--;
                }
            }
        }

        if (colon < 0)
        {
            yield return (start, end);
            yield break;
        }

        foreach (var arm in ResultArms(code, question + 1, colon))
        {
            yield return arm;
        }

        foreach (var arm in ResultArms(code, colon + 1, end))
        {
            yield return arm;
        }
    }

    /// <summary>The conditional operator's <c>?</c> — not <c>?.</c>, <c>?[</c> or either half of <c>??</c>.</summary>
    private static bool IsConditionalQuestion(string code, int i, int end) =>
        code[i] == '?'
        && (i + 1 >= end || code[i + 1] is not ('.' or '[' or '?'))
        && code[i - 1] != '?';

    /// <summary>The operands of a <c>??</c> chain at parenthesis depth zero — the whole expression when it has
    /// none. Read off code whose strings and comments are blanked, so a <c>??</c> inside a literal cannot split it;
    /// a conditional's single <c>?</c> and a null-conditional <c>?.</c> are not the operator.</summary>
    private static List<string> CoalesceOperands(string expression)
    {
        var operands = new List<string>();
        var depth = 0;
        var start = 0;
        for (var i = 0; i < expression.Length; i++)
        {
            var c = expression[i];
            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
            }
            else if (c == '?' && depth == 0 && i + 1 < expression.Length && expression[i + 1] == '?')
            {
                operands.Add(expression[start..i]);
                start = i + 2;
                i++;
            }
        }

        operands.Add(expression[start..]);
        return operands;
    }

    /// <summary>
    /// #3962's walk, witnessed on the shape it exists for — the heatmap's pre-#3897 refusals, a bare sentence in a
    /// ternary arm of an expression-bodied helper and in a block-bodied one — and not on its neighbours: a helper
    /// that builds the refusal, a miss wrapped in the shared builder (its sentence is an ARGUMENT, not an arm), a
    /// null arm, a method in another file, and a <c>?.</c> / <c>??</c> that is not a conditional.
    /// </summary>
    [Fact]
    public void TheHelperWalk_ReadsASameFileHelpersReturnArms_AndNothingElse()
    {
        const string Source = """
            public sealed class FixtureTools
            {
                public static string GetFixture(int bucket_minutes, string? metric)
                {
                    if (bucket_minutes < 1) return ValidateBucketMinutes(bucket_minutes);
                    if (metric is null) return InvalidMetric(metric!);
                    if (bucket_minutes > 5) return Refused(bucket_minutes);
                    if (bucket_minutes > 4) return Quiet(bucket_minutes);
                    if (bucket_minutes > 3) return Chained(metric);
                    return OtherFile.Build();
                }

                private static string? ValidateBucketMinutes(int bucket_minutes) =>
                    bucket_minutes >= 1 ? null : $"Invalid bucket_minutes value '{bucket_minutes}'. Must be between 1 and 1440.";

                private static string InvalidMetric(string metric)
                {
                    // a comment with "quotes" and a ? mark : and a colon
                    return $"Invalid metric '{metric}'.";
                }

                private static string Refused(int value) => McpHelpers.Refusal("bucket_minutes", $"Invalid bucket_minutes value '{value}'.");

                private static string Quiet(int value) =>
                    value > 0 ? McpHelpers.Status("empty", $"Nothing in the last {value} hour(s).") : McpHelpers.Status("unavailable", "never");

                private static string Chained(string? metric) => metric?.Length > 3 ? metric ?? "x" : McpHelpers.Refusal("metric", "bad");
            }
            """;
        var source = Source.Replace("\r\n", "\n", StringComparison.Ordinal);
        var span = source[source.IndexOf("public static string GetFixture", StringComparison.Ordinal)..(source.IndexOf("return OtherFile.Build();", StringComparison.Ordinal) + 40)];

        var arms = HelperReturnArms(span, source).ToList();
        var bareArms = arms.Where(a => StartsWithStringLiteral(a.Arm)).ToList();

        Assert.Equal(new[] { "InvalidMetric", "ValidateBucketMinutes" }, bareArms.Select(a => a.Helper).Order(StringComparer.Ordinal).ToArray());
        Assert.Contains(bareArms, a => a.Arm.StartsWith("$\"Invalid bucket_minutes value", StringComparison.Ordinal));
        Assert.Contains(bareArms, a => a.Arm.StartsWith("$\"Invalid metric", StringComparison.Ordinal));

        /* The neighbours are read — the walk resolved them — and none of their arms is a bare sentence. */
        Assert.Contains(arms, a => a.Helper == "Refused" && a.Arm.StartsWith("McpHelpers.Refusal(", StringComparison.Ordinal));
        Assert.Equal(2, arms.Count(a => a.Helper == "Quiet"));
        Assert.All(arms.Where(a => a.Helper == "Quiet"), a => Assert.StartsWith("McpHelpers.Status(", a.Arm, StringComparison.Ordinal));
        Assert.Equal(new[] { "metric ?? \"x\"", "McpHelpers.Refusal(\"metric\", \"bad\")" }, arms.Where(a => a.Helper == "Chained").Select(a => a.Arm).ToArray());
        Assert.DoesNotContain(arms, a => a.Helper == "Build");
        Assert.Equal(new[] { "null", "$\"Invalid bucket_minutes value '{bucket_minutes}'. Must be between 1 and 1440.\"" }, arms.Where(a => a.Helper == "ValidateBucketMinutes").Select(a => a.Arm).ToArray());
    }

    /// <summary>The right-hand side of the LAST assignment to <paramref name="name"/> that precedes
    /// <paramref name="before"/> in <paramref name="code"/> (strings and comments blanked): a declaration, a
    /// re-assignment, or a tuple destructure naming it in any position. Null when none precedes it.</summary>
    private static string? LastAssignmentOf(string code, string name, int before)
    {
        var escaped = Regex.Escape(name);
        var assignment = new Regex(
            @"(?:\bvar\s+" + escaped + @"|(?<![\w.])" + escaped + @"|\bvar\s*\([^()]*\b" + escaped + @"\b[^()]*\))\s*=(?!=)\s*([^;]+);");
        string? last = null;
        foreach (Match m in assignment.Matches(code[..before]))
        {
            last = Regex.Replace(m.Groups[1].Value, @"\s+", " ").Trim();
        }

        return last;
    }

    /// <summary>Every <c>return …;</c> expression in a tool span, located on the stripped text (so a
    /// <c>return</c> inside prose or a literal is not one, and a <c>;</c> inside a literal does not end one) and
    /// read off a LENGTH-PRESERVING comments-only strip of the same span, so the offsets line up and the literal
    /// that IS the evidence survives. Whitespace collapsed.</summary>
    private static IEnumerable<string> ReturnExpressions(string span, string code)
    {
        var text = StripCommentsPreservingLength(span, code);
        foreach (Match keyword in Regex.Matches(code, @"\breturn\b"))
        {
            var end = code.IndexOf(';', keyword.Index);
            if (end < 0)
            {
                continue;
            }

            var expression = text[(keyword.Index + keyword.Length)..end];
            yield return Regex.Replace(expression, @"\s+", " ").Trim();
        }
    }

    /// <summary>Comments blanked, code AND string literals kept, every offset unchanged: the strings-blanked
    /// text from the walker with each literal's bytes written back at the offset the walker reports for it.
    /// Built on the walker's two entry points rather than a regex of its own so a delimiter the walker
    /// understands cannot desynchronise this from <paramref name="code"/>.</summary>
    private static string StripCommentsPreservingLength(string span, string code)
    {
        var buffer = code.ToCharArray();
        foreach (var (start, body) in CSharpSourceWalker.StringLiteralBodies(span))
        {
            /* The opening delimiter sits just before the body; the walker blanks delimiters with the text, so
               restore from one before the body to one past it — clamped, because a raw literal's delimiter is
               longer than one quote and this only needs the FIRST quote back for the StartsWith tests. */
            for (var i = Math.Max(0, start - 2); i < start; i++)
            {
                if (span[i] is '"' or '$' or '@')
                {
                    buffer[i] = span[i];
                }
            }

            var to = Math.Min(span.Length, start + body.Length + 1);
            for (var i = start; i < to; i++)
            {
                buffer[i] = span[i];
            }
        }

        return new string(buffer);
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
        /* The sentence rides the `invalid` envelope since #3739; the pin is about the words, read back out. */
        Assert.Equal("Invalid days_back value '0'. Must be a positive integer (1-60).", McpHelpers.ErrorMessageOf(McpHelpers.ValidateDaysBack(0, 60)!));
        Assert.Equal("Invalid days_back value '-7'. Must be a positive integer (1-60).", McpHelpers.ErrorMessageOf(McpHelpers.ValidateDaysBack(-7, 60)!));
        Assert.Equal("Invalid days_back value '61'. Must be a positive integer (1-60).", McpHelpers.ErrorMessageOf(McpHelpers.ValidateDaysBack(61, 60)!));
        Assert.Equal("Invalid days_back value '367'. Must be a positive integer (1-366).", McpHelpers.ErrorMessageOf(McpHelpers.ValidateDaysBack(367, McpHelpers.MaxDailySummaryDaysBack)!));
        /* The sentence shape is ValidateHoursBack's first sentence, word for word but for the parameter. */
        Assert.StartsWith("Invalid hours_back value '0'. Must be a positive integer (1-", McpHelpers.ErrorMessageOf(McpHelpers.ValidateHoursBack(0)!), StringComparison.Ordinal);
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
    /// already holds, and <c>IndexUsageMatchCountSql</c> is a <c>COUNT(*)</c> over the same anchor with no row
    /// to stamp.
    ///
    /// <para><b>This roster GREW once, in #3879, and #3880 shrank it back by ruling.</b> It is written
    /// shrink-only, on the reasoning that a read either projects its anchor or is waiting for the A10 lane to
    /// stamp it. <c>IndexLockingSql</c> was invisible to this census before #3878 for a reason that was itself
    /// the defect: its anchor was a per-<c>database_name</c> <c>MAX(collection_time)</c> group inside a CTE,
    /// and this scan deliberately ignores an anchor below depth zero because such an anchor picks a SET rather
    /// than a snapshot. That was exactly true of the old shape — it picked one row per name the store had ever
    /// seen, which is why a database renamed away was returned forever (#3876). Anchoring it on the server's
    /// newest capture made it a snapshot read for the first time, so the census saw it and asked the standing
    /// question, and #3879's lane answered with a roster row on the pre-existing-debt argument: its two
    /// neighbours are unstamped too, and all three would leave together when A10 stamped the family.
    /// <b>Erik ruled the other way</b> (#3880): a read that has just become one honest instant can say WHICH
    /// instant, so stamp it rather than roster it. <c>IndexLockingSql</c> projects <c>ios.collection_time</c>,
    /// <c>get_object_locking</c> publishes <c>captured_at</c> on both SKUs, and this list is back to the two
    /// entries it held before #3879 — the shrink-only direction restored, with the growth episode kept on the
    /// record here rather than quietly erased.</para>
    ///
    /// <para>The two survivors are not the same kind of gap. <c>IndexUsageSql</c> is a genuine A10 residual:
    /// projecting its anchor and stamping <c>get_index_usage</c> is the same small edit #3880 made next door,
    /// and it is available whenever the family is taken. <c>IndexUsageMatchCountSql</c> is structural — a
    /// scalar <c>COUNT(*)</c> has no row for a stamp to ride on, so it leaves this list only if it ever
    /// returns the anchor beside its count.</para>
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
    /// window). The second is spelled <c>&lt;bound&gt;_truncated</c>, observed the same way. <b>The WINDOW
    /// floor is the other member of this class (#3653 item 17):</b> on the #2364 / #2353 trend family
    /// (<c>get_query_trend</c>, the duration-trend trio, both SKUs) and on <c>get_query_store_top</c> the bound
    /// is the store's REACH — the served series begins later than the requested start because the tier that
    /// answered no longer holds the window's head — published beside <c>effective_start</c> /
    /// <c>effective_hours_back</c> and observed off the head of the served series against the shared
    /// ninety-minute slack (<c>DarlingTrendReader.TruncationSlack</c> / <c>McpQueryTools.TruncationSlack</c>),
    /// not off a <c>cap + 1</c> fetch, because no cap is involved: nothing the caller sends changes it. Until
    /// #3653 it rode under the page dialect's <c>truncated</c>, and a client that had learned that spelling read
    /// "raise the limit" off a fact no limit changes. It is <c>window_truncated</c> now, and the census below
    /// holds the two facts apart rather than trusting the roster alone.</item>
    /// <item><b>The prose beside the flag</b> — <see cref="CutNoteKeys"/>: <c>truncation_note</c> is the
    /// <c>*_note</c> idiom every disclosure on the surface uses, null when nothing was cut. Three tools carry
    /// it; on <c>get_query_store_top</c> the flag it explains is <c>window_truncated</c>, the #2364 WINDOW floor
    /// (the store's raw retention did not reach the whole window) — the note keeps its name because it is the
    /// prose for THIS flag and <c>*_note</c> is the idiom, not because the flag is a page cut.</item>
    /// <item><b>Source-side cuts</b> — <see cref="SourceSideCutKeys"/>: the capture, the chain or the text was
    /// cut BEFORE the store, by the collector. They keep their names because they are true and different: a
    /// caller can do nothing about them by re-paging, and folding them into <c>truncated</c> would tell that
    /// caller to raise a limit that changes nothing.</item>
    /// <item><b>A field-level response-budget preview</b> — <see cref="FieldPreviewCutKeys"/>: #4198 sizes
    /// each tool's DEFAULT answer under the shared 32 KB <c>McpResponseBudget.DefaultBytes</c> by previewing
    /// one wide field (query text, a plan fragment, a deadlock graph, an error message) rather than the page —
    /// unlike a source-side cut, a caller CAN get the rest, with an opt-in argument (<c>get_deadlock_detail</c>'s
    /// <c>full_graph</c>; <c>get_active_queries</c> and <c>get_store_query_stats</c> both take
    /// <c>full_text</c>, the same name — get_active_queries' own was renamed from <c>full_query_text</c> to
    /// match; <c>get_collection_log</c> uses <c>full_text</c> for its <c>error_message</c> preview).</item>
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
        ("window_truncated", ["DarlingMcpDataTools.cs", "DarlingMcpQueryStoreClutterTools.cs", "DarlingMcpTrendTools.cs", "McpQueryTools.cs"],
            "the #2364 / #2353 WINDOW floor (#3653 item 17): the tier that answered did not hold the whole requested window, so the served series begins later than asked — beside effective_start / effective_hours_back, observed off the served head against the shared ninety-minute TruncationSlack, no cap involved; get_query_trend and get_query_store_top write it in their initializers, the duration-trend trio through TrendDisclosure.WriteTo (Darling) and WriteDisclosure (Lite); get_query_store_clutter (#3797) writes it in its initializer for its plan-churn and wait arms, which read the same raw tier get_query_store_top does, off the same window-floor read and the same ninety-minute slack"),
        ("findings_truncated", ["DarlingMcpTools.cs", "McpAnalysisTools.cs"],
            "#4198: get_analysis_findings' GROUP PAGE cut — limit caps the collapsed per-chain groups returned (default 18), independent of the pre-existing truncated above (the raw WindowCoveringLimit occurrence read, beside truncation_note): truncated warns occurrence stats may under-report, findings_truncated warns other diagnostic chains exist but are not on this page at all"),
    ];

    public static readonly (string Key, string[] Files, string WhatItExplains)[] CutNoteKeys =
    [
        ("truncation_note", ["DarlingMcpDataTools.cs", "DarlingMcpQueryStoreClutterTools.cs", "DarlingMcpTools.cs", "McpAnalysisTools.cs", "McpQueryTools.cs"],
            "the prose beside the flag: on get_analysis_findings (both SKUs) beside truncated, the WindowCoveringLimit read cap observed off a cap + 1 fetch; on get_query_store_top and get_query_store_clutter beside window_truncated, the #2364 window floor — the store's raw retention did not reach the whole requested window (on the clutter view, for the two arms that read the raw tier); on Lite's get_top_queries_by_cpu / get_top_procedures_by_cpu / get_query_store_top (#4231), the same window-floor note, from LocalDataService.GetQueryWindowFloorAsync"),
        ("findings_truncated_note", ["DarlingMcpTools.cs", "McpAnalysisTools.cs"],
            "#4198: the prose beside findings_truncated — how many diagnostic chains were active in the window and that raising limit or narrowing hours_back would show more of them"),
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

    public static readonly (string Key, string[] Files, string WhatWasCut)[] FieldPreviewCutKeys =
    [
        ("confidence_basis_truncated", ["DarlingMcpTools.cs", "McpAnalysisTools.cs"],
            "#4198: get_analysis_findings' confidence_basis — a near-fixed methodology sentence repeated on every finding (StoryConfidence.DescribeBasis) — previews to 160 characters (FindingTextPreviewLength) by default; full_text returns it whole"),
        ("advice_truncated", ["DarlingMcpTools.cs", "McpAnalysisTools.cs"],
            "#4198: get_analysis_findings' advice.investigation / advice.remediation — free prose repeated on every finding — preview to 160 characters by default; full_text returns both whole. remediation_command is never previewed at any setting, so it carries no *_truncated key of its own"),
        ("deadlock_graph_xml_truncated", ["DarlingMcpBlockingTools.cs", "McpBlockingTools.cs"],
            "#4198: get_deadlock_detail's own wide field — deadlock_graph_xml is a 2000-character preview by default (a busy production store measured 120,454 bytes for 3 graphs), full_graph or a dedup_key call gets the whole XML"),
        ("query_text_truncated", ["DarlingMcpDataTools.cs", "DarlingMcpPlanCorrectionTools.cs", "DarlingMcpQueryStoreRegressionTools.cs", "DarlingMcpSessionTools.cs", "McpPlanCorrectionTools.cs", "McpQueryTools.cs", "McpSessionTools.cs"],
            "#4198: query_text is previewed at read time by four tools: get_active_queries at 500 chars (full_text gets the whole text; a synthetic 50-row page measured 81,489 bytes), get_query_store_regressions at 240 chars (full_text opts back in; a busy production store measured 211 KB at default arguments), get_plan_corrections at 150 chars (full_text gets the whole text; the full text IS in the store, not collector-capped), get_query_store_top at 400 chars (full_text gets the whole text; query_store_stats.query_text is not collector-capped either). get_query_store_top's MCP signature forwards to an internal previewLength overload so the web viewer can keep the OLD 2000-char cap that field already had, rather than switching to full text the way get_deadlock_detail's never-capped field does"),
        ("error_message_truncated", ["DarlingMcpDataTools.cs", "McpHealthTools.cs"],
            "#4198: get_collection_log's own wide field — error_message is a 500-character preview by default (a seeded store measured 90,514 bytes for 200 rows at the old 200-row default), full_text opts back into the whole (up to 4000-character, DarlingObservability.LogCollectionAsync's own write-time ceiling) field"),
        ("last_error_truncated", ["DarlingMcpDataTools.cs", "McpHealthTools.cs"],
            "#4198: get_collection_health's PartialCollectionHealthRow.last_error preview, the row that fails IsCollectionHealthCompactEligible — the same ErrorMessagePreviewLength (500 chars) get_collection_log's error_message_truncated previews to, so a caller never learns two truncation lengths for what is fundamentally the same raw error text; full_detail=true opts back into the whole field"),
        ("output_finding_truncated", ["DarlingMcpDataTools.cs", "McpHealthTools.cs"],
            "#4198: get_collection_health's PartialCollectionHealthRow.output_finding — a template sentence (FormatOutputFinding), not raw free text, and measured as the single dominant cost on a fixture where every row needs a look — previews to OutputFindingPreviewLength (200 chars) by default; full_detail=true opts back into the whole field"),
        ("blocked_sql_text_truncated", ["DarlingMcpBlockingTools.cs", "McpBlockingTools.cs"],
            "#4198: get_blocking/get_blocked_process_reports' blocked_sql_text previewed to SqlTextPreviewLength (150 chars) — the default row LIMIT also halved (30 -> 15), because the row's other ~37 fields, not this column alone, were most of the default page's weight; full_text or a dedup_key call (Darling only) gets the whole text"),
        ("blocking_sql_text_truncated", ["DarlingMcpBlockingTools.cs", "McpBlockingTools.cs"],
            "#4198: get_blocking/get_blocked_process_reports' blocking_sql_text, previewed the same way as blocked_sql_text_truncated"),
        ("top_query_text_truncated", ["DarlingMcpQueryHeatmapTools.cs", "McpQueryTools.cs"],
            "get_query_heatmap's (#4198) per-cell top-query preview width (DefaultPreviewLength on both SKUs) — the full statement is already in the store; full_text opts back into it rather than re-paging, so this is not the page dialect's truncated and nothing was lost the way a source-side cut loses it"),
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
    /// its line — or, since #3653 item 17, an ordered-envelope write <c>envelope["name"] = …</c>, the idiom the
    /// trend disclosure block uses on both SKUs so its data and empty envelopes are built by one method (the
    /// window floor was invisible to this census while it matched initializers only, which is how the
    /// homonym survived a roster that read as complete). Keys only — the value side is not read — so a local
    /// named <c>truncated</c> assigned in the body is counted once, where it is emitted. Group 1 is the
    /// initializer key, group 2 the envelope key; <see cref="KeyOf"/> reads whichever fired.</summary>
    private static readonly Regex PayloadKey = new(
        @"^\s+(?:([a-z][a-z0-9_]*)\s*(?:=(?!=)|,\s*$)|[A-Za-z_][A-Za-z0-9_]*\[""([a-z][a-z0-9_]*)""\]\s*=(?!=))",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private static string KeyOf(Match key) => key.Groups[1].Success ? key.Groups[1].Value : key.Groups[2].Value;

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
            .Concat(FieldPreviewCutKeys.Select(k => (k.Key, k.Files)))
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
                var name = KeyOf(key);
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
            "a cut spelling with no class — a page cut is spelled `truncated` (observed, beside *_returned); a second bound joins SecondBoundCutKeys as <bound>_truncated; a cut made before the store joins SourceSideCutKeys with what was cut; a wide field's own preview cut joins FieldPreviewCutKeys as <field>_truncated; a homonym joins CutHomonyms with what it actually is: "
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
    [InlineData("window_truncated", true)]
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
        Assert.Contains("RunsReadPerTable + 1, cancellationToken);", source, StringComparison.Ordinal);
        Assert.Contains("McpHelpers.BoundPage(runs, RunsReadPerTable)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("history_capped", source, StringComparison.Ordinal);
        Assert.DoesNotContain(">= RunsReadPerTable", source, StringComparison.Ordinal);
    }

    /* ───────────────────────── rule: one vocabulary — the window floor (#3653 item 17) ───────────────────────── */

    /// <summary>
    /// <b>The window floor is spelled <c>window_truncated</c>, beside its reach, and never bare <c>truncated</c>.</b>
    /// The roster equality above says <c>window_truncated</c> exists and where; it cannot say that the FACT is
    /// never published under the page dialect's key again, because a bare <c>truncated</c> is the page dialect
    /// and the roster permits it everywhere. This census holds the fact apart by its NEIGHBOURS: the window
    /// floor is the one cut that rides beside <c>effective_hours_back</c> (the #2353 / #2364 reach vocabulary —
    /// how far back the served series actually begins), and a page cut never does. So the population is
    /// every block on both SKUs that writes <c>effective_hours_back</c> as a payload key — an anonymous-type
    /// initializer (<c>get_query_trend</c>, <c>get_query_store_top</c>) or the ordered envelope the trend
    /// disclosure writes through one method (<c>TrendDisclosure.WriteTo</c> on Darling, <c>WriteDisclosure</c>
    /// on Lite, serving the duration-trend trio's data AND empty envelopes and Lite's <c>get_query_trend</c>) —
    /// and inside each such block the flag must be <c>window_truncated</c>, <c>effective_start</c> must stand
    /// beside it, and <c>truncated</c> must not appear. The block is the innermost brace pair around the reach
    /// key, found over the walker's strings-blanked text so a brace inside a message cannot open or close it.
    ///
    /// <para>Why <c>effective_hours_back</c> and not <c>window_start</c>: <c>get_query_heatmap</c> publishes
    /// <c>window_start</c> / <c>window_end</c> (the requested axis) beside a bare <c>truncated</c> that IS the page
    /// dialect — its cell cap, observed off <c>limit + 1</c>, with <c>first_time_bin</c> / <c>last_time_bin</c>
    /// saying which bins survived. A discriminator on <c>window_start</c> would flag that tool for spelling its
    /// page cut correctly. The reach vocabulary is the one only the window floor speaks.</para>
    ///
    /// <para><see cref="WindowFloorBlocks"/> is an EQUALITY on (file, idiom, count): a new publisher of the
    /// reach vocabulary fails until it is rostered, and a rostered one that stops publishing fails until the
    /// roster shrinks. <see cref="WindowFloorTools"/> is the description half — every tool whose payload can
    /// carry the flag names it through <see cref="McpHelpers.WindowTruncatedDescription"/>, which is executed
    /// here for the three things it must say (the key, the reach beside it, the wire change), and no other
    /// tool carries the clause. The two rosters are two halves of one contract and are held separately
    /// because the trio's four tool bodies reach the envelope through helpers the tool-span slicer does not
    /// follow. The checker is run on a synthetic offender and a synthetic page cut first, so a matcher that
    /// stopped matching reads as red, not as a clean sweep.</para>
    /// </summary>
    public static readonly (string File, string Idiom, int Blocks)[] WindowFloorBlocks =
    [
        /* Four: get_query_store_top's payload, and (#4057) its module_name miss, which hands back the window it
           read as hints so "no rows matched" is never read as a claim about the part the raw tier no longer
           holds; plus (#4231) get_top_queries_by_cpu's and get_top_procedures_by_cpu's payloads, the same
           disclosure over query_stats and procedure_stats. */
        ("DarlingMcpDataTools.cs", "initializer", 4),
        ("DarlingMcpQueryStoreClutterTools.cs", "initializer", 1),
        ("DarlingMcpTrendTools.cs", "envelope", 1),
        ("DarlingMcpTrendTools.cs", "initializer", 1),
        ("McpQueryTools.cs", "envelope", 1),
        /* #4231: Lite's own get_query_store_top initializer (mirroring DarlingMcpDataTools.cs's 2, but Lite's
           get_query_store_top has no module_name-miss hint block of its own, so 1), plus get_top_queries_by_cpu
           and get_top_procedures_by_cpu, newly given the same raw-tier disclosure. */
        ("McpQueryTools.cs", "initializer", 4),
    ];

    public static readonly (string File, string Tool)[] WindowFloorTools =
    [
        ("DarlingMcpDataTools.cs", "get_query_store_top"),
        ("DarlingMcpDataTools.cs", "get_top_procedures_by_cpu"),
        ("DarlingMcpDataTools.cs", "get_top_queries_by_cpu"),
        ("DarlingMcpQueryStoreClutterTools.cs", "get_query_store_clutter"),
        ("DarlingMcpTrendTools.cs", "get_procedure_duration_trend"),
        ("DarlingMcpTrendTools.cs", "get_query_duration_trend"),
        ("DarlingMcpTrendTools.cs", "get_query_store_duration_trend"),
        ("DarlingMcpTrendTools.cs", "get_query_trend"),
        ("McpQueryTools.cs", "get_procedure_duration_trend"),
        ("McpQueryTools.cs", "get_query_duration_trend"),
        ("McpQueryTools.cs", "get_query_store_duration_trend"),
        ("McpQueryTools.cs", "get_query_store_top"),
        ("McpQueryTools.cs", "get_query_trend"),
        ("McpQueryTools.cs", "get_top_procedures_by_cpu"),
        ("McpQueryTools.cs", "get_top_queries_by_cpu"),
    ];

    /// <summary>The reach key: the one neighbour only the window floor has.</summary>
    private const string ReachKey = "effective_hours_back";

    /// <summary>The window-floor spelling, and the page dialect's word it must never wear again in the same block.</summary>
    private const string WindowFloorKey = "window_truncated";
    private const string PageCutKey = "truncated";

    /// <summary>
    /// Every block in <paramref name="source"/> that writes <see cref="ReachKey"/> as a payload key, with the
    /// idiom it used, the 1-based line of the reach write, and every payload key written in the same block.
    /// Comments blanked and literals kept for the key match (so <c>envelope["key"]</c> is readable), literals
    /// blanked for the brace walk (so a message cannot open a block), both at the source's own offsets.
    /// </summary>
    internal static List<(string Idiom, int Line, SortedSet<string> Keys)> WindowFloorBlocksIn(string source)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var text = StripCommentsPreservingLength(source, code);
        var writes = PayloadKey.Matches(text).Select(m => (Match: m, Key: KeyOf(m))).ToList();

        var blocks = new List<(string Idiom, int Line, SortedSet<string> Keys)>();
        var opened = new HashSet<int>();
        foreach (var (match, _) in writes.Where(w => w.Key == ReachKey))
        {
            /* The match begins at the line's indent (or at a blanked comment line above it — `^\s+` spans them);
               the key itself is the first non-blank, and that is the offset the brace walk starts from. */
            var at = match.Index + match.Value.Length - match.Value.TrimStart().Length;
            var (open, close) = EnclosingBlock(code, at);
            if (!opened.Add(open))
            {
                continue;
            }

            var keys = new SortedSet<string>(
                writes.Where(w => w.Match.Index > open && w.Match.Index < close).Select(w => w.Key),
                StringComparer.Ordinal);
            var line = text.AsSpan(0, at).Count('\n') + 1;
            blocks.Add((match.Groups[1].Success ? "initializer" : "envelope", line, keys));
        }

        return blocks;
    }

    /// <summary>The innermost <c>{ … }</c> around <paramref name="at"/> in strings-blanked code: back to the
    /// first unmatched opener, forward to its match.</summary>
    private static (int Open, int Close) EnclosingBlock(string code, int at)
    {
        var depth = 0;
        var open = -1;
        for (var i = at; i >= 0; i--)
        {
            if (code[i] == '}')
            {
                depth++;
            }
            else if (code[i] == '{')
            {
                if (depth == 0)
                {
                    open = i;
                    break;
                }

                depth--;
            }
        }

        Assert.True(open >= 0, $"no block encloses offset {at}");

        depth = 0;
        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '{')
            {
                depth++;
            }
            else if (code[i] == '}' && --depth == 0)
            {
                return (open, i);
            }
        }

        Assert.Fail($"the block opened at offset {open} never closes");
        return default;
    }

    [Fact]
    public void TheWindowFloor_IsSpelledWindowTruncated_BesideItsReach_AndNeverBareTruncated_OnBothSkus()
    {
        var found = new List<(string File, string Idiom, int Line, SortedSet<string> Keys)>();
        foreach (var (file, source) in ToolSources())
        {
            found.AddRange(WindowFloorBlocksIn(source).Select(b => (file, b.Idiom, b.Line, b.Keys)));
        }

        /* The homonym, by file and line: the window floor wearing the page dialect's spelling. */
        var bare = found.Where(b => b.Keys.Contains(PageCutKey)).Select(b => $"{b.File}:{b.Line} ({b.Idiom})").ToList();
        Assert.True(bare.Count == 0,
            $"the window floor is published as bare `{PageCutKey}` beside `{ReachKey}` — that is the page dialect's word for a limit biting, and this fact is the store's reach; spell it `{WindowFloorKey}` (#3653 item 17): "
            + string.Join("; ", bare));

        var unspelled = found.Where(b => !b.Keys.Contains(WindowFloorKey)).Select(b => $"{b.File}:{b.Line} ({b.Idiom}) writes [{string.Join(", ", b.Keys)}]").ToList();
        Assert.True(unspelled.Count == 0,
            $"a block publishes the reach (`{ReachKey}`) without the window floor (`{WindowFloorKey}`) beside it: " + string.Join("; ", unspelled));

        var unanchored = found.Where(b => !b.Keys.Contains("effective_start")).Select(b => $"{b.File}:{b.Line} ({b.Idiom})").ToList();
        Assert.True(unanchored.Count == 0,
            $"`{WindowFloorKey}` says the served head sits later than asked; `effective_start` is where — the block publishes the flag without the instant: " + string.Join("; ", unanchored));

        /* The roster, as an equality on (file, idiom, count): a fifth publisher of the reach vocabulary joins
           here or fails here. */
        var census = found
            .GroupBy(b => (b.File, b.Idiom))
            .Select(g => (g.Key.File, g.Key.Idiom, g.Count()))
            .OrderBy(b => b.File, StringComparer.Ordinal).ThenBy(b => b.Idiom, StringComparer.Ordinal)
            .ToArray();
        var roster = WindowFloorBlocks
            .OrderBy(b => b.File, StringComparer.Ordinal).ThenBy(b => b.Idiom, StringComparer.Ordinal)
            .ToArray();
        Assert.True(roster.SequenceEqual(census),
            "the window-floor publishers moved: rostered [" + string.Join("; ", roster.Select(r => $"{r.File} {r.Idiom} ×{r.Blocks}"))
            + "], found [" + string.Join("; ", census.Select(c => $"{c.File} {c.Idiom} ×{c.Item3}")) + "]");
    }

    /// <summary>The checker on inputs whose verdict is known, so a dead matcher cannot pass as a clean tree: a
    /// window floor under the page dialect's key is an offender; a page cut with a brace in its message is not
    /// a window-floor block at all; the envelope idiom is read, and a key written in a nested block counts as
    /// the enclosing block's (a conditional bare <c>truncated</c> would be no less the homonym).</summary>
    [Fact]
    public void TheWindowFloorChecker_FlagsTheHomonym_AndPassesAPageCut()
    {
        const string offender = """
            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                effective_start = effectiveStart.ToString("o"),
                effective_hours_back = Math.Round((now - effectiveStart).TotalHours, 1),
                truncated,
                queries = result
            }, McpHelpers.JsonOptions);
            """;
        var blocks = WindowFloorBlocksIn(offender);
        var block = Assert.Single(blocks);
        Assert.Equal("initializer", block.Idiom);
        Assert.Equal(6, block.Line);
        Assert.Contains(PageCutKey, block.Keys);
        Assert.DoesNotContain(WindowFloorKey, block.Keys);

        const string pageCut = """
            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                window_start = windowEnd.AddHours(-hours_back).ToString("o"),
                note = $"the cap {limit} bit — {cells.Count} of them",
                cells_returned = cells.Count,
                truncated,
            }, McpHelpers.JsonOptions);
            """;
        Assert.Empty(WindowFloorBlocksIn(pageCut));

        const string envelope = """
            public void WriteTo(Dictionary<string, object?> envelope)
            {
                envelope["source"] = Source;
                envelope["effective_start"] = EffectiveStartUtc.ToString("o");
                envelope["effective_hours_back"] = Math.Round((WindowEndUtc - EffectiveStartUtc).TotalHours, 1);
                envelope["window_truncated"] = Truncated;
                if (Routing is not null)
                {
                    envelope["routing"] = Routing;
                }
            }
            """;
        var written = Assert.Single(WindowFloorBlocksIn(envelope));
        Assert.Equal("envelope", written.Idiom);
        Assert.Equal(new[] { "effective_hours_back", "effective_start", "routing", "source", "window_truncated" }, written.Keys.ToArray());
    }

    [Fact]
    public void EveryWindowFloorTool_CarriesTheSharedClause_AndNoOtherToolDoes()
    {
        /* The description half: the attribute text between the tool marker and the method, by source, so Lite's
           tools are read the same way Darling's are. */
        var carrying = ToolMethods()
            .Where(t =>
            {
                var method = t.Span.IndexOf("public static", StringComparison.Ordinal);
                return (method < 0 ? t.Span : t.Span[..method]).Contains("McpHelpers.WindowTruncatedDescription", StringComparison.Ordinal);
            })
            .Select(t => (t.File, t.Tool))
            .OrderBy(t => t.File, StringComparer.Ordinal).ThenBy(t => t.Tool, StringComparer.Ordinal)
            .ToArray();
        var roster = WindowFloorTools.OrderBy(t => t.File, StringComparer.Ordinal).ThenBy(t => t.Tool, StringComparer.Ordinal).ToArray();
        Assert.True(roster.SequenceEqual(carrying),
            "the tools carrying McpHelpers.WindowTruncatedDescription moved: rostered [" + string.Join(", ", roster.Select(r => $"{r.File}/{r.Tool}"))
            + "], found [" + string.Join(", ", carrying.Select(c => $"{c.File}/{c.Tool}")) + "]");

        /* The clause itself, executed: the key, the reach beside it, the wire change, and the leading space
           every appended description constant carries (BaselineDiscontinuities.DescriptionSentence's shape). */
        var clause = McpHelpers.WindowTruncatedDescription;
        Assert.StartsWith(" ", clause, StringComparison.Ordinal);
        Assert.Contains(WindowFloorKey, clause, StringComparison.Ordinal);
        Assert.Contains("effective_start / effective_hours_back", clause, StringComparison.Ordinal);
        Assert.Contains("not a page cut", clause, StringComparison.Ordinal);
        Assert.Contains("formerly named truncated.", clause, StringComparison.Ordinal);

        /* And by reflection on the SKU this project can load: the Darling five really do publish the clause in
           the attribute an MCP client reads, ahead of the discontinuities sentence they end with. */
        var darling = new[] { typeof(DarlingMcpTrendTools), typeof(DarlingMcpDataTools), typeof(DarlingMcpQueryStoreClutterTools) }
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Select(m => (Name: m.GetCustomAttribute<McpServerToolAttribute>()?.Name, Method: m))
            .Where(x => x.Name is not null)
            .ToDictionary(x => x.Name!, x => x.Method.GetCustomAttribute<DescriptionAttribute>()!.Description, StringComparer.Ordinal);
        foreach (var (file, tool) in WindowFloorTools.Where(t => t.File.StartsWith("Darling", StringComparison.Ordinal)))
        {
            Assert.True(darling.TryGetValue(tool, out var description), $"{tool} ({file}) is not a Darling MCP tool");
            /* #3898 D3/D4, re-pointed deliberately: the whole clause may span head and guide, because its
               "formerly named truncated." WIRE CHANGE notice leaves the wire for the guide tail (D4). What
               a caller needs to read the payload (the key, and that it is a retention floor, not a page cut)
               is a guardrail and must be served in the head. Unconverted, the head is the whole description. */
            Assert.Contains(clause, description!, StringComparison.Ordinal);
            var head = McpToolGuide.Split(description!).Head;
            Assert.Contains(WindowFloorKey, head, StringComparison.Ordinal);
            Assert.Contains("not a page cut", head, StringComparison.Ordinal);
            Assert.DoesNotContain("read truncated", description!, StringComparison.Ordinal);
        }
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
            "audit_config's per-recommendation status (ok / warning / review, plus not_applicable since #3691 line 70 for a PostgreSQL knob the engine does not consult — Aurora's checkpoint_timeout and max_wal_size) — the lower-case status vocabulary every `status` key on the surface speaks, not a band"),
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
