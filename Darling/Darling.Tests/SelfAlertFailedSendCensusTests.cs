/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4795: every send a Darling self-alert makes goes through <c>DarlingSelfAlertEvaluator.FireAsync</c>, which
/// returns what the channels did. A call that throws that answer away is a send whose failure nobody sees: the
/// arm stamped its cooldown before it sent, so an alert no channel delivered waits out the whole cooldown. This
/// census reads the evaluator's source and fails on a new <c>await FireAsync(</c> statement that discards its
/// result, unless its enclosing method is listed below with the reason it has no retry to give.
///
/// <para>The list is keyed by method and carries the number of discarding sites in it, so a second discarding
/// send added to a listed method fails too, and a listed method that no longer discards fails as well: the list
/// stays exactly the set of sends that are left alone on purpose. A method that has both a retried send and one
/// that is left alone lists only the latter (the count is the number left alone).</para>
///
/// <para>Keeping the answer is not enough, so the census reads one step further. A discard (<c>_ = await
/// FireAsync(</c>) is not keeping it, and a kept answer has to reach the call that acts on it: in each method the
/// number of <c>AfterSelfFire</c> and <c>NoteRetrySend</c> calls equals the number of kept sends, and each send's
/// answer is an argument of the hand-off that follows it. The three daily documents are set apart: they keep their
/// answer for <c>RecordDocumentDeliveredAsync</c>, whose stamp is their own retry. Last, one case per
/// <c>AfterSelfFire</c> arm pins the metric and the interval the arm passes, because the interval is the one
/// argument a wrong copy would leave compiling: the back-dated stamp then opens the arm's gate at the wrong time.</para>
/// </summary>
public sealed class SelfAlertFailedSendCensusTests
{
    private const string RestoreNotice =
        "a restore notice is not retried: the outage it reports is over, and the next observation is the server staying up";

    private const string ArmHandOff = "AfterSelfFire";
    private const string RetryHandOff = "NoteRetrySend";
    private const string DocumentHandOff = "RecordDocumentDeliveredAsync";

    /// <summary>
    /// The sends that keep no answer, by enclosing method: how many, and why none of them has a retry. Each is
    /// an edge that fires once on entry with no path that fires it again, or a state-machine or restore notice.
    /// The connection and availability group down alerts, the failover and the data-movement-suspended edge keep
    /// their answer and are retried (#4795).
    /// </summary>
    private static readonly IReadOnlyDictionary<string, (int Sites, string Reason)> Allowed =
        new Dictionary<string, (int, string)>(StringComparer.Ordinal)
        {
            ["ApplyConnectionOutcomeAsync"] = (1, RestoreNotice),
            ["ApplyAgReplicaHealthAsync"] = (1, RestoreNotice),
            ["EvaluateStoreUpgradeAsync"] =
                (2, "one notice per service start about the start's own upgrade: an event, never re-evaluated, so nothing to fire again"),
            ["EvaluateStoreTimescaleAsync"] =
                (1, "one notice per service start about the start's own extension update: an event, never re-evaluated"),
            ["ApplyNotificationChannelsAsync"] =
                (1, "the failing-channel notice is stated once when the policy decides it; the next decision is the channel's recovery"),
            ["ApplyPolicyJobsStuckAsync"] =
                (1, "the auto-re-armed notice is a state-machine edge: the next pass escalates or records the recovery, and neither is gated by a stamp"),
        };

    /// <summary>
    /// The daily documents. Each keeps its answer and hands it to <c>RecordDocumentDeliveredAsync</c>, which
    /// writes the document's stamp only for a delivery not reported failed: the next tick sends again. That is
    /// their retry, so they have no <c>AfterSelfFire</c> call, and the hand-off they owe is the stamp's.
    /// </summary>
    private static readonly IReadOnlySet<string> Digests = new HashSet<string>(StringComparer.Ordinal)
    {
        "ApplyCollectorCostDigestAsync",
        "ApplyFleetSweepRollupAsync",
        "ApplyAnalysisSinglesDigestAsync",
    };

    /// <summary>The evaluator's source with comments and literal text blanked (offsets and newlines kept), read
    /// once for the class.</summary>
    private static readonly Lazy<string> EvaluatorCode = new(() =>
        CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs")));

    /// <summary>A member declaration at the class's own indent, capturing the member's name.</summary>
    private static readonly Regex MemberDeclaration = new(
        @"^    (?:public|internal|private|protected)\b[^\n=;{]*?\b(?<name>\w+)\s*(?:<[^>\n]+>)?\(",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Regex FireCall = new(
        @"\bawait\s+FireAsync\(", RegexOptions.CultureInvariant);

    private static readonly Regex HandOffCall = new(
        @"\b(?<callee>" + ArmHandOff + "|" + RetryHandOff + "|" + DocumentHandOff + @")\s*\(",
        RegexOptions.CultureInvariant);

    /// <summary>One <c>await FireAsync(</c> in code (not in a comment or a literal): where it is, its enclosing
    /// method, whether the statement keeps the result, and the local it lands in.</summary>
    private sealed record FireSite(int Line, int Index, string Method, bool KeepsResult, string? Variable);

    /// <summary>One call that acts on a send's answer: where it is, its enclosing method, which of the hand-off
    /// calls it is, and its top-level arguments.</summary>
    private sealed record HandOff(int Line, int Index, string Method, string Callee, IReadOnlyList<string> Arguments);

    /// <summary>Every <c>await FireAsync(</c> in the evaluator.</summary>
    private static List<FireSite> FireSites() => FireSites(EvaluatorCode.Value);

    /// <summary>Every <c>await FireAsync(</c> in <paramref name="code"/> (already stripped of comments and
    /// literals), in source order.</summary>
    private static List<FireSite> FireSites(string code)
    {
        var declarations = MemberDeclaration.Matches(code).Cast<Match>().ToList();
        var sites = new List<FireSite>();

        foreach (Match call in FireCall.Matches(code))
        {
            var (keeps, variable) = ReadTarget(code[..call.Index].TrimEnd());

            sites.Add(new FireSite(
                code.AsSpan(0, call.Index).Count('\n') + 1, call.Index, EnclosingMethod(declarations, call.Index), keeps, variable));
        }

        return sites;
    }

    /// <summary>Every hand-off call in the evaluator, in source order. The declarations of the hand-off methods
    /// are not calls and are left out.</summary>
    private static List<HandOff> HandOffs(string code)
    {
        var declarations = MemberDeclaration.Matches(code).Cast<Match>().ToList();
        var handOffs = new List<HandOff>();

        foreach (Match call in HandOffCall.Matches(code))
        {
            var callee = call.Groups["callee"].Value;
            var method = EnclosingMethod(declarations, call.Index);

            if (method == callee)
            {
                continue;
            }

            handOffs.Add(new HandOff(
                code.AsSpan(0, call.Index).Count('\n') + 1, call.Index, method, callee, CallArguments(code, call.Index + call.Length - 1)));
        }

        return handOffs;
    }

    private static string EnclosingMethod(List<Match> declarations, int index) =>
        declarations.LastOrDefault(m => m.Index < index)?.Groups["name"].Value ?? "";

    /// <summary>
    /// Whether the statement holding a <c>FireAsync</c> call keeps what it returns, and the local it lands in.
    /// <paramref name="before"/> is the code before <c>await</c>: the statement keeps the answer when that ends
    /// in <c>return</c>, or in an assignment to a NAMED target. An assignment to <c>_</c> (<c>_ = await</c>,
    /// <c>var _ = await</c>) is a discard, and so is anything this cannot read as an assignment (a comparison, a
    /// target with no name): a site that reads as discarding fails loudly and must be listed, while one that
    /// reads as kept passes without anyone looking.
    /// </summary>
    private static (bool Keeps, string? Variable) ReadTarget(string before)
    {
        if (before.EndsWith("return", StringComparison.Ordinal)
            && (before.Length == 6 || !(char.IsLetterOrDigit(before[^7]) || before[^7] is '_' or '.')))
        {
            return (true, null);
        }

        if (!before.EndsWith('=') || before.Length < 2 || "=!<>".Contains(before[^2]))
        {
            return (false, null);
        }

        var target = before[..^1].TrimEnd();
        var start = target.Length;

        while (start > 0 && (char.IsLetterOrDigit(target[start - 1]) || target[start - 1] == '_'))
        {
            start--;
        }

        var name = target[start..];

        return name.Length == 0 || name == "_" ? (false, null) : (true, name);
    }

    /// <summary>The top-level arguments of the call whose opening parenthesis is at <paramref name="open"/>,
    /// trimmed and with runs of white space folded to one space. A comma inside a nested <c>()</c>, <c>[]</c> or
    /// <c>{}</c> does not split.</summary>
    private static List<string> CallArguments(string code, int open)
    {
        var arguments = new List<string>();
        var depth = 0;
        var start = open + 1;

        for (var i = open; i < code.Length; i++)
        {
            switch (code[i])
            {
                case '(' or '[' or '{':
                    depth++;
                    break;

                case ')' or ']' or '}':
                    depth--;

                    if (depth == 0)
                    {
                        var last = Squash(code[start..i]);

                        if (last.Length > 0 || arguments.Count > 0)
                        {
                            arguments.Add(last);
                        }

                        return arguments;
                    }

                    break;

                case ',' when depth == 1:
                    arguments.Add(Squash(code[start..i]));
                    start = i + 1;
                    break;
            }
        }

        return arguments;
    }

    private static readonly Regex WhiteSpaceRun = new(@"\s+", RegexOptions.CultureInvariant);

    private static string Squash(string text) => WhiteSpaceRun.Replace(text, " ").Trim();

    /// <summary>Whether <paramref name="callee"/> is the call that acts on a send's answer in
    /// <paramref name="method"/>: the document stamp for a daily document, the retry hand-offs for every arm.</summary>
    private static bool IsHandOffFor(string method, string callee) =>
        Digests.Contains(method) ? callee == DocumentHandOff : callee is ArmHandOff or RetryHandOff;

    /// <summary>The hand-off that receives <paramref name="site"/>'s answer: the first one in the same method
    /// after the call and before the method's next <c>FireAsync</c>, or null when there is none. A hand-off that
    /// belongs to a later send therefore cannot stand in for this one.</summary>
    private static HandOff? HandOffFollowing(FireSite site, IReadOnlyList<FireSite> sites, IReadOnlyList<HandOff> handOffs)
    {
        var next = sites
            .Where(s => s.Method == site.Method && s.Index > site.Index)
            .Select(s => s.Index)
            .DefaultIfEmpty(int.MaxValue)
            .Min();

        return handOffs
            .Where(h => h.Method == site.Method && h.Index > site.Index && h.Index < next && IsHandOffFor(h.Method, h.Callee))
            .OrderBy(h => h.Index)
            .FirstOrDefault();
    }

    [Fact]
    public void TheCensusFindsTheSends_AndTheMethodsThatMakeThem()
    {
        var sites = FireSites();

        /* A regex that quietly matched nothing would pass every assertion below. */
        Assert.True(sites.Count >= 30, $"expected the evaluator's many FireAsync sends, found {sites.Count}");
        Assert.Contains(sites, s => s.Method == "ApplyCollectionStoppedAsync");
        Assert.Contains(sites, s => s.KeepsResult);
        Assert.DoesNotContain(sites, s => s.Method == "");

        var handOffs = HandOffs(EvaluatorCode.Value);

        Assert.True(
            handOffs.Count(h => h.Callee == ArmHandOff) >= 15 && handOffs.Count(h => h.Callee == RetryHandOff) >= 3,
            $"expected the evaluator's AfterSelfFire and NoteRetrySend calls, found {handOffs.Count} hand-off(s) in all");
        Assert.DoesNotContain(handOffs, h => h.Method == "");
        Assert.All(handOffs, h => Assert.NotEmpty(h.Arguments));
    }

    [Fact]
    public void EveryFireAsyncKeepsItsResult_OrIsListedWithTheReasonItHasNoRetry()
    {
        var discarding = FireSites()
            .Where(s => !s.KeepsResult)
            .GroupBy(s => s.Method, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Select(s => s.Line).ToList(), StringComparer.Ordinal);

        var problems = new List<string>();

        foreach (var (method, lines) in discarding.OrderBy(p => p.Value[0]))
        {
            if (!Allowed.TryGetValue(method, out var allowed))
            {
                problems.Add(
                    $"{method} (line {string.Join(", ", lines)}) discards what FireAsync reports. Keep it "
                    + "(var delivery = await FireAsync(...)) and hand it to AfterSelfFire, or list the method in Allowed with the reason.");
            }
            else if (lines.Count != allowed.Sites)
            {
                problems.Add(
                    $"{method} has {lines.Count} discarding FireAsync send(s) (lines {string.Join(", ", lines)}) but Allowed says {allowed.Sites}.");
            }
        }

        foreach (var method in Allowed.Keys.Where(m => !discarding.ContainsKey(m)))
        {
            problems.Add($"{method} is listed in Allowed but no longer discards a FireAsync result; remove the entry.");
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void EveryAllowedEntry_StatesItsReason()
    {
        foreach (var (method, allowed) in Allowed)
        {
            Assert.False(string.IsNullOrWhiteSpace(allowed.Reason), $"{method} is allowed without a reason");
            Assert.True(allowed.Sites > 0, $"{method} is allowed for no sites");
        }
    }

    [Fact]
    public void ADiscard_IsNotKeepingTheAnswer_AndANamedLocalOrAReturnIs()
    {
        /* The census's own parsing over a made-up method: a bare await, an assignment to the discard `_` (with
           and without `var`), a comparison, a target that has no name, a named local, a member, a return. */
        var code = string.Join(
            '\n',
            "    private async Task<AlertDelivery?> SendsAsync()",
            "    {",
            "        await FireAsync(bare);",
            "        _ = await FireAsync(discarded);",
            "        var _ = await FireAsync(discardedWithVar);",
            "        var same = first == await FireAsync(compared);",
            "        slots[0] = await FireAsync(unnamed);",
            "        var kept = await FireAsync(local);",
            "        this.answer = await FireAsync(member);",
            "        return await FireAsync(returned);",
            "    }");

        var kept = FireSites(code).ToDictionary(s => s.Line);

        Assert.Equal(8, kept.Count);
        Assert.All(new[] { 3, 4, 5, 6, 7 }, line => Assert.False(kept[line].KeepsResult, $"line {line} was counted as keeping the answer"));
        Assert.All(new[] { 8, 9, 10 }, line => Assert.True(kept[line].KeepsResult, $"line {line} was counted as discarding the answer"));
        Assert.Equal("kept", kept[8].Variable);
        Assert.Equal("answer", kept[9].Variable);
        Assert.All(kept.Values, s => Assert.Equal("SendsAsync", s.Method));
    }

    [Fact]
    public void EveryKeptSend_HandsItsAnswerToTheCallThatActsOnIt()
    {
        var code = EvaluatorCode.Value;
        var sites = FireSites(code);
        var handOffs = HandOffs(code);
        var problems = new List<string>();

        var keptByMethod = sites
            .Where(s => s.KeepsResult)
            .GroupBy(s => s.Method, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.Ordinal);

        /* The count: what a method hands on equals what it keeps. Deleting one hand-off leaves the count short;
           a hand-off whose send was dropped leaves it long. */
        var methods = keptByMethod.Keys
            .Concat(handOffs.Where(h => IsHandOffFor(h.Method, h.Callee)).Select(h => h.Method))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(m => m, StringComparer.Ordinal);

        foreach (var method in methods)
        {
            var kept = keptByMethod.GetValueOrDefault(method) ?? [];
            var handed = handOffs.Where(h => h.Method == method && IsHandOffFor(method, h.Callee)).ToList();

            if (kept.Count != handed.Count)
            {
                var callees = Digests.Contains(method) ? DocumentHandOff : $"{ArmHandOff} or {RetryHandOff}";

                problems.Add(
                    $"{method} keeps {kept.Count} FireAsync answer(s) (lines {string.Join(", ", kept.Select(s => s.Line))}) "
                    + $"but makes {handed.Count} {callees} call(s) (lines {string.Join(", ", handed.Select(h => h.Line))}): "
                    + "every kept answer has to reach the call that acts on it.");
            }
        }

        /* The answer itself: each kept send is followed, before the method's next send, by a hand-off that takes
           the local the send landed in. A local that nothing reads is a kept answer nobody looks at. */
        foreach (var site in sites.Where(s => s.KeepsResult))
        {
            var handOff = HandOffFollowing(site, sites, handOffs);

            if (handOff is null)
            {
                problems.Add($"{site.Method}: the FireAsync answer kept at line {site.Line} is not handed on before the method's next send.");
            }
            else if (site.Variable is null || !handOff.Arguments.Contains(site.Variable))
            {
                problems.Add(
                    $"{site.Method}: line {handOff.Line} calls {handOff.Callee} without the answer kept at line {site.Line} "
                    + $"({site.Variable ?? "returned"}).");
            }
        }

        /* An entry that no longer fits fails, so the set stays exactly the documents. */
        foreach (var method in Digests.Where(m => !keptByMethod.ContainsKey(m)))
        {
            problems.Add($"{method} is listed in Digests but keeps no FireAsync answer; remove the entry.");
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// One case per arm that reports a send to <c>AfterSelfFire</c>: the method, which of the method's kept sends
    /// it is (0-based, in source order), the metric the send carries and the interval the arm's gate compares
    /// against. The hand-off has to take that send's own answer, name that metric, and pass that interval: the
    /// shared cooldown for most arms, the arm's own refire interval for the ones that keep one. A copied
    /// hand-off with the wrong interval still compiles, and back-dates the stamp so the gate opens early or never.
    /// </summary>
    [Theory]
    [InlineData("ApplyAgDatabaseHealthAsync", 1, "AgSyncFellBehindMetric", "SharedCooldown")]
    [InlineData("ApplyDiskPressureAsync", 0, "DiskPressureMetric", "SharedCooldown")]
    [InlineData("ApplyCustomRuleHealthAsync", 0, "CustomRuleHealthMetric", "SharedCooldown")]
    [InlineData("ApplyStaleMuteRulesAsync", 0, "StaleMuteMetric", "StaleMuteRefire")]
    [InlineData("ApplyStoreSettingsAsync", 0, "StoreSettingsMetric", "StoreSettingsRefire")]
    [InlineData("ApplyWebTlsCertificateAsync", 0, "WebTlsCertExpiryMetric", "WebTlsCertRefire")]
    [InlineData("ApplyFleetGateAsync", 0, "FleetGateMetric", "SharedCooldown")]
    [InlineData("ApplyStoreJobCadenceAsync", 0, "JobCadenceMetric", "SharedCooldown")]
    [InlineData("ApplyRetentionHoldsAsync", 0, "RetentionHoldMetric", "SharedCooldown")]
    [InlineData("ApplyRawPurgeOverHorizonAsync", 0, "RawPurgeOverHorizonMetric", "SharedCooldown")]
    [InlineData("ApplyToastSlackAsync", 0, "ToastSlackMetric", "ToastSlackRefire")]
    [InlineData("ApplyCheckpointerPressureAsync", 0, "CheckpointerPressureMetric", "SharedCooldown")]
    [InlineData("ApplyPolicyJobsStuckAsync", 0, "band.Metric", "SharedCooldown")]
    [InlineData("ApplyPolicyJobsStuckAsync", 1, "band.Metric", "SharedCooldown")]
    [InlineData("ApplyPolicyJobsStuckAsync", 2, "band.Metric", "SharedCooldown")]
    [InlineData("ApplyPolicyJobsStuckAsync", 3, "band.Metric", "SharedCooldown")]
    public void ArmHandsItsSendsAnswerToAfterSelfFire_WithItsOwnMetricAndInterval(
        string method, int send, string metric, string interval)
    {
        var code = EvaluatorCode.Value;
        var sites = FireSites(code);
        var kept = sites.Where(s => s.Method == method && s.KeepsResult).ToList();

        Assert.True(send < kept.Count, $"{method} keeps {kept.Count} FireAsync answer(s); this case names send {send}");

        var site = kept[send];

        /* The send carries the metric: FireAsync's third argument. */
        var sent = CallArguments(code, code.IndexOf('(', site.Index));
        Assert.True(sent.Count > 2 && sent[2] == metric, $"{method} send {send} (line {site.Line}) carries {(sent.Count > 2 ? sent[2] : "no metric")}, not {metric}");

        var handOff = HandOffFollowing(site, sites, HandOffs(code));
        Assert.True(handOff is not null, $"{method} send {send} (line {site.Line}) is not handed to AfterSelfFire before the method's next send");
        Assert.True(handOff.Callee == ArmHandOff, $"{method} send {send} (line {site.Line}) is handed to {handOff.Callee}, not AfterSelfFire");

        /* AfterSelfFire(family, stamps, key, now, cooldown, delivery) */
        var arguments = handOff.Arguments;
        Assert.True(arguments.Count == 6, $"{method} line {handOff.Line}: AfterSelfFire takes 6 arguments, found {arguments.Count}");
        Assert.True(arguments[0] == metric, $"{method} line {handOff.Line}: AfterSelfFire reports under {arguments[0]}, not {metric}");
        Assert.True(arguments[4] == interval, $"{method} line {handOff.Line}: AfterSelfFire passes {arguments[4]} as the interval, not {interval}");
        Assert.True(arguments[5] == site.Variable, $"{method} line {handOff.Line}: AfterSelfFire is given {arguments[5]}, not the answer kept at line {site.Line} ({site.Variable})");
    }
}
