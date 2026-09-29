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
/// </summary>
public sealed class SelfAlertFailedSendCensusTests
{
    private const string CoveredElsewhere = "covered by the connection and availability group change";

    /// <summary>
    /// The sends that keep no answer, by enclosing method: how many, and why none of them has a retry. Each is
    /// an edge that fires once on entry with no path that fires it again, a state-machine notice, or is covered
    /// by the separate change for the connection and availability group arms.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, (int Sites, string Reason)> Allowed =
        new Dictionary<string, (int, string)>(StringComparer.Ordinal)
        {
            ["ApplyConnectionOutcomeAsync"] = (2, CoveredElsewhere),
            ["ApplyAgReplicaHealthAsync"] = (3, CoveredElsewhere),
            ["ApplyAgDatabaseHealthAsync"] =
                (1, "the data-movement-suspended notice is an edge: it fires once when movement becomes suspended and has no path that fires it again"),
            ["EvaluateStoreUpgradeAsync"] =
                (2, "one notice per service start about the start's own upgrade: an event, never re-evaluated, so nothing to fire again"),
            ["EvaluateStoreTimescaleAsync"] =
                (1, "one notice per service start about the start's own extension update: an event, never re-evaluated"),
            ["ApplyNotificationChannelsAsync"] =
                (1, "the failing-channel notice is stated once when the policy decides it; the next decision is the channel's recovery"),
            ["ApplyPolicyJobsStuckAsync"] =
                (1, "the auto-re-armed notice is a state-machine edge: the next pass escalates or records the recovery, and neither is gated by a stamp"),
        };

    private static string EvaluatorSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingSelfAlertEvaluator.cs");

    /// <summary>A member declaration at the class's own indent, capturing the member's name.</summary>
    private static readonly Regex MemberDeclaration = new(
        @"^    (?:public|internal|private|protected)\b[^\n=;{]*?\b(?<name>\w+)\s*(?:<[^>\n]+>)?\(",
        RegexOptions.Multiline | RegexOptions.CultureInvariant);

    private static readonly Regex FireCall = new(
        @"\bawait\s+FireAsync\(", RegexOptions.CultureInvariant);

    /// <summary>Every <c>await FireAsync(</c> in code (not in a comment or a literal): its line, its enclosing
    /// method, and whether the statement keeps the result.</summary>
    private static List<(int Line, string Method, bool KeepsResult)> FireSites()
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(EvaluatorSource());
        var declarations = MemberDeclaration.Matches(code);
        var sites = new List<(int, string, bool)>();

        foreach (Match call in FireCall.Matches(code))
        {
            var method = declarations.Cast<Match>().LastOrDefault(m => m.Index < call.Index)?.Groups["name"].Value ?? "";
            var line = code.AsSpan(0, call.Index).Count('\n') + 1;

            /* The statement keeps the result when the call is the right-hand side of an assignment, or is
               returned: the last thing before "await" is "=" or "return". */
            var before = code[..call.Index].TrimEnd();
            var keeps = before.EndsWith('=') || before.EndsWith("return", StringComparison.Ordinal);

            sites.Add((line, method, keeps));
        }

        return sites;
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
}
